use super::{
    gen_file_path, transfer::task_transfer::ObjectStorage, FileDescription, FilePosition,
    ListedRecord, TaskDefaultParameters, GLOBAL_TASK_LIST_FILE_POSITON_MAP, OFFSET_PREFIX,
    TRANSFER_OBJECT_LIST_FILE_PREFIX,
};
use crate::{
    commons::{LastModifyFilter, RegexFilter},
    resources::get_checkpoint,
    s3::OSSDescription,
    tasks::LogInfo,
};
use anyhow::{anyhow, Context, Result};
use aws_sdk_s3::types::ObjectIdentifier;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::{self, BufRead},
    sync::{atomic::AtomicBool, Arc},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::task::JoinSet;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeleteTaskAttributes {
    #[serde(default = "TaskDefaultParameters::objects_per_batch_default")]
    pub objects_per_batch: i32,
    #[serde(default = "TaskDefaultParameters::task_parallelism_default")]
    pub task_parallelism: usize,
    #[serde(default = "TaskDefaultParameters::meta_dir_default")]
    pub meta_dir: String,
    #[serde(default = "TaskDefaultParameters::start_from_checkpoint_default")]
    pub start_from_checkpoint: bool,
    #[serde(default = "TaskDefaultParameters::filter_default")]
    pub exclude: Option<Vec<String>>,
    #[serde(default = "TaskDefaultParameters::filter_default")]
    pub include: Option<Vec<String>>,
    #[serde(default = "TaskDefaultParameters::last_modify_filter_default")]
    pub last_modify_filter: Option<LastModifyFilter>,
}

impl Default for DeleteTaskAttributes {
    fn default() -> Self {
        Self {
            objects_per_batch: TaskDefaultParameters::objects_per_batch_default(),
            task_parallelism: TaskDefaultParameters::task_parallelism_default(),
            exclude: TaskDefaultParameters::filter_default(),
            include: TaskDefaultParameters::filter_default(),
            last_modify_filter: TaskDefaultParameters::last_modify_filter_default(),
            meta_dir: TaskDefaultParameters::meta_dir_default(),
            start_from_checkpoint: TaskDefaultParameters::start_from_checkpoint_default(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskDeleteBucket {
    #[serde(default = "TaskDefaultParameters::id_default")]
    pub task_id: String,
    #[serde(default = "TaskDefaultParameters::name_default")]
    pub name: String,
    pub source: ObjectStorage,
    pub attributes: DeleteTaskAttributes,
}

impl Default for TaskDeleteBucket {
    fn default() -> Self {
        Self {
            task_id: TaskDefaultParameters::id_default(),
            name: TaskDefaultParameters::name_default(),
            source: ObjectStorage::OSS(OSSDescription::default()),
            attributes: DeleteTaskAttributes::default(),
        }
    }
}

//Todo
// 增加 start_from_checkpoint arttribute
impl TaskDeleteBucket {
    pub async fn start_delete(&self) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let stop_mark = Arc::new(AtomicBool::new(false));
        let error_occure = Arc::new(AtomicBool::new(false));
        let offset_map = Arc::new(DashMap::<String, FilePosition>::new());
        // 注册offset_map
        GLOBAL_TASK_LIST_FILE_POSITON_MAP.insert(self.task_id.clone(), offset_map.clone());

        let mut list_file_position = FilePosition::default();

        let mut executed_file = FileDescription {
            path: gen_file_path(
                self.attributes.meta_dir.as_str(),
                TRANSFER_OBJECT_LIST_FILE_PREFIX,
                now.as_secs().to_string().as_str(),
            ),
            size: 0,
            total_lines: 0,
        };

        let oss_d = match self.source.clone() {
            ObjectStorage::Local(_) => {
                return Err(anyhow::anyhow!("parse oss description error"));
            }
            ObjectStorage::OSS(d) => d,
        };

        let client = match oss_d.gen_oss_client() {
            Ok(c) => c,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(e);
            }
        };

        let c = client.clone();
        let regex_filter =
            RegexFilter::from_vec_option(&self.attributes.exclude, &self.attributes.include)?;
        let reg_filter = regex_filter.clone();
        let last_modify_filter = self.attributes.last_modify_filter.clone();

        let (executed_list_file, file_desc, file_position) = match self
            .attributes
            .start_from_checkpoint
        {
            true => {
                let checkpoint =
                    get_checkpoint(&self.task_id).context(format!("{}:{}", file!(), line!()))?;
                let f =
                    checkpoint
                        .seeked_execute_file()
                        .context(format!("{}:{}", file!(), line!()))?;
                list_file_position = checkpoint.executed_file_position;
                executed_file = checkpoint.executed_file;
                (f, executed_file, list_file_position)
            }
            false => {
                // 获取删除列表
                let mut info = LogInfo {
                    task_id: self.task_id.clone(),
                    msg: "generate objects list beging".to_string(),
                    additional: None::<String>,
                };
                log::info!("{:?}", info);

                let _ = fs::remove_dir_all(self.attributes.meta_dir.as_str());
                executed_file = c
                    .append_object_list_to_file(
                        oss_d.bucket.clone(),
                        oss_d.prefix.clone(),
                        self.attributes.objects_per_batch,
                        &executed_file.path,
                        reg_filter,
                        last_modify_filter,
                    )
                    .await?;

                info.msg = "generate objects list ok".to_string();
                log::info!("{:?}", info);

                let f = File::open(executed_file.path.as_str())?;
                (
                    f,
                    executed_file,
                    FilePosition {
                        offset: 0,
                        line_num: 0,
                    },
                )
            }
        };

        let o_d = oss_d.clone();

        // execut_set 用于执行任务
        let mut execut_set = JoinSet::new();

        let mut vec_record = vec![];
        let exec_lines = file_desc.total_lines - file_position.line_num;
        let lines = io::BufReader::new(executed_list_file).lines();
        for (idx, line) in lines.enumerate() {
            if stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            match line {
                Ok(key) => {
                    let len = key.bytes().len() + "\n".bytes().len();
                    let record = ListedRecord {
                        key,
                        offset: list_file_position.offset,
                        line_num: list_file_position.line_num,
                    };

                    match regex_filter {
                        Some(ref f) => {
                            if f.is_match(&record.key) {
                                vec_record.push(record);
                            }
                        }
                        None => vec_record.push(record),
                    }

                    list_file_position.offset += len;
                    list_file_position.line_num += 1;
                }
                Err(e) => {
                    log::error!("{:?}", e);
                    stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                }
            }

            if vec_record
                .len()
                .to_string()
                .eq(&self.attributes.objects_per_batch.to_string())
                || idx.eq(&TryInto::<usize>::try_into(exec_lines - 1).unwrap())
                    && vec_record.len() > 0
            {
                if stop_mark.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                };
                while execut_set.len() >= self.attributes.task_parallelism {
                    execut_set.join_next().await;
                }

                let del = DeleteBucketExecutor {
                    source: o_d.clone(),
                    stop_mark: stop_mark.clone(),
                    attributes: self.attributes.clone(),
                    offset_map: offset_map.clone(),
                };
                let keys = vec_record.clone();
                let sm: Arc<AtomicBool> = stop_mark.clone();
                let eo = error_occure.clone();

                execut_set.spawn(async move {
                    match del.delete_listed_records(keys).await {
                        Ok(_) => log::info!("records removed"),
                        Err(e) => {
                            log::error!("{}", e);
                            sm.store(true, std::sync::atomic::Ordering::Relaxed);
                            eo.store(true, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                });

                vec_record.clear();
            }
        }

        while execut_set.len() > 0 {
            execut_set.join_next().await;
        }
        // 配置停止 offset save 标识为 true
        stop_mark.store(true, std::sync::atomic::Ordering::Relaxed);

        if error_occure.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(anyhow!("delete task faile"));
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct DeleteBucketExecutor {
    pub source: OSSDescription,
    pub stop_mark: Arc<AtomicBool>,
    pub offset_map: Arc<DashMap<String, FilePosition>>,
    pub attributes: DeleteTaskAttributes,
}

impl DeleteBucketExecutor {
    pub async fn delete_listed_records(&self, records: Vec<ListedRecord>) -> Result<()> {
        let subffix = records[0].offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);

        let source_client =
            self.source
                .gen_oss_client()
                .context(format!("{}:{}", file!(), line!()))?;
        let s_c = Arc::new(source_client);

        // 插入文件offset记录
        self.offset_map.insert(
            offset_key.clone(),
            FilePosition {
                offset: records[0].offset,
                line_num: records[0].line_num,
            },
        );

        let mut del_objs = vec![];

        for record in records {
            let obj_identifier = ObjectIdentifier::builder()
                .key(record.key)
                .build()
                .context(format!("{}:{}", file!(), line!()))?;
            del_objs.push(obj_identifier);
        }

        s_c.remove_objects(&self.source.bucket, del_objs)
            .await
            .context(format!("{}:{}", file!(), line!()))?;

        self.offset_map.remove(&offset_key);
        Ok(())
    }
}
