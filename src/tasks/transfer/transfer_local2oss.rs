use super::task_transfer::TransferTaskAttributes;
use crate::commons::{
    analyze_folder_files_size, merge_file, scan_folder_files_to_file, struct_to_json_string,
    LastModifyFilter, Modified, ModifyType, NotifyWatcher, PathType, RegexFilter,
};
use crate::s3::OSSDescription;
use crate::s3::OssClient;
use crate::tasks::{
    task::{
        gen_file_path, TaskDefaultParameters, MODIFIED_PREFIX, NOTIFY_FILE_PREFIX, OFFSET_PREFIX,
        REMOVED_PREFIX,
    },
    task_actions::{TransferExecutor, TransferTaskActions},
    FileDescription, FilePosition, IncrementAssistant, ListedRecord, LocalNotify, Opt,
    RecordOption,
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::types::Object;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::from_str;
use std::collections::BTreeMap;
use std::io::{BufRead, Seek, SeekFrom};
use std::sync::Arc;
use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, Write},
    path::Path,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex, Semaphore},
    task::JoinSet,
};
use walkdir::WalkDir;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TransferLocal2Oss {
    #[serde(default = "TaskDefaultParameters::id_default")]
    pub task_id: String,
    #[serde(default = "TaskDefaultParameters::name_default")]
    pub name: String,
    pub source: String,
    pub target: OSSDescription,
    pub attributes: TransferTaskAttributes,
}

impl Default for TransferLocal2Oss {
    fn default() -> Self {
        Self {
            task_id: TaskDefaultParameters::id_default(),
            name: TaskDefaultParameters::name_default(),
            target: OSSDescription::default(),
            source: "/tmp".to_string(),
            attributes: TransferTaskAttributes::default(),
        }
    }
}

#[async_trait]
impl TransferTaskActions for TransferLocal2Oss {
    async fn analyze_source(&self) -> Result<BTreeMap<String, i128>> {
        let filter = RegexFilter::from_vec(&self.attributes.exclude, &self.attributes.include)?;
        analyze_folder_files_size(
            &self.source,
            Some(filter),
            self.attributes.last_modify_filter.clone(),
        )
    }

    fn gen_transfer_executor(
        &self,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        semaphore: Arc<Semaphore>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        list_file_path: String,
    ) -> Arc<dyn TransferExecutor + Send + Sync> {
        let executor = TransferLocal2OssExecuter {
            source: self.source.clone(),
            target: self.target.clone(),
            stop_mark,
            err_occur,
            semaphore,
            offset_map,
            attributes: self.attributes.clone(),
            list_file_path,
        };
        Arc::new(executor)
    }

    // 生成对象列表
    async fn gen_source_object_list_file(&self, object_list_file: &str) -> Result<FileDescription> {
        let regex_filter =
            RegexFilter::from_vec_option(&self.attributes.exclude, &self.attributes.include)?;
        scan_folder_files_to_file(
            self.source.as_str(),
            &object_list_file,
            regex_filter,
            self.attributes.last_modify_filter,
        )
    }

    async fn changed_object_capture_based_target(
        &self,
        timestamp: usize,
    ) -> Result<FileDescription> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let removed = gen_file_path(
            &self.attributes.meta_dir,
            REMOVED_PREFIX,
            now.as_secs().to_string().as_str(),
        );

        let modified = gen_file_path(
            &self.attributes.meta_dir,
            MODIFIED_PREFIX,
            now.as_secs().to_string().as_str(),
        );

        let mut removed_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&removed)?;

        let mut modified_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&modified)?;

        let mut removed_lines = 0;
        let mut modified_lines = 0;

        let last_modify_filter = LastModifyFilter {
            filter_type: crate::commons::LastModifyFilterType::Greater,
            timestamp,
        };

        let mut process_target_objects = |objects: Vec<Object>| {
            for obj in objects {
                if let Some(target_key) = obj.key() {
                    let mut source_key = "";
                    if let Some(p) = &self.target.prefix {
                        source_key = match p.ends_with("/") {
                            true => &p[p.len()..],
                            false => &p[p.len() + 1..],
                        };
                    };
                    let source_key_str = gen_file_path(&self.source, source_key, "");
                    let source_path = Path::new(&source_key_str);
                    if !source_path.exists() {
                        let record = RecordOption {
                            source_key: source_key_str,
                            target_key: target_key.to_string(),
                            list_file_path: "".to_string(),
                            list_file_position: FilePosition::default(),
                            option: Opt::REMOVE,
                        };
                        let record_str = match struct_to_json_string(&record) {
                            Ok(r) => r,
                            Err(e) => {
                                log::error!("{:?}", e);
                                return;
                            }
                        };
                        let _ = removed_file.write_all(record_str.as_bytes());
                        let _ = removed_file.write_all("\n".as_bytes());
                        removed_lines += 1;
                    }
                }
            }
        };

        let target_client = self.target.gen_oss_client()?;
        let resp = target_client
            .list_objects(
                &self.target.bucket,
                self.target.prefix.clone(),
                self.attributes.objects_per_batch,
                None,
            )
            .await?;
        let mut token = resp.next_token;
        if let Some(objects) = resp.object_list {
            process_target_objects(objects);
        }

        while token.is_some() {
            let resp = target_client
                .list_objects(
                    &self.target.bucket,
                    self.target.prefix.clone(),
                    self.attributes.objects_per_batch,
                    None,
                )
                .await?;
            if let Some(objects) = resp.object_list {
                process_target_objects(objects);
            }
            token = resp.next_token;
        }

        for entry in WalkDir::new(&self.source)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| !e.file_type().is_dir())
        {
            if let Some(p) = entry.path().to_str() {
                if p.eq(&self.source) {
                    continue;
                }

                let key = match &self.source.ends_with("/") {
                    true => &p[self.source.len()..],
                    false => &p[self.source.len() + 1..],
                };

                let mut target_key = "".to_string();
                if let Some(p) = &self.target.prefix {
                    target_key.push_str(p);
                }
                target_key.push_str(key);

                let modified_time = entry
                    .metadata()?
                    .modified()?
                    .duration_since(UNIX_EPOCH)?
                    .as_secs();

                if last_modify_filter.filter(usize::try_from(modified_time).unwrap()) {
                    let record = RecordOption {
                        source_key: p.to_string(),
                        target_key: target_key.to_string(),
                        list_file_path: "".to_string(),
                        list_file_position: FilePosition::default(),
                        option: Opt::PUT,
                    };
                    let record_str = match struct_to_json_string(&record) {
                        Ok(r) => r,
                        Err(e) => {
                            log::error!("{:?}", e);
                            return Err(e);
                        }
                    };
                    let _ = modified_file.write_all(record_str.as_bytes());
                    let _ = modified_file.write_all("\n".as_bytes());
                    modified_lines += 1;
                }
            };
        }

        removed_file.flush()?;
        modified_file.flush()?;
        let modified_size = modified_file.metadata()?.len();
        let removed_size = removed_file.metadata()?.len();

        merge_file(&modified, &removed, self.attributes.multi_part_chunk_size)?;
        let total_size = removed_size + modified_size;
        let total_lines = removed_lines + modified_lines;

        fs::rename(&removed, &modified)?;
        let file_desc = FileDescription {
            path: modified.to_string(),
            size: total_size,
            total_lines,
        };

        Ok(file_desc)
    }

    async fn increment_prelude(
        &self,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        assistant: Arc<Mutex<IncrementAssistant>>,
    ) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let notify_file_path = gen_file_path(
            self.attributes.meta_dir.as_str(),
            NOTIFY_FILE_PREFIX,
            now.as_secs().to_string().as_str(),
        );

        let file_for_notify = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(notify_file_path.as_str())?;

        let watcher = NotifyWatcher::new(&self.source)?;
        let notify_file_size = Arc::new(AtomicU64::new(0));

        let local_notify = LocalNotify {
            notify_file_size: Arc::clone(&notify_file_size),
            notify_file_path,
        };

        let mut lock = assistant.lock().await;
        lock.set_local_notify(Some(local_notify));
        drop(lock);

        watcher
            .watch_to_file(
                stop_mark,
                err_occur,
                file_for_notify,
                Arc::clone(&notify_file_size),
            )
            .await;
        log::info!("notify");

        Ok(())
    }

    async fn execute_increment(
        &self,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        semaphore: Arc<Semaphore>,
        _joinset: &mut JoinSet<()>,
        assistant: Arc<Mutex<IncrementAssistant>>,
        offset_map: Arc<DashMap<String, FilePosition>>,
    ) {
        let lock = assistant.lock().await;
        let local_notify = match lock.local_notify.clone() {
            Some(n) => n,
            None => {
                return;
            }
        };
        drop(lock);

        let mut offset = 0;
        let mut line_num = 0;

        let subffix = offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);

        let regex_filter =
            match RegexFilter::from_vec(&self.attributes.exclude, &self.attributes.include) {
                Ok(r) => r,
                Err(e) => {
                    log::error!("{:?}", e);
                    return;
                }
            };

        loop {
            if local_notify
                .notify_file_size
                .load(Ordering::SeqCst)
                .le(&offset)
            {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }

            let mut file = match File::open(&local_notify.notify_file_path) {
                Ok(f) => f,
                Err(e) => {
                    log::error!("{:?}", e);
                    stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                    return;
                }
            };

            if let Err(e) = file.seek(SeekFrom::Start(offset)) {
                log::error!("{:?}", e);
                stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                continue;
            };

            let lines = BufReader::new(file).lines();
            let mut offset_usize: usize = TryInto::<usize>::try_into(offset).unwrap();
            let mut records = vec![];
            for line in lines {
                line_num += 1;
                if let Result::Ok(key) = line {
                    // Modifed 解析
                    offset_usize += key.len();
                    match self
                        .modified_str_to_record_description(
                            &key,
                            &local_notify.notify_file_path,
                            offset_usize,
                            line_num,
                        )
                        .await
                    {
                        Ok(r) => {
                            if regex_filter.is_match(&r.source_key) {
                                records.push(r);
                            }
                        }
                        Err(e) => {
                            let r = RecordOption {
                                source_key: "".to_string(),
                                target_key: "".to_string(),
                                list_file_path: local_notify.notify_file_path.clone(),
                                list_file_position: FilePosition {
                                    offset: offset_usize,
                                    line_num,
                                },
                                option: Opt::UNKOWN,
                            };
                            r.handle_error(stop_mark.clone(), err_occur.clone());
                            err_occur.store(true, std::sync::atomic::Ordering::SeqCst);
                            log::error!("{:?}", e);
                        }
                    }
                }
            }

            let executor = self.gen_transfer_executor(
                stop_mark.clone(),
                Arc::new(AtomicBool::new(false)),
                semaphore.clone(),
                offset_map.clone(),
                local_notify.notify_file_path.clone(),
            );

            if records.len() > 0 {
                let _ = executor.transfer_record_options(records).await;
            }

            offset = local_notify.notify_file_size.load(Ordering::SeqCst);
            let offset_usize = TryInto::<usize>::try_into(offset).unwrap();
            let position = FilePosition {
                offset: offset_usize,
                line_num,
            };

            offset_map.remove(&offset_key);
            offset_key = OFFSET_PREFIX.to_string();
            offset_key.push_str(&offset.to_string());
            offset_map.insert(offset_key.clone(), position);
        }
    }
}

impl TransferLocal2Oss {
    async fn modified_str_to_record_description(
        &self,
        modified_str: &str,
        list_file_path: &str,
        offset: usize,
        line_num: u64,
    ) -> Result<RecordOption> {
        let modified = from_str::<Modified>(modified_str)?;
        let mut target_path = modified.path.clone();

        // 截取 target key 相对路径
        match self.source.ends_with("/") {
            true => target_path.drain(..self.source.len()),
            false => target_path.drain(..self.source.len() + 1),
        };

        // 补全prefix
        if let Some(mut oss_path) = self.target.prefix.clone() {
            match oss_path.ends_with("/") {
                true => target_path.insert_str(0, &oss_path),
                false => {
                    oss_path.push('/');
                    target_path.insert_str(0, &oss_path);
                }
            }
        }

        if PathType::File.eq(&modified.path_type) {
            match modified.modify_type {
                ModifyType::Create | ModifyType::Modify => {
                    let record = RecordOption {
                        source_key: modified.path.clone(),
                        target_key: target_path,
                        list_file_path: list_file_path.to_string(),
                        list_file_position: FilePosition { offset, line_num },
                        option: Opt::PUT,
                    };
                    return Ok(record);
                }
                ModifyType::Delete => {
                    let record = RecordOption {
                        source_key: modified.path.clone(),
                        target_key: target_path,
                        list_file_path: list_file_path.to_string(),
                        list_file_position: FilePosition { offset, line_num },
                        option: Opt::REMOVE,
                    };
                    return Ok(record);
                }
                ModifyType::Unkown => Err(anyhow!("Unkown modify type")),
            }
        } else {
            return Err(anyhow!("Unkown modify type"));
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransferLocal2OssExecuter {
    pub source: String,
    pub target: OSSDescription,
    pub stop_mark: Arc<AtomicBool>,
    pub err_occur: Arc<AtomicBool>,
    pub semaphore: Arc<Semaphore>,
    pub offset_map: Arc<DashMap<String, FilePosition>>,
    pub attributes: TransferTaskAttributes,
    pub list_file_path: String,
}

#[async_trait]
impl TransferExecutor for TransferLocal2OssExecuter {
    async fn transfer_listed_records(&self, records: Vec<ListedRecord>) -> Result<()> {
        let mut offset_key = OFFSET_PREFIX.to_string();
        let subffix = records[0].offset.to_string();
        offset_key.push_str(&subffix);

        let target_oss_client =
            match self
                .target
                .gen_oss_client()
                .context(format!("{}:{}", file!(), line!()))
            {
                Ok(c) => c,
                Err(e) => {
                    self.err_occur
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    self.stop_mark
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    log::error!("{:?}", e);
                    return Err(anyhow!(e));
                }
            };

        for record in records {
            if self.stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }

            let source_file_path = gen_file_path(self.source.as_str(), &record.key.as_str(), "");
            let mut target_key = "".to_string();
            if let Some(s) = self.target.prefix.clone() {
                target_key.push_str(&s);
            };

            target_key.push_str(&record.key);

            if let Err(e) = self
                .listed_record_handler(&source_file_path, &target_oss_client, &target_key)
                .await
            {
                let record_option = RecordOption {
                    source_key: source_file_path.clone(),
                    target_key: target_key.clone(),
                    list_file_path: self.list_file_path.clone(),
                    list_file_position: FilePosition {
                        offset: record.offset,
                        line_num: record.line_num,
                    },
                    option: Opt::PUT,
                };
                record_option.handle_error(self.stop_mark.clone(), self.err_occur.clone());
                self.err_occur
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                self.stop_mark
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                log::error!("{:?} {:?}", e, record_option);
            }

            // 文件位置记录后置，避免中断时已记录而传输未完成，续传时丢记录
            self.offset_map.insert(
                offset_key.clone(),
                FilePosition {
                    offset: record.offset,
                    line_num: record.line_num,
                },
            );
        }

        self.offset_map.remove(&offset_key);

        Ok(())
    }

    async fn transfer_record_options(&self, records: Vec<RecordOption>) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let mut subffix = records[0].list_file_position.offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);

        subffix.push_str("_");
        subffix.push_str(now.as_secs().to_string().as_str());

        let c_t = self.target.gen_oss_client()?;

        // Todo
        // 增加去重逻辑，当两条记录相邻为 create和modif时只put一次
        // 增加目录删除逻辑，对应oss删除指定prefix下的所有文件，文件系统删除目录
        for record in records {
            // 记录执行文件位置
            self.offset_map
                .insert(offset_key.clone(), record.list_file_position.clone());

            // 目标object存在则不推送
            if self.attributes.target_exists_skip {
                match c_t
                    .object_exists(self.target.bucket.as_str(), &record.target_key)
                    .await
                {
                    Ok(b) => {
                        if b {
                            continue;
                        }
                    }
                    Err(e) => {
                        record.handle_error(self.stop_mark.clone(), self.err_occur.clone());
                        log::error!("{:?}", e);
                        continue;
                    }
                }
            }

            if let Err(e) = match record.option {
                Opt::PUT => {
                    // 判断源文件是否存在
                    let s_path = Path::new(&record.source_key);
                    if !s_path.exists() {
                        continue;
                    }

                    c_t.upload_local_file(
                        self.target.bucket.as_str(),
                        &record.target_key,
                        &record.source_key,
                        self.attributes.large_file_size,
                        self.attributes.multi_part_chunk_size,
                    )
                    .await
                }
                Opt::REMOVE => {
                    match c_t
                        .remove_object(self.target.bucket.as_str(), &record.target_key)
                        .await
                    {
                        Ok(_) => Ok(()),
                        Err(e) => Err(anyhow!("{:?}", e)),
                    }
                }
                _ => Err(anyhow!("option unkown")),
            } {
                record.handle_error(self.stop_mark.clone(), self.err_occur.clone());
                log::error!("{:?}", e);
                continue;
            }
        }

        self.offset_map.remove(&offset_key);

        Ok(())
    }
}

impl TransferLocal2OssExecuter {
    async fn listed_record_handler(
        &self,
        source_file: &str,
        target_oss: &OssClient,
        target_key: &str,
    ) -> Result<()> {
        // 判断源文件是否存在
        let s_path = Path::new(source_file);
        if !s_path.exists() {
            return Ok(());
        }

        // 目标object存在则不推送
        if self.attributes.target_exists_skip {
            let target_obj_exists = target_oss
                .object_exists(self.target.bucket.as_str(), target_key)
                .await?;

            if target_obj_exists {
                return Ok(());
            }
        }

        // ToDo
        // 新增阐述chunk_batch 定义分片上传每批上传分片的数量
        target_oss
            .upload_local_file_paralle(
                source_file,
                self.target.bucket.as_str(),
                target_key,
                self.attributes.large_file_size,
                self.semaphore.clone(),
                self.attributes.multi_part_chunk_size,
                self.attributes.multi_part_chunks_per_batch,
                self.attributes.multi_part_parallelism,
            )
            .await
    }
}
