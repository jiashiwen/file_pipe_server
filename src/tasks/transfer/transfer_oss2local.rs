use super::task_transfer::TransferTaskAttributes;
use crate::resources::get_checkpoint;
use crate::tasks::{
    task::{
        gen_file_path, TaskDefaultParameters, TransferStage, MODIFIED_PREFIX, OFFSET_PREFIX,
        REMOVED_PREFIX, TRANSFER_OBJECT_LIST_FILE_PREFIX,
    },
    task_actions::{TransferExecutor, TransferTaskActions},
    FileDescription, FilePosition, IncrementAssistant, ListedRecord, Opt, RecordOption,
};
use crate::{
    commons::{merge_file, struct_to_json_string, LastModifyFilter, RegexFilter},
    s3::{download_object, OSSDescription, OssClient},
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_sdk_s3::types::Object;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::from_str;
use std::collections::BTreeMap;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, Write},
    path::Path,
    sync::{atomic::AtomicBool, Arc},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Semaphore;
use tokio::{sync::Mutex, task::JoinSet};
use walkdir::WalkDir;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TransferOss2Local {
    #[serde(default = "TaskDefaultParameters::id_default")]
    pub task_id: String,
    #[serde(default = "TaskDefaultParameters::name_default")]
    pub name: String,
    pub source: OSSDescription,
    pub target: String,
    pub attributes: TransferTaskAttributes,
}

impl Default for TransferOss2Local {
    fn default() -> Self {
        Self {
            task_id: TaskDefaultParameters::id_default(),
            name: TaskDefaultParameters::name_default(),
            source: OSSDescription::default(),
            target: "/tmp".to_string(),
            attributes: TransferTaskAttributes::default(),
        }
    }
}

#[async_trait]
impl TransferTaskActions for TransferOss2Local {
    async fn analyze_source(&self) -> Result<BTreeMap<String, i128>> {
        let regex_filter =
            RegexFilter::from_vec(&self.attributes.exclude, &self.attributes.include)?;
        let client = self.source.gen_oss_client()?;
        client
            .analyze_objects_size(
                &self.source.bucket,
                self.source.prefix.clone(),
                Some(regex_filter),
                self.attributes.last_modify_filter.clone(),
                self.attributes.objects_per_batch,
            )
            .await
    }

    fn gen_transfer_executor(
        &self,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        semaphore: Arc<Semaphore>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        list_file_path: String,
    ) -> Arc<dyn TransferExecutor + Send + Sync> {
        let executor = TransferOss2LocalRecordsExecutor {
            task_id: self.task_id.clone(),
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

    async fn gen_source_object_list_file(&self, object_list_file: &str) -> Result<FileDescription> {
        let client_source = self.source.gen_oss_client()?;
        // 若为持续同步模式，且 last_modify_timestamp 大于 0，则将 last_modify 属性大于last_modify_timestamp变量的对象加入执行列表
        let regex_filter =
            RegexFilter::from_vec_option(&self.attributes.exclude, &self.attributes.include)?;
        client_source
            .append_object_list_to_file(
                self.source.bucket.clone(),
                self.source.prefix.clone(),
                self.attributes.objects_per_batch,
                object_list_file,
                regex_filter,
                self.attributes.last_modify_filter,
            )
            .await
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

        let target_object_list = gen_file_path(
            &self.attributes.meta_dir,
            TRANSFER_OBJECT_LIST_FILE_PREFIX,
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

        let reg_filter = RegexFilter::from_vec(&self.attributes.exclude, &self.attributes.include)?;
        let last_modify_filter = LastModifyFilter {
            filter_type: crate::commons::LastModifyFilterType::Greater,
            timestamp,
        };

        let source_client = self.source.gen_oss_client()?;

        // 遍历本地目录，找出远端删除的key
        for entry in WalkDir::new(&self.target)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| !e.file_type().is_dir())
        {
            if let Some(p) = entry.path().to_str() {
                if p.eq(&self.target) {
                    continue;
                }

                let mut source_key = "".to_string();
                let key = match &self.target.ends_with("/") {
                    true => &p[self.target.len()..],
                    false => &p[self.target.len() + 1..],
                };

                if let Some(p) = &self.source.prefix {
                    source_key.push_str(&p);
                }

                source_key.push_str(key);

                if !source_client
                    .object_exists(&self.source.bucket, &source_key)
                    .await?
                {
                    let record = RecordOption {
                        source_key,
                        target_key: p.to_string(),
                        list_file_path: "".to_string(),
                        list_file_position: FilePosition::default(),
                        option: Opt::REMOVE,
                    };
                    let record_str = struct_to_json_string(&record)?;
                    let _ = removed_file.write_all(record_str.as_bytes());
                    let _ = removed_file.write_all("\n".as_bytes());
                    removed_lines += 1;
                }
            };
        }

        let mut process_source_objects = |objects: Vec<Object>| -> Result<()> {
            for obj in objects {
                if let Some(source_key) = obj.key() {
                    if !reg_filter.is_match(source_key) {
                        continue;
                    }

                    if let Some(d) = obj.last_modified() {
                        if last_modify_filter.filter(usize::try_from(d.secs())?) {
                            let target_key_str = gen_file_path(&self.target, source_key, "");
                            let record = RecordOption {
                                source_key: source_key.to_string(),
                                target_key: target_key_str,
                                list_file_path: "".to_string(),
                                list_file_position: FilePosition::default(),
                                option: Opt::PUT,
                            };

                            let record_str = struct_to_json_string(&record)?;
                            let _ = modified_file.write_all(record_str.as_bytes());
                            let _ = modified_file.write_all("\n".as_bytes());
                            modified_lines += 1;
                        }
                    }
                }
            }
            Ok(())
        };
        let resp = source_client
            .list_objects(
                &self.source.bucket,
                self.source.prefix.clone(),
                self.attributes.objects_per_batch,
                None,
            )
            .await?;
        let mut token = resp.next_token;
        if let Some(objects) = resp.object_list {
            process_source_objects(objects)?;
        }

        while token.is_some() {
            let resp = source_client
                .list_objects(
                    &self.source.bucket,
                    self.source.prefix.clone(),
                    self.attributes.objects_per_batch,
                    None,
                )
                .await?;
            if let Some(objects) = resp.object_list {
                process_source_objects(objects)?;
            }
            token = resp.next_token;
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
            path: target_object_list.to_string(),
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
        // 记录当前时间戳
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let timestampe = TryInto::<i64>::try_into(now.as_secs())?;
        let mut lock = assistant.lock().await;
        lock.last_modify_timestamp = Some(timestampe);
        drop(lock);
        Ok(())
    }
    async fn execute_increment(
        &self,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        semaphore: Arc<Semaphore>,
        execute_set: &mut JoinSet<()>,
        assistant: Arc<Mutex<IncrementAssistant>>,
        offset_map: Arc<DashMap<String, FilePosition>>,
    ) {
        // 循环执行获取lastmodify 大于checkpoint指定的时间戳的对象
        let mut checkpoint = match get_checkpoint(&self.task_id) {
            Ok(c) => c,
            Err(e) => {
                log::error!("{}", e);
                return;
            }
        };
        checkpoint.task_stage = TransferStage::Increment;

        let regex_filter =
            match RegexFilter::from_vec(&self.attributes.exclude, &self.attributes.include) {
                Ok(r) => r,
                Err(e) => {
                    log::error!("{:?}", e);
                    return;
                }
            };

        let mut sleep_time = 5;

        let mut finished_total_objects = 0;

        while !stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
            let modified = match self
                .changed_object_capture_based_target(
                    usize::try_from(checkpoint.modify_checkpoint_timestamp).unwrap(),
                )
                .await
            {
                Ok(f) => f,
                Err(e) => {
                    log::error!("{:?}", e);
                    return;
                }
            };

            let mut vec_keys = vec![];
            // 生成执行文件
            let mut list_file_position = FilePosition::default();
            let modified_file = match File::open(&modified.path) {
                Ok(f) => f,
                Err(e) => {
                    log::error!("{:?}", e);
                    err_occur.store(true, std::sync::atomic::Ordering::SeqCst);
                    stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                    return;
                }
            };

            let modified_file_is_empty = modified_file.metadata().unwrap().len().eq(&0);

            // 按列表传输object from source to target
            let lines: io::Lines<io::BufReader<File>> = io::BufReader::new(modified_file).lines();
            for line in lines {
                if let Result::Ok(line_str) = line {
                    let len = line_str.bytes().len() + "\n".bytes().len();
                    let mut record = match from_str::<RecordOption>(&line_str) {
                        Ok(r) => r,
                        Err(e) => {
                            log::error!("{:?}", e);
                            err_occur.store(true, std::sync::atomic::Ordering::SeqCst);
                            stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                            return;
                        }
                    };

                    if regex_filter.is_match(&record.source_key) {
                        let t_file_name =
                            gen_file_path(self.target.as_str(), &record.target_key, "");
                        record.target_key = t_file_name;
                        vec_keys.push(record);
                    }
                    list_file_position.offset += len;
                    list_file_position.line_num += 1;
                };

                if vec_keys
                    .len()
                    .to_string()
                    .eq(&self.attributes.objects_per_batch.to_string())
                {
                    while execute_set.len() >= self.attributes.task_parallelism {
                        execute_set.join_next().await;
                    }
                    let vk = vec_keys.clone();
                    let executor = self.gen_transfer_executor(
                        stop_mark.clone(),
                        err_occur.clone(),
                        semaphore.clone(),
                        offset_map.clone(),
                        modified.path.clone(),
                    );
                    executor.transfer_record_options(vk).await;

                    // 清理临时key vec
                    vec_keys.clear();
                }
            }

            if vec_keys.len() > 0 {
                while execute_set.len() >= self.attributes.task_parallelism {
                    execute_set.join_next().await;
                }

                let vk = vec_keys.clone();
                let executor = self.gen_transfer_executor(
                    stop_mark.clone(),
                    err_occur.clone(),
                    semaphore.clone(),
                    offset_map.clone(),
                    modified.path.clone(),
                );
                executor.transfer_record_options(vk).await;
            }

            while execute_set.len() > 0 {
                execute_set.join_next().await;
            }

            finished_total_objects += modified.total_lines;
            if !modified.total_lines.eq(&0) {
                let msg = format!(
                    "executing transfer modified finished this batch {} total {};",
                    modified.total_lines, finished_total_objects
                );
                // pd.set_message(msg);
            }

            let _ = fs::remove_file(&modified.path);

            checkpoint.executed_file_position = FilePosition {
                offset: modified.size.try_into().unwrap(),
                line_num: modified.total_lines,
            };
            checkpoint.executed_file = modified.clone();
            // checkpoint.current_stock_object_list_file = new_object_list_desc.path.clone();
            // let _ = checkpoint.save_to(&checkpoint_path);
            let _ = checkpoint.save_to_rocksdb_cf();

            //递增等待时间
            if modified_file_is_empty {
                if sleep_time.ge(&300) {
                    sleep_time = 60;
                } else {
                    sleep_time += 5;
                }
            } else {
                sleep_time = 5;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(sleep_time)).await;
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransferOss2LocalRecordsExecutor {
    pub task_id: String,
    pub source: OSSDescription,
    pub target: String,
    pub stop_mark: Arc<AtomicBool>,
    pub err_occur: Arc<AtomicBool>,
    pub semaphore: Arc<Semaphore>,
    pub offset_map: Arc<DashMap<String, FilePosition>>,
    pub attributes: TransferTaskAttributes,
    pub list_file_path: String,
}

#[async_trait]
impl TransferExecutor for TransferOss2LocalRecordsExecutor {
    async fn transfer_listed_records(&self, records: Vec<ListedRecord>) -> Result<()> {
        let subffix = records[0].offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);

        let c_s = self.source.gen_oss_client()?;
        for record in records {
            if self.stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }

            let t_file_name = gen_file_path(self.target.as_str(), &record.key.as_str(), "");

            if let Err(e) = self
                .listed_record_handler(&record, &c_s, t_file_name.as_str())
                .await
            {
                let record_option = RecordOption {
                    source_key: record.key.clone(),
                    target_key: t_file_name.clone(),
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

        let source_client = self.source.gen_oss_client()?;

        for record in records {
            let t_path = Path::new(&record.target_key);
            if let Some(p) = t_path.parent() {
                std::fs::create_dir_all(p)?
            };

            // 目标object存在则不推送
            if self.attributes.target_exists_skip {
                if t_path.exists() {
                    continue;
                }
            }

            match self
                .record_description_handler(&source_client, &record)
                .await
            {
                Ok(_) => {
                    // 记录执行文件位置
                    self.offset_map
                        .insert(offset_key.clone(), record.list_file_position.clone());
                }
                Err(e) => {
                    record.handle_error(self.stop_mark.clone(), self.err_occur.clone());
                    log::error!("{:?},{:?}", e, record);
                }
            };
        }
        self.offset_map.remove(&offset_key);

        Ok(())
    }
}

impl TransferOss2LocalRecordsExecutor {
    async fn listed_record_handler(
        &self,
        record: &ListedRecord,
        source_oss_client: &OssClient,
        target_file: &str,
    ) -> Result<()> {
        let t_path = Path::new(target_file);
        if let Some(p) = t_path.parent() {
            std::fs::create_dir_all(p)?;
        };

        // 目标object存在则不下载
        if self.attributes.target_exists_skip {
            if t_path.exists() {
                return Ok(());
            }
        }

        let s_obj_output = match source_oss_client
            .get_object(&self.source.bucket.as_str(), record.key.as_str())
            .await
        {
            core::result::Result::Ok(resp) => resp,
            Err(e) => {
                // 源端文件不存在按传输成功处理
                let service_err = e.into_service_error();
                match service_err.is_no_such_key() {
                    true => {
                        return Ok(());
                    }
                    false => {
                        return Err(service_err.into());
                    }
                }
            }
        };
        let content_len = match s_obj_output.content_length() {
            Some(l) => l,
            None => return Err(anyhow!("content length is None")),
        };
        let content_len_usize: usize = content_len.try_into()?;

        match content_len_usize.le(&self.attributes.large_file_size) {
            true => {
                download_object(
                    s_obj_output,
                    target_file,
                    self.attributes.large_file_size,
                    self.attributes.multi_part_chunk_size,
                )
                .await
            }
            false => {
                source_oss_client
                    .download_object_by_range(
                        &self.source.bucket.clone(),
                        &record.key,
                        target_file,
                        self.semaphore.clone(),
                        self.attributes.multi_part_chunk_size,
                        self.attributes.multi_part_chunks_per_batch,
                        self.attributes.multi_part_parallelism,
                    )
                    .await
            }
        }
    }

    async fn record_description_handler(
        &self,
        source_oss_client: &OssClient,
        record: &RecordOption,
    ) -> Result<()> {
        match record.option {
            Opt::PUT => {
                let obj = match source_oss_client
                    .get_object(&self.source.bucket, &record.source_key)
                    .await
                {
                    Ok(o) => o,
                    Err(e) => {
                        let service_err = e.into_service_error();
                        match service_err.is_no_such_key() {
                            true => {
                                return Ok(());
                            }
                            false => {
                                log::error!("{:?}", service_err);
                                return Err(service_err.into());
                            }
                        }
                    }
                };
                download_object(
                    obj,
                    &record.target_key,
                    self.attributes.large_file_size,
                    self.attributes.multi_part_chunk_size,
                )
                .await?
            }
            Opt::REMOVE => {
                let _ = fs::remove_file(record.target_key.as_str());
            }
            _ => return Err(anyhow!("option unkown")),
        }
        Ok(())
    }
}
