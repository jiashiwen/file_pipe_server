use super::task_transfer::TransferTaskAttributes;
use crate::commons::{
    analyze_folder_files_size, copy_file, merge_file, scan_folder_files_to_file,
    struct_to_json_string, LastModifyFilter, Modified, ModifyType, NotifyWatcher, PathType,
    RegexFilter,
};
use crate::tasks::task::TaskDefaultParameters;
use crate::tasks::task::{
    gen_file_path, MODIFIED_PREFIX, NOTIFY_FILE_PREFIX, OFFSET_PREFIX, REMOVED_PREFIX,
};
use crate::tasks::task_actions::TransferExecutor;
use crate::tasks::{
    task_actions::TransferTaskActions, FileDescription, FilePosition, IncrementAssistant,
    ListedRecord, LocalNotify, Opt, RecordOption,
};
use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::from_str;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    sync::Arc,
};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use walkdir::WalkDir;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TransferLocal2Local {
    #[serde(default = "TaskDefaultParameters::id_default")]
    pub task_id: String,
    #[serde(default = "TaskDefaultParameters::name_default")]
    pub name: String,
    pub source: String,
    pub target: String,
    pub attributes: TransferTaskAttributes,
}

#[async_trait]
impl TransferTaskActions for TransferLocal2Local {
    async fn analyze_source(&self) -> Result<BTreeMap<String, i128>> {
        let filter = RegexFilter::from_vec(&self.attributes.exclude, &self.attributes.include)?;
        analyze_folder_files_size(
            &self.source,
            Some(filter),
            self.attributes.last_modify_filter,
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
        let executor = TransferLocal2LocalExecutor {
            task_id: self.task_id.clone(),
            source: self.source.clone(),
            target: self.target.clone(),
            stop_mark,
            err_occur,
            offset_map,
            attributes: self.attributes.clone(),
            list_file_path,
        };
        Arc::new(executor)
    }

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
        // 获取target object 列表 和removed 列表
        // 根据时间戳生成增量列表
        // 合并删除及新增列表

        let regex_filter =
            RegexFilter::from_vec_option(&self.attributes.exclude, &self.attributes.include)?;
        let last_modify_filter = LastModifyFilter {
            filter_type: crate::commons::LastModifyFilterType::Greater,
            timestamp,
        };

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

        for entry in WalkDir::new(&self.target)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| !e.file_type().is_dir())
        {
            if let Some(p) = entry.path().to_str() {
                if p.eq(&self.target) {
                    continue;
                }

                if let Some(ref f) = regex_filter {
                    if !f.is_match(p) {
                        continue;
                    }
                }

                let key = match &self.target.ends_with("/") {
                    true => &p[self.target.len()..],
                    false => &p[self.target.len() + 1..],
                };

                let source_key_str = gen_file_path(&self.source, key, "");
                let source_path = Path::new(&source_key_str);
                if !source_path.exists() {
                    let record = RecordOption {
                        source_key: source_key_str,
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
                    true => &p[self.target.len()..],
                    false => &p[self.target.len() + 1..],
                };

                let target_key_str = gen_file_path(&self.target, key, "");

                let modified_time = TryFrom::try_from(
                    entry
                        .metadata()?
                        .modified()?
                        .duration_since(UNIX_EPOCH)?
                        .as_secs(),
                )?;
                if last_modify_filter.filter(modified_time) {
                    let record = RecordOption {
                        source_key: p.to_string(),
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

        let watcher = NotifyWatcher::new(&self.source)?;
        let notify_file_size = Arc::new(AtomicU64::new(0));

        let file_for_notify = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(notify_file_path.as_str())?;

        watcher
            .watch_to_file(
                stop_mark,
                err_occur,
                file_for_notify,
                Arc::clone(&notify_file_size),
            )
            .await;

        let local_notify = LocalNotify {
            notify_file_path,
            notify_file_size: Arc::clone(&notify_file_size),
        };

        let mut lock = assistant.lock().await;
        lock.set_local_notify(Some(local_notify));
        drop(lock);

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
            None => return,
        };
        drop(lock);

        let file_position = FilePosition::default();

        let mut offset = match TryInto::<u64>::try_into(file_position.offset) {
            Ok(o) => o,
            Err(e) => {
                log::error!("{:?}", e);
                return;
            }
        };
        let mut line_num = file_position.line_num;

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
                            log::error!("{:?}", e);
                        }
                    }
                }
            }

            if records.len() > 0 {
                let copy = TransferLocal2LocalExecutor {
                    task_id: self.task_id.clone(),
                    source: self.source.clone(),
                    target: self.target.clone(),
                    stop_mark: stop_mark.clone(),
                    err_occur: Arc::new(AtomicBool::new(false)),
                    offset_map: Arc::clone(&offset_map),
                    attributes: self.attributes.clone(),
                    list_file_path: local_notify.notify_file_path.clone(),
                };
                let _ = copy.transfer_record_options(records).await;
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

impl TransferLocal2Local {
    async fn modified_str_to_record_description(
        &self,
        modified_str: &str,
        list_file_path: &str,
        offset: usize,
        line_num: u64,
    ) -> Result<RecordOption> {
        let modified = from_str::<Modified>(modified_str)?;
        let mut target_path = modified.path.clone();

        match self.source.ends_with("/") {
            true => target_path.drain(..self.source.len()),
            false => target_path.drain(..self.source.len() + 1),
        };

        match self.target.ends_with("/") {
            true => {
                target_path.insert_str(0, &self.target);
            }
            false => {
                let target_prefix = self.target.clone() + "/";
                target_path.insert_str(0, &target_prefix);
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
pub struct TransferLocal2LocalExecutor {
    pub task_id: String,
    pub source: String,
    pub target: String,
    pub stop_mark: Arc<AtomicBool>,
    pub err_occur: Arc<AtomicBool>,
    pub offset_map: Arc<DashMap<String, FilePosition>>,
    pub attributes: TransferTaskAttributes,
    pub list_file_path: String,
}

#[async_trait]
impl TransferExecutor for TransferLocal2LocalExecutor {
    async fn transfer_listed_records(&self, records: Vec<ListedRecord>) -> Result<()> {
        let subffix = records[0].offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);

        for record in records {
            if self.stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(());
            }

            let s_file_name = gen_file_path(self.source.as_str(), record.key.as_str(), "");
            let t_file_name = gen_file_path(self.target.as_str(), record.key.as_str(), "");

            match self
                .listed_record_handler(s_file_name.as_str(), t_file_name.as_str())
                .await
            {
                Ok(_) => {
                    // 文件位置记录后置，避免中断时已记录而传输未完成，续传时丢记录
                    self.offset_map.insert(
                        offset_key.clone(),
                        FilePosition {
                            offset: record.offset,
                            line_num: record.line_num,
                        },
                    );
                }
                Err(e) => {
                    // 记录错误记录
                    let opt = RecordOption {
                        source_key: s_file_name,
                        target_key: t_file_name,
                        list_file_path: self.list_file_path.clone(),
                        list_file_position: FilePosition {
                            offset: record.offset,
                            line_num: record.line_num,
                        },
                        option: Opt::PUT,
                    };
                    opt.handle_error(self.stop_mark.clone(), self.err_occur.clone());
                    log::error!("{:?},{:?},{:?}", self.task_id, e, opt);
                }
            };
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

        // let error_file_name = gen_file_path(
        //     &self.attributes.meta_dir,
        //     TRANSFER_ERROR_RECORD_PREFIX,
        //     &subffix,
        // );

        // let error_file = OpenOptions::new()
        //     .create(true)
        //     .write(true)
        //     .truncate(true)
        //     .open(error_file_name.as_str())?;

        // drop(error_file);

        for record in records {
            if self.stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            if let Err(e) = self.record_description_handler(&record).await {
                record.handle_error(self.stop_mark.clone(), self.err_occur.clone());
                self.err_occur
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                log::error!("{:?}", e);
            }
        }

        // let error_file = match File::open(&error_file_name) {
        //     Ok(f) => f,
        //     Err(e) => {
        //         self.err_occur
        //             .store(true, std::sync::atomic::Ordering::SeqCst);
        //         log::error!("{:?}", e);
        //         return Err(anyhow!(e));
        //     }
        // };
        // match error_file.metadata() {
        //     Ok(meta) => {
        //         if meta.len() == 0 {
        //             let _ = fs::remove_file(error_file_name.as_str());
        //         }
        //     }
        //     Err(_) => {}
        // };

        Ok(())
    }
}

impl TransferLocal2LocalExecutor {
    async fn listed_record_handler(&self, source_file: &str, target_file: &str) -> Result<()> {
        // 判断源文件是否存在，若不存判定为成功传输
        let s_path = Path::new(source_file);
        if !s_path.exists() {
            return Ok(());
        }

        let t_path = Path::new(target_file);
        if let Some(p) = t_path.parent() {
            std::fs::create_dir_all(p)?
        };

        // 目标object存在则不推送
        if self.attributes.target_exists_skip {
            if t_path.exists() {
                return Ok(());
            }
        }

        copy_file(
            source_file,
            target_file,
            self.attributes.large_file_size,
            self.attributes.multi_part_chunk_size,
        )
    }

    pub async fn record_description_handler(&self, record: &RecordOption) -> Result<()> {
        match record.option {
            Opt::PUT => {
                copy_file(
                    &record.source_key,
                    &record.target_key,
                    self.attributes.large_file_size,
                    self.attributes.multi_part_chunk_size,
                )?;
            }
            Opt::REMOVE => fs::remove_file(record.target_key.as_str())?,
            _ => return Err(anyhow!("unknow option")),
        };
        Ok(())
    }
}
