use crate::commons::{json_to_struct, quantify_processbar, LastModifyFilter};
use crate::resources::get_checkpoint;
use crate::resources::save_task_status_to_cf;
use crate::s3::OSSDescription;
use crate::tasks::task::{de_usize_from_str, gen_file_path, se_usize_to_str};
use crate::tasks::task_status_saver::{
    GLOBAL_TASK_LIST_FILE_POSITON_MAP, GLOBAL_TASK_STOP_MARK_MAP,
};
use crate::tasks::{
    modules::{CheckPoint, FilePosition, ListedRecord},
    task::{
        TaskDefaultParameters, TaskStopReason, TransferStage, NOTIFY_FILE_PREFIX, OFFSET_PREFIX,
        TRANSFER_OBJECT_LIST_FILE_PREFIX,
    },
    task_actions::TransferTaskActions,
    transfer::{
        transfer_local2local::TransferLocal2Local, transfer_local2oss::TransferLocal2Oss,
        transfer_oss2local::TransferOss2Local, transfer_oss2oss::TransferOss2Oss,
    },
    FileDescription, IncrementAssistant, LogInfo, RecordOption, Status, TaskStatus, TransferStatus,
};
use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, BufRead},
    sync::{atomic::AtomicBool, Arc},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex, Semaphore},
    task::{self, JoinSet},
};
use walkdir::WalkDir;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TransferTaskStatusType {
    Starting,
    Running(TransferStage),
    Stopped(TaskStopReason),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransferTaskStatus {
    pub task_id: String,
    pub start_time: u64,
    pub status: TransferTaskStatusType,
}

impl TransferTaskStatusType {
    pub fn is_starting(&self) -> bool {
        match self {
            TransferTaskStatusType::Starting => true,
            _ => false,
        }
    }

    pub fn is_stock_running(&self) -> bool {
        match self {
            TransferTaskStatusType::Running(ts) => match ts {
                TransferStage::Stock => true,
                TransferStage::Increment => false,
            },
            _ => false,
        }
    }

    pub fn is_running(&self) -> bool {
        match self {
            TransferTaskStatusType::Running(_) => true,
            _ => false,
        }
    }

    pub fn is_increment_running(&self) -> bool {
        match self {
            TransferTaskStatusType::Running(ts) => match ts {
                TransferStage::Stock => false,
                TransferStage::Increment => true,
            },
            _ => false,
        }
    }

    pub fn is_stopped(&self) -> bool {
        match self {
            TransferTaskStatusType::Stopped(_) => true,
            _ => false,
        }
    }

    pub fn is_stopped_finish(&self) -> bool {
        match self {
            TransferTaskStatusType::Stopped(s) => match s {
                TaskStopReason::Finish => true,
                TaskStopReason::Broken => false,
            },
            _ => false,
        }
    }

    pub fn is_stopped_broken(&self) -> bool {
        match self {
            TransferTaskStatusType::Stopped(s) => match s {
                TaskStopReason::Finish => false,
                TaskStopReason::Broken => true,
            },
            _ => false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum TransferType {
    Full,
    Stock,
    Increment,
}

impl TransferType {
    pub fn is_full(&self) -> bool {
        match self {
            TransferType::Full => true,
            _ => false,
        }
    }

    #[allow(dead_code)]
    pub fn is_stock(&self) -> bool {
        match self {
            TransferType::Stock => true,
            _ => false,
        }
    }

    pub fn is_increment(&self) -> bool {
        match self {
            TransferType::Increment => true,
            _ => false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(untagged)]
#[serde(rename_all = "lowercase")]
// #[serde(tag = "type")]
pub enum ObjectStorage {
    Local(String),
    OSS(OSSDescription),
}

impl Default for ObjectStorage {
    fn default() -> Self {
        ObjectStorage::OSS(OSSDescription::default())
    }
}

// ToDo 规范属性名称
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransferTaskAttributes {
    #[serde(default = "TaskDefaultParameters::objects_per_batch_default")]
    pub objects_per_batch: i32,
    #[serde(default = "TaskDefaultParameters::task_parallelism_default")]
    pub task_parallelism: usize,
    #[serde(default = "TaskDefaultParameters::meta_dir_default")]
    pub meta_dir: String,
    #[serde(default = "TaskDefaultParameters::target_exists_skip_default")]
    pub target_exists_skip: bool,
    #[serde(default = "TaskDefaultParameters::start_from_checkpoint_default")]
    pub start_from_checkpoint: bool,
    #[serde(default = "TaskDefaultParameters::large_file_size_default")]
    #[serde(serialize_with = "se_usize_to_str")]
    #[serde(deserialize_with = "de_usize_from_str")]
    pub large_file_size: usize,
    #[serde(default = "TaskDefaultParameters::multi_part_chunk_size_default")]
    #[serde(serialize_with = "se_usize_to_str")]
    #[serde(deserialize_with = "de_usize_from_str")]
    pub multi_part_chunk_size: usize,
    #[serde(default = "TaskDefaultParameters::multi_part_chunks_per_batch_default")]
    pub multi_part_chunks_per_batch: usize,
    #[serde(default = "TaskDefaultParameters::multi_part_parallelism_default")]
    pub multi_part_parallelism: usize,
    #[serde(default = "TaskDefaultParameters::multi_part_parallelism_default")]
    pub multi_part_max_parallelism: usize,
    #[serde(default = "TaskDefaultParameters::filter_default")]
    pub exclude: Option<Vec<String>>,
    #[serde(default = "TaskDefaultParameters::filter_default")]
    pub include: Option<Vec<String>>,
    #[serde(default = "TaskDefaultParameters::transfer_type_default")]
    pub transfer_type: TransferType,
    #[serde(default = "TaskDefaultParameters::last_modify_filter_default")]
    pub last_modify_filter: Option<LastModifyFilter>,
}

impl Default for TransferTaskAttributes {
    fn default() -> Self {
        Self {
            objects_per_batch: TaskDefaultParameters::objects_per_batch_default(),
            task_parallelism: TaskDefaultParameters::task_parallelism_default(),
            meta_dir: TaskDefaultParameters::meta_dir_default(),
            target_exists_skip: TaskDefaultParameters::target_exists_skip_default(),
            start_from_checkpoint: TaskDefaultParameters::target_exists_skip_default(),
            large_file_size: TaskDefaultParameters::large_file_size_default(),
            multi_part_chunk_size: TaskDefaultParameters::multi_part_chunk_size_default(),
            multi_part_chunks_per_batch: TaskDefaultParameters::multi_part_chunks_per_batch_default(
            ),
            multi_part_parallelism: TaskDefaultParameters::multi_part_parallelism_default(),
            multi_part_max_parallelism: TaskDefaultParameters::multi_part_max_parallelism_default(),
            exclude: TaskDefaultParameters::filter_default(),
            include: TaskDefaultParameters::filter_default(),
            transfer_type: TaskDefaultParameters::transfer_type_default(),
            last_modify_filter: TaskDefaultParameters::last_modify_filter_default(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TransferTask {
    #[serde(default = "TaskDefaultParameters::id_default")]
    pub task_id: String,
    #[serde(default = "TaskDefaultParameters::name_default")]
    pub name: String,
    pub source: ObjectStorage,
    pub target: ObjectStorage,
    pub attributes: TransferTaskAttributes,
}

impl Default for TransferTask {
    fn default() -> Self {
        Self {
            task_id: TaskDefaultParameters::id_default(),
            name: TaskDefaultParameters::name_default(),
            source: ObjectStorage::OSS(OSSDescription::default()),
            target: ObjectStorage::OSS(OSSDescription::default()),
            attributes: TransferTaskAttributes::default(),
        }
    }
}

impl TransferTask {
    pub fn gen_transfer_actions(&self) -> Arc<dyn TransferTaskActions + Send + Sync> {
        match &self.source {
            ObjectStorage::Local(path_s) => match &self.target {
                ObjectStorage::Local(path_t) => {
                    let t = TransferLocal2Local {
                        task_id: self.task_id.clone(),
                        name: self.name.clone(),
                        source: path_s.to_string(),
                        target: path_t.to_string(),
                        attributes: self.attributes.clone(),
                    };
                    Arc::new(t)
                }
                ObjectStorage::OSS(oss_t) => {
                    let t = TransferLocal2Oss {
                        task_id: self.task_id.clone(),
                        name: self.name.clone(),
                        source: path_s.to_string(),
                        target: oss_t.clone(),
                        attributes: self.attributes.clone(),
                    };
                    Arc::new(t)
                }
            },
            ObjectStorage::OSS(oss_s) => match &self.target {
                ObjectStorage::Local(path_t) => {
                    let t = TransferOss2Local {
                        task_id: self.task_id.clone(),
                        name: self.name.clone(),
                        source: oss_s.clone(),
                        target: path_t.to_string(),
                        attributes: self.attributes.clone(),
                    };
                    Arc::new(t)
                }
                ObjectStorage::OSS(oss_t) => {
                    let t = TransferOss2Oss {
                        task_id: self.task_id.clone(),
                        name: self.name.clone(),
                        source: oss_s.clone(),
                        target: oss_t.clone(),
                        attributes: self.attributes.clone(),
                    };
                    Arc::new(t)
                }
            },
        }
    }

    pub async fn analyze(&self) -> Result<BTreeMap<String, i128>> {
        let task = self.gen_transfer_actions();
        task.analyze_source().await
    }

    pub async fn start_task(&self) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        // sys_set 用于执行checkpoint、notify等辅助任务
        let mut sys_set = JoinSet::new();
        // execut_set 用于执行任务
        let mut exec_set = JoinSet::new();

        // sys_set 用于执行checkpoint、notify等辅助任务
        // let sys_set = Arc::new(RwLock::new(JoinSet::<()>::new()));
        // GLOBAL_TASKS_SYS_JOINSET.insert(self.task_id.clone(), sys_set.clone());

        // execut_set 用于执行任务
        // let task_exec_set = Arc::new(RwLock::new(JoinSet::<()>::new()));
        // GLOBAL_TASKS_EXEC_JOINSET.insert(self.task_id.clone(), task_exec_set.clone());

        //注册活动任务
        let task_status = &mut TaskStatus {
            task_id: self.task_id.clone(),
            start_time: now.as_secs(),
            status: Status::Transfer(TransferStatus::Starting),
        };
        let _ = save_task_status_to_cf(task_status)?;

        // 正在执行的任务数量，用于控制分片上传并行度
        let task = self.gen_transfer_actions();

        // 任务停止标准，用于通知所有协程任务结束
        let execute_stop_mark = Arc::new(AtomicBool::new(false));
        let notify_stop_mark = Arc::new(AtomicBool::new(false));
        let task_err_occur = Arc::new(AtomicBool::new(false));
        let multi_part_semaphore =
            Arc::new(Semaphore::new(self.attributes.multi_part_max_parallelism));
        let offset_map = Arc::new(DashMap::<String, FilePosition>::new());
        // 注册offset_map
        GLOBAL_TASK_LIST_FILE_POSITON_MAP.insert(self.task_id.clone(), offset_map.clone());
        // 注册stop_mark
        GLOBAL_TASK_STOP_MARK_MAP.insert(self.task_id.clone(), execute_stop_mark.clone());

        let assistant = IncrementAssistant::default();
        let increment_assistant = Arc::new(Mutex::new(assistant));
        let err_occur = task_err_occur.clone();

        //获取 对象列表文件，列表文件描述，increame_start_from_checkpoint 标识
        let (
            object_list_file,
            executed_file,
            mut list_file_position,
            start_from_checkpoint_stage_is_increment,
        ) = self
            .generate_list_file(
                execute_stop_mark.clone(),
                multi_part_semaphore.clone(),
                task,
            )
            .await?;

        // 全量同步时: 执行增量助理
        let mut file_for_notify = None;
        let task_increment_prelude = self.gen_transfer_actions();

        // 增量或全量同步时，提前启动 increment_assistant 用于记录 notify 文件
        if self.attributes.transfer_type.is_full() || self.attributes.transfer_type.is_increment() {
            let assistant = Arc::clone(&increment_assistant);
            let n_s_m = notify_stop_mark.clone();
            let e_o = err_occur.clone();

            // 启动increment_prelude 监听增量时的本地目录，或记录间戳
            sys_set.spawn(async move {
                if let Err(e) = task_increment_prelude
                    .increment_prelude(n_s_m, e_o, assistant)
                    .await
                {
                    log::error!("{:?}", e);
                    return;
                }
            });

            // 当源存储为本地时，获取notify文件
            if let ObjectStorage::Local(dir) = self.source.clone() {
                while file_for_notify.is_none() {
                    let lock = increment_assistant.lock().await;
                    file_for_notify = match lock.get_notify_file_path() {
                        Some(s) => Some(s),
                        None => None,
                    };
                    drop(lock);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }

                // 启动心跳，notify 需要有 event 输入才能有效结束任务
                let n_s_m = notify_stop_mark.clone();
                let e_o = err_occur.clone();
                sys_set.spawn(async move {
                    let tmp_file = gen_file_path(dir.as_str(), "oss_pipe_tmp", "");
                    while !e_o.load(std::sync::atomic::Ordering::SeqCst)
                        && !n_s_m.load(std::sync::atomic::Ordering::SeqCst)
                    {
                        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                        task::yield_now().await;
                    }
                    let f = OpenOptions::new()
                        .truncate(true)
                        .create(true)
                        .write(true)
                        .open(tmp_file.as_str())
                        .unwrap();
                    drop(f);
                    let _ = fs::remove_file(tmp_file);
                });
            }
        }

        // start from checkpoint 且 stage 为 increment
        match start_from_checkpoint_stage_is_increment {
            true => {
                self.exec_record_descriptions_file(
                    execute_stop_mark.clone(),
                    err_occur.clone(),
                    multi_part_semaphore.clone(),
                    &mut exec_set,
                    offset_map.clone(),
                    object_list_file,
                    executed_file,
                )
                .await;
            }
            false => {
                // 存量或全量场景时，执行对象列表文件
                match self.attributes.transfer_type {
                    TransferType::Full | TransferType::Stock => {
                        let mut checkpoint: CheckPoint = CheckPoint {
                            task_id: self.task_id.clone(),
                            executed_file: executed_file.clone(),
                            executed_file_position: list_file_position.clone(),
                            file_for_notify: None,
                            task_stage: TransferStage::Stock,
                            modify_checkpoint_timestamp: now.as_secs(),
                            task_begin_timestamp: now.as_secs(),
                        };
                        checkpoint.save_to_rocksdb_cf()?;

                        //变更任务状态为stock阶段
                        task_status.status =
                            Status::Transfer(TransferStatus::Running(TransferStage::Stock));
                        let _ = save_task_status_to_cf(task_status)?;

                        // 启动进度条线程
                        let map = Arc::clone(&offset_map);
                        let s_m = Arc::clone(&execute_stop_mark);
                        let total = executed_file.total_lines;
                        let id = self.task_id.clone();
                        sys_set.spawn(async move {
                            // Todo 调整进度条
                            quantify_processbar(id, total, s_m, map, OFFSET_PREFIX).await;
                        });

                        self.exec_records_file(
                            execute_stop_mark.clone(),
                            err_occur.clone(),
                            multi_part_semaphore.clone(),
                            &mut exec_set,
                            offset_map.clone(),
                            &mut list_file_position,
                            object_list_file,
                            executed_file,
                        )
                        .await;
                    }
                    TransferType::Increment => {}
                }
            }
        }

        while exec_set.len() > 0 {
            exec_set.join_next().await;
        }

        // 清理 offset 标记
        GLOBAL_TASK_LIST_FILE_POSITON_MAP.remove(self.task_id.as_str());

        if task_err_occur.load(std::sync::atomic::Ordering::Relaxed) {
            notify_stop_mark.store(true, std::sync::atomic::Ordering::Relaxed);
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            return Err(anyhow!("task {} error occure", self.task_id));
        }

        // 配置停止 offset save 标识为 true
        execute_stop_mark.store(true, std::sync::atomic::Ordering::Relaxed);

        let mut checkpoint = get_checkpoint(&self.task_id)?;

        // 设置存量文件executed_file_position为文件执行完成的位置
        checkpoint.executed_file_position = list_file_position;

        if self.attributes.transfer_type.is_stock() {
            //变更任务状态为 finish
            task_status.status = Status::Transfer(TransferStatus::Stopped(TaskStopReason::Finish));
            let _ =
                save_task_status_to_cf(task_status).context(format!("{}:{}", file!(), line!()))?;
            checkpoint.save_to_rocksdb_cf()?;
            return Ok(());
        } else {
            //变更任务状态为 Increment
            task_status.status =
                Status::Transfer(TransferStatus::Running(TransferStage::Increment));
            let _ =
                save_task_status_to_cf(task_status).context(format!("{}:{}", file!(), line!()))?;
            checkpoint.task_stage = TransferStage::Increment;
            checkpoint.save_to_rocksdb_cf()?;
        }

        // 执行增量逻辑
        if self.attributes.transfer_type.is_full() || self.attributes.transfer_type.is_increment() {
            // 准备增量任务
            let lock = increment_assistant.lock().await;
            let notify = lock.get_notify_file_path();
            drop(lock);
            //更新checkpoint 更改stage
            let mut checkpoint = get_checkpoint(&self.task_id)?;
            checkpoint.task_stage = TransferStage::Increment;
            checkpoint.file_for_notify = notify;
            checkpoint.save_to_rocksdb_cf()?;

            let stop_mark = Arc::new(AtomicBool::new(false));
            let offset_map = Arc::new(DashMap::<String, FilePosition>::new());
            let task_increment = self.gen_transfer_actions();

            task_increment
                .execute_increment(
                    Arc::clone(&stop_mark),
                    err_occur.clone(),
                    multi_part_semaphore.clone(),
                    &mut exec_set,
                    Arc::clone(&increment_assistant),
                    Arc::clone(&offset_map),
                )
                .await;
        }

        while sys_set.len() > 0 {
            task::yield_now().await;
            sys_set.join_next().await;
        }

        //判断任务是否异常退出
        if task_err_occur.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(anyhow!("task error occur"));
        }

        Ok(())
    }

    pub async fn list_transfer_objects(&self) -> Result<()> {
        let task = self.gen_transfer_actions();
        log::info!("generate objects list beging");

        // 清理 meta 目录
        // 重新生成object list file
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context(format!("{}:{}", file!(), line!()))?;
        let _ = fs::remove_dir_all(self.attributes.meta_dir.as_str());

        let executed_file = FileDescription {
            path: gen_file_path(
                self.attributes.meta_dir.as_str(),
                TRANSFER_OBJECT_LIST_FILE_PREFIX,
                now.as_secs().to_string().as_str(),
            ),
            size: 0,
            total_lines: 0,
        };
        let list_file_desc = task
            .gen_source_object_list_file(&executed_file.path)
            .await?;

        log::info!("generate objects list ok");
        println!("{:?}", list_file_desc);
        // });

        Ok(())
    }
}

impl TransferTask {
    // 生成执行列表文件，列表文件描述，是否为增量场景下从checkpoint执行
    // object_list_file，executed_file,list_file_position,start_from_checkpoint_stage_is_increment,
    async fn generate_list_file(
        &self,
        stop_mark: Arc<AtomicBool>,
        semaphore: Arc<Semaphore>,
        task: Arc<dyn TransferTaskActions + Send + Sync>,
    ) -> Result<(File, FileDescription, FilePosition, bool)> {
        let mut start_from_checkpoint_stage_is_increment = false;
        return match self.attributes.start_from_checkpoint {
            true => {
                let checkpoint =
                    get_checkpoint(&self.task_id).context(format!("{}:{}", file!(), line!()))?;

                // 清理notify file
                for entry in WalkDir::new(&self.attributes.meta_dir)
                    .into_iter()
                    .filter_map(Result::ok)
                    .filter(|e| !e.file_type().is_dir())
                {
                    if let Some(p) = entry.path().to_str() {
                        if p.contains(NOTIFY_FILE_PREFIX) {
                            let _ = fs::remove_file(p);
                        }
                    };
                }

                match checkpoint.task_stage {
                    TransferStage::Stock => {
                        let list_file_desc = checkpoint.executed_file.clone();
                        let list_file_position = checkpoint.executed_file_position;
                        let list_file = checkpoint.seeked_execute_file().context(format!(
                            "{}:{}",
                            file!(),
                            line!()
                        ))?;
                        Ok((
                            list_file,
                            list_file_desc,
                            list_file_position,
                            start_from_checkpoint_stage_is_increment,
                        ))
                    }
                    TransferStage::Increment => {
                        // Todo 重新分析逻辑，需要再checkpoint中记录每次增量执行前的起始时间点
                        // 清理文件重新生成object list 文件需大于指定时间戳,并根据原始object list 删除位于目标端但源端不存在的文件
                        // 流程逻辑
                        // 扫描target 文件list-> 抓取自扫描时间开始，源端的变动数据 -> 生成objlist，action 新增target change capture
                        // exec_modified = true;
                        let modified = task
                            .changed_object_capture_based_target(
                                usize::try_from(checkpoint.task_begin_timestamp).unwrap(),
                            )
                            .await?;
                        start_from_checkpoint_stage_is_increment = true;
                        let list_file = File::open(&modified.path)?;
                        let list_file_position = FilePosition::default();
                        Ok((
                            list_file,
                            modified,
                            list_file_position,
                            start_from_checkpoint_stage_is_increment,
                        ))
                    }
                }
            }
            false => {
                let mut info = LogInfo {
                    task_id: self.task_id.clone(),
                    msg: "generate objects list beging".to_string(),
                    additional: None::<String>,
                };
                log::info!("{:?}", info);

                // 清理 meta 目录
                // 重新生成object list file
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .context(format!("{}:{}", file!(), line!()))?;
                let _ = fs::remove_dir_all(self.attributes.meta_dir.as_str());

                let executed_file = FileDescription {
                    path: gen_file_path(
                        self.attributes.meta_dir.as_str(),
                        TRANSFER_OBJECT_LIST_FILE_PREFIX,
                        now.as_secs().to_string().as_str(),
                    ),
                    size: 0,
                    total_lines: 0,
                };
                let list_file_desc =
                    match task.gen_source_object_list_file(&executed_file.path).await {
                        Ok(f) => f,
                        Err(e) => {
                            return Err(e);
                        }
                    };

                let list_file =
                    File::open(&list_file_desc.path).context(format!("{}:{}", file!(), line!()))?;
                let list_file_position = FilePosition::default();

                info.msg = "generate objects list ok".to_string();
                log::info!("{:?}", info);
                Ok((
                    list_file,
                    list_file_desc,
                    list_file_position,
                    start_from_checkpoint_stage_is_increment,
                ))
            }
        };
    }

    // 存量迁移，执行列表文件
    async fn exec_records_file(
        &self,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        multi_parts_semaphore: Arc<Semaphore>,
        exec_set: &mut JoinSet<()>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        file_position: &mut FilePosition,
        records_file: File,
        executing_file: FileDescription,
    ) {
        let task_stock = self.gen_transfer_actions();
        let mut vec_keys: Vec<ListedRecord> = vec![];
        let lines: io::Lines<io::BufReader<File>> = io::BufReader::new(records_file).lines();
        let exec_lines = executing_file.total_lines - file_position.line_num;
        for (idx, line) in lines.enumerate() {
            // 若错误达到上限，则停止任务
            if stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }

            match line {
                Ok(key) => {
                    // 先写入当前key 开头的 offset，然后更新list_file_position 作为下一个key的offset,待验证效果
                    let len = key.bytes().len() + "\n".bytes().len();

                    // #[cfg(target_family = "unix")]
                    if !key.ends_with("/") {
                        let record = ListedRecord {
                            key,
                            offset: file_position.offset,
                            line_num: file_position.line_num,
                        };
                        vec_keys.push(record);
                    }

                    // #[cfg(target_family = "windows")]
                    // if !key.ends_with("\\") {
                    //     let record = ListedRecord {
                    //         key,
                    //         offset: file_position.offset,
                    //         line_num: file_position.line_num,
                    //     };
                    //     vec_keys.push(record);
                    // }

                    file_position.offset += len;
                    file_position.line_num += 1;
                }
                Err(e) => {
                    log::error!("{:?},file position:{:?}", e, file_position);
                    stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                    break;
                }
            }

            if vec_keys
                .len()
                .to_string()
                .eq(&self.attributes.objects_per_batch.to_string())
                || idx.eq(&TryInto::<usize>::try_into(exec_lines - 1).unwrap())
                    && vec_keys.len() > 0
            {
                while exec_set.len() >= self.attributes.task_parallelism {
                    exec_set.join_next().await;
                }

                // 提前插入 offset 保证顺序性
                let subffix = vec_keys[0].offset.to_string();
                let mut offset_key = OFFSET_PREFIX.to_string();
                offset_key.push_str(&subffix);

                offset_map.insert(
                    offset_key.clone(),
                    FilePosition {
                        offset: vec_keys[0].offset,
                        line_num: vec_keys[0].line_num,
                    },
                );

                let vk = vec_keys.clone();

                // Todo
                // 验证gen_transfer_executor 使用exec_set.spawn
                let record_executer = task_stock.gen_transfer_executor(
                    stop_mark.clone(),
                    err_occur.clone(),
                    multi_parts_semaphore.clone(),
                    offset_map.clone(),
                    executing_file.path.to_string(),
                );
                let eo = err_occur.clone();
                let sm = stop_mark.clone();
                exec_set.spawn(async move {
                    if let Err(e) = record_executer.transfer_listed_records(vk).await {
                        eo.store(true, std::sync::atomic::Ordering::SeqCst);
                        sm.store(true, std::sync::atomic::Ordering::SeqCst);
                        log::error!("{:?}", e);
                    };
                });

                // 清理临时key vec
                vec_keys.clear();
            }
        }
    }

    // 增量场景下执行record_descriptions 文件
    async fn exec_record_descriptions_file(
        &self,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        semaphore: Arc<Semaphore>,
        exec_set: &mut JoinSet<()>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        records_desc_file: File,
        executing_file: FileDescription,
    ) {
        let task_modify = self.gen_transfer_actions();
        let mut vec_keys: Vec<RecordOption> = vec![];

        // 按列表传输object from source to target
        let total_lines = executing_file.total_lines;
        let lines: io::Lines<io::BufReader<File>> = io::BufReader::new(records_desc_file).lines();

        for (idx, line) in lines.enumerate() {
            // 若错误达到上限，则停止任务
            if stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            if let Result::Ok(l) = line {
                let record = match json_to_struct::<RecordOption>(&l) {
                    Ok(r) => r,
                    Err(e) => {
                        log::error!("{:?}", e);
                        err_occur.store(true, std::sync::atomic::Ordering::SeqCst);
                        stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                        return;
                    }
                };
                vec_keys.push(record);
            };

            if vec_keys
                .len()
                .to_string()
                .eq(&self.attributes.objects_per_batch.to_string())
                || idx.to_string().eq(&(total_lines - 1).to_string()) && vec_keys.len() > 0
            {
                while exec_set.len() >= self.attributes.task_parallelism {
                    exec_set.join_next().await;
                }

                let vk = vec_keys.clone();
                let record_executer = task_modify.gen_transfer_executor(
                    stop_mark.clone(),
                    err_occur.clone(),
                    semaphore.clone(),
                    offset_map.clone(),
                    executing_file.path.to_string(),
                );

                exec_set.spawn(async move {
                    if let Err(e) = record_executer.transfer_record_options(vk).await {
                        log::error!("{:?}", e);
                    };
                });

                // 清理临时key vec
                vec_keys.clear();
            }
        }
    }
}
