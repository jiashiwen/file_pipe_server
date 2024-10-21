use crate::commons::json_to_struct;
use crate::commons::struct_to_json_string;
use crate::configure::get_config;
use crate::tasks::CheckPoint;
use crate::tasks::CompareStatus;
use crate::tasks::Status;
use crate::tasks::Task;
use crate::tasks::TaskStatus;
use crate::tasks::TaskStopReason;
use crate::tasks::TransferStatus;
use anyhow::anyhow;
use anyhow::Result;
use once_cell::sync::Lazy;
use rocksdb::IteratorMode;
use rocksdb::{DBWithThreadMode, MultiThreaded, Options};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub const CF_TASK_CHECKPOINTS: &'static str = "cf_task_checkpoints";
pub const CF_TASK: &'static str = "cf_task";
pub const CF_TASK_STATUS: &'static str = "cf_task_status";

pub static GLOBAL_ROCKSDB: Lazy<Arc<DBWithThreadMode<MultiThreaded>>> = Lazy::new(|| {
    let config = get_config().unwrap();
    // let rocksdb = match init_rocksdb("oss_pipe_rocksdb") {
    let rocksdb = match init_rocksdb(&config.rocksdb.path) {
        Ok(db) => db,
        Err(err) => panic!("{}", err),
    };
    Arc::new(rocksdb)
});

pub fn init_rocksdb(db_path: &str) -> Result<DBWithThreadMode<MultiThreaded>> {
    let mut cf_opts = Options::default();
    cf_opts.set_allow_concurrent_memtable_write(true);
    cf_opts.set_max_write_buffer_number(16);
    cf_opts.set_write_buffer_size(128 * 1024 * 1024);
    cf_opts.set_disable_auto_compactions(true);

    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);

    let db = DBWithThreadMode::<MultiThreaded>::open_cf_with_opts(
        &db_opts,
        db_path,
        vec![
            (CF_TASK_CHECKPOINTS, cf_opts.clone()),
            (CF_TASK, cf_opts.clone()),
            (CF_TASK_STATUS, cf_opts.clone()),
        ],
    )?;
    Ok(db)
}

// Todo
// 增加server初始化时将任务状态为running的变更为broken
pub fn change_taskstatus_to_stop() -> Result<()> {
    let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK_STATUS) {
        Some(cf) => cf,
        None => return Err(anyhow!("column family not exist")),
    };

    for item in GLOBAL_ROCKSDB.iterator_cf(&cf, IteratorMode::Start) {
        if let Ok(kv) = item {
            let mut task_status: TaskStatus = bincode::deserialize(&kv.1)?;
            if !task_status.is_stopped() {
                task_status.status = match task_status.status {
                    crate::tasks::Status::Transfer(_) => {
                        Status::Transfer(TransferStatus::Stopped(TaskStopReason::Broken))
                    }
                    crate::tasks::Status::Compare(_) => Status::Compare(CompareStatus::Stopped),
                }
            }
            let _ = save_task_status_to_cf(&mut task_status)?;
        }
    }
    Ok(())
}

pub fn save_checkpoint_to_cf(checkpoint: &mut CheckPoint) -> Result<()> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    checkpoint.modify_checkpoint_timestamp = i128::from(now.as_secs());

    let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK_CHECKPOINTS) {
        Some(cf) => cf,
        None => return Err(anyhow!("column family not exist")),
    };
    let encoded: Vec<u8> = bincode::serialize(checkpoint)?;
    GLOBAL_ROCKSDB.put_cf(&cf, checkpoint.task_id.as_bytes(), encoded)?;
    Ok(())
}

pub fn get_checkpoint(task_id: &str) -> Result<CheckPoint> {
    let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK_CHECKPOINTS) {
        Some(cf) => cf,
        None => return Err(anyhow!("column family not exist")),
    };
    let chekpoint_bytes = match GLOBAL_ROCKSDB.get_cf(&cf, task_id)? {
        Some(b) => b,
        None => return Err(anyhow!("checkpoint not exist")),
    };
    let checkpoint: CheckPoint = bincode::deserialize(&chekpoint_bytes)?;
    Ok(checkpoint)
}

pub fn remove_checkpoint_from_cf(task_id: &str) -> Result<()> {
    let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK_CHECKPOINTS) {
        Some(cf) => cf,
        None => return Err(anyhow!("column family not exist")),
    };
    GLOBAL_ROCKSDB.delete_cf(&cf, task_id.as_bytes())?;
    Ok(())
}

pub fn save_task_to_cf(task: Task) -> Result<()> {
    let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK) {
        Some(cf) => cf,
        None => return Err(anyhow!("column family not exist")),
    };
    let task_json = struct_to_json_string(&task)?;
    GLOBAL_ROCKSDB.put_cf(&cf, task.task_id().as_bytes(), task_json.as_bytes())?;
    Ok(())
}

pub fn get_task(task_id: &str) -> Result<Option<Task>> {
    let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK) {
        Some(cf) => cf,
        None => return Err(anyhow!("column family not exist")),
    };

    let value = GLOBAL_ROCKSDB.get_cf(&cf, task_id)?;
    return match value {
        Some(v) => {
            let task_json_str = String::from_utf8(v)?;
            let task = json_to_struct::<Task>(task_json_str.as_str())?;
            Ok(Some(task))
        }
        None => Ok(None),
    };
}

pub fn remove_task_from_cf(task_id: &str) -> Result<()> {
    let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK) {
        Some(cf) => cf,
        None => return Err(anyhow!("column family not exist")),
    };
    GLOBAL_ROCKSDB.delete_cf(&cf, task_id.as_bytes())?;
    Ok(())
}

pub fn get_task_status(task_id: &str) -> Result<TaskStatus> {
    let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK_STATUS) {
        Some(cf) => cf,
        None => return Err(anyhow!("column family not exist")),
    };
    let status_bytes = match GLOBAL_ROCKSDB.get_cf(&cf, task_id)? {
        Some(b) => b,
        None => return Err(anyhow!("task {} status not exist", task_id)),
    };
    let status = bincode::deserialize(&status_bytes)?;

    Ok(status)
}

pub fn save_task_status_to_cf(status: &mut TaskStatus) -> Result<()> {
    if status.is_starting() {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        status.start_time = now.as_secs();
    }

    let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK_STATUS) {
        Some(cf) => cf,
        None => return Err(anyhow!("column family not exist")),
    };
    let encoded: Vec<u8> = bincode::serialize(status)?;
    GLOBAL_ROCKSDB.put_cf(&cf, status.task_id.as_bytes(), encoded)?;
    Ok(())
}

pub fn remove_task_status_from_cf(task_id: &str) -> Result<()> {
    let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK_STATUS) {
        Some(cf) => cf,
        None => return Err(anyhow!("column family not exist")),
    };
    GLOBAL_ROCKSDB.delete_cf(&cf, task_id.as_bytes())?;
    Ok(())
}

pub fn living_tasks() -> Result<Vec<TaskStatus>> {
    let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK_STATUS) {
        Some(cf) => cf,
        None => return Err(anyhow!("column family not exist")),
    };
    let mut vec_task_status = vec![];
    for item in GLOBAL_ROCKSDB.iterator_cf(&cf, IteratorMode::Start) {
        if let Ok(kv) = item {
            let status: TaskStatus = bincode::deserialize(&kv.1)?;
            if !status.is_stopped() {
                vec_task_status.push(status);
            }
        }
    }
    Ok(vec_task_status)
}

pub fn task_is_living(task_id: &str) -> bool {
    return match get_task_status(task_id) {
        Ok(s) => match s.is_stopped() {
            true => false,
            false => true,
        },
        Err(_) => false,
    };
}
