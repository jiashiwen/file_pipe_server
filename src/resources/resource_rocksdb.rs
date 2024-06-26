use crate::commons::json_to_struct;
use crate::tasks::CheckPoint;
use crate::tasks::Task;
use anyhow::anyhow;
use anyhow::Result;
use once_cell::sync::Lazy;
use rocksdb::{DBWithThreadMode, MultiThreaded, Options};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub const CF_TASK_CHECKPOINTS: &'static str = "cf_task_checkpoints";
pub const CF_TASK: &'static str = "cf_task";

pub static GLOBAL_ROCKSDB: Lazy<Arc<DBWithThreadMode<MultiThreaded>>> = Lazy::new(|| {
    let rocksdb = match init_rocksdb("oss_pipe_rocksdb") {
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
        vec![(CF_TASK_CHECKPOINTS, cf_opts.clone()), (CF_TASK, cf_opts)],
    )?;
    Ok(db)
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

pub fn get_task(task_id: &str) -> Result<Task> {
    let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK) {
        Some(cf) => cf,
        None => return Err(anyhow!("column family not exist")),
    };

    let value = GLOBAL_ROCKSDB.get_cf(&cf, task_id)?;
    return match value {
        Some(v) => {
            let task_json_str = String::from_utf8(v)?;
            let task = json_to_struct::<Task>(task_json_str.as_str())?;
            Ok(task)
        }
        None => Err(anyhow!("task {} not exist", task_id)),
    };
}
