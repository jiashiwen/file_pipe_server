use crate::{
    commons::{json_to_struct, struct_to_json_string},
    resources::{CF_TASK, GLOBAL_ROCKSDB},
    tasks::Task,
};
use anyhow::anyhow;
use anyhow::Result;
use rocksdb::IteratorMode;

pub fn service_task_create(task: &mut Task) -> Result<i64> {
    let id = task.update_task_id_rand();
    let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK) {
        Some(cf) => cf,
        None => return Err(anyhow!("column family not exist")),
    };
    // let encoded: Vec<u8> = bincode::serialize(task)?;
    let task_json = struct_to_json_string(task)?;
    // let encoded: Vec<u8> = bincode::options().with_varint_encoding().serialize(task)?;
    GLOBAL_ROCKSDB.put_cf(&cf, id.to_string().as_bytes(), task_json.as_bytes())?;
    Ok(id)
}

pub fn service_remove_task(task_ids: Vec<String>) -> Result<()> {
    let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK) {
        Some(cf) => cf,
        None => return Err(anyhow!("column family not exist")),
    };

    for id in task_ids {
        GLOBAL_ROCKSDB.delete_cf(&cf, id)?;
    }

    Ok(())
}

pub fn service_show_task(task_id: &str) -> Result<Task> {
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

pub fn service_list_all_tasks() -> Result<Vec<Task>> {
    let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK) {
        Some(cf) => cf,
        None => return Err(anyhow!("column family not exist")),
    };
    let cf_task_iter = GLOBAL_ROCKSDB.iterator_cf(&cf, IteratorMode::Start);
    let mut vec_task = vec![];
    for item in cf_task_iter {
        if let Ok(kv) = item {
            let task_json_str = String::from_utf8(kv.1.to_vec())?;
            let task = json_to_struct::<Task>(task_json_str.as_str())?;
            vec_task.push(task);
        }
    }
    Ok(vec_task)
}
