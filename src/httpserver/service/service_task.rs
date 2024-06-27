use crate::{
    commons::{json_to_struct, struct_to_json_string},
    configure::get_config,
    httpserver::module::RespListTask,
    resources::{CF_TASK, GLOBAL_ROCKSDB},
    tasks::{gen_file_path, Task},
};
use anyhow::anyhow;
use anyhow::Result;
use dashmap::DashMap;
use rocksdb::IteratorMode;

pub fn service_task_create(task: &mut Task) -> Result<i64> {
    // let id = task_id_generator();
    // let global_meta_dir = get_config()?.meta_dir;
    // let meta_dir = gen_file_path(&global_meta_dir, id.to_string().as_str(), "");
    // print!("meta_dir:{:?}", meta_dir);
    // task.set_task_id(id.to_string().as_str());
    // task.set_meta_dir(&meta_dir);

    // let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK) {
    //     Some(cf) => cf,
    //     None => return Err(anyhow!("column family not exist")),
    // };
    // // let encoded: Vec<u8> = bincode::serialize(task)?;
    // let task_json = struct_to_json_string(task)?;
    // // let encoded: Vec<u8> = bincode::options().with_varint_encoding().serialize(task)?;
    // GLOBAL_ROCKSDB.put_cf(&cf, id.to_string().as_bytes(), task_json.as_bytes())?;
    task.create()
    // Ok(id)
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

pub fn service_update_task(task_id: &str, task: &mut Task) -> Result<()> {
    let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK) {
        Some(cf) => cf,
        None => return Err(anyhow!("column family not exist")),
    };
    let global_meta_dir = get_config()?.meta_dir;
    let meta_dir = gen_file_path(&global_meta_dir, task_id, "");
    task.set_task_id(task_id);
    task.set_meta_dir(&meta_dir);
    let task_json = struct_to_json_string(task)?;
    GLOBAL_ROCKSDB.put_cf(&cf, task_id.to_string().as_bytes(), task_json.as_bytes())?;
    Ok(())
}

pub async fn service_analyze_task(task_id: &str) -> Result<DashMap<String, i128>> {
    // let hash = HashMap::<String, i128>::new();
    let task = service_show_task(task_id)?;
    match task {
        Task::Transfer(t) => {
            let r = t.gen_transfer_actions().analyze_source().await?;
            Ok(r)
        }
        _ => Err(anyhow!("task not transfer task")),
    }
    // Ok(hash)
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

pub fn service_list_all_tasks() -> Result<Vec<RespListTask>> {
    let cf = match GLOBAL_ROCKSDB.cf_handle(CF_TASK) {
        Some(cf) => cf,
        None => return Err(anyhow!("column family not exist")),
    };
    let cf_task_iter = GLOBAL_ROCKSDB.iterator_cf(&cf, IteratorMode::Start);
    let mut vec_task = vec![];
    for item in cf_task_iter {
        if let Ok(kv) = item {
            let cf_id = String::from_utf8(kv.0.to_vec())?;
            let task_json_str = String::from_utf8(kv.1.to_vec())?;
            let task = json_to_struct::<Task>(task_json_str.as_str())?;
            let resp = RespListTask { cf_id, task };
            vec_task.push(resp);
        }
    }
    Ok(vec_task)
}
