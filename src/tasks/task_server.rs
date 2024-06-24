use anyhow::Result;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::sync::{atomic::AtomicBool, Arc};
use tokio::{
    runtime::{self, Runtime},
    sync::RwLock,
    task::{yield_now, JoinSet},
};

use crate::{modules::FilePosition, resources::get_checkpoint};

pub static GLOBAL_TASK_RUNTIME: Lazy<Arc<Runtime>> = Lazy::new(|| {
    let rocksdb = match init_task_runtime() {
        Ok(db) => db,
        Err(err) => panic!("{}", err),
    };
    Arc::new(rocksdb)
});

pub static GLOBAL_TASK_JOINSET: Lazy<Arc<RwLock<JoinSet<()>>>> = Lazy::new(|| {
    let joinset = init_global_joinset();
    let joinset_rw = RwLock::new(joinset);
    Arc::new(joinset_rw)
});

pub static GLOBAL_STOP_MARK: Lazy<Arc<AtomicBool>> = Lazy::new(|| {
    let mark = AtomicBool::new(false);
    Arc::new(mark)
});

pub static GLOBAL_LIVING_TASK_MAP: Lazy<Arc<DashMap<String, i128>>> = Lazy::new(|| {
    let map = DashMap::<String, i128>::new();
    Arc::new(map)
});

pub static GLOBAL_LIST_FILE_POSITON_MAP: Lazy<Arc<DashMap<String, FilePosition>>> =
    Lazy::new(|| {
        let map = DashMap::<String, FilePosition>::new();
        Arc::new(map)
    });

fn init_task_runtime() -> Result<Runtime> {
    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .enable_all()
        .max_io_events_per_tick(32)
        .build()?;
    Ok(rt)
}

fn init_global_joinset() -> JoinSet<()> {
    let set: JoinSet<()> = JoinSet::new();
    set
}

pub struct TasksStatusSaver {
    pub interval: u64,
}

impl TasksStatusSaver {
    pub async fn snapshot_to_cf(&self) {
        log::info!("Task status saver run!");
        while !GLOBAL_STOP_MARK.load(std::sync::atomic::Ordering::Relaxed) {
            println!("task status server runing");
            // for kv in self.living_tasks_map.iter() {
            for kv in GLOBAL_LIVING_TASK_MAP.iter() {
                // 获取最小offset的FilePosition
                let taskid = kv.key();
                let mut checkpoint = match get_checkpoint(taskid) {
                    Ok(c) => c,
                    Err(e) => {
                        log::error!("{:?}", e);
                        continue;
                    }
                };
                let mut file_position = FilePosition {
                    offset: 0,
                    line_num: 0,
                };

                GLOBAL_LIST_FILE_POSITON_MAP
                    .iter()
                    .filter(|item| item.key().starts_with(taskid))
                    .map(|m| {
                        file_position = m.clone();
                        m.offset
                    })
                    .min();

                GLOBAL_LIST_FILE_POSITON_MAP.shrink_to_fit();
                checkpoint.executed_file_position = file_position.clone();

                if let Err(e) = checkpoint.save_to_rocksdb_cf() {
                    log::error!("{},{}", e, taskid);
                } else {
                    log::debug!("checkpoint:\n{:?}", checkpoint);
                };
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(self.interval)).await;
            yield_now().await;
        }
    }
}

pub async fn init_tasks_status_server() {
    let server = TasksStatusSaver { interval: 10 };
    server.snapshot_to_cf().await;
}
