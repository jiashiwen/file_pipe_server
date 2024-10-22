use crate::resources::get_checkpoint;
use crate::resources::living_tasks;
use crate::tasks::FilePosition;
use anyhow::anyhow;
use anyhow::Result;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::sync::{atomic::AtomicBool, Arc};
use tokio::runtime;
use tokio::runtime::Runtime;
use tokio::task::yield_now;
use tokio::{sync::RwLock, task::JoinSet};

pub static GLOBAL_TASK_RUNTIME: Lazy<Arc<Runtime>> = Lazy::new(|| {
    let runtime = match init_task_runtime() {
        Ok(db) => db,
        Err(err) => panic!("{}", err),
    };
    Arc::new(runtime)
});

pub static GLOBAL_TASKS_SYS_JOINSET: Lazy<DashMap<String, Arc<RwLock<JoinSet<()>>>>> =
    Lazy::new(|| {
        let map: DashMap<String, Arc<RwLock<JoinSet<()>>>> = DashMap::new();
        map
    });

pub static GLOBAL_TASKS_EXEC_JOINSET: Lazy<DashMap<String, Arc<RwLock<JoinSet<()>>>>> =
    Lazy::new(|| {
        let map: DashMap<String, Arc<RwLock<JoinSet<()>>>> = DashMap::new();
        map
    });

// pub static GLOBAL_TASKS_BIGFILE_JOINSET: Lazy<DashMap<String, Arc<RwLock<JoinSet<()>>>>> =
//     Lazy::new(|| {
//         let map: DashMap<String, Arc<RwLock<JoinSet<()>>>> = DashMap::new();
//         map
//     });

pub static GLOBAL_TASK_STOP_MARK_MAP: Lazy<Arc<DashMap<String, Arc<AtomicBool>>>> =
    Lazy::new(|| {
        let map = DashMap::<String, Arc<AtomicBool>>::new();
        Arc::new(map)
    });

// pub static GLOBAL_LIVING_COMPARE_TASK_MAP: Lazy<Arc<DashMap<String, TransferTaskStatus>>> =
//     Lazy::new(|| {
//         let map = DashMap::<String, TransferTaskStatus>::new();
//         Arc::new(map)
//     });

pub static GLOBAL_LIST_FILE_POSITON_MAP: Lazy<Arc<DashMap<String, FilePosition>>> =
    Lazy::new(|| {
        let map = DashMap::<String, FilePosition>::new();
        Arc::new(map)
    });

/// task id 与 task offset 映射
pub static GLOBAL_TASK_LIST_FILE_POSITON_MAP: Lazy<
    Arc<DashMap<String, Arc<DashMap<String, FilePosition>>>>,
> = Lazy::new(|| {
    let task_offset = DashMap::<String, Arc<DashMap<String, FilePosition>>>::new();
    Arc::new(task_offset)
});

fn init_task_runtime() -> Result<Runtime> {
    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .enable_all()
        // .max_io_events_per_tick(num_cpus::get() * 3)
        .build()?;
    Ok(rt)
}

pub struct TasksStatusSaver {
    pub interval: u64,
}

impl TasksStatusSaver {
    pub async fn run(&self) {
        loop {
            if let Err(e) = snapshot_living_tasks_checkpoints_to_cf().await {
                log::error!("{}", e);
            };
            tokio::time::sleep(tokio::time::Duration::from_secs(self.interval)).await;
        }
    }
}

pub async fn init_tasks_status_server() {
    let server = TasksStatusSaver { interval: 10 };
    server.run().await
}

pub fn get_exec_joinset(task_id: &str) -> Result<Arc<RwLock<JoinSet<()>>>> {
    let kv = match GLOBAL_TASKS_EXEC_JOINSET.get(task_id) {
        Some(s) => s,
        None => return Err(anyhow!("execute joinset not exist")),
    };
    let exec_set = kv.value().clone();
    Ok(exec_set)
}

pub fn remove_exec_joinset(task_id: &str) {
    GLOBAL_TASKS_EXEC_JOINSET.remove(task_id);
}

pub async fn snapshot_living_tasks_checkpoints_to_cf() -> Result<()> {
    for status in living_tasks()? {
        // 获取最小offset的FilePosition
        let taskid = status.task_id;
        let mut checkpoint = match get_checkpoint(&taskid) {
            Ok(c) => c,
            Err(e) => {
                log::error!("{:?}", e);
                continue;
            }
        };

        let mut file_position = checkpoint.executed_file_position.clone();

        let offset_map = match GLOBAL_TASK_LIST_FILE_POSITON_MAP.get(&taskid) {
            Some(m) => m,
            None => {
                continue;
            }
        };

        for (idx, iterm) in offset_map.iter().enumerate() {
            if idx.eq(&0) {
                file_position = iterm.value().clone();
            } else {
                if file_position.offset > iterm.value().offset {
                    file_position = iterm.value().clone();
                }
            }
        }

        offset_map.shrink_to_fit();
        checkpoint.executed_file_position = file_position.clone();

        if let Err(e) = checkpoint.save_to_rocksdb_cf() {
            log::error!("{},{}", e, taskid);
        } else {
            log::debug!("checkpoint:\n{:?}", checkpoint);
        };
    }
    Ok(())
}
