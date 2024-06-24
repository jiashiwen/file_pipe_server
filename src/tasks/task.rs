use std::time::Duration;

use serde::{Deserialize, Serialize};
use snowflake::SnowflakeIdGenerator;
use tokio::time::sleep;

use super::{task_transfer::TransferType, GLOBAL_TASK_JOINSET};

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum LastModifyFilterType {
    Greater,
    Less,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct LastModifyFilter {
    pub filter_type: LastModifyFilterType,
    pub timestamp: i128,
}

impl LastModifyFilter {
    pub fn filter(&self, timestamp: i128) -> bool {
        match self.filter_type {
            LastModifyFilterType::Greater => timestamp.ge(&self.timestamp),
            LastModifyFilterType::Less => timestamp.le(&self.timestamp),
        }
    }
}

/// 任务阶段，包括存量曾量全量
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum TransferStage {
    Stock,
    Increment,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct Tasks {
    pub task_id: String,
    pub loop_size: usize,
}

pub struct TaskDefaultParameters {}

impl TaskDefaultParameters {
    pub fn id_default() -> String {
        task_id_generator().to_string()
    }

    pub fn name_default() -> String {
        "default_name".to_string()
    }

    pub fn objects_per_batch_default() -> i32 {
        100
    }

    pub fn task_parallelism_default() -> usize {
        num_cpus::get()
    }

    pub fn max_errors_default() -> usize {
        1
    }

    pub fn start_from_checkpoint_default() -> bool {
        false
    }

    pub fn exprirs_diff_scope_default() -> i64 {
        10
    }

    pub fn target_exists_skip_default() -> bool {
        false
    }
    pub fn large_file_size_default() -> usize {
        // 50M
        10485760 * 5
    }
    pub fn multi_part_chunk_size_default() -> usize {
        // 10M
        10485760
    }

    pub fn multi_part_chunks_per_batch_default() -> usize {
        10
    }
    pub fn multi_part_parallelism_default() -> usize {
        num_cpus::get() * 2
    }

    pub fn meta_dir_default() -> String {
        "/tmp/meta_dir".to_string()
    }

    pub fn filter_default() -> Option<Vec<String>> {
        None
    }
    pub fn continuous_default() -> bool {
        false
    }

    pub fn transfer_type_default() -> TransferType {
        TransferType::Stock
    }

    pub fn last_modify_filter_default() -> Option<LastModifyFilter> {
        None
    }
}

impl Tasks {
    pub async fn run(&self, loop_size: usize) {
        while GLOBAL_TASK_JOINSET.read().await.len() > 2 {
            let mut global_set = GLOBAL_TASK_JOINSET.write().await;
            global_set.join_next().await;
        }
        for i in 0..loop_size {
            println!("{}", i);
            log::info!("id:{},{}", self.task_id, i);
            sleep(Duration::from_secs(1)).await;
        }
    }
}

pub fn task_id_generator() -> i64 {
    let mut id_generator_generator = SnowflakeIdGenerator::new(1, 1);
    let id = id_generator_generator.real_time_generate();
    id
}
