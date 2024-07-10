use serde::{Deserialize, Serialize};

use crate::tasks::Task;

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct TaskId {
    pub task_id: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct TaskIds {
    pub task_ids: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ReqTaskUpdate {
    pub task_id: String,
    pub task: Task,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RespListTask {
    pub cf_id: String,
    pub task: Task,
}
