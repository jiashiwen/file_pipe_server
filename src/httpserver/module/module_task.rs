use crate::tasks::Task;
use crate::tasks::TaskDefaultParameters;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct TaskId {
    pub task_id: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct TaskIds {
    pub task_ids: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ReqStartTask {
    pub task_id: String,
    #[serde(default = "TaskDefaultParameters::start_from_checkpoint_default")]
    pub from_checkpoint: bool,
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
