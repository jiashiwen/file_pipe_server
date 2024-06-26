use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct ReqTaskId {
    pub task_id: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct ReqTaskIds {
    pub task_ids: Vec<String>,
}
