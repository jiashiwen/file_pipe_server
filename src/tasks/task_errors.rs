// Todo
// 定义任务错误，包括任务停止错误，注明停止原因，通过查看error_count 是否达到上限判断类型为正常结束finish 还是 错误超上线broken

use std::fmt::Display;

#[derive(Debug)]
pub struct TaskError {
    pub task_id: String,
    pub error: anyhow::Error,
}

impl std::error::Error for TaskError {}

impl Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

// impl TaskError{
//     pub fn from_error()
// }
