use super::task::{TaskStopReason, TaskType, TransferStage};
use serde::{Deserialize, Serialize};

// #[derive(Debug, Serialize, Deserialize, Clone)]
// pub enum Status {
//     Transfer(TransferStatus),
//     Compare(CompareStatus),
// }

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskStatus {
    pub task_id: String,
    pub start_time: u64,
    pub status: Status,
}

impl TaskStatus {
    // pub fn status_type(&self) -> TaskType {
    //     match self.status {
    //         Status::Transfer(_) => TaskType::Transfer,
    //         Status::Compare(_) => TaskType::Compare,
    //     }
    // }

    pub fn is_starting(&self) -> bool {
        // match &self.status {
        //     Status::Transfer(t) => match t {
        //         Status::Starting => true,
        //         _ => false,
        //     },
        //     Status::Compare(c) => match c {
        //         CompareStatus::Starting => true,
        //         _ => false,
        //     },
        // }
        match self.status {
            Status::Starting => true,
            _ => false,
        }
    }

    pub fn is_running(&self) -> bool {
        // match &self.status {
        //     Status::Transfer(t) => match t {
        //         Status::Running(_) => true,
        //         _ => false,
        //     },
        //     Status::Compare(c) => match c {
        //         CompareStatus::Running => true,
        //         _ => false,
        //     },
        // }

        match &self.status {
            Status::Running(_) => true,
            _ => false,
        }
    }

    pub fn is_running_stock(&self) -> bool {
        // return match &self.status {
        //     Status::Transfer(t) => match t {
        //         Status::Running(r) => match r {
        //             TransferStage::Stock => true,
        //             _ => false,
        //         },
        //         _ => false,
        //     },
        //     Status::Compare(_) => todo!(),
        // };
        match &self.status {
            Status::Running(stage) => match stage {
                TransferStage::Stock => true,
                TransferStage::Increment => false,
            },
            _ => false,
        }
    }

    pub fn is_running_increment(&self) -> bool {
        match &self.status {
            Status::Running(stage) => match stage {
                TransferStage::Stock => false,
                TransferStage::Increment => true,
            },
            _ => false,
        }
    }

    pub fn is_stopped(&self) -> bool {
        match &self.status {
            Status::Stopped(_) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Status {
    Starting,
    Running(TransferStage),
    Stopped(TaskStopReason),
}

impl Status {
    pub fn is_running_stock(&self) -> bool {
        match self {
            Status::Running(r) => match r {
                TransferStage::Stock => true,
                TransferStage::Increment => false,
            },
            _ => false,
        }
    }

    pub fn is_running_increment(&self) -> bool {
        match self {
            Status::Running(r) => match r {
                TransferStage::Stock => false,
                TransferStage::Increment => true,
            },
            _ => false,
        }
    }
}

// #[derive(Debug, Serialize, Deserialize, Clone)]
// pub enum CompareStatus {
//     Starting,
//     Running,
//     Stopped,
// }
