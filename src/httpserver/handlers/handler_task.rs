use super::HandlerResult;
use crate::httpserver::module::ReqStartTask;
use crate::httpserver::service::service_task::{
    service_clean_task, service_remove_task, service_task_checkpoint, service_task_status,
};
use crate::resources::living_tasks;
use crate::tasks::{CheckPoint, TaskStatus};
use crate::{
    httpserver::{
        exception::{AppError, AppErrorType},
        module::{ReqTaskUpdate, RespListTask, Response, TaskId},
        service::service_task::{
            service_analyze_task, service_list_all_tasks, service_show_task, service_start_task,
            service_stop_task, service_task_create, service_update_task,
        },
    },
    tasks::Task,
};
use axum::Json;
use std::collections::BTreeMap;

pub async fn task_create(Json(mut task): Json<Task>) -> HandlerResult<TaskId> {
    match service_task_create(&mut task) {
        Ok(id) => Ok(Json(Response::ok(TaskId {
            task_id: id.to_string(),
        }))),
        Err(e) => {
            let err = AppError {
                message: Some(e.to_string()),
                cause: None,
                error_type: AppErrorType::UnknowErr,
            };
            return Err(err);
        }
    }
}

pub async fn task_update(Json(mut update): Json<ReqTaskUpdate>) -> HandlerResult<()> {
    match service_update_task(&update.task_id, &mut update.task) {
        // Ok(_) => Ok(Json(Response::ok(json!({"update":"ok"})))),
        Ok(_) => Ok(Json(Response::ok(()))),
        Err(e) => {
            let err = AppError {
                message: Some(e.to_string()),
                cause: None,
                error_type: AppErrorType::UnknowErr,
            };
            log::error!("{:?}", e);
            return Err(err);
        }
    }
}

pub async fn task_remove(Json(id): Json<TaskId>) -> HandlerResult<()> {
    match service_remove_task(&id.task_id) {
        Ok(_) => Ok(Json(Response::ok(()))),
        Err(e) => {
            let err = AppError {
                message: Some(e.to_string()),
                cause: None,
                error_type: AppErrorType::UnknowErr,
            };
            return Err(err);
        }
    }
}

pub async fn task_clean(Json(id): Json<TaskId>) -> HandlerResult<()> {
    match service_clean_task(&id.task_id) {
        Ok(_) => Ok(Json(Response::ok(()))),
        Err(e) => {
            let err = AppError {
                message: Some(e.to_string()),
                cause: None,
                error_type: AppErrorType::UnknowErr,
            };
            return Err(err);
        }
    }
}

pub async fn task_analyze(Json(id): Json<TaskId>) -> HandlerResult<BTreeMap<String, i128>> {
    match service_analyze_task(&id.task_id).await {
        Ok(map) => Ok(Json(Response::ok(map))),
        Err(e) => {
            let err = AppError {
                message: Some(e.to_string()),
                cause: None,
                error_type: AppErrorType::UnknowErr,
            };
            return Err(err);
        }
    }
}

pub async fn task_start(Json(start): Json<ReqStartTask>) -> HandlerResult<()> {
    match service_start_task(&start) {
        Ok(_) => Ok(Json(Response::ok(()))),
        Err(e) => {
            let err = AppError {
                message: Some(e.to_string()),
                cause: None,
                error_type: AppErrorType::UnknowErr,
            };
            return Err(err);
        }
    }
}

pub async fn task_stop(Json(id): Json<TaskId>) -> HandlerResult<()> {
    match service_stop_task(id.task_id.as_str()) {
        // Ok(_) => Ok(Json(Response::ok(json!({"stop":&id.task_id})))),
        Ok(_) => Ok(Json(Response::ok(()))),
        Err(e) => {
            let err = AppError {
                message: Some(e.to_string()),
                cause: None,
                error_type: AppErrorType::UnknowErr,
            };
            return Err(err);
        }
    }
}

pub async fn task_checkpoint(Json(id): Json<TaskId>) -> HandlerResult<CheckPoint> {
    match service_task_checkpoint(&id.task_id) {
        Ok(c) => Ok(Json(Response::ok(c))),
        Err(e) => {
            let err = AppError {
                message: Some(e.to_string()),
                cause: None,
                error_type: AppErrorType::UnknowErr,
            };
            return Err(err);
        }
    }
}

pub async fn task_show(Json(id): Json<TaskId>) -> HandlerResult<Task> {
    match service_show_task(&id.task_id) {
        Ok(task) => Ok(Json(Response::ok(task))),
        Err(e) => {
            let err = AppError {
                message: Some(e.to_string()),
                cause: None,
                error_type: AppErrorType::UnknowErr,
            };
            return Err(err);
        }
    }
}
pub async fn task_status(Json(id): Json<TaskId>) -> HandlerResult<TaskStatus> {
    match service_task_status(&id.task_id) {
        Ok(status) => Ok(Json(Response::ok(status))),
        Err(e) => {
            let err = AppError {
                message: Some(e.to_string()),
                cause: None,
                error_type: AppErrorType::UnknowErr,
            };
            return Err(err);
        }
    }
}

pub async fn task_all() -> HandlerResult<Vec<RespListTask>> {
    match service_list_all_tasks() {
        Ok(task_vec) => Ok(Json(Response::ok(task_vec))),
        Err(e) => {
            let err = AppError {
                message: Some(e.to_string()),
                cause: None,
                error_type: AppErrorType::UnknowErr,
            };
            return Err(err);
        }
    }
}

pub async fn task_all_living() -> HandlerResult<Vec<TaskStatus>> {
    match living_tasks() {
        Ok(v) => Ok(Json(Response::ok(v))),
        Err(e) => {
            let err = AppError {
                message: Some(e.to_string()),
                cause: None,
                error_type: AppErrorType::UnknowErr,
            };
            return Err(err);
        }
    }
}
