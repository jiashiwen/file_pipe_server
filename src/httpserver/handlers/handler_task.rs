use super::HandlerResult;
use crate::{
    httpserver::{
        exception::{AppError, AppErrorType},
        module::{ReqTaskId, ReqTaskIds, Response},
        service::service_task::{
            service_list_all_tasks, service_remove_task, service_show_task, service_task_create,
        },
    },
    tasks::{task_id_generator, Task, TestGlobalJoinsetTask, GLOBAL_TASK_RUNTIME},
};
use axum::Json;
use serde_json::{json, Value};

pub async fn task_create(Json(mut task): Json<Task>) -> HandlerResult<Value> {
    match service_task_create(&mut task) {
        Ok(id) => Ok(Json(Response::ok(json!({"task_id":id.to_string()})))),
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

pub async fn task_update() -> HandlerResult<Value> {
    Ok(Json(Response::ok(json!({"update":"ok"}))))
}

pub async fn task_remove(Json(ids): Json<ReqTaskIds>) -> HandlerResult<()> {
    match service_remove_task(ids.task_ids) {
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

pub async fn task_start(Json(mut payload): Json<TestGlobalJoinsetTask>) -> HandlerResult<Value> {
    payload.task_id = task_id_generator().to_string();
    GLOBAL_TASK_RUNTIME.spawn(async move {
        payload.run(10).await;
    });

    Ok(Json(Response::ok(json!({"start":"ok"}))))
}

pub async fn task_stop() -> HandlerResult<Value> {
    Ok(Json(Response::ok(json!({"stop":"ok"}))))
}

pub async fn task_status() -> HandlerResult<Value> {
    Ok(Json(Response::ok(json!({"status":"ok"}))))
}

pub async fn task_show(Json(id): Json<ReqTaskId>) -> HandlerResult<Task> {
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
    // Ok(Json(Response::ok(json!({"update":"ok"}))))
}
pub async fn task_all() -> HandlerResult<Vec<Task>> {
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

pub async fn task_all_living() -> HandlerResult<Value> {
    Ok(Json(Response::ok(json!({"all_living":"ok"}))))
}
