use std::collections::{BTreeMap, HashMap};

use super::HandlerResult;
use crate::tasks::TransferTaskStatus;
use crate::{
    httpserver::{
        exception::{AppError, AppErrorType},
        module::{ReqTaskId, ReqTaskIds, ReqTaskUpdate, RespListTask, Response},
        service::service_task::{
            service_analyze_task, service_list_all_tasks, service_remove_task, service_show_task,
            service_start_task, service_stop_task, service_task_create, service_update_task,
        },
    },
    tasks::{Task, GLOBAL_LIVING_TRANSFER_TASK_MAP},
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

pub async fn task_update(Json(mut update): Json<ReqTaskUpdate>) -> HandlerResult<Value> {
    match service_update_task(&update.task_id, &mut update.task) {
        Ok(_) => Ok(Json(Response::ok(json!({"update":"ok"})))),
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

pub async fn task_analyze(Json(id): Json<ReqTaskId>) -> HandlerResult<BTreeMap<String, i128>> {
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

pub async fn task_start(Json(id): Json<ReqTaskId>) -> HandlerResult<Value> {
    match service_start_task(id.task_id.as_str()) {
        Ok(_) => Ok(Json(Response::ok(json!({"start":&id.task_id})))),
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

// pub async fn task_start(Json(mut payload): Json<TestGlobalJoinsetTask>) -> HandlerResult<Value> {
//     payload.task_id = task_id_generator().to_string();
//     GLOBAL_TASK_RUNTIME.spawn(async move {
//         payload.run(10).await;
//     });

//     Ok(Json(Response::ok(json!({"start":"ok"}))))
// }

pub async fn task_stop(Json(id): Json<ReqTaskId>) -> HandlerResult<Value> {
    match service_stop_task(id.task_id.as_str()) {
        Ok(_) => Ok(Json(Response::ok(json!({"stop":&id.task_id})))),
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

pub async fn task_all_living() -> HandlerResult<HashMap<String, TransferTaskStatus>> {
    let mut map = HashMap::new();
    for item in GLOBAL_LIVING_TRANSFER_TASK_MAP.iter() {
        map.insert(item.key().to_string(), item.value().clone());
    }
    Ok(Json(Response::ok(map)))
}
