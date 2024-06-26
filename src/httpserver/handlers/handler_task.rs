use super::HandlerResult;
use crate::{
    httpserver::module::Response,
    tasks::{task_id_generator, TestGlobalJoinsetTask, GLOBAL_TASK_RUNTIME},
};
use axum::Json;
use serde_json::{json, Value};

pub async fn task_create() -> HandlerResult<Value> {
    Ok(Json(Response::ok(json!({"create":"ok"}))))
}

pub async fn task_update() -> HandlerResult<Value> {
    Ok(Json(Response::ok(json!({"update":"ok"}))))
}

pub async fn task_remove() -> HandlerResult<Value> {
    Ok(Json(Response::ok(json!({"remove":"ok"}))))
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

pub async fn task_all() -> HandlerResult<Value> {
    Ok(Json(Response::ok(json!({"all":"ok"}))))
}

pub async fn task_all_living() -> HandlerResult<Value> {
    Ok(Json(Response::ok(json!({"all_living":"ok"}))))
}
