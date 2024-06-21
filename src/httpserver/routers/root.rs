use crate::httpserver::handlers::{current_config, rbatis_t_insert, redis_put, root};

use axum::body::Body;
use axum::error_handling::HandleErrorLayer;
use axum::http::StatusCode;
use axum::routing::{get, get_service, post, MethodRouter};
use axum::{BoxError, Router};
// use hyper::Body;

use std::time::Duration;
use tower::ServiceBuilder;
use tower_http::services::ServeDir;
use tower_http::{compression::CompressionLayer, trace::TraceLayer};

pub fn router_root() -> Router {
    let tracer = TraceLayer::new_for_http();

    let middleware_stack = ServiceBuilder::new()
        .layer(tracer)
        .layer(CompressionLayer::new())
        .layer(HandleErrorLayer::new(handle_timeout_error))
        .layer(tower::timeout::TimeoutLayer::new(Duration::from_secs(2)))
        // .layer(RequireAuthorizationLayer::custom(MyAuth {
        //     _ty: PhantomData,
        // }))
        .into_inner();

    // let static_files_service: MethodRouter<Body> = get_service(
    //     ServeDir::new("./dist").append_index_html_on_directories(true),
    // )
    // .handle_error(|error: std::io::Error| async move {
    //     (
    //         StatusCode::INTERNAL_SERVER_ERROR,
    //         format!("Unhandled internal error: {}", error),
    //     )
    // });

    let root = Router::new()
        // .route("/gethead", post(get_headers))
        .route("/health", get(root))
        .route("/health", post(root));

    let api = Router::new()
        .route("/v1/currentconfig", post(current_config))
        .route("/v1/redis/put", post(redis_put))
        .route("/v1/mysql/insert", post(rbatis_t_insert))
        .layer(middleware_stack);

    // return root.fallback(static_files_service).nest("/api", api);
    return root.nest("/api", api);
}

async fn handle_timeout_error(err: BoxError) -> (StatusCode, String) {
    if err.is::<tower::timeout::error::Elapsed>() {
        (
            StatusCode::REQUEST_TIMEOUT,
            "Request took too long".to_string(),
        )
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Unhandled internal error: {}", err),
        )
    }
}
