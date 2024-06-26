use logger::init_log;
mod cmd;
mod commons;
mod configure;
mod httpserver;
mod logger;
mod resources;
mod s3;
mod tasks;

fn main() {
    init_log();
    cmd::run_app();
}
