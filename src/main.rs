use logger::init_log;
mod cmd;
mod commons;
mod configure;
mod httpserver;
mod logger;
mod resources;

fn main() {
    init_log();
    cmd::run_app();
}
