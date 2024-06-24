use logger::init_log;
mod cmd;
mod commons;
mod configure;
mod httpserver;
mod logger;
mod modules;
mod resources;
mod tasks;

fn main() {
    init_log();
    cmd::run_app();
}
