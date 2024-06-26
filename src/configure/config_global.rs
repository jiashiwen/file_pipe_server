use crate::configure::config_error::{ConfigError, ConfigErrorType};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_yaml::from_str;
use std::fs;
use std::path::Path;
use std::sync::Mutex;
use std::sync::RwLock;

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct TiKVConfig {
    pub pdaddrs: Vec<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct HttpConfig {
    #[serde(default = "HttpConfig::port_default")]
    pub port: u16,
    #[serde(default = "HttpConfig::bind_default")]
    pub bind: String,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            port: HttpConfig::port_default(),
            bind: HttpConfig::bind_default(),
        }
    }
}

impl HttpConfig {
    pub fn port_default() -> u16 {
        3000
    }
    pub fn bind_default() -> String {
        "::0".to_string()
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct TaskPoolConfig {
    pub max_execute_parallel: usize,
    pub max_bigfile_parallel: usize,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct RedisPool {
    #[serde(default = "RedisPool::max_size_default")]
    pub max_size: usize,
    #[serde(default = "RedisPool::mini_idle_default")]
    pub mini_idle: usize,
    #[serde(default = "RedisPool::connection_timeout_default")]
    pub connection_timeout: usize,
}

impl Default for RedisPool {
    fn default() -> Self {
        Self {
            max_size: 1,
            mini_idle: 1,
            connection_timeout: 10,
        }
    }
}

impl RedisPool {
    pub fn max_size_default() -> usize {
        1
    }

    pub fn mini_idle_default() -> usize {
        1
    }

    pub fn connection_timeout_default() -> usize {
        10
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DatasourceMySql {
    pub mysql_uri: String,
    pub pool_size: usize,
}

impl Default for DatasourceMySql {
    fn default() -> Self {
        Self {
            mysql_uri: "mysql://root:123@127.0.0.1:3306".to_string(),
            pool_size: 1,
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Config {
    pub tikv: TiKVConfig,
    #[serde(default = "Config::http_default")]
    pub http: HttpConfig,
    pub meta_dir: String,
    pub datasource_mysql: DatasourceMySql,
}

impl Config {
    pub fn default() -> Self {
        Self {
            tikv: TiKVConfig::default(),
            http: HttpConfig::default(),
            datasource_mysql: DatasourceMySql::default(),
            meta_dir: "meta_dir".to_string(),
        }
    }

    pub fn http_default() -> HttpConfig {
        HttpConfig::default()
    }
    pub fn set_self(&mut self, config: Config) {
        self.tikv = config.tikv;
        self.http = config.http;
        self.datasource_mysql = config.datasource_mysql;
    }

    pub fn get_config_image(&self) -> Self {
        self.clone()
    }
}

impl TiKVConfig {
    pub fn default() -> Self {
        Self {
            pdaddrs: vec!["127.0.0.1:2379".to_string()],
        }
    }
}

impl HttpConfig {
    pub fn default() -> Self {
        Self {
            port: 3000,
            bind: "0.0.0.0".to_string(),
        }
    }
}

pub fn generate_default_config(path: &str) -> Result<()> {
    let config = Config::default();
    let yml = serde_yaml::to_string(&config)?;
    fs::write(path, yml)?;
    Ok(())
}

lazy_static::lazy_static! {
    static ref GLOBAL_CONFIG: Mutex<Config> = {
        let global_config = Config::default();
        Mutex::new(global_config)
    };
    static ref CONFIG_FILE_PATH: RwLock<String> = RwLock::new({
        let path = "".to_string();
        path
    });
}

pub fn set_config(path: &str) {
    if path.is_empty() {
        if Path::new("config.yml").exists() {
            let contents =
                fs::read_to_string("config.yml").expect("Read config file config.yml error!");
            let config = from_str::<Config>(contents.as_str()).expect("Parse config.yml error!");
            GLOBAL_CONFIG.lock().unwrap().set_self(config);
        }
        return;
    }

    let err_str = format!("Read config file {} error!", path);
    let contents = fs::read_to_string(path).expect(err_str.as_str());
    let config = from_str::<Config>(contents.as_str()).expect("Parse config.yml error!");
    GLOBAL_CONFIG.lock().unwrap().set_self(config);
}

pub fn set_config_file_path(path: String) {
    CONFIG_FILE_PATH
        .write()
        .expect("clear config file path error!")
        .clear();
    CONFIG_FILE_PATH.write().unwrap().push_str(path.as_str());
}

pub fn get_config_file_path() -> String {
    CONFIG_FILE_PATH.read().unwrap().clone()
}

pub fn get_config() -> Result<Config> {
    let locked_config = GLOBAL_CONFIG.lock().map_err(|e| {
        return ConfigError::from_err(e.to_string(), ConfigErrorType::UnknowErr);
    })?;
    Ok(locked_config.get_config_image())
}

pub fn get_current_config_yml() -> Result<String> {
    let c = get_config()?;
    let yml = serde_yaml::to_string(&c)?;
    Ok(yml)
}
