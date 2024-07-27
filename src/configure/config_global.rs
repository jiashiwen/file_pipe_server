use crate::configure::config_error::{ConfigError, ConfigErrorType};
use anyhow::Result;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_yaml::from_str;
use std::fs;
use std::path::Path;
use std::sync::RwLock;

pub static GLOBAL_NEW_CONFIG: Lazy<RwLock<Config>> = Lazy::new(|| {
    let config = RwLock::new(Config::default());
    config
});
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Config {
    #[serde(default = "Config::http_default")]
    pub http: HttpConfig,
    pub meta_dir: String,
}

impl Config {
    pub fn default() -> Self {
        Self {
            http: HttpConfig::default(),
            meta_dir: "meta_dir".to_string(),
        }
    }

    pub fn http_default() -> HttpConfig {
        HttpConfig::default()
    }

    pub fn get_config_image(&self) -> Self {
        self.clone()
    }
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

pub fn generate_default_config(path: &str) -> Result<()> {
    let config = Config::default();
    let yml = serde_yaml::to_string(&config)?;
    fs::write(path, yml)?;
    Ok(())
}

pub fn set_config(path: &str) {
    let mut global_config = GLOBAL_NEW_CONFIG.write().unwrap();
    if path.is_empty() {
        if Path::new("config.yml").exists() {
            let contents =
                fs::read_to_string("config.yml").expect("Read config file config.yml error!");
            let config = from_str::<Config>(contents.as_str()).expect("Parse config.yml error!");
            *global_config = config;
        }
        return;
    }

    let err_str = format!("Read config file {} error!", path);
    let contents = fs::read_to_string(path).expect(err_str.as_str());
    let config = from_str::<Config>(contents.as_str()).expect("Parse config.yml error!");
    *global_config = config;
}

pub fn get_config() -> Result<Config> {
    let locked_config = GLOBAL_NEW_CONFIG.read().map_err(|e| {
        return ConfigError::from_err(e.to_string(), ConfigErrorType::UnknowErr);
    })?;
    Ok(locked_config.get_config_image())
}

pub fn get_current_config_yml() -> Result<String> {
    let c = get_config()?;
    let yml = serde_yaml::to_string(&c)?;
    Ok(yml)
}
