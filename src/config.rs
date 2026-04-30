use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub storage: StorageConfig,
    #[serde(default)]
    pub server: ServerConfig,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StorageConfig {
    Dev,
    Memory,
    Local {
        path: PathBuf,
    },
    Remote {
        endpoint: String,
        bucket: String,
        access_key: String,
        secret_key: String,
        #[serde(default = "default_region")]
        region: String,
        log_path: PathBuf,
    },
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

fn default_region() -> String {
    "eu-central-1".to_string()
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    5490
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            host: default_host(),
            port: default_port(),
        }
    }
}
