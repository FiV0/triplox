use std::path::PathBuf;

use anyhow::{anyhow, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub storage: StorageConfig,
    /// Required for every storage type except `dev`, which ignores it.
    #[serde(default)]
    pub log: Option<LogConfig>,
    #[serde(default)]
    pub server: ServerConfig,
}

#[cfg(feature = "kafka")]
fn default_kafka_topic() -> String {
    "triplox-tx-log".to_string()
}

fn default_region() -> String {
    "eu-central-1".to_string()
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum StorageConfig {
    /// Per-connection ephemeral `DevServer`; ignores `[log]`.
    Dev,
    Memory,
    Local {
        path: PathBuf,
    },
    Remote(RemoteStorageConfig),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteStorageConfig {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    #[serde(default = "default_region")]
    pub region: String,
    /// On-disk root for the SlateDB object-store cache and the dbsp scratch
    /// directory (`{cache_path}/cache`, `{cache_path}/dbsp`).
    pub cache_path: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum LogConfig {
    Memory,
    File {
        path: PathBuf,
    },
    #[cfg(feature = "kafka")]
    Kafka(KafkaLogConfig),
}

#[cfg(feature = "kafka")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaLogConfig {
    pub bootstrap_servers: String,
    #[serde(default = "default_kafka_topic")]
    pub topic: String,
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    5490
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            host: default_host(),
            port: default_port(),
        }
    }
}

/// A validated (log, storage) pairing ready to construct a node. Produced by
/// [`Config::resolve`]; only the four supported combinations are representable.
#[derive(Debug)]
pub enum NodeConfig {
    Dev,
    Memory,
    Local {
        storage_path: PathBuf,
        log_path: PathBuf,
    },
    Remote {
        storage: RemoteStorageConfig,
        log_path: PathBuf,
    },
    #[cfg(feature = "kafka")]
    Kafka {
        storage: RemoteStorageConfig,
        log: KafkaLogConfig,
    },
}

impl NodeConfig {
    /// The node kind as a static string. Safe to log (carries no secrets).
    pub fn mode(&self) -> &'static str {
        match self {
            NodeConfig::Dev => "dev",
            NodeConfig::Memory => "memory",
            NodeConfig::Local { .. } => "local",
            NodeConfig::Remote { .. } => "remote",
            #[cfg(feature = "kafka")]
            NodeConfig::Kafka { .. } => "kafka",
        }
    }
}

fn storage_kind(storage: &StorageConfig) -> &'static str {
    match storage {
        StorageConfig::Dev => "dev",
        StorageConfig::Memory => "memory",
        StorageConfig::Local { .. } => "local",
        StorageConfig::Remote(_) => "remote",
    }
}

fn log_kind(log: &LogConfig) -> &'static str {
    match log {
        LogConfig::Memory => "memory",
        LogConfig::File { .. } => "file",
        #[cfg(feature = "kafka")]
        LogConfig::Kafka(_) => "kafka",
    }
}

impl Config {
    /// Validate the configured (log, storage) pair and resolve it to a [`NodeConfig`].
    pub fn resolve(self) -> Result<NodeConfig> {
        if matches!(self.storage, StorageConfig::Dev) {
            return Ok(NodeConfig::Dev);
        }

        let log = self
            .log
            .ok_or_else(|| anyhow!("configuration must specify a [log] section"))?;

        match (log, self.storage) {
            (LogConfig::Memory, StorageConfig::Memory) => Ok(NodeConfig::Memory),
            (LogConfig::File { path }, StorageConfig::Local { path: storage_path }) => {
                Ok(NodeConfig::Local {
                    storage_path,
                    log_path: path,
                })
            }
            (LogConfig::File { path }, StorageConfig::Remote(storage)) => Ok(NodeConfig::Remote {
                storage,
                log_path: path,
            }),
            #[cfg(feature = "kafka")]
            (LogConfig::Kafka(log), StorageConfig::Remote(storage)) => {
                Ok(NodeConfig::Kafka { storage, log })
            }
            (log, storage) => Err(anyhow!(
                "unsupported (log, storage) combination: log={}, storage={}. \
                 Valid: (memory,memory), (file,local), (file,remote), (kafka,remote)",
                log_kind(&log),
                storage_kind(&storage)
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_memory() {
        let config: Config = toml::from_str(
            r#"
            [storage]
            type = "memory"

            [log]
            type = "memory"
            "#,
        )
        .unwrap();
        assert!(matches!(config.resolve().unwrap(), NodeConfig::Memory));
    }

    #[test]
    fn resolves_local_with_explicit_log_path() {
        let config: Config = toml::from_str(
            r#"
            [storage]
            type = "local"
            path = "./data"

            [log]
            type = "file"
            path = "./data/log"
            "#,
        )
        .unwrap();
        let NodeConfig::Local {
            storage_path,
            log_path,
        } = config.resolve().unwrap()
        else {
            panic!("expected local node");
        };
        assert_eq!(storage_path, PathBuf::from("./data"));
        assert_eq!(log_path, PathBuf::from("./data/log"));
        assert_eq!(storage_path.join("dbsp"), PathBuf::from("./data/dbsp"));
    }

    #[test]
    fn resolves_remote_with_cache_path_and_defaults() {
        let config: Config = toml::from_str(
            r#"
            [storage]
            type = "remote"
            endpoint = "http://localhost:9000"
            bucket = "triplox"
            access_key = "triplox"
            secret_key = "triplox123"
            cache_path = "/tmp/triplox-disk"

            [log]
            type = "file"
            path = "/tmp/triplox-log/log"
            "#,
        )
        .unwrap();
        let NodeConfig::Remote { storage, log_path } = config.resolve().unwrap() else {
            panic!("expected remote node");
        };
        assert_eq!(storage.region, "eu-central-1");
        assert_eq!(storage.cache_path, PathBuf::from("/tmp/triplox-disk"));
        assert_eq!(
            storage.cache_path.join("cache"),
            PathBuf::from("/tmp/triplox-disk/cache")
        );
        assert_eq!(
            storage.cache_path.join("dbsp"),
            PathBuf::from("/tmp/triplox-disk/dbsp")
        );
        assert_eq!(log_path, PathBuf::from("/tmp/triplox-log/log"));
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn resolves_kafka_with_topic_default() {
        let config: Config = toml::from_str(
            r#"
            [storage]
            type = "remote"
            endpoint = "http://localhost:9000"
            bucket = "triplox-kafka"
            access_key = "triplox"
            secret_key = "triplox123"
            cache_path = "/tmp/triplox-disk"

            [log]
            type = "kafka"
            bootstrap_servers = "automq:9092"
            "#,
        )
        .unwrap();
        let NodeConfig::Kafka { storage, log } = config.resolve().unwrap() else {
            panic!("expected kafka node");
        };
        assert_eq!(storage.bucket, "triplox-kafka");
        assert_eq!(log.topic, "triplox-tx-log");
    }

    #[test]
    fn dev_storage_resolves_without_log() {
        let config: Config = toml::from_str(
            r#"
            [storage]
            type = "dev"
            "#,
        )
        .unwrap();
        assert!(matches!(config.resolve().unwrap(), NodeConfig::Dev));
    }

    #[test]
    fn dev_storage_ignores_present_log() {
        let config: Config = toml::from_str(
            r#"
            [storage]
            type = "dev"

            [log]
            type = "memory"
            "#,
        )
        .unwrap();
        assert!(matches!(config.resolve().unwrap(), NodeConfig::Dev));
    }

    #[test]
    fn rejects_memory_log_with_local_storage() {
        let config: Config = toml::from_str(
            r#"
            [storage]
            type = "local"
            path = "./data"

            [log]
            type = "memory"
            "#,
        )
        .unwrap();
        let err = config.resolve().unwrap_err().to_string();
        assert!(
            err.contains("unsupported (log, storage) combination"),
            "got: {err}"
        );
        assert!(err.contains("(memory,memory)"), "got: {err}");
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn rejects_kafka_log_with_local_storage() {
        let config: Config = toml::from_str(
            r#"
            [storage]
            type = "local"
            path = "./data"

            [log]
            type = "kafka"
            bootstrap_servers = "automq:9092"
            "#,
        )
        .unwrap();
        assert!(config.resolve().is_err());
    }

    #[test]
    fn missing_storage_fails_to_parse() {
        let result: std::result::Result<Config, _> = toml::from_str(
            r#"
            [log]
            type = "memory"
            "#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn rejects_missing_log_for_non_dev_storage() {
        let config: Config = toml::from_str(
            r#"
            [storage]
            type = "memory"
            "#,
        )
        .unwrap();
        let err = config.resolve().unwrap_err().to_string();
        assert!(err.contains("[log]"), "got: {err}");
    }

    #[test]
    fn remote_storage_missing_cache_path_fails_to_parse() {
        let result: std::result::Result<Config, _> = toml::from_str(
            r#"
            [storage]
            type = "remote"
            endpoint = "http://localhost:9000"
            bucket = "triplox"
            access_key = "triplox"
            secret_key = "triplox123"

            [log]
            type = "file"
            path = "/tmp/triplox-log/log"
            "#,
        );
        assert!(result.is_err());
    }
}
