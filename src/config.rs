use std::path::PathBuf;

use serde::Deserialize;

const DBSP_STORAGE_DIR: &str = "dbsp";
const REMOTE_CACHE_DIR: &str = "cache";

#[derive(Debug, Deserialize)]
pub struct Config {
    pub storage: StorageConfig,
    #[serde(default)]
    pub local_disk_storage: LocalDiskStorageConfig,
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
        file_log_path: PathBuf,
    },
    #[cfg(feature = "kafka")]
    Kafka {
        bootstrap_servers: String,
        #[serde(default = "default_kafka_topic")]
        topic: String,
        endpoint: String,
        bucket: String,
        access_key: String,
        secret_key: String,
        #[serde(default = "default_region")]
        region: String,
        cache_path: PathBuf,
    },
}

#[derive(Debug, Default, Deserialize)]
pub struct LocalDiskStorageConfig {
    pub path: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

#[cfg(feature = "kafka")]
fn default_kafka_topic() -> String {
    "triplox-tx-log".to_string()
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

pub fn default_ephemeral_dbsp_storage_path() -> PathBuf {
    std::env::temp_dir().join(format!("triplox-dbsp-{}", crate::util::random_string(10)))
}

impl Config {
    pub fn dbsp_storage_path(&self) -> PathBuf {
        match &self.storage {
            StorageConfig::Dev => default_ephemeral_dbsp_storage_path(),

            StorageConfig::Memory => default_ephemeral_dbsp_storage_path(),

            StorageConfig::Local { path } => path.join(DBSP_STORAGE_DIR),

            StorageConfig::Remote { .. } => self
                .local_disk_storage_path()
                .expect("remote storage requires local_disk_storage.path")
                .join(DBSP_STORAGE_DIR),

            #[cfg(feature = "kafka")]
            StorageConfig::Kafka { cache_path, .. } => cache_path
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
                .map_or_else(
                    || cache_path.join(DBSP_STORAGE_DIR),
                    |path| path.join(DBSP_STORAGE_DIR),
                ),
        }
    }

    pub fn remote_cache_path(&self) -> Option<PathBuf> {
        match &self.storage {
            StorageConfig::Remote { .. } => self
                .local_disk_storage_path()
                .map(|path| path.join(REMOTE_CACHE_DIR)),
            _ => None,
        }
    }

    pub fn local_disk_storage_path(&self) -> Option<PathBuf> {
        match &self.storage {
            StorageConfig::Remote { .. } => self.local_disk_storage.path.clone(),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_remote_file_log_and_local_disk_storage_paths() {
        let config: Config = toml::from_str(
            r#"
            [storage]
            type = "remote"
            endpoint = "http://localhost:9000"
            bucket = "triplox"
            access_key = "triplox"
            secret_key = "triplox123"
            file_log_path = "/tmp/triplox-log/log"

            [local_disk_storage]
            path = "/tmp/triplox-disk"
            "#,
        )
        .unwrap();

        let StorageConfig::Remote { file_log_path, .. } = &config.storage else {
            panic!("expected remote storage");
        };
        assert_eq!(file_log_path, &PathBuf::from("/tmp/triplox-log/log"));
        assert_eq!(
            config.remote_cache_path().unwrap(),
            PathBuf::from("/tmp/triplox-disk/cache")
        );
        assert_eq!(
            config.dbsp_storage_path(),
            PathBuf::from("/tmp/triplox-disk/dbsp")
        );
    }

    #[test]
    fn remote_without_local_disk_storage_has_no_local_disk_storage_path() {
        let config: Config = toml::from_str(
            r#"
            [storage]
            type = "remote"
            endpoint = "http://localhost:9000"
            bucket = "triplox"
            access_key = "triplox"
            secret_key = "triplox123"
            file_log_path = "/tmp/triplox-log/log"
            "#,
        )
        .unwrap();

        assert_eq!(config.local_disk_storage_path(), None);
        assert_eq!(config.remote_cache_path(), None);
    }

    #[test]
    fn local_node_derives_dbsp_storage_from_single_storage_path() {
        let config: Config = toml::from_str(
            r#"
            [storage]
            type = "local"
            path = "./data"

            [local_disk_storage]
            path = "/ignored-for-local"
            "#,
        )
        .unwrap();

        assert_eq!(config.remote_cache_path(), None);
        assert_eq!(config.dbsp_storage_path(), PathBuf::from("./data/dbsp"));
    }

    #[test]
    fn memory_node_uses_default_dbsp_storage_when_local_disk_storage_is_present() {
        let config: Config = toml::from_str(
            r#"
            [storage]
            type = "memory"

            [local_disk_storage]
            path = "/tmp/triplox-disk"
            "#,
        )
        .unwrap();

        let dbsp_storage_path = config.dbsp_storage_path();
        assert_ne!(dbsp_storage_path, PathBuf::from("/tmp/triplox-disk/dbsp"));
        assert!(dbsp_storage_path.starts_with(std::env::temp_dir()));
        assert!(dbsp_storage_path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("triplox-dbsp-"));
    }
}
