use std::path::PathBuf;

use serde::Deserialize;

const DBSP_STORAGE_DIR: &str = "dbsp";
const REMOTE_CACHE_DIR: &str = "cache";
const REMOTE_DEFAULT_LOCAL_DISK_STORAGE_DIR: &str = "local_disk_storage";

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

fn default_remote_local_disk_storage_root(file_log_path: &std::path::Path) -> PathBuf {
    file_log_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."))
        .join(REMOTE_DEFAULT_LOCAL_DISK_STORAGE_DIR)
}

impl Config {
    pub fn dbsp_storage_path(&self) -> PathBuf {
        match &self.storage {
            StorageConfig::Local { path } => path.join(DBSP_STORAGE_DIR),
            StorageConfig::Remote { file_log_path, .. } => self
                .remote_local_disk_storage_root(file_log_path)
                .join(DBSP_STORAGE_DIR),
            StorageConfig::Dev | StorageConfig::Memory => self
                .local_disk_storage
                .path
                .as_ref()
                .map(|path| path.join(DBSP_STORAGE_DIR))
                .unwrap_or_else(default_ephemeral_dbsp_storage_path),
        }
    }

    pub fn remote_cache_path(&self) -> Option<PathBuf> {
        match &self.storage {
            StorageConfig::Remote { file_log_path, .. } => Some(
                self.remote_local_disk_storage_root(file_log_path)
                    .join(REMOTE_CACHE_DIR),
            ),
            _ => None,
        }
    }

    pub fn remote_local_disk_storage_path(&self) -> Option<PathBuf> {
        match &self.storage {
            StorageConfig::Remote { file_log_path, .. } => {
                Some(self.remote_local_disk_storage_root(file_log_path))
            }
            _ => None,
        }
    }

    fn remote_local_disk_storage_root(&self, file_log_path: &std::path::Path) -> PathBuf {
        self.local_disk_storage
            .path
            .clone()
            .unwrap_or_else(|| default_remote_local_disk_storage_root(file_log_path))
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
    fn remote_defaults_local_disk_storage_next_to_file_log_parent() {
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

        assert_eq!(
            config.remote_cache_path().unwrap(),
            PathBuf::from("/tmp/triplox-log/local_disk_storage/cache")
        );
        assert_eq!(
            config.dbsp_storage_path(),
            PathBuf::from("/tmp/triplox-log/local_disk_storage/dbsp")
        );
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
    fn memory_node_uses_configured_local_disk_storage_when_present() {
        let config: Config = toml::from_str(
            r#"
            [storage]
            type = "memory"

            [local_disk_storage]
            path = "/tmp/triplox-disk"
            "#,
        )
        .unwrap();

        assert_eq!(
            config.dbsp_storage_path(),
            PathBuf::from("/tmp/triplox-disk/dbsp")
        );
    }
}
