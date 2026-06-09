use std::sync::Arc;

use anyhow::{bail, Context, Result};
use tokio_util::sync::CancellationToken;
use tracing::info;

use triplox::config::{Config, StorageConfig};
use triplox::node::Node;
use triplox::server::{DevServer, Server};

fn load_config() -> Result<Config> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config/triplox.toml".to_string());
    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read config file: {}", path))?;
    let config: Config = toml::from_str(&contents)
        .with_context(|| format!("Failed to parse config file: {}", path))?;
    Ok(config)
}

async fn run_server(config: Config) -> Result<()> {
    let bind_addr = format!("{}:{}", config.server.host, config.server.port);

    let token = CancellationToken::new();
    let shutdown_token = token.clone();

    tokio::spawn(async move {
        let ctrl_c = tokio::signal::ctrl_c();

        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm =
                signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
            tokio::select! {
                _ = ctrl_c => info!("SIGINT received, shutting down..."),
                _ = sigterm.recv() => info!("SIGTERM received, shutting down..."),
            }
        }

        #[cfg(not(unix))]
        {
            ctrl_c.await.ok();
            info!("Ctrl+C received, shutting down...");
        }

        shutdown_token.cancel();
    });

    match &config.storage {
        StorageConfig::Dev => {
            let server = DevServer::new();
            server.listen(&bind_addr, token).await
        }
        StorageConfig::Memory => {
            let node = Arc::new(Node::memory_node().await);
            let server = Server::new(node);
            server.listen(&bind_addr, token).await
        }
        StorageConfig::Local { path } => {
            let node = Arc::new(Node::local_node(path).await?);
            let server = Server::new(node);
            server.listen(&bind_addr, token).await
        }
        StorageConfig::Remote {
            endpoint,
            bucket,
            access_key,
            secret_key,
            region,
            file_log_path,
        } => {
            let Some(local_disk_storage_path) = config.local_disk_storage_path() else {
                bail!("remote storage requires local_disk_storage.path");
            };
            let node = Arc::new(
                Node::remote_node(
                    file_log_path,
                    &local_disk_storage_path,
                    endpoint,
                    bucket,
                    access_key,
                    secret_key,
                    region,
                )
                .await?,
            );
            let server = Server::new(node);
            server.listen(&bind_addr, token).await
        }
        #[cfg(feature = "kafka")]
        StorageConfig::Kafka(kafka_config) => {
            let Some(local_disk_storage_path) = config.local_disk_storage_path() else {
                bail!("kafka storage requires local_disk_storage.path");
            };
            let node = Arc::new(Node::kafka_node(kafka_config, &local_disk_storage_path).await?);
            let server = Server::new(node);
            server.listen(&bind_addr, token).await
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    triplox::logging::init_with_default_info();

    let config = load_config()?;
    info!("Starting triplox with {:?} storage", config.storage);

    run_server(config).await
}
