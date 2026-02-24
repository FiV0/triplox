use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use tokio_util::sync::CancellationToken;
use tracing::info;

use triplox::config::{Config, StorageConfig};
use triplox::node::Node;
use triplox::server::Server;

fn load_config() -> Result<Config> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "triplox.toml".to_string());
    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read config file: {}", path))?;
    let config: Config =
        toml::from_str(&contents).with_context(|| format!("Failed to parse config file: {}", path))?;
    Ok(config)
}

async fn run_server(config: Config) -> Result<()> {
    let bind_addr = config.server.bind_addr();
    let max_open_dbs = config.server.max_open_dbs;

    let token = CancellationToken::new();
    let shutdown_token = token.clone();

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Ctrl+C received, shutting down...");
        shutdown_token.cancel();
    });

    match config.storage {
        StorageConfig::Memory => {
            let node = Arc::new(Node::memory_node().await);
            let server = Server::new(node, max_open_dbs);
            server.listen(&bind_addr, token).await
        }
        StorageConfig::Local { path } => {
            let node = Arc::new(Node::local_node(&path).await);
            let server = Server::new(node, max_open_dbs);
            server.listen(&bind_addr, token).await
        }
        StorageConfig::Remote { .. } => {
            bail!("Remote storage is not yet supported")
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    triplox::logging::init();

    let config = load_config()?;
    info!("Starting triplox with {:?} storage", config.storage);

    run_server(config).await
}
