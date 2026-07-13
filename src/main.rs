use std::sync::Arc;

use anyhow::{Context, Result};
use tokio_util::sync::CancellationToken;
use tracing::info;

use triplox::config::{Config, NodeConfig};
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

async fn run_server(bind_addr: String, resolved: NodeConfig) -> Result<()> {
    let token = CancellationToken::new();
    let shutdown_token = token.clone();

    // Install the SIGTERM handler before spawning: a failure here surfaces as
    // a startup error instead of a swallowed panic in a detached task that
    // would silently lose shutdown signaling.
    #[cfg(unix)]
    let mut sigterm = {
        use tokio::signal::unix::{signal, SignalKind};
        signal(SignalKind::terminate()).context("failed to install SIGTERM handler")?
    };

    tokio::spawn(async move {
        let ctrl_c = tokio::signal::ctrl_c();

        #[cfg(unix)]
        tokio::select! {
            _ = ctrl_c => info!("SIGINT received, shutting down..."),
            _ = sigterm.recv() => info!("SIGTERM received, shutting down..."),
        }

        #[cfg(not(unix))]
        {
            ctrl_c.await.ok();
            info!("Ctrl+C received, shutting down...");
        }

        shutdown_token.cancel();
    });

    match resolved {
        NodeConfig::Dev => {
            let server = DevServer::new();
            server.listen(&bind_addr, token).await
        }
        NodeConfig::Memory => {
            let node = Arc::new(Node::memory_node().await);
            let server = Server::new(node);
            server.listen(&bind_addr, token).await
        }
        NodeConfig::Local {
            storage_path,
            log_path,
        } => {
            let node = Arc::new(Node::local_node(&storage_path, &log_path).await?);
            let server = Server::new(node);
            server.listen(&bind_addr, token).await
        }
        NodeConfig::Remote { storage, log_path } => {
            let node = Arc::new(Node::remote_node(&storage, &log_path).await?);
            let server = Server::new(node);
            server.listen(&bind_addr, token).await
        }
        #[cfg(feature = "kafka")]
        NodeConfig::Kafka { storage, log } => {
            let node = Arc::new(Node::kafka_node(&storage, &log).await?);
            let server = Server::new(node);
            server.listen(&bind_addr, token).await
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    triplox::logging::init_with_default_info();

    let config = load_config()?;
    let bind_addr = format!("{}:{}", config.server.host, config.server.port);
    let node_config = config.resolve()?;
    info!("Starting triplox in {} mode", node_config.mode());

    run_server(bind_addr, node_config).await
}
