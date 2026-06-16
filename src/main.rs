use std::sync::Arc;

use anyhow::{Context, Result};
use tokio_util::sync::CancellationToken;
use tracing::info;

use triplox::config::{Config, ResolvedNode};
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

async fn run_server(bind_addr: String, resolved: ResolvedNode) -> Result<()> {
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

    match resolved {
        ResolvedNode::Dev => {
            let server = DevServer::new();
            server.listen(&bind_addr, token).await
        }
        ResolvedNode::Memory => {
            let node = Arc::new(Node::memory_node().await);
            let server = Server::new(node);
            server.listen(&bind_addr, token).await
        }
        ResolvedNode::Local {
            storage_path,
            log_path,
        } => {
            let node = Arc::new(Node::local_node(&storage_path, &log_path).await?);
            let server = Server::new(node);
            server.listen(&bind_addr, token).await
        }
        ResolvedNode::Remote { storage, log_path } => {
            let node = Arc::new(Node::remote_node(&storage, &log_path).await?);
            let server = Server::new(node);
            server.listen(&bind_addr, token).await
        }
        #[cfg(feature = "kafka")]
        ResolvedNode::Kafka { storage, log } => {
            let node = Arc::new(Node::kafka_node(&storage, &log).await?);
            let server = Server::new(node);
            server.listen(&bind_addr, token).await
        }
    }
}

/// Avoid logging the full config (potentially leaking secrets) 
fn node_mode(resolved: &ResolvedNode) -> &'static str {
    match resolved {
        ResolvedNode::Dev => "dev",
        ResolvedNode::Memory => "memory",
        ResolvedNode::Local { .. } => "local",
        ResolvedNode::Remote { .. } => "remote",
        #[cfg(feature = "kafka")]
        ResolvedNode::Kafka { .. } => "kafka",
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    triplox::logging::init_with_default_info();

    let config = load_config()?;
    let bind_addr = format!("{}:{}", config.server.host, config.server.port);
    let resolved = config.resolve()?;
    info!("Starting triplox in {} mode", node_mode(&resolved));

    run_server(bind_addr, resolved).await
}