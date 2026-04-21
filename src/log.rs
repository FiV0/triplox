#![allow(unused)]

use std::future::Future;
use std::sync::Arc;

use tokio::sync::RwLock;

use anyhow::Result;
use log::{error, info, trace, warn};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::transaction::TxKey;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct Record {
    pub tx_key: TxKey,
    pub record: Vec<u8>,
}

#[allow(async_fn_in_trait)]
pub(crate) trait Subscriber: Send + Sync {
    fn accept(&mut self, record: Record) -> impl Future<Output = ()> + Send;
}

pub type TxId = i64;

pub(crate) async fn subscribe<L: TxLogReader, S: Subscriber + 'static>(
    log: Arc<RwLock<L>>,
    after_tx_id: Option<TxId>,
    subscriber: Arc<tokio::sync::RwLock<S>>,
) -> CancellationToken {
    let (_next_tx_id, mut tx_receiver) = log.read().await.subscribe_txs();

    let token = CancellationToken::new();
    let task_token = token.clone();

    tokio::spawn(async move {
        let mut last_tx_id = after_tx_id;
        info!("Starting subscriber, after tx id: {:?}", last_tx_id);

        // Catch-up phase: read historical transactions after last_tx_id
        loop {
            if task_token.is_cancelled() {
                break;
            }
            let txs = log.read().await.read_txs_after(last_tx_id, 100).await;
            match txs {
                Ok(txs) if txs.is_empty() => break,
                Ok(txs) => {
                    trace!("Processing {} txs catching up", txs.len());
                    let mut subscriber = subscriber.write().await;
                    for tx in &txs {
                        subscriber.accept(tx.clone()).await;
                    }
                    last_tx_id = Some(txs.last().unwrap().tx_key.tx_id);
                }
                Err(e) => {
                    error!("Error reading txs: {}", e);
                    break;
                }
            }
        }

        // Live updates phase
        loop {
            tokio::select! {
                _ = task_token.cancelled() => break,
                result = tx_receiver.recv() => {
                    match result {
                        Ok(record) => {
                            let already_seen = last_tx_id.is_some_and(|id| record.tx_key.tx_id <= id);
                            if !already_seen {
                                trace!("Processed live tx {}", record.tx_key.tx_id);
                                last_tx_id = Some(record.tx_key.tx_id);
                                subscriber.write().await.accept(record).await;
                            }
                        },
                        Err(broadcast::error::RecvError::Lagged(missed)) => {
                            let txs = log.read().await.read_txs_after(last_tx_id, missed.try_into().unwrap()).await;
                            match txs {
                                Ok(txs) => {
                                    if !txs.is_empty() {
                                        info!("Processing {} txs catching up after lag", txs.len());
                                        let mut subscriber = subscriber.write().await;
                                        for tx in &txs {
                                            subscriber.accept(tx.clone()).await;
                                        }
                                        last_tx_id = Some(txs.last().unwrap().tx_key.tx_id);
                                    }
                                },
                                Err(e) => {
                                    error!("Error reading txs: {}", e);
                                }
                            }
                        },
                        Err(broadcast::error::RecvError::Closed) => {
                            warn!("Log closed, subscriber was running");
                            break;
                        },
                    }
                }
            }
        }

        info!("Stopping subscriber thread");
    });

    token
}

#[allow(async_fn_in_trait)]
pub trait TxLogReader: Send + Sync + 'static {
    /// Read up to `limit` records written after `after_tx_id`.
    /// `None` means from the beginning. `Some(id)` means records strictly after `id`.
    fn read_txs_after(&self, after_tx_id: Option<TxId>, limit: u16) -> impl Future<Output = Result<Vec<Record>>> + Send;
    /// Returns (next_tx_id, receiver). next_tx_id is where the next write will go (0 for empty log).
    fn subscribe_txs(&self) -> (TxId, broadcast::Receiver<Record>);
}

pub trait TxLogWriter: Send + Sync + 'static {
    fn append_tx(&mut self, record: Vec<u8>) -> impl std::future::Future<Output = TxKey> + Send;
}

pub trait TxLog: TxLogReader + TxLogWriter {}

// Mock subscriber for testing
#[allow(unused)]
pub(crate) struct MockSubscriber {
    pub records: Vec<Record>,
}

impl MockSubscriber {
    pub fn new() -> Self {
        Self { records: vec![] }
    }
}

impl Subscriber for MockSubscriber {
    async fn accept(&mut self, record: Record) {
        self.records.push(record);
    }
}
