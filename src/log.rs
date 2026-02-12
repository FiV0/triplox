#![allow(unused)]

use std::future::Future;
use std::sync::{Arc, RwLock};

use anyhow::Result;
use log::{info, warn, error, trace};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::transaction::TxKey;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct Record{
    pub tx_key: TxKey,
    pub record: Vec<u8>,
}

#[allow(async_fn_in_trait)]
pub(crate) trait Subscriber : Send + Sync {
    fn accept(&mut self, record: Record) -> impl Future<Output = ()> + Send;
}   

pub type TxId = i64;

pub(crate) fn subscribe<S: Subscriber + 'static>(log: Arc<RwLock<dyn TxLogReader>>, after_tx_id: Option<TxId>, subscriber: Arc<tokio::sync::RwLock<S>>) -> CancellationToken {
    let (latest_tx_id, mut tx_receiver) = log.read().unwrap().subscribe_txs();
    trace!("Latest tx id: {}", latest_tx_id);

    let token = CancellationToken::new();
    let task_token = token.clone();

    tokio::spawn(async move {
        let mut after_tx_id = after_tx_id.unwrap_or(-1);
        info!("After tx id: {}", after_tx_id);
        info!("Starting subscriber thread");

        // Catch-up phase: read historical transactions
        while after_tx_id <= latest_tx_id && latest_tx_id != -1 && !task_token.is_cancelled() {
            let txs = {
                let log = log.read().unwrap();
                log.read_txs(after_tx_id.max(0), 100)
            };
            match txs {
                Ok(txs_to_process) if !txs_to_process.is_empty() => {
                    after_tx_id = txs_to_process.last().unwrap().tx_key.tx_id;
                    trace!("Processing {} txs catching up", txs_to_process.len());
                    let mut subscriber = subscriber.write().await;
                    for tx in txs_to_process {
                        subscriber.accept(tx).await;
                    }
                },
                Ok(_) => break,
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
                            if after_tx_id < record.tx_key.tx_id {
                                trace!("Processed live tx {}", record.tx_key.tx_id);
                                after_tx_id = record.tx_key.tx_id;
                                subscriber.write().await.accept(record).await;
                            }
                        },
                        Err(broadcast::error::RecvError::Lagged(missed)) => {
                            let txs = {
                                let log = log.read().unwrap();
                                log.read_txs(after_tx_id, missed.try_into().unwrap())
                            };
                            match txs {
                                Ok(txs_to_process) => {
                                    after_tx_id = txs_to_process.last().unwrap().tx_key.tx_id;
                                    info!("Processing {} txs catching up", txs_to_process.len());
                                    let mut subscriber = subscriber.write().await;
                                    for tx in txs_to_process {
                                        subscriber.accept(tx).await;
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

pub trait TxLogReader: Send + Sync + 'static {
    fn read_txs(&self, tx_id: TxId, limit: u16) -> Result<Vec<Record>>;
    fn subscribe_txs(&self) -> (TxId, broadcast::Receiver<Record>);
}

#[allow(async_fn_in_trait)]
pub trait TxLogWriter: Send + Sync + 'static {
    async fn append_tx(&mut self, record: Vec<u8>) -> TxKey;
}

pub trait TxLog: TxLogReader + TxLogWriter {}


// Mock subscriber for testing
#[allow(unused)]
pub(crate) struct MockSubscriber {
    pub records: Vec<Record>
}

impl MockSubscriber {
    pub fn new() -> Self {
        Self {
            records: vec![],
        }
    }
}

impl Subscriber for MockSubscriber {
    async fn accept(&mut self, record: Record) {
        self.records.push(record);
    }
}