#![allow(unused)]

use async_trait::async_trait;
use std::sync::{atomic::{AtomicBool, Ordering}, Arc, RwLock};
use log::{info, warn, error};

use serde::{Deserialize, Serialize};
use crate::transaction::TxKey;
use tokio::sync::broadcast;
use anyhow::Result;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub(crate) struct Record{
    pub tx_key: TxKey,
    pub record: Vec<u8>,
}

#[allow(unused)]
pub(crate) trait Subscriber : Send + Sync {
    fn on_subscribe(&mut self, close_hook: Box<dyn FnOnce() + Send + Sync>);
    fn accept(&mut self, record: Record);
}   

pub(crate) type TxId = i64;

pub(crate) fn subscribe(log: Arc<RwLock<dyn TxLogReader>>, after_tx_id: Option<TxId>, mut subscriber: Arc<RwLock<dyn Subscriber>>) {
    let (latest_tx_id, mut tx_receiver) = log.read().unwrap().subscribe_txs();

    // Consider using tokio::sync::oneshot to send stop signal
    let stop_flag = Arc::new(AtomicBool::new(false));
    let stop_flag_clone = stop_flag.clone();

    subscriber.write().unwrap().on_subscribe(Box::new(move || { 
        stop_flag_clone.store(true, Ordering::Relaxed);
    }));

    tokio::spawn(async move {
        // TODO: Set thread priority to max

        // Start processing transactions
        let mut after_tx_id = after_tx_id.unwrap_or(-1);

        info!("Starting subscriber thread");

        while after_tx_id < latest_tx_id {
            let log = log.read().unwrap();
            match log.read_txs(after_tx_id, 100) {
                Ok(txs_to_process) => {
                    after_tx_id = txs_to_process.last().unwrap().tx_key.tx_id;
                    for tx in txs_to_process {
                        subscriber.write().unwrap().accept(tx);
                    }
                },
                Err(e) => {
                    error!("Error reading txs: {}", e);
                    // TODO: Handle error
                }
            }
        }

        // Process live updates
        while !stop_flag.load(Ordering::Relaxed) {
            match tx_receiver.recv().await {
                Ok(record) => {
                    if after_tx_id < record.tx_key.tx_id {
                        after_tx_id = record.tx_key.tx_id;
                        subscriber.write().unwrap().accept(record);
                    }
                },
                Err(broadcast::error::RecvError::Lagged(missed)) => {
                    let log = log.read().unwrap();
                    // We fell behind, need to catch up using read_txs
                    // TODO should limit be cropped to a 100?
                    match log.read_txs(after_tx_id, missed.try_into().unwrap()) {
                        Ok(txs_to_process) => {
                            after_tx_id = txs_to_process.last().unwrap().tx_key.tx_id;
                            for tx in txs_to_process {
                                subscriber.write().unwrap().accept(tx);
                            }
                        },
                        Err(e) => {
                            error!("Error reading txs: {}", e);
                            // TODO: Handle error
                        }
                    }
                },
                Err(broadcast::error::RecvError::Closed) => {
                    warn!("Log closed, subscriber was running");
                    break;
                },
            }
        }

        info!("Stopping subscriber thread");

    });
}

pub(crate) trait TxLogReader: Send + Sync + 'static {
    fn read_txs(&self, tx_id: TxId, limit: u16) -> Result<Vec<Record>>;
    fn subscribe_txs(&self) -> (TxId, broadcast::Receiver<Record>);
}

#[async_trait]
pub(crate) trait TxLogWriter: Send + Sync + 'static {
    async fn append_tx(&mut self, record: Vec<u8>) -> TxKey;
}

#[async_trait]
pub(crate) trait TxLog: TxLogReader + TxLogWriter {}


// Mock subscriber for testing
#[allow(unused)]
pub(crate) struct MockSubscriber {
    close_hook: Option<Box<dyn FnOnce() + Send + Sync>>,
    pub records: Vec<Record>
}

impl MockSubscriber {
    pub fn new() -> Self {
        Self {
            close_hook: None,
            records: vec![],
        }
    }

    pub fn close(&mut self) {
        if let Some(close_hook) = self.close_hook.take() {
            close_hook();
        } else {
            panic!("close_hook not set");
        }
    }
}

impl Subscriber for MockSubscriber {
    fn on_subscribe(&mut self, close_hook: Box<dyn FnOnce() + Send + Sync>) {
        self.close_hook = Some(close_hook);
    }

    fn accept(&mut self, record: Record) {
        self.records.push(record);
    }
}