#![allow(unused)]

use std::future::Future;
use std::sync::Arc;

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
    log: Arc<L>,
    after_tx_id: Option<TxId>,
    subscriber: Arc<tokio::sync::RwLock<S>>,
) -> CancellationToken {
    let mut tx_receiver = log.subscribe_txs().await;

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
            let txs = log.read_txs_after(last_tx_id, 100).await;
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
                            // TODO this into might blow up
                            let txs = log.read_txs_after(last_tx_id, missed.try_into().unwrap()).await;
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

pub trait TxLogReader: Send + Sync + 'static {
    /// Read up to `limit` records written after `after_tx_id`.
    /// `None` means from the beginning. `Some(id)` means records strictly after `id`.
    fn read_txs_after(
        &self,
        after_tx_id: Option<TxId>,
        limit: u16,
    ) -> impl Future<Output = Result<Vec<Record>>> + Send;
    /// Subscribe to transactions appended after the receiver is created.
    fn subscribe_txs(&self) -> impl Future<Output = broadcast::Receiver<Record>> + Send;
}

pub trait TxLogWriter: Send + Sync + 'static {
    fn append_tx(&self, record: Vec<u8>) -> impl Future<Output = TxKey> + Send;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::st_from_unix_epoch;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::{Barrier, Mutex, Notify, RwLock};

    struct SlowReadLog {
        records: Mutex<Vec<Record>>,
        tx_sender: broadcast::Sender<Record>,
        read_started: Arc<Barrier>,
        release_read: Notify,
    }

    impl SlowReadLog {
        fn new() -> Self {
            Self {
                records: Mutex::new(vec![]),
                tx_sender: broadcast::channel(1024).0,
                read_started: Arc::new(Barrier::new(2)),
                release_read: Notify::new(),
            }
        }
    }

    impl TxLogReader for SlowReadLog {
        async fn read_txs_after(
            &self,
            after_tx_id: Option<TxId>,
            limit: u16,
        ) -> Result<Vec<Record>> {
            self.read_started.wait().await;
            self.release_read.notified().await;

            let records = self.records.lock().await;
            let start = after_tx_id.map(|id| id as usize + 1).unwrap_or(0);
            let end = std::cmp::min(start + limit as usize, records.len());
            if start >= records.len() {
                return Ok(vec![]);
            }
            Ok(records[start..end].to_vec())
        }

        async fn subscribe_txs(&self) -> broadcast::Receiver<Record> {
            self.tx_sender.subscribe()
        }
    }

    impl TxLogWriter for SlowReadLog {
        async fn append_tx(&self, record: Vec<u8>) -> TxKey {
            let mut records = self.records.lock().await;
            let tx_key = TxKey {
                tx_id: records.len() as TxId,
                system_time: st_from_unix_epoch(records.len() as u64),
            };
            let record = Record { tx_key, record };
            records.push(record.clone());
            drop(records);
            let _ = self.tx_sender.send(record);
            tx_key
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn append_does_not_wait_for_pending_subscription_read() {
        let log = Arc::new(SlowReadLog::new());
        let read_started = log.read_started.clone();
        let subscriber = Arc::new(RwLock::new(MockSubscriber::new()));
        let token = subscribe(log.clone(), None, subscriber).await;

        tokio::time::timeout(Duration::from_secs(1), read_started.wait())
            .await
            .expect("subscription should start catch-up read");

        tokio::time::timeout(Duration::from_millis(100), log.append_tx(vec![1, 2, 3]))
            .await
            .expect("append should not wait for subscription read");

        log.release_read.notify_waiters();
        token.cancel();
    }
}
