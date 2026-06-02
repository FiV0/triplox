#![allow(unused)]

use std::cmp::min;

use crate::clock::SystemTimeSource;
use crate::error::TriploxError;
use crate::log::TxId;
use crate::log::{Record, TxLog, TxLogReader, TxLogWriter, BOOTSTRAP_RECORD};
use crate::logging::init;
use crate::transaction::TxKey;
use anyhow::Result;
use log::warn;
use tokio::sync::{broadcast, RwLock};

pub struct MemoryLog {
    state: RwLock<MemoryLogState>,
    tx_sender: broadcast::Sender<Record>,
}

struct MemoryLogState {
    txs: Vec<Record>,
    clock: Box<dyn SystemTimeSource>,
}

impl MemoryLog {
    pub fn new(clock: Box<dyn SystemTimeSource>) -> Self {
        MemoryLog {
            state: RwLock::new(MemoryLogState { txs: vec![], clock }),
            tx_sender: broadcast::channel(1024).0,
        }
    }
}

impl TxLog for MemoryLog {
    async fn ensure_bootstrap_record(&self) -> Result<()> {
        let bootstrap_record = BOOTSTRAP_RECORD.clone();
        let mut state = self.state.write().await;
        match state.txs.first() {
            Some(record) if record == &bootstrap_record => Ok(()),
            Some(record) => Err(anyhow::anyhow!(
                "memory log starts with non-bootstrap record {:?}",
                record.tx_key
            )),
            None => {
                state.txs.push(bootstrap_record);
                Ok(())
            }
        }
    }
}

impl TxLogReader for MemoryLog {
    async fn read_txs_after(&self, after_tx_id: Option<TxId>, limit: u16) -> Result<Vec<Record>> {
        let state = self.state.read().await;
        let start = match after_tx_id {
            None => 0,
            Some(id) => id as usize + 1,
        };
        let end = min(start + limit as usize, state.txs.len());
        if start >= state.txs.len() {
            return Ok(vec![]);
        }
        Ok(state.txs[start..end].to_vec())
    }

    async fn subscribe_txs(&self) -> broadcast::Receiver<Record> {
        self.tx_sender.subscribe()
    }
}

impl TxLogWriter for MemoryLog {
    async fn append_tx(&self, record: Vec<u8>) -> TxKey {
        let mut state = self.state.write().await;
        let record = Record {
            tx_key: TxKey {
                tx_id: state.txs.len() as i64,
                system_time: state.clock.now(),
            },
            record,
        };
        let tx_key = record.tx_key;
        state.txs.push(record.clone());
        drop(state);
        // TODO: verify if warning on no receivers is idiomatic Rust broadcast channel pattern
        if let Err(e) = self.tx_sender.send(record) {
            warn!(
                "Failed to send record from memory log to subscribers: {}",
                e
            );
        }
        tx_key
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{st_from_unix_epoch, MockClock};
    use crate::log::{subscribe, MockSubscriber};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::RwLock;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    // TODO: refactor with file log test into a single test where only the log is changed
    async fn test_memory_log() {
        init();
        let subscriber = Arc::new(RwLock::new(MockSubscriber::new()));
        let clock = MockClock::new(vec![
            st_from_unix_epoch(0),
            st_from_unix_epoch(100),
            st_from_unix_epoch(200),
            st_from_unix_epoch(300),
            st_from_unix_epoch(400),
        ]);
        let log = Arc::new(MemoryLog::new(Box::new(clock)));
        let token = subscribe(log.clone(), None, subscriber.clone()).await;

        log.append_tx(vec![1, 2, 3]).await;
        log.append_tx(vec![4, 5, 6]).await;
        log.append_tx(vec![7, 8, 9]).await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        token.cancel();

        let subscriber = subscriber.read().await;

        assert_eq!(subscriber.records.len(), 3);
        assert_eq!(
            subscriber.records[0],
            Record {
                tx_key: TxKey {
                    tx_id: 0,
                    system_time: st_from_unix_epoch(0)
                },
                record: vec![1, 2, 3]
            }
        );
        assert_eq!(
            subscriber.records[1],
            Record {
                tx_key: TxKey {
                    tx_id: 1,
                    system_time: st_from_unix_epoch(100)
                },
                record: vec![4, 5, 6]
            }
        );
        assert_eq!(
            subscriber.records[2],
            Record {
                tx_key: TxKey {
                    tx_id: 2,
                    system_time: st_from_unix_epoch(200)
                },
                record: vec![7, 8, 9]
            }
        );

        let tx_id_1 = subscriber.records[1].tx_key.tx_id;
        drop(subscriber);

        let subscriber2 = Arc::new(RwLock::new(MockSubscriber::new()));
        let token2 = subscribe(log.clone(), Some(tx_id_1), subscriber2.clone()).await; // Subscribe after second transaction

        log.append_tx(vec![10, 11, 12]).await;
        log.append_tx(vec![13, 14, 15]).await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        token2.cancel();

        let subscriber2 = subscriber2.read().await;

        assert_eq!(subscriber2.records.len(), 3);
        assert_eq!(subscriber2.records[0].record, vec![7, 8, 9]); // Third tx from first log
        assert_eq!(subscriber2.records[1].record, vec![10, 11, 12]); // First tx after restart
        assert_eq!(subscriber2.records[2].record, vec![13, 14, 15]); // Second tx after restart
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_memory_log_infinite_loop_bug() {
        init();
        let clock = MockClock::new(vec![st_from_unix_epoch(0), st_from_unix_epoch(100)]);
        let log = Arc::new(MemoryLog::new(Box::new(clock)));

        // Write one transaction
        log.append_tx(vec![1, 2, 3]).await;

        // Subscribe from the beginning — should process the one transaction exactly once
        let subscriber = Arc::new(RwLock::new(MockSubscriber::new()));
        let token = subscribe(log.clone(), None, subscriber.clone()).await;

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if !subscriber.read().await.records.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("timed out waiting for subscriber to process first record");
        tokio::time::sleep(Duration::from_millis(25)).await;

        token.cancel();

        let subscriber = subscriber.read().await;

        // Without the fix, the subscriber would process the same transaction
        // multiple times in an infinite loop until cancellation
        // With the fix, it should process it exactly once
        assert_eq!(
            subscriber.records.len(),
            1,
            "Should process transaction exactly once, not in an infinite loop"
        );
        assert_eq!(subscriber.records[0].record, vec![1, 2, 3]);
    }
}
