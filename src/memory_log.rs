#![allow(unused)]

use std::cmp::min;

use async_trait::async_trait;
use log::warn;
use tokio::sync::broadcast;
use crate::clock::SystemTimeSource;
use crate::log::{Record, TxLog, TxLogReader, TxLogWriter};
use crate::transaction::TxKey;
use crate::log::TxId;

struct MemoryLog {
    txs: Vec<Record>,
    tx_sender: broadcast::Sender<Record>,
    clock: Box<dyn SystemTimeSource>
}

impl MemoryLog {
    pub fn new(clock: Box<dyn SystemTimeSource>) -> Self {
        MemoryLog { txs: vec![], tx_sender: broadcast::channel(1024).0, clock }
    }
}

#[async_trait]
impl TxLogReader for MemoryLog {
    fn read_txs(&self, tx_id: TxId, limit: u16) -> Vec<Record> {
        let available_records = min(self.txs.len().saturating_sub(tx_id as usize), limit as usize);
        self.txs[tx_id as usize..tx_id as usize + available_records as usize].to_vec()
    }

    fn subscribe_txs(&self) -> (TxId, broadcast::Receiver<Record>) {
        (self.txs.len() as TxId - 1, self.tx_sender.subscribe())
    }
}

#[async_trait]
impl TxLogWriter for MemoryLog {
    async fn append_tx(&mut self, record: Vec<u8>) -> TxKey {
        let record = Record {
            tx_key: TxKey {
                tx_id: self.txs.len() as i64,
                system_time: self.clock.now()
            },
            record
        };
        let tx_key = record.tx_key;
        self.txs.push(record.clone());
        if let Err(e) = self.tx_sender.send(record) {
            warn!("Failed to send record from memory log to subscribers: {}", e);
        }
        tx_key
    }
}

impl TxLog for MemoryLog {}

#[cfg(test)]
mod tests {
    use tokio::sync::Mutex;

    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use crate::log::{Subscriber, subscribe};
    use crate::clock::{st_from_unix_epoch, MockClock};

    struct MockSubscriber {
        close_hook: Option<Box<dyn FnOnce() + Send + Sync>>,
        records: Vec<Record>
    }

    impl MockSubscriber {
        fn new() -> Self {
            Self {
                close_hook: None,
                records: vec![],
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

    impl Drop for MockSubscriber {
        fn drop(&mut self) {
            self.close_hook.take().unwrap()();
        }
    }

    #[tokio::test]
    async fn test_memory_log() {
        let subscriber = MockSubscriber::new();
        let clock = MockClock::new(vec![st_from_unix_epoch(0), st_from_unix_epoch(100), st_from_unix_epoch(200)]);
        let log = Arc::new(Mutex::new(MemoryLog::new(Box::new(clock))));
        let log_reader: Arc<dyn TxLogReader> = log.clone();
        let log_writer: Arc<Mutex<dyn TxLogWriter>> = log.clone();
        subscribe(log_reader, None, Box::new(subscriber));
        let mut writer = log_writer.lock().await;
        writer.append_tx(vec![1, 2, 3]).await;
        writer.append_tx(vec![4, 5, 6]).await;
        writer.append_tx(vec![7, 8, 9]).await;

        thread::sleep(Duration::from_millis(100));

        drop(subscriber);

        assert_eq!(subscriber.records.len(), 3);
        assert_eq!(subscriber.records[0], Record { tx_key: TxKey { tx_id: 0, system_time: st_from_unix_epoch(0) }, record: vec![1, 2, 3] });
        assert_eq!(subscriber.records[1], Record { tx_key: TxKey { tx_id: 1, system_time: st_from_unix_epoch(100) }, record: vec![4, 5, 6] });
        assert_eq!(subscriber.records[2], Record { tx_key: TxKey { tx_id: 2, system_time: st_from_unix_epoch(200) }, record: vec![7, 8, 9] });
    }
}
