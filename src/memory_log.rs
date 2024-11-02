#![allow(unused)]

use std::cmp::min;

use async_trait::async_trait;
use log::warn;
use tokio::sync::broadcast;
use crate::clock::SystemTimeSource;
use crate::log::{Record, TxLog};
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
impl TxLog for MemoryLog {
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
            // Log the error but continue - this isn't fatal
            warn!("Failed to send record from memory log to subscribers: {}", e);
        }
        tx_key
    }

    fn read_txs(&self, tx_id: TxId, limit: u16) -> Vec<Record> {
        let available_records = min(self.txs.len().saturating_sub(tx_id as usize), limit as usize);
        self.txs[tx_id as usize..tx_id as usize + available_records as usize].to_vec()
    }

    // TODO: There is potentially a race here, but in the worst we read 
    // a record once while catching up and once from the receiver.
    fn subscribe_txs(&self) -> (TxId, broadcast::Receiver<Record>) {
        (self.txs.len() as TxId - 1, self.tx_sender.subscribe())
    }
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn test_memory_log() {
        let subscriber = MockSubscriber::new();
        let clock = MockClock::new(vec![st_from_unix_epoch(0), st_from_unix_epoch(100), st_from_unix_epoch(200)]);
        let mut log = Arc::new(MemoryLog::new(Box::new(clock)));
        subscribe(log.clone(), None, Box::new(subscriber));
        log.append_tx(vec![1, 2, 3]);
        log.append_tx(vec![4, 5, 6]);
        log.append_tx(vec![7, 8, 9]);

        thread::sleep(Duration::from_millis(100));

        drop(subscriber);

        assert_eq!(subscriber.records.len(), 3);
        assert_eq!(subscriber.records[0], Record { tx_key: TxKey { tx_id: 0, system_time: st_from_unix_epoch(0) }, record: vec![1, 2, 3] });
        assert_eq!(subscriber.records[1], Record { tx_key: TxKey { tx_id: 1, system_time: st_from_unix_epoch(100) }, record: vec![4, 5, 6] });
        assert_eq!(subscriber.records[2], Record { tx_key: TxKey { tx_id: 2, system_time: st_from_unix_epoch(200) }, record: vec![7, 8, 9] });
    }
}
