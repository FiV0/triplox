use std::sync::Arc;

use async_trait::async_trait;
use crate::clock::SystemTimeSource;
use crate::log::{Record, Subscriber, SubscriberHandler, TxLog};
use crate::transaction::TxKey;

struct MemoryLog {
    txs: Vec<Record>,
    subscriber_handler: SubscriberHandler,
    clock: &'static mut dyn SystemTimeSource
}

impl MemoryLog {
    pub fn new(clock: &'static mut dyn SystemTimeSource) -> Self {
        MemoryLog { txs: vec![], subscriber_handler: SubscriberHandler::new(), clock }
    }
}

#[async_trait]
impl TxLog for MemoryLog {
    async fn append_tx(&mut self, record: Vec<u8>) -> TxKey {
        self.txs.push(Record {
            tx_key: TxKey {
                tx_id: self.txs.len() as i64,
                system_time: self.clock.now()
            },
            record
        });
        self.subscriber_handler.notify_tx(self.txs.last().unwrap().tx_key);
        self.txs.last().unwrap().tx_key
    }

    fn read_txs(&self, tx_id: i64, limit: u16) -> Vec<Record> {
        self.txs[tx_id as usize..tx_id as usize + limit as usize].to_vec()
    }

    fn subscribe_txs(&mut self, after_tx_id: Option<i64>, subscriber: Box<dyn Subscriber>) {
        self.subscriber_handler.subscribe(self, after_tx_id, subscriber);
    }
}
