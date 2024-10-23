use crate::clock::SystemTimeSource;
use crate::log::{Record, Subscriber, SubscriberHandler, TxLog};
use crate::transaction::TxKey;

struct MemoryLog<'a> {
    txs: Vec<Record>,
    subscriber_handler: SubscriberHandler,
    clock: &'a mut dyn SystemTimeSource
}

impl<'a> MemoryLog<'a> {
    pub fn new(clock: &mut dyn SystemTimeSource) -> Self {
        MemoryLog { txs: vec![], subscriber_handler: SubscriberHandler::new(), clock }
    }
}

impl<'a> TxLog for MemoryLog<'a> {
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

    fn subscribe_txs(&mut self, after_tx_id: Option<i64>, subscriber: impl Subscriber) {
        self.subscriber_handler.subscribe(self, after_tx_id, subscriber);
    }
}