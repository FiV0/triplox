use std::{cmp::min, sync::{atomic::{AtomicBool, Ordering}, Arc}, thread};
use log::info;

use serde::{Deserialize, Serialize};
use crate::transaction::TxKey;
use tokio::sync::Semaphore;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct Record {
    pub tx_key: TxKey,
    pub record: Vec<u8>,
}

#[allow(unused)]
pub(crate) trait Subscriber {
    fn on_subscribe(&self, close_hook: impl FnOnce());
    fn accept(&self, record: Record);
}

pub(crate) struct SubscriberHandler {
    latest_submitted_tx_id: Option<i64>,
    semaphores: Vec<Semaphore>
}

impl SubscriberHandler {
    fn new() -> Self {
        Self {
            latest_submitted_tx_id: None,
            semaphores: vec![],
        }
    }

    pub fn notify_tx(&mut self, tx_key: TxKey) {
        self.latest_submitted_tx_id = Some(tx_key.tx_id);
        for sem in self.semaphores.iter_mut() {
            sem.add_permits(1);
        }
    }

    pub fn subscribe<T: TxLog>(&mut self, log: &T, after_tx_id: Option<i64>, subscriber: impl Subscriber) {
        let sem = Semaphore::new(0);
        self.semaphores.push(sem);

        thread::spawn(move || {
            // TODO: Set thread priority to max

            let stop_flag = Arc::new(AtomicBool::new(false));
            let stop_flag_clone = stop_flag.clone();

            subscriber.on_subscribe(|| {
                stop_flag_clone.store(true, Ordering::Relaxed);
            });

            // Start processing transactions
            let mut after_tx_id = after_tx_id;

            info!("Starting subscriber thread");

            loop {
                let mut txs_to_process: Vec<Record>;

                // Catching up: process transactions up to the latest submitted ID
                if self.latest_submitted_tx_id.is_some() && (after_tx_id.is_none() || after_tx_id.unwrap() < self.latest_submitted_tx_id.unwrap()) {
                    txs_to_process = log.read_txs(after_tx_id.unwrap(), 100)
                        .iter()
                        .filter(|tx| tx.tx_key.tx_id <= self.latest_submitted_tx_id.unwrap())
                        .collect();
                } else {
                // Live processing: process transactions from the latest submitted ID
                    sem.acquire();
                    let permits = sem.available_permits() + 1;
                    let to_process = min(permits, 100);
                    txs_to_process = log.read_txs(after_tx_id.unwrap(), to_process);
                    sem.forget_permits(to_process);
                }

                for tx in txs_to_process {
                    // Is this extra check needed?
                    if stop_flag.load(Ordering::Relaxed) {
                        break;
                    }
                    subscriber.accept(tx);
                    self.latest_submitted_tx_id = Some(tx.tx_key.tx_id);
                }

                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
            }

            info!("Stopping subscriber thread");

            self.semaphores.retain(|semaphore| semaphore != &sem);
        });
    }
}


pub(crate) trait TxLog {
    async fn append_tx(&mut self, record: Vec<u8>) -> TxKey;
    fn read_txs(&self, tx_id: i64, limit: u16) -> Vec<Record>;
    fn subscribe_txs(&self, after_tx_id: Option<i64>, subscriber: impl Subscriber);
}
