use async_trait::async_trait;
use std::{cmp::min, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex}, thread};
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
pub(crate) trait Subscriber : Send + Sync {
    fn on_subscribe(&self, close_hook: Box<dyn FnOnce() + Send>);
    fn accept(&self, record: Record);
}

pub(crate) struct SubscriberHandler {
    latest_submitted_tx_id: Option<i64>,
    semaphores: Arc<Mutex<Vec<Arc<Semaphore>>>>
}

impl SubscriberHandler {
    pub fn new() -> Self {
        Self {
            latest_submitted_tx_id: None,
            semaphores: Arc::new(Mutex::new(vec![])),
        }
    }

    pub fn notify_tx(&mut self, tx_key: TxKey) {
        self.latest_submitted_tx_id = Some(tx_key.tx_id);
        for sem in self.semaphores.lock().unwrap().iter_mut() {
            sem.add_permits(1);
        }
    }

    pub fn subscribe(&mut self, log: &'static dyn TxLog, after_tx_id: Option<i64>, subscriber: Box<dyn Subscriber>) {
        let sem = Arc::new(Semaphore::new(0));
        self.semaphores.lock().unwrap().push(sem.clone());

        let latest_submitted_tx_id = self.latest_submitted_tx_id.clone();
        let semaphores = self.semaphores.clone();

        thread::spawn(move || {
            // TODO: Set thread priority to max

            // Consider using tokio::sync::oneshot to send stop signal
            let stop_flag = Arc::new(AtomicBool::new(false));
            let stop_flag_clone = stop_flag.clone();

            subscriber.on_subscribe(Box::new(move || {
                stop_flag_clone.store(true, Ordering::Relaxed);
            }));

            // Start processing transactions
            let mut after_tx_id = after_tx_id.unwrap_or(0);

            info!("Starting subscriber thread");

            loop {
                let txs_to_process: Vec<Record>;

                // Catching up: process transactions up to the latest submitted ID
                if latest_submitted_tx_id.is_some() && after_tx_id < latest_submitted_tx_id.unwrap() {
                    txs_to_process = log.read_txs(after_tx_id, 100)
                        .into_iter()
                        .filter(|tx| tx.tx_key.tx_id <= latest_submitted_tx_id.unwrap())
                        .collect();
                } else {
                // Live processing: process transactions from the latest submitted ID
                    sem.acquire();
                    let permits = sem.available_permits() + 1;
                    let to_process = min(permits, 100);
                    txs_to_process = log.read_txs(after_tx_id, to_process as u16);
                    sem.forget_permits(to_process);
                }

                for tx in txs_to_process {
                    // Is this extra check needed?
                    if stop_flag.load(Ordering::Relaxed) {
                        break;
                    }
                    after_tx_id = tx.tx_key.tx_id;
                    subscriber.accept(tx);
                }

                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
            }

            info!("Stopping subscriber thread");

            semaphores.lock().unwrap().retain(|semaphore| !Arc::ptr_eq(semaphore, &sem));
        });
    }
}


#[async_trait]
pub(crate) trait TxLog : Send + Sync + 'static {
    async fn append_tx(&mut self, record: Vec<u8>) -> TxKey;
    fn read_txs(&self, tx_id: i64, limit: u16) -> Vec<Record>;
    fn subscribe_txs(&mut self, after_tx_id: Option<i64>, subscriber: Box<dyn Subscriber>);
}
