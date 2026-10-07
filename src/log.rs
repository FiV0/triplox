#![allow(unused)]

use std::future::Future;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

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

pub(crate) static BOOTSTRAP_RECORD: LazyLock<Record> = LazyLock::new(|| Record {
    tx_key: *crate::bootstrap::BOOTSTRAP_TX_KEY,
    record: Vec::new(),
});

pub(crate) trait Subscriber: Send + Sync {
    fn accept(&mut self, record: Record) -> impl Future<Output = ()> + Send;
}

pub type TxId = i64;

const CATCH_UP_BATCH_SIZE: u16 = 100;
const CATCH_UP_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(100);
const CATCH_UP_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);

async fn catch_up_transactions<L: TxLogReader, S: Subscriber + 'static>(
    log: &L,
    last_tx_id: &mut Option<TxId>,
    subscriber: &Arc<tokio::sync::RwLock<S>>,
    max_records: Option<u64>,
    task_token: &CancellationToken,
) {
    let mut remaining = max_records;
    let mut retry_delay = CATCH_UP_RETRY_INITIAL_DELAY;

    loop {
        if task_token.is_cancelled() {
            break;
        }

        let read_limit = match remaining {
            Some(0) => break,
            Some(count) => count.min(CATCH_UP_BATCH_SIZE as u64) as u16,
            None => CATCH_UP_BATCH_SIZE,
        };

        let txs = log.read_txs_after(*last_tx_id, read_limit).await;
        match txs {
            Ok(txs) if txs.is_empty() => break,
            Ok(txs) => {
                retry_delay = CATCH_UP_RETRY_INITIAL_DELAY;
                let read_count = txs.len();
                trace!("Processing {} txs catching up", read_count);
                for tx in txs {
                    let tx_id = tx.tx_key.tx_id;
                    // Lock per tx so readers aren't blocked for the whole catch-up.
                    subscriber.write().await.accept(tx).await;
                    *last_tx_id = Some(tx_id);
                }
                if let Some(count) = remaining.as_mut() {
                    *count = count.saturating_sub(read_count as u64);
                }
                if read_count < read_limit as usize {
                    break;
                }
            }
            // We retry until the node gets shut down. Dying here would mean that the node
            // potentially starts with gap of transactions that are unprocessed. The
            // live phase accepts any later tx_id.
            Err(e) => {
                error!(
                    "Error reading txs during catch-up; retrying in {:?}: {}",
                    retry_delay, e
                );
                tokio::select! {
                    _ = task_token.cancelled() => break,
                    _ = tokio::time::sleep(retry_delay) => {}
                }
                retry_delay = std::cmp::min(retry_delay * 2, CATCH_UP_RETRY_MAX_DELAY);
            }
        }
    }
}

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
        catch_up_transactions(
            log.as_ref(),
            &mut last_tx_id,
            &subscriber,
            None,
            &task_token,
        )
        .await;

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
                            info!("Subscriber lagged by {} records; catching up from log", missed);
                            catch_up_transactions(
                                log.as_ref(),
                                &mut last_tx_id,
                                &subscriber,
                                Some(missed),
                                &task_token,
                            )
                            .await;
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
    /// Append a record to the log and return its assigned TxKey.
    /// An error does not guarantee the record was kept out of the log (e.g. a
    /// lost ack on a distributed log); blindly retrying may append it twice.
    fn append_tx(&self, record: Vec<u8>) -> impl Future<Output = Result<TxKey>> + Send;
}

pub trait TxLog: TxLogReader + TxLogWriter {
    fn ensure_bootstrap_record(&self) -> impl Future<Output = Result<()>> + Send;
}

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
    use crate::clock::{st_from_unix_epoch, SystemClock};
    use crate::memory_log::MemoryLog;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::{mpsc, Barrier, Notify, RwLock, Semaphore};

    // Counts the records returned by reads.
    struct CountingLog {
        inner: MemoryLog,
        records_read: AtomicUsize,
    }

    impl CountingLog {
        fn new(channel_capacity: usize) -> Self {
            Self {
                inner: MemoryLog::with_channel_capacity(Box::new(SystemClock), channel_capacity),
                records_read: AtomicUsize::new(0),
            }
        }
    }

    impl TxLogReader for CountingLog {
        async fn read_txs_after(
            &self,
            after_tx_id: Option<TxId>,
            limit: u16,
        ) -> Result<Vec<Record>> {
            let records = self.inner.read_txs_after(after_tx_id, limit).await?;
            self.records_read.fetch_add(records.len(), Ordering::SeqCst);
            Ok(records)
        }

        async fn subscribe_txs(&self) -> broadcast::Receiver<Record> {
            self.inner.subscribe_txs().await
        }
    }

    impl TxLogWriter for CountingLog {
        async fn append_tx(&self, record: Vec<u8>) -> Result<TxKey> {
            self.inner.append_tx(record).await
        }
    }

    struct SlowReadLog {
        inner: MemoryLog,
        read_started: Arc<Barrier>,
        release_read: Notify,
    }

    impl SlowReadLog {
        fn new() -> Self {
            Self {
                inner: MemoryLog::new(Box::new(SystemClock)),
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
            self.inner.read_txs_after(after_tx_id, limit).await
        }

        async fn subscribe_txs(&self) -> broadcast::Receiver<Record> {
            self.inner.subscribe_txs().await
        }
    }

    impl TxLogWriter for SlowReadLog {
        async fn append_tx(&self, record: Vec<u8>) -> Result<TxKey> {
            self.inner.append_tx(record).await
        }
    }

    struct SnapshotLog {
        records: Vec<Record>,
        tx_sender: broadcast::Sender<Record>,
    }

    impl SnapshotLog {
        fn new(record_count: usize) -> Self {
            let records = (0..record_count)
                .map(|id| Record {
                    tx_key: TxKey {
                        tx_id: id as TxId,
                        system_time: st_from_unix_epoch(id as u64),
                    },
                    record: vec![(id % 256) as u8],
                })
                .collect();

            Self {
                records,
                tx_sender: broadcast::channel(1).0,
            }
        }
    }

    impl TxLogReader for SnapshotLog {
        async fn read_txs_after(
            &self,
            after_tx_id: Option<TxId>,
            limit: u16,
        ) -> Result<Vec<Record>> {
            let start = after_tx_id.map(|id| id as usize + 1).unwrap_or(0);
            let end = std::cmp::min(start + limit as usize, self.records.len());
            if start >= self.records.len() {
                return Ok(vec![]);
            }
            Ok(self.records[start..end].to_vec())
        }

        async fn subscribe_txs(&self) -> broadcast::Receiver<Record> {
            self.tx_sender.subscribe()
        }
    }

    struct FlakyLog {
        inner: SnapshotLog,
        remaining_failures: AtomicUsize,
    }

    impl FlakyLog {
        fn new(record_count: usize, failures: usize) -> Self {
            Self {
                inner: SnapshotLog::new(record_count),
                remaining_failures: AtomicUsize::new(failures),
            }
        }
    }

    impl TxLogReader for FlakyLog {
        async fn read_txs_after(
            &self,
            after_tx_id: Option<TxId>,
            limit: u16,
        ) -> Result<Vec<Record>> {
            let remaining = self.remaining_failures.load(Ordering::SeqCst);
            if remaining > 0 {
                self.remaining_failures
                    .store(remaining.saturating_sub(1), Ordering::SeqCst);
                anyhow::bail!("transient read failure");
            }
            self.inner.read_txs_after(after_tx_id, limit).await
        }

        async fn subscribe_txs(&self) -> broadcast::Receiver<Record> {
            self.inner.subscribe_txs().await
        }
    }

    // Reports each tx as it enters `accept`, then blocks until the test adds a permit.
    struct GatedSubscriber {
        records: Vec<Record>,
        entered: mpsc::UnboundedSender<TxId>,
        gate: Arc<Semaphore>,
    }

    impl Subscriber for GatedSubscriber {
        async fn accept(&mut self, record: Record) {
            let _ = self.entered.send(record.tx_key.tx_id);
            self.gate.acquire().await.unwrap().forget();
            self.records.push(record);
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
            .expect("append should not wait for subscription read")
            .unwrap();

        log.release_read.notify_waiters();
        token.cancel();
    }

    #[tokio::test]
    async fn catch_up_handles_more_than_u16_max_records() {
        let log = SnapshotLog::new(u16::MAX as usize + 3);
        let subscriber = Arc::new(RwLock::new(MockSubscriber::new()));
        let token = CancellationToken::new();
        let mut last_tx_id = None;

        catch_up_transactions(
            &log,
            &mut last_tx_id,
            &subscriber,
            Some(u16::MAX as u64 + 3),
            &token,
        )
        .await;

        let subscriber = subscriber.read().await;
        assert_eq!(subscriber.records.len(), u16::MAX as usize + 3);
        assert_eq!(last_tx_id, Some(u16::MAX as TxId + 2));
    }

    #[tokio::test]
    async fn catch_up_stops_after_max_records() {
        let log = SnapshotLog::new(10);
        let subscriber = Arc::new(RwLock::new(MockSubscriber::new()));
        let token = CancellationToken::new();
        let mut last_tx_id = None;

        catch_up_transactions(&log, &mut last_tx_id, &subscriber, Some(3), &token).await;

        let subscriber = subscriber.read().await;
        assert_eq!(subscriber.records.len(), 3);
        assert_eq!(last_tx_id, Some(2));
    }

    // A transient read error must not end catch-up early: the live phase accepts
    // any later tx_id, so records skipped here would be lost permanently.
    #[tokio::test]
    async fn catch_up_retries_after_transient_read_error() {
        let log = FlakyLog::new(10, 2);
        let subscriber = Arc::new(RwLock::new(MockSubscriber::new()));
        let token = CancellationToken::new();
        let mut last_tx_id = None;

        catch_up_transactions(&log, &mut last_tx_id, &subscriber, None, &token).await;

        let subscriber = subscriber.read().await;
        assert_eq!(subscriber.records.len(), 10);
        assert_eq!(last_tx_id, Some(9));
    }

    #[tokio::test]
    async fn catch_up_retry_stops_on_cancellation() {
        let log = Arc::new(FlakyLog::new(10, usize::MAX));
        let subscriber = Arc::new(RwLock::new(MockSubscriber::new()));
        let token = CancellationToken::new();

        let task_token = token.clone();
        let task_log = log.clone();
        let task_subscriber = subscriber.clone();
        let handle = tokio::spawn(async move {
            let mut last_tx_id = None;
            catch_up_transactions(
                task_log.as_ref(),
                &mut last_tx_id,
                &task_subscriber,
                None,
                &task_token,
            )
            .await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        token.cancel();

        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("catch-up should stop on cancellation")
            .unwrap();
        assert!(subscriber.read().await.records.is_empty());
    }

    // A lagged catch-up reads in bounded chunks and lets readers in between txs.
    #[tokio::test]
    async fn lagged_catch_up_is_chunked_and_lets_readers_in() {
        let record_count = 3 * CATCH_UP_BATCH_SIZE as usize;
        let log = Arc::new(CountingLog::new(1));
        let gate = Arc::new(Semaphore::new(0));
        let (entered, mut entered_rx) = mpsc::unbounded_channel();
        let subscriber = Arc::new(RwLock::new(GatedSubscriber {
            records: vec![],
            entered,
            gate: gate.clone(),
        }));
        let token = subscribe(log.clone(), None, subscriber.clone()).await;

        // Block the startup catch-up on tx 0 while the remaining txs overflow the channel.
        log.append_tx(vec![]).await.unwrap();
        assert_eq!(entered_rx.recv().await, Some(0));
        for _ in 1..record_count {
            log.append_tx(vec![]).await.unwrap();
        }
        gate.add_permits(1);
        assert_eq!(entered_rx.recv().await, Some(1), "tx 1 is only in the log");
        // Only tx 0 and the first chunk have been read, not the whole lag.
        assert!(log.records_read.load(Ordering::SeqCst) <= 1 + CATCH_UP_BATCH_SIZE as usize);

        // Queue a reader behind the catch-up's write lock, then let one tx finish.
        let reader = subscriber.read();
        tokio::pin!(reader);
        assert!(futures::poll!(reader.as_mut()).is_pending());
        gate.add_permits(1);
        let accepted = tokio::time::timeout(Duration::from_secs(1), reader)
            .await
            .expect("reader should not wait for the whole catch-up")
            .records
            .len();
        assert_eq!(accepted, 2);

        gate.add_permits(record_count);
        tokio::time::timeout(Duration::from_secs(5), async {
            while subscriber.read().await.records.len() < record_count {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("catch-up should finish");
        token.cancel();

        let tx_ids: Vec<TxId> = subscriber
            .read()
            .await
            .records
            .iter()
            .map(|record| record.tx_key.tx_id)
            .collect();
        assert_eq!(tx_ids, (0..record_count as TxId).collect::<Vec<_>>());
    }
}
