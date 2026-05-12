use crate::clock::SystemTimeSource;
use crate::log::{Record, TxId, TxLog, TxLogReader, TxLogWriter};
use crate::transaction::TxKey;
use anyhow::Result;
use log::warn;
use std::io::{self, Cursor, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{broadcast, Mutex};

const READ_CHUNK_SIZE: usize = 8 * 1024;

pub struct FileLog {
    path: PathBuf,
    state: Mutex<FileLogState>,
    committed_len: AtomicU64,
    tx_sender: broadcast::Sender<Record>,
}

struct FileLogState {
    file: File,
    clock: Box<dyn SystemTimeSource>,
}

impl FileLog {
    pub async fn new(path: &Path, clock: Box<dyn SystemTimeSource>) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .await?;
        let committed_len = file.seek(SeekFrom::End(0)).await?;

        Ok(FileLog {
            path: path.to_path_buf(),
            state: Mutex::new(FileLogState { file, clock }),
            committed_len: AtomicU64::new(committed_len),
            tx_sender: broadcast::channel(1024).0,
        })
    }
}

impl TxLogReader for FileLog {
    async fn read_txs_after(&self, after_tx_id: Option<TxId>, limit: u16) -> Result<Vec<Record>> {
        let committed_len = self.committed_len.load(Ordering::Acquire);
        let mut file = OpenOptions::new().read(true).open(&self.path).await?;
        let mut records = Vec::new();

        let start = after_tx_id.map(|id| id as u64).unwrap_or(0);
        if start >= committed_len {
            return Ok(records);
        }

        file.seek(SeekFrom::Start(start)).await?;
        let mut bytes = Vec::new();
        let mut file_pos = start;
        let mut consumed = 0;
        let mut skip_next_record = after_tx_id.is_some();

        while records.len() < limit as usize {
            let record = loop {
                let before = consumed;
                let mut cursor = Cursor::new(&bytes);
                cursor.set_position(before);

                match bincode::deserialize_from(&mut cursor) {
                    Ok(record) => {
                        consumed = cursor.position();
                        break Some(record);
                    }
                    Err(_) if file_pos < committed_len => {
                        let remaining = (committed_len - file_pos) as usize;
                        let read_len = remaining.min(READ_CHUNK_SIZE);
                        let previous_len = bytes.len();

                        bytes.resize(previous_len + read_len, 0);
                        file.read_exact(&mut bytes[previous_len..]).await?;
                        file_pos += read_len as u64;
                        consumed = before;
                    }
                    Err(_) => break None,
                }
            };

            let Some(record) = record else {
                break;
            };

            if skip_next_record {
                skip_next_record = false;
            } else {
                records.push(record);
            }
        }

        Ok(records)
    }

    async fn subscribe_txs(&self) -> broadcast::Receiver<Record> {
        self.tx_sender.subscribe()
    }
}

impl TxLogWriter for FileLog {
    async fn append_tx(&self, record: Vec<u8>) -> TxKey {
        let mut state = self.state.lock().await;
        let tx_id = self.committed_len.load(Ordering::Acquire) as TxId;

        let record = Record {
            tx_key: TxKey {
                tx_id,
                system_time: state.clock.now(),
            },
            record,
        };

        let bytes = bincode::serialize(&record).expect("failed to serialize file log record");
        state
            .file
            .write_all(&bytes)
            .await
            .expect("failed to write file log record");
        state
            .file
            .flush()
            .await
            .expect("failed to flush file log record");
        state
            .file
            .sync_data()
            .await
            .expect("failed to sync file log record");
        self.committed_len
            .store(tx_id as u64 + bytes.len() as u64, Ordering::Release);
        drop(state);

        // Notify subscribers
        if let Err(e) = self.tx_sender.send(record.clone()) {
            warn!("Failed to send record from file log to subscribers: {}", e);
        }

        record.tx_key
    }
}

impl TxLog for FileLog {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{st_from_unix_epoch, MockClock};
    use crate::log::{subscribe, MockSubscriber};
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;
    use tokio::sync::RwLock;

    use crate::logging::init;
    // Reuse your MockSubscriber implementation here...

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_file_log() {
        init();
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.log");

        let subscriber = Arc::new(RwLock::new(MockSubscriber::new()));
        let clock = MockClock::new(vec![
            st_from_unix_epoch(0),
            st_from_unix_epoch(100),
            st_from_unix_epoch(200),
            st_from_unix_epoch(300),
            st_from_unix_epoch(400),
        ]);

        let log = Arc::new(FileLog::new(&file_path, Box::new(clock)).await.unwrap());
        let token = subscribe(log.clone(), None, subscriber.clone()).await;

        log.append_tx(vec![1, 2, 3]).await;
        log.append_tx(vec![4, 5, 6]).await;
        log.append_tx(vec![7, 8, 9]).await;

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        token.cancel();

        let subscriber = subscriber.read().await;

        assert_eq!(subscriber.records.len(), 3);
        assert_eq!(subscriber.records[0].record, vec![1, 2, 3]);
        assert_eq!(subscriber.records[1].record, vec![4, 5, 6]);
        assert_eq!(subscriber.records[2].record, vec![7, 8, 9]);

        let tx_id_1 = subscriber.records[1].tx_key.tx_id;
        drop(subscriber);

        // Restart log and subscribe after second transaction
        let subscriber2 = Arc::new(RwLock::new(MockSubscriber::new()));
        let token2 = subscribe(log.clone(), Some(tx_id_1), subscriber2.clone()).await; // Subscribe after second transaction

        log.append_tx(vec![10, 11, 12]).await;
        log.append_tx(vec![13, 14, 15]).await;

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        token2.cancel();

        let subscriber2 = subscriber2.read().await;

        assert_eq!(subscriber2.records.len(), 3);
        assert_eq!(subscriber2.records[0].record, vec![7, 8, 9]); // Third tx from first log
        assert_eq!(subscriber2.records[1].record, vec![10, 11, 12]); // First tx after restart
        assert_eq!(subscriber2.records[2].record, vec![13, 14, 15]); // Second tx after restart
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_file_log_infinite_loop_bug() {
        init();
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_infinite_loop.log");

        let clock = MockClock::new(vec![st_from_unix_epoch(0), st_from_unix_epoch(100)]);

        let log = Arc::new(FileLog::new(&file_path, Box::new(clock)).await.unwrap());

        // Write one transaction
        log.append_tx(vec![1, 2, 3]).await;

        // Subscribe from the beginning — should process the one transaction exactly once
        let subscriber = Arc::new(RwLock::new(MockSubscriber::new()));
        let token = subscribe(log.clone(), None, subscriber.clone()).await;

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if !subscriber.read().await.records.is_empty() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("timed out waiting for subscriber to process first record");
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;

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

    #[tokio::test]
    async fn test_file_log_reopen_appends_at_end() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_reopen_append.log");

        let log = FileLog::new(
            &file_path,
            Box::new(MockClock::new(vec![st_from_unix_epoch(0)])),
        )
        .await
        .unwrap();
        let _receiver = log.subscribe_txs().await;
        log.append_tx(vec![1, 2, 3]).await;
        drop(log);

        let log = FileLog::new(
            &file_path,
            Box::new(MockClock::new(vec![st_from_unix_epoch(100)])),
        )
        .await
        .unwrap();
        log.append_tx(vec![4, 5, 6]).await;

        let records = log.read_txs_after(None, u16::MAX).await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].record, vec![1, 2, 3]);
        assert_eq!(records[1].record, vec![4, 5, 6]);
    }

    #[tokio::test]
    async fn test_file_log_ignores_bytes_past_committed_len() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_committed_len.log");

        let log = FileLog::new(
            &file_path,
            Box::new(MockClock::new(vec![st_from_unix_epoch(0)])),
        )
        .await
        .unwrap();
        log.append_tx(vec![1, 2, 3]).await;

        let mut external = OpenOptions::new()
            .append(true)
            .open(&file_path)
            .await
            .unwrap();
        external.write_all(b"not a committed record").await.unwrap();
        external.flush().await.unwrap();

        let records = log.read_txs_after(None, u16::MAX).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record, vec![1, 2, 3]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_file_log_concurrent_appends_are_sequential() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_concurrent_append.log");
        let append_count = 32usize;
        let clock = MockClock::new(
            (0..append_count)
                .map(|id| st_from_unix_epoch(id as u64))
                .collect(),
        );
        let log = Arc::new(FileLog::new(&file_path, Box::new(clock)).await.unwrap());
        let _receiver = log.subscribe_txs().await;

        let mut handles = Vec::new();
        for id in 0..append_count {
            let log = log.clone();
            handles.push(tokio::spawn(
                async move { log.append_tx(vec![id as u8]).await },
            ));
        }

        let mut appended_tx_ids = BTreeSet::new();
        for handle in handles {
            appended_tx_ids.insert(handle.await.unwrap().tx_id);
        }

        let records = log.read_txs_after(None, u16::MAX).await.unwrap();
        let record_tx_ids: BTreeSet<_> = records.iter().map(|record| record.tx_key.tx_id).collect();

        assert_eq!(records.len(), append_count);
        assert_eq!(appended_tx_ids, record_tx_ids);
        assert!(records
            .windows(2)
            .all(|window| window[0].tx_key.tx_id < window[1].tx_key.tx_id));
    }
}
