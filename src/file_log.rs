use crate::clock::SystemTimeSource;
use crate::log::{Record, TxId, TxLog, TxLogReader, TxLogWriter};
use crate::transaction::TxKey;
use anyhow::Result;
use log::{error, warn};
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::path::Path;
use tokio::sync::broadcast;

pub struct FileLog {
    file: BufWriter<File>,
    tx_sender: broadcast::Sender<Record>,
    clock: Box<dyn SystemTimeSource>,
}

impl FileLog {
    #[allow(unused)]
    pub fn new(path: &Path, clock: Box<dyn SystemTimeSource>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        Ok(FileLog {
            file: BufWriter::new(file),
            tx_sender: broadcast::channel(1024).0,
            clock,
        })
    }

    fn current_offset(&mut self) -> io::Result<i64> {
        Ok(self.file.stream_position()? as i64)
    }
}

impl TxLogReader for FileLog {
    fn read_txs_after(&self, after_tx_id: Option<TxId>, limit: u16) -> Result<Vec<Record>> {
        let mut file = self.file.get_ref().try_clone().unwrap();
        let mut records = Vec::new();

        match after_tx_id {
            None => {
                file.seek(SeekFrom::Start(0))?;
            }
            Some(id) => {
                file.seek(SeekFrom::Start(id as u64))?;
                // Skip the record at `id` — we've already processed it
                let _skipped: Result<Record, _> = bincode::deserialize_from(&mut file);
                if _skipped.is_err() {
                    return Ok(records); // nothing after this record
                }
            }
        }

        for _ in 0..limit {
            match bincode::deserialize_from(&mut file) {
                Ok(record) => records.push(record),
                Err(_) => break, // EOF or corrupted data
            }
        }

        Ok(records)
    }

    fn subscribe_txs(&self) -> (TxId, broadcast::Receiver<Record>) {
        let end_of_file = self.file.get_ref().seek(SeekFrom::End(0)).unwrap_or(0) as TxId;

        (end_of_file, self.tx_sender.subscribe())
    }
}

impl TxLogWriter for FileLog {
    async fn append_tx(&mut self, record: Vec<u8>) -> TxKey {
        let tx_id = self.current_offset().unwrap();

        let record = Record {
            tx_key: TxKey {
                tx_id,
                system_time: self.clock.now(),
            },
            record,
        };

        // Serialize and write the record
        bincode::serialize_into(&mut self.file, &record).unwrap();
        self.file.flush().unwrap();
        self.file.get_ref().sync_data().unwrap();

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
    use std::sync::Arc;
    use tempfile::tempdir;
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

        let log = Arc::new(RwLock::new(
            FileLog::new(&file_path, Box::new(clock)).unwrap(),
        ));
        let token = subscribe(log.clone(), None, subscriber.clone()).await;

        {
            let mut writer = log.write().await;
            writer.append_tx(vec![1, 2, 3]).await;
            writer.append_tx(vec![4, 5, 6]).await;
            writer.append_tx(vec![7, 8, 9]).await;
        }

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

        {
            let mut writer = log.write().await;
            writer.append_tx(vec![10, 11, 12]).await;
            writer.append_tx(vec![13, 14, 15]).await;
        }

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

        let log = Arc::new(RwLock::new(
            FileLog::new(&file_path, Box::new(clock)).unwrap(),
        ));

        // Write one transaction
        {
            let mut writer = log.write().await;
            writer.append_tx(vec![1, 2, 3]).await;
        }

        // Subscribe from the beginning — should process the one transaction exactly once
        let subscriber = Arc::new(RwLock::new(MockSubscriber::new()));
        let token = subscribe(log.clone(), None, subscriber.clone()).await;

        // Give it some time to process
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

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
