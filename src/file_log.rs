use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::path::Path;
use tokio::sync::broadcast;
use log::{error, warn};
use crate::clock::SystemTimeSource;
use crate::log::{Record, TxLog, TxLogReader, TxLogWriter, TxId};
use crate::transaction::TxKey;
use anyhow::Result;

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
            .open(path)?;
        
        Ok(FileLog {
            file: BufWriter::new(file),
            tx_sender: broadcast::channel(1024).0,
            clock,
        })
    }

    fn current_offset(&mut self) -> io::Result<i64> {
        Ok(self.file.seek(SeekFrom::Current(0))? as i64)
    }
}

impl TxLogReader for FileLog {
    fn read_txs(&self, tx_id: TxId, limit: u16) -> Result<Vec<Record>> {
        let mut file = self.file.get_ref().try_clone().unwrap();
        let mut records = Vec::new();
        
        // Seek to the position indicated by tx_id
        if let Err(e) = file.seek(SeekFrom::Start(tx_id as u64)) {
            error!("Failed to seek to position {}: {}", tx_id, e);
            return Err(e.into());
        }

        // Read records until we hit limit or EOF
        for _ in 0..limit {
            match bincode::deserialize_from(&mut file) {
                Ok(record) => records.push(record),
                Err(_) => break, // EOF or corrupted data
            }
        }

        Ok(records)
    }

    fn subscribe_txs(&self) -> (TxId, broadcast::Receiver<Record>) {
        let current_pos = self.file.get_ref()
            .seek(SeekFrom::End(0))
            .unwrap_or(0) as TxId;
        
        (current_pos - 1, self.tx_sender.subscribe())
    }
}

impl TxLogWriter for FileLog {
    async fn append_tx(&mut self, record: Vec<u8>) -> TxKey {
        let tx_id = self.current_offset().unwrap();
        
        let record = Record {
            tx_key: TxKey {
                tx_id,
                system_time: self.clock.now()
            },
            record,
        };

        // Serialize and write the record
        bincode::serialize_into(&mut self.file, &record).unwrap();
        self.file.flush().unwrap();

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
    use std::sync::Arc;
    use tempfile::tempdir;
    use crate::clock::{MockClock, st_from_unix_epoch};
    use crate::log::{subscribe, MockSubscriber};
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
            st_from_unix_epoch(400)

        ]);

        let log = Arc::new(std::sync::RwLock::new(FileLog::new(&file_path, Box::new(clock)).unwrap()));
        let token = subscribe(log.clone(), None, subscriber.clone());

        {
            let mut writer = log.write().unwrap();
            writer.append_tx(vec![1, 2, 3]).await;
            writer.append_tx(vec![4, 5, 6]).await;
            writer.append_tx(vec![7, 8, 9]).await;
        }

        std::thread::sleep(std::time::Duration::from_millis(100));

        token.cancel();

        let subscriber = subscriber.read().await;

        assert_eq!(subscriber.records.len(), 3);
        assert_eq!(subscriber.records[0].record, vec![1, 2, 3]);
        assert_eq!(subscriber.records[1].record, vec![4, 5, 6]);
        assert_eq!(subscriber.records[2].record, vec![7, 8, 9]);

        let tx_id_2 = subscriber.records[2].tx_key.tx_id;
        drop(subscriber);

        // Restart log and subscribe from second transaction
        let subscriber2 = Arc::new(RwLock::new(MockSubscriber::new()));
        let token2 = subscribe(log.clone(), Some(tx_id_2), subscriber2.clone()); // Subscribe after third transaction

        {
            let mut writer = log.write().unwrap();
            writer.append_tx(vec![10, 11, 12]).await;
            writer.append_tx(vec![13, 14, 15]).await;
        }

        std::thread::sleep(std::time::Duration::from_millis(100));

        token2.cancel();

        let subscriber2 = subscriber2.read().await;

        assert_eq!(subscriber2.records.len(), 3);
        assert_eq!(subscriber2.records[0].record, vec![7, 8, 9]); // Third tx from first log
        assert_eq!(subscriber2.records[1].record, vec![10, 11, 12]); // First tx after restart
        assert_eq!(subscriber2.records[2].record, vec![13, 14, 15]); // Second tx after restart
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_file_log_infinite_loop_bug() {
        // This test specifically reproduces the infinite loop bug where after_tx_id == latest_tx_id
        // The bug would cause the same transaction to be processed repeatedly in a loop
        init();

        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_infinite_loop.log");

        let clock = MockClock::new(vec![
            st_from_unix_epoch(0),
            st_from_unix_epoch(100),
            st_from_unix_epoch(200),
            st_from_unix_epoch(300),
        ]);

        let log = Arc::new(std::sync::RwLock::new(FileLog::new(&file_path, Box::new(clock)).unwrap()));

        // Write 3 transactions (tx_id will be file offsets)
        let tx_id_2 = {
            let mut writer = log.write().unwrap();
            writer.append_tx(vec![1]).await;
            writer.append_tx(vec![2]).await;
            writer.append_tx(vec![3]).await.tx_id
        };

        // Subscribe starting from the last transaction
        // This creates the scenario where after_tx_id == latest_tx_id
        // Without the fix, this would cause an infinite loop processing the same transaction repeatedly
        let subscriber = Arc::new(RwLock::new(MockSubscriber::new()));
        let token = subscribe(log.clone(), Some(tx_id_2), subscriber.clone());

        // Add a new transaction after subscribing
        {
            let mut writer = log.write().unwrap();
            writer.append_tx(vec![4]).await;
        }

        // Wait for subscriber to process
        std::thread::sleep(std::time::Duration::from_millis(100));

        token.cancel();

        let subscriber = subscriber.read().await;

        // Should have processed exactly 2 records:
        // 1. The catch-up transaction at tx_id_2 (vec![3])
        // 2. The new live transaction (vec![4])
        // NOT infinite copies of the catch-up transaction
        assert_eq!(subscriber.records.len(), 2);
        assert_eq!(subscriber.records[0].record, vec![3]);
        assert_eq!(subscriber.records[1].record, vec![4]);
    }
}