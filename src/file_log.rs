use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::path::Path;
use async_trait::async_trait;
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

#[async_trait]
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
            .seek(SeekFrom::Current(0))
            .unwrap_or(0) as TxId;
        
        (current_pos - 1, self.tx_sender.subscribe())
    }
}

#[async_trait]
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
    use std::sync::RwLock;
    use tempfile::tempdir;
    use crate::clock::{MockClock, st_from_unix_epoch};
    use crate::log::{MockSubscriber, subscribe};

    // Reuse your MockSubscriber implementation here...

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_file_log() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        
        let subscriber = Arc::new(RwLock::new(MockSubscriber::new()));
        let clock = MockClock::new(vec![
            st_from_unix_epoch(0),
            st_from_unix_epoch(100),
            st_from_unix_epoch(200)
        ]);
        
        let log = Arc::new(RwLock::new(FileLog::new(&file_path, Box::new(clock)).unwrap()));
        subscribe(log.clone(), None, subscriber.clone());

        let mut writer = log.write().unwrap();
        writer.append_tx(vec![1, 2, 3]).await;
        writer.append_tx(vec![4, 5, 6]).await;
        writer.append_tx(vec![7, 8, 9]).await;

        std::thread::sleep(std::time::Duration::from_millis(100));

        let mut subscriber = subscriber.write().unwrap();
        subscriber.close();

        assert_eq!(subscriber.records.len(), 3);
        assert_eq!(subscriber.records[0].record, vec![1, 2, 3]);
        assert_eq!(subscriber.records[1].record, vec![4, 5, 6]);
        assert_eq!(subscriber.records[2].record, vec![7, 8, 9]);

        // let tx_id_3 = subscriber.records[2].tx_key.tx_id;

        // // Create new clock for restarted log
        // let clock = MockClock::new(vec![
        //     st_from_unix_epoch(300),
        //     st_from_unix_epoch(400)
        // ]);

        // // Restart log and subscribe from second transaction
        // let subscriber2 = Arc::new(RwLock::new(MockSubscriber::new()));
        // let log2 = Arc::new(RwLock::new(FileLog::new(&file_path, Box::new(clock)).unwrap()));
        // subscribe(log2.clone(), Some(tx_id_3), subscriber2.clone()); // Subscribe after third transaction

        // let mut writer = log2.write().unwrap();
        // writer.append_tx(vec![10, 11, 12]).await;
        // writer.append_tx(vec![13, 14, 15]).await;

        // std::thread::sleep(std::time::Duration::from_millis(100));

        // let mut subscriber2 = subscriber2.write().unwrap();
        // subscriber2.close();

        // assert_eq!(subscriber2.records.len(), 3);
        // assert_eq!(subscriber2.records[0].record, vec![7, 8, 9]); // Third tx from first log
        // assert_eq!(subscriber2.records[1].record, vec![10, 11, 12]); // First tx after restart
        // assert_eq!(subscriber2.records[2].record, vec![13, 14, 15]); // Second tx after restart
    }
}