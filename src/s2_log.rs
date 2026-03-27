#![allow(unused)]

use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use log::warn;
use tokio::sync::broadcast;

use s2_sdk::types::{
    AppendInput, AppendRecord, AppendRecordBatch, ReadFrom, ReadInput, ReadLimits, ReadStart,
    ReadStop, S2Config,
};
use s2_sdk::{S2, S2Basin, S2Stream};

use crate::log::{Record, TxId, TxLog, TxLogReader, TxLogWriter};
use crate::transaction::TxKey;

fn s2_timestamp_to_instant(ts_ms: u64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ts_ms as i64).unwrap_or_else(Utc::now)
}

pub struct S2Log {
    stream: S2Stream,
    tx_sender: broadcast::Sender<Record>,
}

impl S2Log {
    pub fn new(stream: S2Stream) -> Self {
        S2Log {
            stream,
            tx_sender: broadcast::channel(1024).0,
        }
    }
}

impl TxLogReader for S2Log {
    async fn read_txs_after(&self, after_tx_id: Option<TxId>, limit: u16) -> Result<Vec<Record>> {
        let start_seq = match after_tx_id {
            None => 0u64,
            Some(id) => (id + 1) as u64,
        };

        // Check tail to avoid reading past the end of the stream
        let tail = self.stream.check_tail().await?.seq_num;
        if start_seq >= tail {
            return Ok(vec![]);
        }

        let read_input = ReadInput::new()
            .with_start(ReadStart::new().with_from(ReadFrom::SeqNum(start_seq)))
            .with_stop(ReadStop::new().with_limits(ReadLimits::new().with_count(limit as usize)));

        let batch = self.stream.read(read_input).await?;

        let mut records = Vec::with_capacity(batch.records.len());
        for sr in batch.records {
            records.push(Record {
                tx_key: TxKey {
                    tx_id: sr.seq_num as i64,
                    system_time: s2_timestamp_to_instant(sr.timestamp),
                },
                record: sr.body.to_vec(),
            });
        }

        Ok(records)
    }

    async fn subscribe_txs(&self) -> (TxId, broadcast::Receiver<Record>) {
        let tail_seq = self
            .stream
            .check_tail()
            .await
            .map(|pos| pos.seq_num as TxId)
            .unwrap_or(0);
        (tail_seq, self.tx_sender.subscribe())
    }
}

impl TxLogWriter for S2Log {
    async fn append_tx(&mut self, record: Vec<u8>) -> TxKey {
        let append_record = AppendRecord::new(record.clone()).expect("record should be valid");
        let batch =
            AppendRecordBatch::try_from_iter([append_record]).expect("single record batch valid");
        let ack = self
            .stream
            .append(AppendInput::new(batch))
            .await
            .expect("S2 append failed");

        let tx_key = TxKey {
            tx_id: ack.start.seq_num as i64,
            system_time: s2_timestamp_to_instant(ack.start.timestamp),
        };

        let log_record = Record {
            tx_key,
            record,
        };

        if let Err(e) = self.tx_sender.send(log_record) {
            warn!("Failed to send record from S2 log to subscribers: {}", e);
        }

        tx_key
    }
}

impl TxLog for S2Log {}

#[cfg(all(test, feature = "s2-integration-test"))]
mod tests {
    use super::*;
    use std::process::{Command, Stdio};
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::net::TcpListener;
    use tokio::sync::RwLock;

    use s2_sdk::types::{
        AccountEndpoint, BasinEndpoint, CreateBasinInput, CreateStreamInput, S2Config, S2Endpoints,
    };
    use s2_sdk::S2;

    use crate::log::{subscribe, MockSubscriber};
    use crate::logging::init;

    const S2_LITE_BINARY: &str = "server";

    fn install_crypto_provider() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }

    async fn start_s2_lite() -> Option<(std::process::Child, u16)> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let child = Command::new(S2_LITE_BINARY)
            .args(["--port", &port.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();

        match child {
            Ok(child) => {
                // Poll until server accepts TCP connections
                for _ in 0..50 {
                    if TcpListener::bind(("127.0.0.1", port)).await.is_err() {
                        // Port is in use = server is listening
                        return Some((child, port));
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                panic!("s2-lite failed to start on port {port}");
            }
            Err(_) => {
                eprintln!("s2-lite binary '{S2_LITE_BINARY}' not found, skipping test");
                None
            }
        }
    }

    async fn setup_s2_stream(port: u16, stream_name: &str) -> S2Stream {
        let endpoint = format!("http://127.0.0.1:{port}");
        let endpoints = S2Endpoints::new(
            AccountEndpoint::new(&endpoint).unwrap(),
            BasinEndpoint::new(&endpoint).unwrap(),
        )
        .unwrap();
        let config = S2Config::new("ignored").with_endpoints(endpoints);
        let s2 = S2::new(config).unwrap();

        let basin_name: s2_sdk::types::BasinName =
            format!("test-{}", uuid::Uuid::new_v4()).parse().unwrap();
        s2.create_basin(CreateBasinInput::new(basin_name.clone()))
            .await
            .unwrap();
        let basin = s2.basin(basin_name);

        let stream_name: s2_sdk::types::StreamName = stream_name.parse().unwrap();
        basin
            .create_stream(CreateStreamInput::new(stream_name.clone()))
            .await
            .unwrap();
        basin.stream(stream_name)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_s2_log() {
        init();
        install_crypto_provider();
        let Some((mut child, port)) = start_s2_lite().await else {
            return;
        };

        let stream = setup_s2_stream(port, "log").await;
        let subscriber = Arc::new(RwLock::new(MockSubscriber::new()));
        let log = Arc::new(RwLock::new(S2Log::new(stream)));
        let token = subscribe(log.clone(), None, subscriber.clone()).await;

        {
            let mut writer = log.write().await;
            writer.append_tx(vec![1, 2, 3]).await;
            writer.append_tx(vec![4, 5, 6]).await;
            writer.append_tx(vec![7, 8, 9]).await;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
        token.cancel();

        let subscriber = subscriber.read().await;
        assert_eq!(subscriber.records.len(), 3);
        assert_eq!(subscriber.records[0].record, vec![1, 2, 3]);
        assert_eq!(subscriber.records[1].record, vec![4, 5, 6]);
        assert_eq!(subscriber.records[2].record, vec![7, 8, 9]);

        let tx_id_1 = subscriber.records[1].tx_key.tx_id;
        drop(subscriber);

        // Subscribe after second transaction
        let subscriber2 = Arc::new(RwLock::new(MockSubscriber::new()));
        let token2 = subscribe(log.clone(), Some(tx_id_1), subscriber2.clone()).await;

        {
            let mut writer = log.write().await;
            writer.append_tx(vec![10, 11, 12]).await;
            writer.append_tx(vec![13, 14, 15]).await;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
        token2.cancel();

        let subscriber2 = subscriber2.read().await;
        assert_eq!(subscriber2.records.len(), 3);
        assert_eq!(subscriber2.records[0].record, vec![7, 8, 9]);
        assert_eq!(subscriber2.records[1].record, vec![10, 11, 12]);
        assert_eq!(subscriber2.records[2].record, vec![13, 14, 15]);

        child.kill().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_s2_log_infinite_loop_bug() {
        init();
        install_crypto_provider();
        let Some((mut child, port)) = start_s2_lite().await else {
            return;
        };

        let stream = setup_s2_stream(port, "log").await;
        let log = Arc::new(RwLock::new(S2Log::new(stream)));

        // Write one transaction
        {
            let mut writer = log.write().await;
            writer.append_tx(vec![1, 2, 3]).await;
        }

        // Subscribe from the beginning
        let subscriber = Arc::new(RwLock::new(MockSubscriber::new()));
        let token = subscribe(log.clone(), None, subscriber.clone()).await;

        tokio::time::sleep(Duration::from_millis(200)).await;
        token.cancel();

        let subscriber = subscriber.read().await;
        assert_eq!(
            subscriber.records.len(),
            1,
            "Should process transaction exactly once, not in an infinite loop"
        );
        assert_eq!(subscriber.records[0].record, vec![1, 2, 3]);

        child.kill().unwrap();
    }
}
