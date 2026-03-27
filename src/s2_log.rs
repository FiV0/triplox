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

        let read_input = ReadInput::new()
            .with_start(
                ReadStart::new()
                    .with_from(ReadFrom::SeqNum(start_seq))
                    .with_clamp_to_tail(true),
            )
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
