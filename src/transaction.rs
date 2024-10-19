#![allow(unused)]
use serde::{Deserialize, Serialize};
use std::error::Error;
use time::OffsetDateTime;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Instant(u64);

impl Instant {
    pub fn now() -> Self {
        let now = OffsetDateTime::now_utc();
        let millis_since_epoch = now.unix_timestamp() * 1000 + now.millisecond() as i64;
        Instant(millis_since_epoch.try_into().unwrap())
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub struct TxKey {
    pub tx_id: i64,
    pub system_time: Instant
}

pub enum TransactionResult {
    TxCommited(TxKey),
    TxAborted(TxKey, Box<dyn Error>),
}
