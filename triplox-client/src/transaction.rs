#![allow(unused)]
use serde::{Deserialize, Serialize};
use std::error::Error;

pub type Instant = chrono::DateTime<chrono::Utc>;

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TxKey {
    pub tx_id: i64,
    pub system_time: Instant,
}

#[derive(Debug)]
pub enum TransactionResult {
    TxCommited(TxKey),
    TxAborted(TxKey, Box<dyn Error>),
}
