use serde::{Deserialize, Serialize};
use std::error::Error;

pub type Instant = chrono::DateTime<chrono::Utc>;

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TxKey {
    pub tx_id: i64,
    pub system_time: Instant,
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TxBasis {
    pub tx_key: TxKey,
    pub tx_eid: i64,
}

#[derive(Debug)]
pub enum TransactionResult {
    TxCommited(TxBasis),
    TxAborted(TxBasis, Box<dyn Error>),
}
