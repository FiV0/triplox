use std::error::Error;
use std::time::Instant;

pub struct TxKey {
    pub tx_id: i64,
    pub system_time: Instant
}

pub struct TxCommited(TxKey);
pub struct TxAborted(TxKey, dyn Error);