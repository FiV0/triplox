#![allow(unused)]
use serde::{Deserialize, Serialize};
use std::error::Error;
use time::OffsetDateTime;
use crate::clock::Instant;

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub struct TxKey {
    pub tx_id: i64,
    pub system_time: Instant
}

pub enum TransactionResult {
    TxCommited(TxKey),
    TxAborted(TxKey, Box<dyn Error>),
}
