#![allow(unused)]
use serde::{Deserialize, Serialize};
use std::error::Error;
use time::OffsetDateTime;
use crate::clock::Instant;

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TxKey {
    pub tx_id: i64,
    pub system_time: Instant
}

#[derive(Clone, Debug)]
pub struct Basis {
    pub tx_key: TxKey,
    pub seq_num: u64,
}

pub enum TransactionResult {
    TxCommited(Basis),
    TxAborted(TxKey, Box<dyn Error>),
}
