use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum TriploxError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("transaction {tx_id} was not indexed within {timeout:?}")]
    TxIndexingTimeout { tx_id: i64, timeout: Duration },
    #[error("No tx entity found for tx_id={tx_id} system_time={system_time}")]
    TxNotFound {
        tx_id: i64,
        system_time: crate::clock::Instant,
    },
}
