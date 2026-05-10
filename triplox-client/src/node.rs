use anyhow::{Error, Result};
use edn::query::ParsedQuery;

use crate::ops::{QueryArg, TxOp};
use crate::query::QueryResult;
use crate::transaction::{TransactionResult, TxBasis, TxKey};

#[allow(async_fn_in_trait)]
pub trait SubmitNode {
    async fn submit_tx<O: IntoTxOp>(&self, ops: Vec<O>) -> Result<TxKey, Error>;
    async fn execute_tx<O: IntoTxOp>(&self, ops: Vec<O>) -> Result<TransactionResult, Error>;
}

/// Per-element conversion to `TxOp`. Lets `submit_tx`/`execute_tx` accept
/// either `Vec<TxOp>` or `Vec<&str>`/`Vec<String>` (each string parsed as one EDN tx op).
pub trait IntoTxOp {
    fn into_tx_op(self) -> Result<TxOp, Error>;
}

impl IntoTxOp for TxOp {
    fn into_tx_op(self) -> Result<TxOp, Error> {
        Ok(self)
    }
}

impl IntoTxOp for &str {
    fn into_tx_op(self) -> Result<TxOp, Error> {
        self.parse()
    }
}

impl IntoTxOp for String {
    fn into_tx_op(self) -> Result<TxOp, Error> {
        self.as_str().into_tx_op()
    }
}

pub fn collect_tx_ops<O: IntoTxOp>(ops: Vec<O>) -> Result<Vec<TxOp>, Error> {
    ops.into_iter().map(IntoTxOp::into_tx_op).collect()
}

pub trait IntoQuery {
    fn into_query(self) -> Result<ParsedQuery, Error>;
}

impl IntoQuery for ParsedQuery {
    fn into_query(self) -> Result<ParsedQuery, Error> {
        Ok(self)
    }
}

// TODO: consider using Cow or a lifetime on the trait to avoid cloning
impl IntoQuery for &ParsedQuery {
    fn into_query(self) -> Result<ParsedQuery, Error> {
        Ok(self.clone())
    }
}

impl IntoQuery for &str {
    fn into_query(self) -> Result<ParsedQuery, Error> {
        self.parse()
            .map_err(|e| anyhow::anyhow!("EDN parse error: {}", e))
    }
}

impl IntoQuery for String {
    fn into_query(self) -> Result<ParsedQuery, Error> {
        self.as_str().into_query()
    }
}

#[allow(async_fn_in_trait)]
pub trait Database {
    async fn query(&self, query: impl IntoQuery) -> Result<QueryResult, Error>;

    async fn query_with_args(
        &self,
        query: &ParsedQuery,
        args: &[QueryArg],
    ) -> Result<QueryResult, Error>;
}

#[allow(async_fn_in_trait)]
pub trait QueryNode {
    type DB: Database;
    async fn db(&self) -> Result<Self::DB, Error>;
    async fn db_as_of(&self, basis: TxBasis) -> Result<Self::DB, Error>;
}
