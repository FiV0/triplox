use anyhow::{Error, Result};
use edn::query::ParsedQuery;

use crate::ops::{QueryArg, TxOp};
use crate::query::QueryResult;
use crate::transaction::{TransactionResult, TxKey};

#[allow(async_fn_in_trait)]
/// A node that accepts Triplox transactions.
pub trait SubmitNode {
    /// Appends one transaction for asynchronous processing.
    ///
    /// Each item in `ops` is converted to a [`TxOp`] through [`IntoTxOp`].
    /// The returned [`TxKey`] identifies the appended transaction, but does not
    /// mean that it has been indexed or committed. Use
    /// [`execute_tx`](SubmitNode::execute_tx) when the transaction outcome is
    /// required.
    ///
    /// Returns an error if an operation cannot be converted or the transaction
    /// cannot be appended.
    async fn submit_tx<O: IntoTxOp>(&self, ops: Vec<O>) -> Result<TxKey, Error>;

    /// Appends one transaction and waits for its indexed outcome.
    ///
    /// Committed and aborted transactions are both normal outcomes returned
    /// inside `Ok` as [`TransactionResult::TxCommitted`] and
    /// [`TransactionResult::TxAborted`], respectively.
    ///
    /// Returns an error if an operation cannot be converted, the transaction
    /// cannot be appended, or processing fails before an outcome is available.
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
/// A read-only database value pinned to one indexed transaction basis.
pub trait Database {
    /// Executes a query against this database basis without input arguments.
    ///
    /// `query` may be a parsed query or an EDN query accepted by [`IntoQuery`].
    /// Use [`query_with_args`](Database::query_with_args) for a query with
    /// `:in` bindings.
    ///
    /// Returns an error if the query cannot be parsed, validated, or executed.
    async fn query(&self, query: impl IntoQuery) -> Result<QueryResult, Error>;

    /// Executes a query against this database basis with input arguments.
    ///
    /// `query` may be a parsed query or an EDN query accepted by [`IntoQuery`].
    /// Arguments correspond positionally to the query's `:in` bindings. Scalar
    /// and collection bindings require the matching [`QueryArg`] variant.
    ///
    /// Returns an error if the arguments do not match the query or the query
    /// cannot be validated or executed.
    async fn query_with_args(
        &self,
        query: impl IntoQuery,
        args: &[QueryArg],
    ) -> Result<QueryResult, Error>;
}

#[allow(async_fn_in_trait)]
/// A node that opens read-only database values.
pub trait QueryNode {
    /// The database value returned by this node.
    type DB: Database;

    /// Opens a database value at the latest indexed transaction.
    async fn db(&self) -> Result<Self::DB, Error>;

    /// Opens a database value pinned to `tx_key`.
    ///
    /// If the transaction has been submitted but not indexed yet, this waits
    /// for indexing. It returns an error if the requested basis cannot be made
    /// available.
    async fn db_as_of(&self, tx_key: TxKey) -> Result<Self::DB, Error>;
}
