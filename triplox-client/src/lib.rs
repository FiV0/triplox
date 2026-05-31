//! HTTP/2 client for [Triplox](https://github.com/FiV0/triplox), a Datalog database.
//!
//! [`ClientNode`] mirrors the server's `Node` API and [`ClientDb`] mirrors the
//! `DB` API, both operating over HTTP/2 with MessagePack encoding.

pub mod client;
pub mod msgpack_codec;
pub mod node;
pub mod ops;
pub mod protocol;
pub mod query;
pub mod subscription;
pub mod transaction;

pub use client::{ClientDb, ClientNode};
pub use node::{collect_tx_ops, Database, IntoQuery, IntoTxOp, QueryNode, SubmitNode};
pub use ops::{DataType, Entid, EntityRef, QueryArg, TxOp};
pub use query::QueryResult;
pub use subscription::{Delta, Subscription};
pub use transaction::{TransactionResult, TxBasis, TxKey};
