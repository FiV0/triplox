mod log;
mod memory_log;
mod datalog;
mod ops;
mod clock;
mod transaction;
mod file_log;
mod error;
mod logging;
mod indexer;
mod codec;
mod slate;
mod util;
use ops::TxOp;
use std::sync::Arc;
use crate::slate::in_memory_slate;

pub use transaction::{TransactionResult, TxKey};
pub struct Basis {}

// TODO: remove async_fn_in_trait warning
#[allow(async_fn_in_trait)]
pub trait SubmitNode {
    async fn submit_tx(&self, ops: Vec<TxOp>) -> TxKey;
    async fn execute_tx(&self, ops: Vec<TxOp>) -> TransactionResult;

}

#[allow(async_fn_in_trait)]
pub trait QueryNode {
    async fn db(&self) -> DB;
    async fn db_with_basis(&self, basis: Basis) -> DB;
}

pub struct DB { }


#[allow(unused)]
pub struct Eid {}

#[allow(unused)]
pub struct Query {}

#[allow(unused)]
pub struct QueryResult{}

#[allow(unused)]
impl DB {
    pub fn entity(&self, _eid: Eid) {
        todo!()
    }
    pub fn query(&self, _query: Query) {
        todo!()
    }
    // pub fn pull(&self, _pattern: Any) { 
    //     todo!()
    // }
    // pub fn pull_many(&self, _pattern: Any) {
    //     todo!()
    // }
}
pub struct Node {
    log: Box<dyn TxLog>,
    indexer: Box<dyn Indexer>,
    slatedb: Arc<slatedb::db::Db>,
}

impl Node {
    fn memory_node() -> Self {
        let slatedb = Arc::new(in_memory_slate());
        Node { 
            log: Box::new(MemoryLog::new(Box::new(clock::SystemTimeSource::new()))), 
            indexer: Box::new(Indexer::new(slatedb.clone())), 
            slatedb: slatedb.clone()
        }
    }
}

impl SubmitNode for Node {
    async fn submit_tx(&self, ops: Vec<TxOp>) -> TxKey {
        todo!()
    }
    async fn execute_tx(&self, ops: Vec<TxOp>) -> TransactionResult {
        todo!()
    }
}

impl QueryNode for Node {
    async fn db(&self) -> DB {
        todo!()
    }
    async fn db_with_basis(&self, basis: Basis) -> DB {
        todo!()
    }
}

