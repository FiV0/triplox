mod memory_node;
mod remote_node;
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
use ops::TxOp;
use indexer::Indexer;

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
    fn entity(&self, _eid: Eid) {
        todo!()
    }
    fn query(&self, _query: Query) {
        todo!()
    }
}


pub struct Node : SubmitNode + QueryNode {
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


