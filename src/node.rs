use std::sync::{Arc, RwLock};

use crate::clock;
use crate::indexer::Indexer;
use crate::log::{subscribe, TxLog};
use crate::memory_log::MemoryLog;
use crate::ops::TxOp;
use crate::slate::in_memory_slate;
pub use crate::transaction::{TransactionResult, TxKey};
use tokio_util::sync::CancellationToken;

pub struct Basis {}

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

#[allow(unused)]
pub struct Node<L: TxLog> {
    log: Arc<RwLock<L>>,
    indexer: Arc<tokio::sync::RwLock<Indexer>>,
    slatedb: Arc<slatedb::Db>,
    _subscription: CancellationToken,
}

impl Node<MemoryLog> {
    pub async fn memory_node() -> Self {
        let slatedb = Arc::new(in_memory_slate().await);
        let indexer = Arc::new(tokio::sync::RwLock::new(Indexer::new(slatedb.clone())));
        let log = Arc::new(RwLock::new(MemoryLog::new(Box::new(clock::SystemClock))));

        let subscription = subscribe(log.clone(), None, indexer.clone());

        Node { log, indexer, slatedb, _subscription: subscription }
    }
}

impl<L: TxLog> SubmitNode for Node<L> {
    async fn submit_tx(&self, _ops: Vec<TxOp>) -> TxKey {
        todo!()
    }

    async fn execute_tx(&self, ops: Vec<TxOp>) -> TransactionResult {
        let serialized = bincode::serialize(&ops)
            .expect("Failed to serialize TxOps");

        let tx_key = self.log.write().unwrap().append_tx(serialized).await;

        let wait_future = self.indexer.read().await.await_tx(tx_key);
        match wait_future.await {
            Ok(_) => TransactionResult::TxCommited(tx_key),
            Err(e) => TransactionResult::TxAborted(tx_key, e.into()),
        }
    }
}

impl<L: TxLog> QueryNode for Node<L> {
    async fn db(&self) -> DB {
        todo!()
    }
    async fn db_with_basis(&self, _basis: Basis) -> DB {
        todo!()
    }
}
