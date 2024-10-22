mod memory_node;
mod remote_node;
mod log;
mod memory_log;
mod datalog;
mod ops;
mod clock;
mod transaction;
mod local_log;
use ops::TxOp;

pub use transaction::{TransactionResult, TxKey};
pub struct Basis {}

// TODO: remove async_fn_in_trait warning
// #[allow(async_fn_in_trait)]
pub trait SubmitNode {
    async fn submit_tx(&self, ops: Vec<TxOp>) -> TxKey;
    async fn execute_tx(&self, ops: Vec<TxOp>) -> TransactionResult;

}

#[allow(async_fn_in_trait)]
pub trait QueryNode {
    async fn db(&self) -> DB;
    async fn db_with_basis(&self, basis: Basis) -> DB;
}

pub struct DB {}

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


pub trait Node : SubmitNode + QueryNode {}