mod memory_node;
mod remote_node;
mod log;
mod memory_log;
mod datalog;

struct TransactionResult {}
struct Basis {}
pub trait SubmitNode {
    async fn transact(&self, ) -> TransactionResult;

}

pub trait QueryNode {
    async fn db(&self) -> DB;
    async fn db_with_basis(&self, basis: Basis) -> DB;

}

struct DB {}
struct Eid {}

struct Query {}
struct QueryResult{}

impl DB {
    fn entity(&self, eid: Eid) {
        todo!()
    }
    fn query(&self, query: Query) {
        todo!()
    }
}


pub trait Node : SubmitNode + QueryNode {}