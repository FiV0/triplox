use crate::{Basis, Node, QueryNode, SubmitNode, TransactionResult, DB, Op};

#[allow(unused)]
struct MemoryNode {

}

impl SubmitNode for MemoryNode {
    async fn transact(&self, _ops: Vec<Op>) -> TransactionResult {
        todo!()
    }
}

impl QueryNode for MemoryNode {
    async fn db(&self) -> DB {
        todo!()
    }

    #[allow(unused_variables)]
    async fn db_with_basis(&self, basis: Basis) -> DB {
        todo!()
    }
}

impl Node for MemoryNode {

}
