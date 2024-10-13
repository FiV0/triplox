use crate::{Basis, Node, QueryNode, SubmitNode, TransactionResult, DB};

struct MemoryNode {

}

impl SubmitNode for MemoryNode {
    async fn transact(&self) -> TransactionResult {
        todo!()
    }
}

impl QueryNode for MemoryNode {
    async fn db(&self) -> DB {
        todo!()
    }

    async fn db_with_basis(&self, basis: Basis) -> DB {
        todo!()
    }
}

impl Node for MemoryNode {

}
