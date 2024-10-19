use crate::{Basis, Node, QueryNode, SubmitNode, TransactionResult, TxKey, TxOp, DB};

#[allow(unused)]
struct MemoryNode {

}

impl SubmitNode for MemoryNode {
    async fn submit_tx(&self, _ops: Vec<TxOp>) -> TxKey{
        todo!()
    }
    async fn execute_tx(&self, _ops: Vec<TxOp>) -> TransactionResult {
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
