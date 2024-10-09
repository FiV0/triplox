use crate::{Basis, Node, QueryNode, SubmitNode, TransactionResult, DB};

struct MemoryNode {

}

impl SubmitNode for MemoryNode {
    fn transact(&self) -> TransactionResult {
        todo!()
    }
}

impl QueryNode for MemoryNode {
    fn db(&self) -> DB {
        todo!()
    }

    fn db_with_basis(&self, basis: Basis) -> DB {
        todo!()
    }
}

impl Node for MemoryNode {

}
