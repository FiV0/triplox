use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::Error;
use tokio::runtime::Handle;

use crate::clock;
use crate::datalog::Query;
use crate::indexer::Indexer;
use crate::log::{subscribe, TxLog};
use crate::memory_log::MemoryLog;
use crate::ops::TxOp;
use crate::query::{execute_query, validate_query, QueryResult};
use crate::slate::{in_memory_slate, read_attribute_map};
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

pub struct DB {
    snapshot: Arc<slatedb::DbSnapshot>,
    attribute_map: HashMap<String, u64>,
    handle: Handle,
}

#[allow(unused)]
pub struct Eid {}

#[allow(unused)]
impl DB {
    pub fn new(snapshot: Arc<slatedb::DbSnapshot>, attribute_map: HashMap<String, u64>, handle: Handle) -> Self {
        Self { snapshot, attribute_map, handle }
    }

    pub fn entity(&self, _eid: Eid) {
        todo!()
    }

    /// Execute a query against this database snapshot.
    /// Runs the sync join algorithm in a blocking task to avoid blocking the async runtime.
    pub async fn query(&self, query: &Query) -> Result<QueryResult, Error> {
        validate_query(query)?;

        let snapshot = self.snapshot.clone();
        let handle = self.handle.clone();
        let attribute_map = self.attribute_map.clone();
        let query = query.clone();

        tokio::task::spawn_blocking(move || {
            execute_query(&query, snapshot, handle, &attribute_map)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Query task failed: {}", e))?
    }
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
    async fn submit_tx(&self, ops: Vec<TxOp>) -> TxKey {
        let serialized = bincode::serialize(&ops)
            .expect("Failed to serialize TxOps");

        self.log.write().unwrap().append_tx(serialized).await
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
        let snapshot = self.slatedb.snapshot().await.expect("Failed to create snapshot");
        let attribute_map = read_attribute_map(self.slatedb.clone()).await;
        let handle = Handle::current();
        DB::new(snapshot, attribute_map, handle)
    }
    async fn db_with_basis(&self, _basis: Basis) -> DB {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bytes::Bytes;
    use slatedb::config::ScanOptions;

    use crate::codec;
    use crate::datalog::{FindElement, FindSpec, OrBranch, PatternElement, Query, TriplePattern, WhereClause};
    use crate::indexer::{eav_key_to_parts, ave_key_to_parts, aev_key_to_parts, ae_key_to_parts, av_key_to_parts};
    use crate::ops::{Attribute, DataType, Document, EntityId, Triple, TxOp, Value};
    use crate::slate::{get_and_create_attribute_id, read_attribute_map};
    use crate::transaction::TransactionResult;
    use super::*;

    #[tokio::test]
    async fn test_execute_tx_updates_indices() {
        let node = Node::memory_node().await;

        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(1));
        map.insert("name".to_string(), DataType::String("alice".to_string()));
        let doc = Document(map);
        let tx_ops = vec![TxOp::Put(doc)];

        let result = node.execute_tx(tx_ops).await;
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        let slate = node.slatedb.clone();
        let mut attribute_map = read_attribute_map(slate.clone()).await;
        let name_id = get_and_create_attribute_id(slate.clone(), "name", &mut attribute_map).await;

        // Check EAV index
        let mut iter = slate.scan_prefix_with_options(&[codec::EAV], &ScanOptions::default()).await.unwrap();
        let kv = iter.next().await.unwrap().expect("EAV index should have an entry");
        let (entity_id, attribute, value, suffix) = eav_key_to_parts(kv.key).unwrap();
        assert_eq!(entity_id, DataType::Long(1));
        assert_eq!(attribute, name_id);
        assert_eq!(value, DataType::String("alice".to_string()));
        assert_eq!(suffix, codec::ADD);
        assert_eq!(kv.value, Bytes::from(""));

        // Check AVE index
        let mut iter = slate.scan_prefix_with_options(&[codec::AVE], &ScanOptions::default()).await.unwrap();
        let kv = iter.next().await.unwrap().expect("AVE index should have an entry");
        let (attribute, value, entity_id, suffix) = ave_key_to_parts(kv.key).unwrap();
        assert_eq!(entity_id, DataType::Long(1));
        assert_eq!(attribute, name_id);
        assert_eq!(value, DataType::String("alice".to_string()));
        assert_eq!(suffix, codec::ADD);

        // Check AEV index
        let mut iter = slate.scan_prefix_with_options(&[codec::AEV], &ScanOptions::default()).await.unwrap();
        let kv = iter.next().await.unwrap().expect("AEV index should have an entry");
        let (attribute, entity_id, value, suffix) = aev_key_to_parts(kv.key).unwrap();
        assert_eq!(entity_id, DataType::Long(1));
        assert_eq!(attribute, name_id);
        assert_eq!(value, DataType::String("alice".to_string()));
        assert_eq!(suffix, codec::ADD);

        // Check AE index
        let mut iter = slate.scan_prefix_with_options(&[codec::AE], &ScanOptions::default()).await.unwrap();
        let kv = iter.next().await.unwrap().expect("AE index should have an entry");
        let (attribute, entity_id, suffix) = ae_key_to_parts(kv.key).unwrap();
        assert_eq!(entity_id, DataType::Long(1));
        assert_eq!(attribute, name_id);
        assert_eq!(suffix, codec::ADD);

        // Check AV index
        let mut iter = slate.scan_prefix_with_options(&[codec::AV], &ScanOptions::default()).await.unwrap();
        let kv = iter.next().await.unwrap().expect("AV index should have an entry");
        let (attribute, value, suffix) = av_key_to_parts(kv.key).unwrap();
        assert_eq!(attribute, name_id);
        assert_eq!(value, DataType::String("alice".to_string()));
        assert_eq!(suffix, codec::ADD);
    }

    #[tokio::test]
    async fn test_submit_tx_returns_tx_key_and_indices_updated_async() {
        let node = Node::memory_node().await;

        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(2));
        map.insert("name".to_string(), DataType::String("bob".to_string()));
        let doc = Document(map);
        let tx_ops = vec![TxOp::Put(doc)];

        // submit_tx returns immediately with a TxKey
        let tx_key = node.submit_tx(tx_ops).await;
        assert_eq!(tx_key.tx_id, 0);

        // Wait for indexer to process the transaction
        let wait_future = node.indexer.read().await.await_tx(tx_key);
        wait_future.await.expect("Transaction should be indexed");

        // Verify indices are updated
        let slate = node.slatedb.clone();
        let mut attribute_map = read_attribute_map(slate.clone()).await;
        let name_id = get_and_create_attribute_id(slate.clone(), "name", &mut attribute_map).await;

        let mut iter = slate.scan_prefix_with_options(&[codec::EAV], &ScanOptions::default()).await.unwrap();
        let kv = iter.next().await.unwrap().expect("EAV index should have an entry");
        let (entity_id, attribute, value, suffix) = eav_key_to_parts(kv.key).unwrap();
        assert_eq!(entity_id, DataType::Long(2));
        assert_eq!(attribute, name_id);
        assert_eq!(value, DataType::String("bob".to_string()));
        assert_eq!(suffix, codec::ADD);
        assert_eq!(kv.value, Bytes::from(""));
    }

    #[tokio::test]
    async fn test_execute_tx_with_add_triple() {
        let node = Node::memory_node().await;

        let triple = Triple {
            entity: EntityId::new(10),
            attribute: Attribute("email".to_string()),
            value: Value::new(DataType::String("test@example.com".to_string())),
        };
        let tx_ops = vec![TxOp::Add(triple)];

        let result = node.execute_tx(tx_ops).await;
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        let slate = node.slatedb.clone();
        let mut attribute_map = read_attribute_map(slate.clone()).await;
        let email_id = get_and_create_attribute_id(slate.clone(), "email", &mut attribute_map).await;

        // Check EAV index
        let mut iter = slate.scan_prefix_with_options(&[codec::EAV], &ScanOptions::default()).await.unwrap();
        let kv = iter.next().await.unwrap().expect("EAV index should have an entry");
        let (entity_id, attribute, value, suffix) = eav_key_to_parts(kv.key).unwrap();
        assert_eq!(entity_id, DataType::Long(10));
        assert_eq!(attribute, email_id);
        assert_eq!(value, DataType::String("test@example.com".to_string()));
        assert_eq!(suffix, codec::ADD);
    }

    // End-to-end query tests

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_single_pattern_var_const_var() {
        // Insert data: entity 1 has name "alice", entity 2 has name "bob"
        let node = Node::memory_node().await;

        let mut doc1 = BTreeMap::new();
        doc1.insert("db/id".to_string(), DataType::Long(1));
        doc1.insert("name".to_string(), DataType::String("alice".to_string()));

        let mut doc2 = BTreeMap::new();
        doc2.insert("db/id".to_string(), DataType::Long(2));
        doc2.insert("name".to_string(), DataType::String("bob".to_string()));

        node.execute_tx(vec![TxOp::Put(Document(doc1))]).await;
        node.execute_tx(vec![TxOp::Put(Document(doc2))]).await;

        // Query: {:find [?e ?name] :where [[?e "name" ?name]]}
        let query = Query {
            find: FindSpec::FindRel(vec![
                FindElement::Variable("?e".to_string()),
                FindElement::Variable("?name".to_string()),
            ]),
            where_clauses: vec![WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?e".to_string()),
                attribute: PatternElement::Constant(DataType::String("name".to_string())),
                value: PatternElement::Variable("?name".to_string()),
            })],
        };

        let db = node.db().await;
        let result = db.query(&query).await.unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.contains(&vec![DataType::Long(1), DataType::String("alice".to_string())]));
        assert!(result.contains(&vec![DataType::Long(2), DataType::String("bob".to_string())]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_single_pattern_var_const_const() {
        // Insert data
        let node = Node::memory_node().await;

        let mut doc1 = BTreeMap::new();
        doc1.insert("db/id".to_string(), DataType::Long(1));
        doc1.insert("name".to_string(), DataType::String("alice".to_string()));

        let mut doc2 = BTreeMap::new();
        doc2.insert("db/id".to_string(), DataType::Long(2));
        doc2.insert("name".to_string(), DataType::String("bob".to_string()));

        node.execute_tx(vec![TxOp::Put(Document(doc1))]).await;
        node.execute_tx(vec![TxOp::Put(Document(doc2))]).await;

        // Query: {:find [?e] :where [[?e "name" "alice"]]}
        let query = Query {
            find: FindSpec::FindRel(vec![FindElement::Variable("?e".to_string())]),
            where_clauses: vec![WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?e".to_string()),
                attribute: PatternElement::Constant(DataType::String("name".to_string())),
                value: PatternElement::Constant(DataType::String("alice".to_string())),
            })],
        };

        let db = node.db().await;
        let result = db.query(&query).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![DataType::Long(1)]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_two_patterns_join() {
        // Insert data: entity 1 has name "alice" and age 30
        let node = Node::memory_node().await;

        let mut doc1 = BTreeMap::new();
        doc1.insert("db/id".to_string(), DataType::Long(1));
        doc1.insert("name".to_string(), DataType::String("alice".to_string()));
        doc1.insert("age".to_string(), DataType::Long(30));

        let mut doc2 = BTreeMap::new();
        doc2.insert("db/id".to_string(), DataType::Long(2));
        doc2.insert("name".to_string(), DataType::String("bob".to_string()));
        // bob has no age

        node.execute_tx(vec![TxOp::Put(Document(doc1))]).await;
        node.execute_tx(vec![TxOp::Put(Document(doc2))]).await;

        // Query: {:find [?name ?age] :where [[?e "name" ?name] [?e "age" ?age]]}
        let query = Query {
            find: FindSpec::FindRel(vec![
                FindElement::Variable("?name".to_string()),
                FindElement::Variable("?age".to_string()),
            ]),
            where_clauses: vec![
                WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(DataType::String("name".to_string())),
                    value: PatternElement::Variable("?name".to_string()),
                }),
                WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(DataType::String("age".to_string())),
                    value: PatternElement::Variable("?age".to_string()),
                }),
            ],
        };

        let db = node.db().await;
        let result = db.query(&query).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![DataType::String("alice".to_string()), DataType::Long(30)]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_entity_value_join() {
        // Entity 1 (alice) follows entity 2 (bob)
        // Entity 2 (bob) has name "bob"
        // ?friend appears in value position of :follows and entity position of :name.
        // Works because entity IDs are encoded as DataType::Long in both positions.
        let node = Node::memory_node().await;

        let mut doc1 = BTreeMap::new();
        doc1.insert("db/id".to_string(), DataType::Long(1));
        doc1.insert("name".to_string(), DataType::String("alice".to_string()));
        doc1.insert("follows".to_string(), DataType::Long(2));

        let mut doc2 = BTreeMap::new();
        doc2.insert("db/id".to_string(), DataType::Long(2));
        doc2.insert("name".to_string(), DataType::String("bob".to_string()));

        node.execute_tx(vec![TxOp::Put(Document(doc1))]).await;
        node.execute_tx(vec![TxOp::Put(Document(doc2))]).await;

        // Query: {:find [?name] :where [[?e :follows ?friend] [?friend :name ?name]]}
        // ?friend is in value position of :follows and entity position of :name
        let query = Query {
            find: FindSpec::FindRel(vec![FindElement::Variable("?name".to_string())]),
            where_clauses: vec![
                WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(DataType::String("follows".to_string())),
                    value: PatternElement::Variable("?friend".to_string()),
                }),
                WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?friend".to_string()),
                    attribute: PatternElement::Constant(DataType::String("name".to_string())),
                    value: PatternElement::Variable("?name".to_string()),
                }),
            ],
        };

        let db = node.db().await;
        let result = db.query(&query).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![DataType::String("bob".to_string())]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_or_clause_basic() {
        // Entity 1: name "alice", Entity 2: name "bob", Entity 3: name "charlie"
        // OR query for alice or bob should return entities 1 and 2
        let node = Node::memory_node().await;

        let mut doc1 = BTreeMap::new();
        doc1.insert("db/id".to_string(), DataType::Long(1));
        doc1.insert("name".to_string(), DataType::String("alice".to_string()));

        let mut doc2 = BTreeMap::new();
        doc2.insert("db/id".to_string(), DataType::Long(2));
        doc2.insert("name".to_string(), DataType::String("bob".to_string()));

        let mut doc3 = BTreeMap::new();
        doc3.insert("db/id".to_string(), DataType::Long(3));
        doc3.insert("name".to_string(), DataType::String("charlie".to_string()));

        node.execute_tx(vec![TxOp::Put(Document(doc1))]).await;
        node.execute_tx(vec![TxOp::Put(Document(doc2))]).await;
        node.execute_tx(vec![TxOp::Put(Document(doc3))]).await;

        // Query: {:find [?e] :where [(or [?e "name" "alice"] [?e "name" "bob"])]}
        let query = Query {
            find: FindSpec::FindRel(vec![FindElement::Variable("?e".to_string())]),
            where_clauses: vec![WhereClause::Or(vec![
                OrBranch::Clause(WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(DataType::String("name".to_string())),
                    value: PatternElement::Constant(DataType::String("alice".to_string())),
                })),
                OrBranch::Clause(WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(DataType::String("name".to_string())),
                    value: PatternElement::Constant(DataType::String("bob".to_string())),
                })),
            ])],
        };

        let db = node.db().await;
        let result = db.query(&query).await.unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.contains(&vec![DataType::Long(1)]));
        assert!(result.contains(&vec![DataType::Long(2)]));
        assert!(!result.contains(&vec![DataType::Long(3)]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_or_clause_with_additional_join() {
        // Entity 1: name "alice", age 30
        // Entity 2: name "bob", age 25
        // Entity 3: name "charlie", age 35
        // OR for name + join on age: only alice and bob should appear
        let node = Node::memory_node().await;

        let mut doc1 = BTreeMap::new();
        doc1.insert("db/id".to_string(), DataType::Long(1));
        doc1.insert("name".to_string(), DataType::String("alice".to_string()));
        doc1.insert("age".to_string(), DataType::Long(30));

        let mut doc2 = BTreeMap::new();
        doc2.insert("db/id".to_string(), DataType::Long(2));
        doc2.insert("name".to_string(), DataType::String("bob".to_string()));
        doc2.insert("age".to_string(), DataType::Long(25));

        let mut doc3 = BTreeMap::new();
        doc3.insert("db/id".to_string(), DataType::Long(3));
        doc3.insert("name".to_string(), DataType::String("charlie".to_string()));
        doc3.insert("age".to_string(), DataType::Long(35));

        node.execute_tx(vec![TxOp::Put(Document(doc1))]).await;
        node.execute_tx(vec![TxOp::Put(Document(doc2))]).await;
        node.execute_tx(vec![TxOp::Put(Document(doc3))]).await;

        // Query: {:find [?e ?age] :where [(or [?e "name" "alice"] [?e "name" "bob"]) [?e "age" ?age]]}
        let query = Query {
            find: FindSpec::FindRel(vec![
                FindElement::Variable("?e".to_string()),
                FindElement::Variable("?age".to_string()),
            ]),
            where_clauses: vec![
                WhereClause::Or(vec![
                    OrBranch::Clause(WhereClause::Triple(TriplePattern {
                        entity: PatternElement::Variable("?e".to_string()),
                        attribute: PatternElement::Constant(DataType::String(
                            "name".to_string(),
                        )),
                        value: PatternElement::Constant(DataType::String(
                            "alice".to_string(),
                        )),
                    })),
                    OrBranch::Clause(WhereClause::Triple(TriplePattern {
                        entity: PatternElement::Variable("?e".to_string()),
                        attribute: PatternElement::Constant(DataType::String(
                            "name".to_string(),
                        )),
                        value: PatternElement::Constant(DataType::String("bob".to_string())),
                    })),
                ]),
                WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(DataType::String("age".to_string())),
                    value: PatternElement::Variable("?age".to_string()),
                }),
            ],
        };

        let db = node.db().await;
        let result = db.query(&query).await.unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.contains(&vec![DataType::Long(1), DataType::Long(30)]));
        assert!(result.contains(&vec![DataType::Long(2), DataType::Long(25)]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_and_inside_or() {
        // Entity 1: name "alice", age 30
        // Entity 2: name "bob", age 25
        // Entity 3: name "charlie", age 35
        // Query: OR of two AND branches:
        //   (or (and [?e "name" "alice"] [?e "age" 30])    -> entity 1
        //       (and [?e "name" "charlie"] [?e "age" 35])) -> entity 3
        // Should return entities 1 and 3 but not 2
        let node = Node::memory_node().await;

        let mut doc1 = BTreeMap::new();
        doc1.insert("db/id".to_string(), DataType::Long(1));
        doc1.insert("name".to_string(), DataType::String("alice".to_string()));
        doc1.insert("age".to_string(), DataType::Long(30));

        let mut doc2 = BTreeMap::new();
        doc2.insert("db/id".to_string(), DataType::Long(2));
        doc2.insert("name".to_string(), DataType::String("bob".to_string()));
        doc2.insert("age".to_string(), DataType::Long(25));

        let mut doc3 = BTreeMap::new();
        doc3.insert("db/id".to_string(), DataType::Long(3));
        doc3.insert("name".to_string(), DataType::String("charlie".to_string()));
        doc3.insert("age".to_string(), DataType::Long(35));

        node.execute_tx(vec![TxOp::Put(Document(doc1))]).await;
        node.execute_tx(vec![TxOp::Put(Document(doc2))]).await;
        node.execute_tx(vec![TxOp::Put(Document(doc3))]).await;

        // Query: {:find [?e] :where [(or (and [?e "name" "alice"] [?e "age" 30])
        //                                (and [?e "name" "charlie"] [?e "age" 35]))]}
        let query = Query {
            find: FindSpec::FindRel(vec![FindElement::Variable("?e".to_string())]),
            where_clauses: vec![WhereClause::Or(vec![
                OrBranch::And(vec![
                    WhereClause::Triple(TriplePattern {
                        entity: PatternElement::Variable("?e".to_string()),
                        attribute: PatternElement::Constant(DataType::String(
                            "name".to_string(),
                        )),
                        value: PatternElement::Constant(DataType::String(
                            "alice".to_string(),
                        )),
                    }),
                    WhereClause::Triple(TriplePattern {
                        entity: PatternElement::Variable("?e".to_string()),
                        attribute: PatternElement::Constant(DataType::String(
                            "age".to_string(),
                        )),
                        value: PatternElement::Constant(DataType::Long(30)),
                    }),
                ]),
                OrBranch::And(vec![
                    WhereClause::Triple(TriplePattern {
                        entity: PatternElement::Variable("?e".to_string()),
                        attribute: PatternElement::Constant(DataType::String(
                            "name".to_string(),
                        )),
                        value: PatternElement::Constant(DataType::String(
                            "charlie".to_string(),
                        )),
                    }),
                    WhereClause::Triple(TriplePattern {
                        entity: PatternElement::Variable("?e".to_string()),
                        attribute: PatternElement::Constant(DataType::String(
                            "age".to_string(),
                        )),
                        value: PatternElement::Constant(DataType::Long(35)),
                    }),
                ]),
            ])],
        };

        let db = node.db().await;
        let result = db.query(&query).await.unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.contains(&vec![DataType::Long(1)]));
        assert!(result.contains(&vec![DataType::Long(3)]));
        assert!(!result.contains(&vec![DataType::Long(2)]));
    }
}
