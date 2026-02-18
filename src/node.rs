use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::Error;
use tokio::runtime::Handle;

use crate::clock;
use crate::datalog::Query;
use crate::file_log::FileLog;
use crate::indexer::Indexer;
use crate::log::{subscribe, TxLog, TxLogReader};
use tokio::sync::RwLock;
use crate::memory_log::MemoryLog;
use crate::ops::TxOp;
use crate::query::{execute_query, validate_query, QueryResult};
use crate::slate::{in_memory_slate, local_slate, read_attribute_map};
pub use crate::transaction::{Basis, TransactionResult, TxKey};
use tokio_util::sync::CancellationToken;

#[allow(async_fn_in_trait)]
pub trait SubmitNode {
    async fn submit_tx(&self, ops: Vec<TxOp>) -> Result<TxKey, Error>;
    async fn execute_tx(&self, ops: Vec<TxOp>) -> Result<TransactionResult, Error>;
}

#[allow(async_fn_in_trait)]
pub trait Database {
    async fn query(&self, query: &Query) -> Result<QueryResult, Error>;
}

#[allow(async_fn_in_trait)]
pub trait QueryNode {
    type DB: Database;
    async fn db(&self) -> Result<Self::DB, Error>;
    async fn db_with_basis(&self, basis: Basis) -> Result<Self::DB, Error>;
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
}

impl Database for DB {
    /// Execute a query against this database snapshot.
    /// Runs the sync join algorithm in a blocking task to avoid blocking the async runtime.
    async fn query(&self, query: &Query) -> Result<QueryResult, Error> {
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
    subscription: CancellationToken,
}

impl Node<MemoryLog> {
    pub async fn memory_node() -> Self {
        let slatedb = Arc::new(in_memory_slate().await);
        let (_version, is_fresh) = crate::bootstrap::init_db(slatedb.clone()).await;
        let indexer = Arc::new(tokio::sync::RwLock::new(Indexer::new(slatedb.clone())));
        let log = Arc::new(RwLock::new(MemoryLog::new(Box::new(clock::SystemClock))));

        let subscription = subscribe(log.clone(), None, indexer.clone()).await;

        let node = Node { log, indexer, slatedb, subscription };
        if is_fresh {
            node.bootstrap().await;
        }
        node
    }
}

impl Node<FileLog> {
    pub async fn local_node(root_path: &Path) -> Self {
        std::fs::create_dir_all(root_path.join("db")).unwrap();
        let slatedb = Arc::new(local_slate(root_path.join("db").to_str().unwrap()).await);
        let (_version, is_fresh) = crate::bootstrap::init_db(slatedb.clone()).await;
        let indexer = Arc::new(tokio::sync::RwLock::new(Indexer::new(slatedb.clone())));
        let log = Arc::new(RwLock::new(
            FileLog::new(&root_path.join("log"), Box::new(clock::SystemClock)).unwrap(),
        ));

        // Read the last tx_key from the log before subscribing (for catch-up awaiting)
        let last_tx_key = {
            let log_reader = log.read().await;
            let records = log_reader.read_txs_after(None, u16::MAX).unwrap();
            records.last().map(|r| r.tx_key)
        };

        let subscription = subscribe(log.clone(), None, indexer.clone()).await;

        // Wait for catch-up to complete if there are existing transactions
        if let Some(tx_key) = last_tx_key {
            let wait = indexer.read().await.await_tx(tx_key);
            wait.await.unwrap();
        }

        let node = Node { log, indexer, slatedb, subscription };

        if is_fresh {
            node.bootstrap().await;
        }

        node
    }
}

impl<L: TxLog> Node<L> {
    /// Look up the Basis for a given TxKey from the persisted index.
    /// Returns None if the tx_id has not been indexed yet.
    pub async fn basis_for_tx(&self, tx_key: TxKey) -> Result<Basis, Error> {
        self.indexer.read().await.await_tx(tx_key).await?;
        crate::indexer::get_basis_for_tx(self.slatedb.clone(), tx_key.tx_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("No basis found for tx {}", tx_key.tx_id))
    }

    /// Transact the bootstrap schema as the first transaction on a fresh database.
    async fn bootstrap(&self) {
        let tx_ops = crate::schema::bootstrap_schema_tx();
        let result = self.execute_tx(tx_ops).await;
        match result {
            TransactionResult::TxCommited(_) => {},
            TransactionResult::TxAborted(_, err) => {
                panic!("Failed to transact bootstrap schema: {}", err);
            }
        }
    }

    pub async fn close(self) {
        self.subscription.cancel();
        self.slatedb.close().await.unwrap();
    }
}

impl<L: TxLog> SubmitNode for Node<L> {
    async fn submit_tx(&self, ops: Vec<TxOp>) -> Result<TxKey, Error> {
        let serialized = bincode::serialize(&ops)?;
        Ok(self.log.write().await.append_tx(serialized).await)
    }

    async fn execute_tx(&self, ops: Vec<TxOp>) -> Result<TransactionResult, Error> {
        let serialized = bincode::serialize(&ops)?;

        let tx_key = self.log.write().await.append_tx(serialized).await;

        let wait_future = self.indexer.read().await.await_tx(tx_key);
        match wait_future.await {
            Ok(seq_num) => Ok(TransactionResult::TxCommited(Basis { tx_key, seq_num })),
            Err(e) => Ok(TransactionResult::TxAborted(tx_key, e.into())),
        }
    }
}

impl<L: TxLog> QueryNode for Node<L> {
    type DB = DB;
    async fn db(&self) -> Result<DB, Error> {
        let snapshot = self.slatedb.snapshot().await?;
        let attribute_map = read_attribute_map(self.slatedb.clone()).await;
        let handle = Handle::current();
        Ok(DB::new(snapshot, attribute_map, handle))
    }
    async fn db_with_basis(&self, basis: Basis) -> Result<DB, Error> {
        let snapshot = self.slatedb.snapshot_as_of(basis.seq_num).await?;
        let attribute_map = read_attribute_map(self.slatedb.clone()).await;
        let handle = Handle::current();
        Ok(DB::new(snapshot, attribute_map, handle))
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
    use edn::Keyword;
    use super::*;

    fn kw(name: &str) -> DataType {
        DataType::Keyword(Keyword::plain(name))
    }

    #[tokio::test]
    async fn test_execute_tx_updates_indices() {
        let node = Node::memory_node().await;

        // Use entity ID 100 to avoid reserved bootstrap range (1-31)
        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(100));
        map.insert("name".to_string(), DataType::String("alice".to_string()));
        let doc = Document(map);
        let tx_ops = vec![TxOp::Put(doc)];

        let result = node.execute_tx(tx_ops).await.unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        let slate = node.slatedb.clone();
        let mut attribute_map = read_attribute_map(slate.clone()).await;
        let name_id = get_and_create_attribute_id(slate.clone(), "name", &mut attribute_map).await;

        // Check EAV index — find entry for entity 100
        let mut iter = slate.scan_prefix_with_options(&[codec::EAV], &ScanOptions::default()).await.unwrap();
        let mut found = false;
        while let Some(kv) = iter.next().await.unwrap() {
            let (entity_id, attribute, value, suffix) = eav_key_to_parts(kv.key).unwrap();
            if entity_id == DataType::Long(100) {
                assert_eq!(attribute, name_id);
                assert_eq!(value, DataType::String("alice".to_string()));
                assert_eq!(suffix, codec::ADD);
                assert_eq!(kv.value, Bytes::from(""));
                found = true;
                break;
            }
        }
        assert!(found, "Expected EAV entry for entity 100");

        // Check AVE index — find entry for name attribute
        let mut iter = slate.scan_prefix_with_options(&[codec::AVE], &ScanOptions::default()).await.unwrap();
        let mut found = false;
        while let Some(kv) = iter.next().await.unwrap() {
            let (attribute, value, entity_id, suffix) = ave_key_to_parts(kv.key).unwrap();
            if entity_id == DataType::Long(100) {
                assert_eq!(attribute, name_id);
                assert_eq!(value, DataType::String("alice".to_string()));
                assert_eq!(suffix, codec::ADD);
                found = true;
                break;
            }
        }
        assert!(found, "Expected AVE entry for entity 100");

        // Check AEV index
        let mut iter = slate.scan_prefix_with_options(&[codec::AEV], &ScanOptions::default()).await.unwrap();
        let mut found = false;
        while let Some(kv) = iter.next().await.unwrap() {
            let (attribute, entity_id, value, suffix) = aev_key_to_parts(kv.key).unwrap();
            if entity_id == DataType::Long(100) {
                assert_eq!(attribute, name_id);
                assert_eq!(value, DataType::String("alice".to_string()));
                assert_eq!(suffix, codec::ADD);
                found = true;
                break;
            }
        }
        assert!(found, "Expected AEV entry for entity 100");

        // Check AE index
        let mut iter = slate.scan_prefix_with_options(&[codec::AE], &ScanOptions::default()).await.unwrap();
        let mut found = false;
        while let Some(kv) = iter.next().await.unwrap() {
            let (attribute, entity_id, suffix) = ae_key_to_parts(kv.key).unwrap();
            if entity_id == DataType::Long(100) {
                assert_eq!(attribute, name_id);
                assert_eq!(suffix, codec::ADD);
                found = true;
                break;
            }
        }
        assert!(found, "Expected AE entry for entity 100");

        // Check AV index
        let mut iter = slate.scan_prefix_with_options(&[codec::AV], &ScanOptions::default()).await.unwrap();
        let mut found = false;
        while let Some(kv) = iter.next().await.unwrap() {
            let (attribute, value, suffix) = av_key_to_parts(kv.key).unwrap();
            if attribute == name_id && value == DataType::String("alice".to_string()) {
                assert_eq!(suffix, codec::ADD);
                found = true;
                break;
            }
        }
        assert!(found, "Expected AV entry for name=alice");
    }

    #[tokio::test]
    async fn test_submit_tx_returns_tx_key_and_indices_updated_async() {
        let node = Node::memory_node().await;

        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(100));
        map.insert("name".to_string(), DataType::String("bob".to_string()));
        let doc = Document(map);
        let tx_ops = vec![TxOp::Put(doc)];

        // submit_tx returns immediately with a TxKey
        // tx_id=0 is test schema, so first data tx is tx_id=1
        let tx_key = node.submit_tx(tx_ops).await.unwrap();
        assert_eq!(tx_key.tx_id, 1);

        // Wait for indexer to process the transaction
        let wait_future = node.indexer.read().await.await_tx(tx_key);
        wait_future.await.expect("Transaction should be indexed");

        // Verify indices are updated
        let slate = node.slatedb.clone();
        let mut attribute_map = read_attribute_map(slate.clone()).await;
        let name_id = get_and_create_attribute_id(slate.clone(), "name", &mut attribute_map).await;

        let mut iter = slate.scan_prefix_with_options(&[codec::EAV], &ScanOptions::default()).await.unwrap();
        let mut found = false;
        while let Some(kv) = iter.next().await.unwrap() {
            let (entity_id, attribute, value, suffix) = eav_key_to_parts(kv.key).unwrap();
            if entity_id == DataType::Long(100) {
                assert_eq!(attribute, name_id);
                assert_eq!(value, DataType::String("bob".to_string()));
                assert_eq!(suffix, codec::ADD);
                assert_eq!(kv.value, Bytes::from(""));
                found = true;
                break;
            }
        }
        assert!(found, "Expected EAV entry for entity 100");
    }

    #[tokio::test]
    async fn test_execute_tx_with_add_triple() {
        let node = Node::memory_node().await;

        // Use entity ID 100 to avoid reserved bootstrap range (1-31)
        let triple = Triple {
            entity: EntityId::new(100),
            attribute: Attribute("email".to_string()),
            value: Value::new(DataType::String("test@example.com".to_string())),
        };
        let tx_ops = vec![TxOp::Add(triple)];

        let result = node.execute_tx(tx_ops).await.unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        let slate = node.slatedb.clone();
        let mut attribute_map = read_attribute_map(slate.clone()).await;
        let email_id = get_and_create_attribute_id(slate.clone(), "email", &mut attribute_map).await;

        // Check EAV index — find entry for entity 100
        let mut iter = slate.scan_prefix_with_options(&[codec::EAV], &ScanOptions::default()).await.unwrap();
        let mut found = false;
        while let Some(kv) = iter.next().await.unwrap() {
            let (entity_id, attribute, value, suffix) = eav_key_to_parts(kv.key).unwrap();
            if entity_id == DataType::Long(100) {
                assert_eq!(attribute, email_id);
                assert_eq!(value, DataType::String("test@example.com".to_string()));
                assert_eq!(suffix, codec::ADD);
                found = true;
                break;
            }
        }
        assert!(found, "Expected EAV entry for entity 100");
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

        node.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap();
        node.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap();

        // Query: {:find [?e ?name] :where [[?e "name" ?name]]}
        let query = Query {
            find: FindSpec::FindRel(vec![
                FindElement::Variable("?e".to_string()),
                FindElement::Variable("?name".to_string()),
            ]),
            where_clauses: vec![WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?e".to_string()),
                attribute: PatternElement::Constant(kw("name")),
                value: PatternElement::Variable("?name".to_string()),
            })],
        };

        let db = node.db().await.unwrap();
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

        node.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap();
        node.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap();

        // Query: {:find [?e] :where [[?e "name" "alice"]]}
        let query = Query {
            find: FindSpec::FindRel(vec![FindElement::Variable("?e".to_string())]),
            where_clauses: vec![WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?e".to_string()),
                attribute: PatternElement::Constant(kw("name")),
                value: PatternElement::Constant(DataType::String("alice".to_string())),
            })],
        };

        let db = node.db().await.unwrap();
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

        node.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap();
        node.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap();

        // Query: {:find [?name ?age] :where [[?e "name" ?name] [?e "age" ?age]]}
        let query = Query {
            find: FindSpec::FindRel(vec![
                FindElement::Variable("?name".to_string()),
                FindElement::Variable("?age".to_string()),
            ]),
            where_clauses: vec![
                WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(kw("name")),
                    value: PatternElement::Variable("?name".to_string()),
                }),
                WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(kw("age")),
                    value: PatternElement::Variable("?age".to_string()),
                }),
            ],
        };

        let db = node.db().await.unwrap();
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

        node.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap();
        node.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap();

        // Query: {:find [?name] :where [[?e :follows ?friend] [?friend :name ?name]]}
        // ?friend is in value position of :follows and entity position of :name
        let query = Query {
            find: FindSpec::FindRel(vec![FindElement::Variable("?name".to_string())]),
            where_clauses: vec![
                WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(kw("follows")),
                    value: PatternElement::Variable("?friend".to_string()),
                }),
                WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?friend".to_string()),
                    attribute: PatternElement::Constant(kw("name")),
                    value: PatternElement::Variable("?name".to_string()),
                }),
            ],
        };

        let db = node.db().await.unwrap();
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

        node.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap();
        node.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap();
        node.execute_tx(vec![TxOp::Put(Document(doc3))]).await.unwrap();

        // Query: {:find [?e] :where [(or [?e "name" "alice"] [?e "name" "bob"])]}
        let query = Query {
            find: FindSpec::FindRel(vec![FindElement::Variable("?e".to_string())]),
            where_clauses: vec![WhereClause::Or(vec![
                OrBranch::Clause(WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(kw("name")),
                    value: PatternElement::Constant(DataType::String("alice".to_string())),
                })),
                OrBranch::Clause(WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(kw("name")),
                    value: PatternElement::Constant(DataType::String("bob".to_string())),
                })),
            ])],
        };

        let db = node.db().await.unwrap();
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

        node.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap();
        node.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap();
        node.execute_tx(vec![TxOp::Put(Document(doc3))]).await.unwrap();

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
                        attribute: PatternElement::Constant(kw("name")),
                        value: PatternElement::Constant(DataType::String(
                            "alice".to_string(),
                        )),
                    })),
                    OrBranch::Clause(WhereClause::Triple(TriplePattern {
                        entity: PatternElement::Variable("?e".to_string()),
                        attribute: PatternElement::Constant(kw("name")),
                        value: PatternElement::Constant(DataType::String("bob".to_string())),
                    })),
                ]),
                WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(kw("age")),
                    value: PatternElement::Variable("?age".to_string()),
                }),
            ],
        };

        let db = node.db().await.unwrap();
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

        node.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap();
        node.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap();
        node.execute_tx(vec![TxOp::Put(Document(doc3))]).await.unwrap();

        // Query: {:find [?e] :where [(or (and [?e "name" "alice"] [?e "age" 30])
        //                                (and [?e "name" "charlie"] [?e "age" 35]))]}
        let query = Query {
            find: FindSpec::FindRel(vec![FindElement::Variable("?e".to_string())]),
            where_clauses: vec![WhereClause::Or(vec![
                OrBranch::And(vec![
                    WhereClause::Triple(TriplePattern {
                        entity: PatternElement::Variable("?e".to_string()),
                        attribute: PatternElement::Constant(kw("name")),
                        value: PatternElement::Constant(DataType::String(
                            "alice".to_string(),
                        )),
                    }),
                    WhereClause::Triple(TriplePattern {
                        entity: PatternElement::Variable("?e".to_string()),
                        attribute: PatternElement::Constant(kw("age")),
                        value: PatternElement::Constant(DataType::Long(30)),
                    }),
                ]),
                OrBranch::And(vec![
                    WhereClause::Triple(TriplePattern {
                        entity: PatternElement::Variable("?e".to_string()),
                        attribute: PatternElement::Constant(kw("name")),
                        value: PatternElement::Constant(DataType::String(
                            "charlie".to_string(),
                        )),
                    }),
                    WhereClause::Triple(TriplePattern {
                        entity: PatternElement::Variable("?e".to_string()),
                        attribute: PatternElement::Constant(kw("age")),
                        value: PatternElement::Constant(DataType::Long(35)),
                    }),
                ]),
            ])],
        };

        let db = node.db().await.unwrap();
        let result = db.query(&query).await.unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.contains(&vec![DataType::Long(1)]));
        assert!(result.contains(&vec![DataType::Long(3)]));
        assert!(!result.contains(&vec![DataType::Long(2)]));
    }

    // TODO(triplox-5ox): Once cardinality/one override is implemented, update this test to use
    // the same entity in both transactions and verify only the latest value is returned.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_db_with_basis_time_travel() {
        let node = Node::memory_node().await;

        // Tx1: entity 1 with name "alice"
        let mut doc1 = BTreeMap::new();
        doc1.insert("db/id".to_string(), DataType::Long(1));
        doc1.insert("name".to_string(), DataType::String("alice".to_string()));

        let basis1 = match node.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap() {
            TransactionResult::TxCommited(basis) => basis,
            _ => panic!("Tx1 should commit"),
        };

        // Tx2: entity 2 with name "bob"
        let mut doc2 = BTreeMap::new();
        doc2.insert("db/id".to_string(), DataType::Long(2));
        doc2.insert("name".to_string(), DataType::String("bob".to_string()));

        let basis2 = match node.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap() {
            TransactionResult::TxCommited(basis) => basis,
            _ => panic!("Tx2 should commit"),
        };

        // Query: {:find [?e ?name] :where [[?e "name" ?name]]}
        let query = Query {
            find: FindSpec::FindRel(vec![
                FindElement::Variable("?e".to_string()),
                FindElement::Variable("?name".to_string()),
            ]),
            where_clauses: vec![WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?e".to_string()),
                attribute: PatternElement::Constant(kw("name")),
                value: PatternElement::Variable("?name".to_string()),
            })],
        };

        // db_with_basis(basis1): should only see entity 1
        let db1 = node.db_with_basis(basis1).await.unwrap();
        let result1 = db1.query(&query).await.unwrap();
        assert_eq!(result1.len(), 1);
        assert_eq!(result1[0], vec![DataType::Long(1), DataType::String("alice".to_string())]);

        // db_with_basis(basis2): should see both entities
        let db2 = node.db_with_basis(basis2).await.unwrap();
        let result2 = db2.query(&query).await.unwrap();
        assert_eq!(result2.len(), 2);
        assert!(result2.contains(&vec![DataType::Long(1), DataType::String("alice".to_string())]));
        assert!(result2.contains(&vec![DataType::Long(2), DataType::String("bob".to_string())]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_basis_for_tx_returns_correct_seq_num_per_tx() {
        // basis_for_tx should return the seq_num corresponding to the specific tx,
        // not the latest indexed tx's seq_num.
        let node = Node::memory_node().await;

        // Tx1: entity 1 with name "alice"
        let mut doc1 = BTreeMap::new();
        doc1.insert("db/id".to_string(), DataType::Long(1));
        doc1.insert("name".to_string(), DataType::String("alice".to_string()));

        let basis1_from_execute = match node.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap() {
            TransactionResult::TxCommited(basis) => basis,
            _ => panic!("Tx1 should commit"),
        };

        // Tx2: entity 2 with name "bob"
        let mut doc2 = BTreeMap::new();
        doc2.insert("db/id".to_string(), DataType::Long(2));
        doc2.insert("name".to_string(), DataType::String("bob".to_string()));

        let basis2_from_execute = match node.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap() {
            TransactionResult::TxCommited(basis) => basis,
            _ => panic!("Tx2 should commit"),
        };

        // Both txs are now indexed. Call basis_for_tx for each.
        // BUG: basis_for_tx hits the fast path and returns the latest indexed seq_num
        // for both, instead of the seq_num specific to each tx.
        let basis1 = node.basis_for_tx(basis1_from_execute.tx_key).await.unwrap();
        let basis2 = node.basis_for_tx(basis2_from_execute.tx_key).await.unwrap();

        // The two bases must have different seq_nums since they are different transactions
        assert_ne!(
            basis1.seq_num, basis2.seq_num,
            "basis_for_tx(tx1) and basis_for_tx(tx2) should have different seq_nums, \
             but both returned {}",
            basis1.seq_num
        );

        // basis1.seq_num should be less than basis2.seq_num
        assert!(
            basis1.seq_num < basis2.seq_num,
            "basis1.seq_num ({}) should be less than basis2.seq_num ({})",
            basis1.seq_num,
            basis2.seq_num
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_local_node_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let root_path = dir.path().to_path_buf();

        let query = Query {
            find: FindSpec::FindRel(vec![
                FindElement::Variable("?e".to_string()),
                FindElement::Variable("?name".to_string()),
            ]),
            where_clauses: vec![WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?e".to_string()),
                attribute: PatternElement::Constant(kw("name")),
                value: PatternElement::Variable("?name".to_string()),
            })],
        };

        // First node: insert data
        let node = Node::local_node(&root_path).await;

        let mut doc1 = BTreeMap::new();
        doc1.insert("db/id".to_string(), DataType::Long(1));
        doc1.insert("name".to_string(), DataType::String("alice".to_string()));

        let result = node.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        let db = node.db().await.unwrap();
        let results = db.query(&query).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], vec![DataType::Long(1), DataType::String("alice".to_string())]);

        node.close().await;

        // Second node: reopen at same path, verify data persisted, add more
        let node = Node::local_node(&root_path).await;

        let db = node.db().await.unwrap();
        let results = db.query(&query).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], vec![DataType::Long(1), DataType::String("alice".to_string())]);

        let mut doc2 = BTreeMap::new();
        doc2.insert("db/id".to_string(), DataType::Long(2));
        doc2.insert("name".to_string(), DataType::String("bob".to_string()));

        let result = node.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        let db = node.db().await.unwrap();
        let results = db.query(&query).await.unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.contains(&vec![DataType::Long(1), DataType::String("alice".to_string())]));
        assert!(results.contains(&vec![DataType::Long(2), DataType::String("bob".to_string())]));

        node.close().await;
    }
}
