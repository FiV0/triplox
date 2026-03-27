use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::Error;
use tokio::runtime::Handle;

use crate::clock;
use crate::datalog::Query;
use crate::file_log::FileLog;
use crate::indexer::{Indexer, latest_tx_key_from_snapshot};
use crate::log::{subscribe, TxLog, TxLogReader};
use tokio::sync::RwLock;
use crate::memory_log::MemoryLog;
use crate::ops::TxOp;
use crate::query::{execute_query, validate_query, QueryResult};
use crate::slate::{in_memory_slate, local_slate};
pub use crate::transaction::{TransactionResult, TxKey};
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
    async fn db_as_of(&self, tx_key: TxKey) -> Result<Self::DB, Error>;
}

pub struct DB {
    snapshot: Arc<slatedb::DbSnapshot>,
    attribute_map: HashMap<String, i64>,
    handle: Handle,
    tx_key: TxKey,
}

#[allow(unused)]
pub struct Eid {}

#[allow(unused)]
impl DB {
    pub fn new(snapshot: Arc<slatedb::DbSnapshot>, attribute_map: HashMap<String, i64>, handle: Handle, tx_key: TxKey) -> Self {
        Self { snapshot, attribute_map, handle, tx_key }
    }

    /// Construct a DB from a snapshot by scanning TX_TO_META keys to find the latest TxKey.
    pub async fn from_latest_snapshot(snapshot: Arc<slatedb::DbSnapshot>, attribute_map: HashMap<String, i64>, handle: Handle) -> Result<Self, Error> {
        let tx_key = crate::indexer::latest_tx_key_from_snapshot(&snapshot).await?;
        Ok(Self { snapshot, attribute_map, handle, tx_key })
    }

    pub fn tx_key(&self) -> &TxKey {
        &self.tx_key
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
        let as_of = self.tx_key.system_time;

        tokio::task::spawn_blocking(move || {
            execute_query(&query, snapshot, handle, &attribute_map, as_of)
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
        let cache = crate::bootstrap::init_db(slatedb.clone()).await;
        let indexer = Arc::new(tokio::sync::RwLock::new(Indexer::new(slatedb.clone(), cache, None)));
        let log = Arc::new(RwLock::new(MemoryLog::new(Box::new(clock::SystemClock))));

        let subscription = subscribe(log.clone(), None, indexer.clone()).await;

        Node { log, indexer, slatedb, subscription }
    }
}

impl Node<FileLog> {
    pub async fn local_node(root_path: &Path) -> Result<Self, Error> {
        std::fs::create_dir_all(root_path.join("db"))?;
        let db_path = root_path.join("db");
        let db_path_str = db_path.to_str()
            .ok_or_else(|| anyhow::anyhow!("database path is not valid UTF-8: {:?}", db_path))?;
        let slatedb = Arc::new(local_slate(db_path_str).await);
        let cache = crate::bootstrap::init_db(slatedb.clone()).await;

        // Determine the latest already-indexed tx_id so we skip replaying it
        // and restore the indexer's in-memory state for TxWaiter fast-path.
        let snapshot = Arc::new(slatedb.snapshot().await?);
        let latest_indexed = latest_tx_key_from_snapshot(&snapshot).await?;
        let latest_indexed_tx = if latest_indexed.tx_id > 0 {
            Some(latest_indexed)
        } else {
            None
        };

        let indexer = Arc::new(tokio::sync::RwLock::new(Indexer::new(slatedb.clone(), cache, latest_indexed_tx)));
        let log = Arc::new(RwLock::new(
            FileLog::new(&root_path.join("log"), Box::new(clock::SystemClock))?,
        ));

        let after_tx_id = latest_indexed_tx.map(|k| k.tx_id);

        // Read the last tx_key from the log before subscribing (for catch-up awaiting)
        let last_tx_key = {
            let log_reader = log.read().await;
            let records = log_reader.read_txs_after(after_tx_id, u16::MAX).await?;
            records.last().map(|r| r.tx_key)
        };

        // Create a waiter for catch-up completion
        let waiter = match last_tx_key {
            Some(_) => Some(indexer.read().await.tx_waiter()),
            None => None,
        };

        let subscription = subscribe(log.clone(), after_tx_id, indexer.clone()).await;

        // Wait for catch-up to complete if there are existing transactions
        if let Some((tx_key, waiter)) = last_tx_key.zip(waiter) {
            waiter.await_tx(tx_key).await?;
        }

        Ok(Node { log, indexer, slatedb, subscription })
    }
}

#[cfg(feature = "s2")]
impl Node<crate::s2_log::S2Log> {
    pub async fn remote_node(
        stream: s2_sdk::S2Stream,
        db_path: &str,
    ) -> Result<Self, Error> {
        let slatedb = Arc::new(local_slate(db_path).await);
        let cache = crate::bootstrap::init_db(slatedb.clone()).await;

        let snapshot = Arc::new(slatedb.snapshot().await?);
        let latest_indexed = latest_tx_key_from_snapshot(&snapshot).await?;
        let latest_indexed_tx = if latest_indexed.tx_id > 0 {
            Some(latest_indexed)
        } else {
            None
        };

        let indexer = Arc::new(tokio::sync::RwLock::new(Indexer::new(slatedb.clone(), cache, latest_indexed_tx)));
        let log = Arc::new(RwLock::new(crate::s2_log::S2Log::new(stream)));

        let after_tx_id = latest_indexed_tx.map(|k| k.tx_id);

        let last_tx_key = {
            let log_reader = log.read().await;
            let records = log_reader.read_txs_after(after_tx_id, u16::MAX).await?;
            records.last().map(|r| r.tx_key)
        };

        let waiter = match last_tx_key {
            Some(_) => Some(indexer.read().await.tx_waiter()),
            None => None,
        };

        let subscription = subscribe(log.clone(), after_tx_id, indexer.clone()).await;

        if let Some((tx_key, waiter)) = last_tx_key.zip(waiter) {
            waiter.await_tx(tx_key).await?;
        }

        Ok(Node { log, indexer, slatedb, subscription })
    }
}

impl<L: TxLog> Node<L> {
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

        let waiter = self.indexer.read().await.tx_waiter();

        let tx_key = self.log.write().await.append_tx(serialized).await;

        match waiter.await_tx(tx_key).await {
            Ok(()) => Ok(TransactionResult::TxCommited(tx_key)),
            Err(e) => Ok(TransactionResult::TxAborted(tx_key, e.into())),
        }
    }
}

impl<L: TxLog> QueryNode for Node<L> {
    type DB = DB;
    async fn db(&self) -> Result<DB, Error> {
        let snapshot = self.slatedb.snapshot().await?;
        let attribute_map = self.indexer.read().await.schema_cache().attribute_map();
        let handle = Handle::current();
        DB::from_latest_snapshot(snapshot, attribute_map, handle).await
    }
    async fn db_as_of(&self, tx_key: TxKey) -> Result<DB, Error> {
        let snapshot = self.slatedb.snapshot().await?;
        let attribute_map = self.indexer.read().await.schema_cache().attribute_map();
        let handle = Handle::current();
        Ok(DB::new(snapshot, attribute_map, handle, tx_key))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bytes::Bytes;
    use slatedb::config::ScanOptions;

    use crate::codec;
    use crate::datalog::{FindElement, FindSpec, FnExpr, OrBranch, PatternElement, Query, TriplePattern, WhereClause};
    use crate::expr::{BinaryExpr, BinaryOp, Expr};
    use crate::indexer::{eav_key_to_parts, ave_key_to_parts, aev_key_to_parts, ae_key_to_parts, av_key_to_parts};
    use crate::ops::{Attribute, DataType, Document, EntityId, TxOp};
    use crate::schema::test_schema_tx;
    // "name" has entity_id 50, "age" 51, "email" 52, "follows" 53 from test_schema_tx
    use crate::transaction::TransactionResult;
    use edn::Keyword;
    use super::*;

    fn kw(name: &str) -> DataType {
        DataType::Keyword(Keyword::plain(name))
    }

    /// Define common test attributes (name, age, email, follows) through the standard tx path.
    async fn define_test_schema(node: &impl SubmitNode) {
        let result = node.execute_tx(test_schema_tx()).await.unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));
    }

    #[tokio::test]
    async fn test_execute_tx_updates_indices() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        // Use entity ID 100 to avoid reserved bootstrap range (1-31)
        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(100));
        map.insert("name".to_string(), DataType::String("alice".to_string()));
        let doc = Document(map);
        let tx_ops = vec![TxOp::Put(doc)];

        let result = node.execute_tx(tx_ops).await.unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        let slate = node.slatedb.clone();
        let name_id: i64 = 50; // from test_schema_tx

        // Check EAV index — find entry for entity 100
        let mut iter = slate.scan_prefix_with_options(&[codec::EAV], &ScanOptions::default()).await.unwrap();
        let mut found = false;
        while let Some(kv) = iter.next().await.unwrap() {
            let (entity_id, attribute, value, _timestamp, suffix) = eav_key_to_parts(kv.key).unwrap();
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
            let (attribute, value, entity_id, _timestamp, suffix) = ave_key_to_parts(kv.key).unwrap();
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
            let (attribute, entity_id, value, _timestamp, suffix) = aev_key_to_parts(kv.key).unwrap();
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
            let (attribute, entity_id) = ae_key_to_parts(kv.key).unwrap();
            if entity_id == DataType::Long(100) {
                assert_eq!(attribute, name_id);
                found = true;
                break;
            }
        }
        assert!(found, "Expected AE entry for entity 100");

        // Check AV index
        let mut iter = slate.scan_prefix_with_options(&[codec::AV], &ScanOptions::default()).await.unwrap();
        let mut found = false;
        while let Some(kv) = iter.next().await.unwrap() {
            let (attribute, value) = av_key_to_parts(kv.key).unwrap();
            if attribute == name_id && value == DataType::String("alice".to_string()) {
                found = true;
                break;
            }
        }
        assert!(found, "Expected AV entry for name=alice");
    }

    #[tokio::test]
    async fn test_submit_tx_returns_tx_key_and_indices_updated_async() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(100));
        map.insert("name".to_string(), DataType::String("bob".to_string()));
        let doc = Document(map);
        let tx_ops = vec![TxOp::Put(doc)];

        let waiter = node.indexer.read().await.tx_waiter();

        // submit_tx returns immediately with a TxKey
        // bootstrap goes directly through transact_tx (not log), so:
        // tx_id=0 is test schema, tx_id=1 is first data tx
        let tx_key = node.submit_tx(tx_ops).await.unwrap();
        assert_eq!(tx_key.tx_id, 1);

        // Wait for indexer to process the transaction
        waiter.await_tx(tx_key).await.expect("Transaction should be indexed");

        // Verify indices are updated
        let slate = node.slatedb.clone();
        let name_id: i64 = 50;

        let mut iter = slate.scan_prefix_with_options(&[codec::EAV], &ScanOptions::default()).await.unwrap();
        let mut found = false;
        while let Some(kv) = iter.next().await.unwrap() {
            let (entity_id, attribute, value, _timestamp, suffix) = eav_key_to_parts(kv.key).unwrap();
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
        define_test_schema(&node).await;

        // Use entity ID 100 to avoid reserved bootstrap range (1-31)
        let tx_ops = vec![TxOp::Add {
            entity_id: EntityId::new(100),
            attribute: Attribute("email".to_string()),
            value: DataType::String("test@example.com".to_string()),
        }];

        let result = node.execute_tx(tx_ops).await.unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        let slate = node.slatedb.clone();
        let email_id: i64 = 52; // from test_schema_tx

        // Check EAV index — find entry for entity 100
        let mut iter = slate.scan_prefix_with_options(&[codec::EAV], &ScanOptions::default()).await.unwrap();
        let mut found = false;
        while let Some(kv) = iter.next().await.unwrap() {
            let (entity_id, attribute, value, _timestamp, suffix) = eav_key_to_parts(kv.key).unwrap();
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
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let mut doc1 = BTreeMap::new();
        doc1.insert("db/id".to_string(), DataType::Long(100));
        doc1.insert("name".to_string(), DataType::String("alice".to_string()));

        let mut doc2 = BTreeMap::new();
        doc2.insert("db/id".to_string(), DataType::Long(101));
        doc2.insert("name".to_string(), DataType::String("bob".to_string()));

        node.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap();
        node.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap();

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
        assert!(result.contains(&vec![DataType::Long(100), DataType::String("alice".to_string())]));
        assert!(result.contains(&vec![DataType::Long(101), DataType::String("bob".to_string())]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_single_pattern_var_const_const() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let mut doc1 = BTreeMap::new();
        doc1.insert("db/id".to_string(), DataType::Long(100));
        doc1.insert("name".to_string(), DataType::String("alice".to_string()));

        let mut doc2 = BTreeMap::new();
        doc2.insert("db/id".to_string(), DataType::Long(101));
        doc2.insert("name".to_string(), DataType::String("bob".to_string()));

        node.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap();
        node.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap();

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
        assert_eq!(result[0], vec![DataType::Long(100)]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_two_patterns_join() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let mut doc1 = BTreeMap::new();
        doc1.insert("db/id".to_string(), DataType::Long(100));
        doc1.insert("name".to_string(), DataType::String("alice".to_string()));
        doc1.insert("age".to_string(), DataType::Long(30));

        let mut doc2 = BTreeMap::new();
        doc2.insert("db/id".to_string(), DataType::Long(101));
        doc2.insert("name".to_string(), DataType::String("bob".to_string()));

        node.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap();
        node.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap();

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
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let mut doc1 = BTreeMap::new();
        doc1.insert("db/id".to_string(), DataType::Long(100));
        doc1.insert("name".to_string(), DataType::String("alice".to_string()));
        doc1.insert("follows".to_string(), DataType::Long(101));

        let mut doc2 = BTreeMap::new();
        doc2.insert("db/id".to_string(), DataType::Long(101));
        doc2.insert("name".to_string(), DataType::String("bob".to_string()));

        node.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap();
        node.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap();

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
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let mut doc1 = BTreeMap::new();
        doc1.insert("db/id".to_string(), DataType::Long(100));
        doc1.insert("name".to_string(), DataType::String("alice".to_string()));

        let mut doc2 = BTreeMap::new();
        doc2.insert("db/id".to_string(), DataType::Long(101));
        doc2.insert("name".to_string(), DataType::String("bob".to_string()));

        let mut doc3 = BTreeMap::new();
        doc3.insert("db/id".to_string(), DataType::Long(102));
        doc3.insert("name".to_string(), DataType::String("charlie".to_string()));

        node.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap();
        node.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap();
        node.execute_tx(vec![TxOp::Put(Document(doc3))]).await.unwrap();

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
        assert!(result.contains(&vec![DataType::Long(100)]));
        assert!(result.contains(&vec![DataType::Long(101)]));
        assert!(!result.contains(&vec![DataType::Long(102)]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_or_clause_with_additional_join() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let mut doc1 = BTreeMap::new();
        doc1.insert("db/id".to_string(), DataType::Long(100));
        doc1.insert("name".to_string(), DataType::String("alice".to_string()));
        doc1.insert("age".to_string(), DataType::Long(30));

        let mut doc2 = BTreeMap::new();
        doc2.insert("db/id".to_string(), DataType::Long(101));
        doc2.insert("name".to_string(), DataType::String("bob".to_string()));
        doc2.insert("age".to_string(), DataType::Long(25));

        let mut doc3 = BTreeMap::new();
        doc3.insert("db/id".to_string(), DataType::Long(102));
        doc3.insert("name".to_string(), DataType::String("charlie".to_string()));
        doc3.insert("age".to_string(), DataType::Long(35));

        node.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap();
        node.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap();
        node.execute_tx(vec![TxOp::Put(Document(doc3))]).await.unwrap();

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
                        value: PatternElement::Constant(DataType::String("alice".to_string())),
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
        assert!(result.contains(&vec![DataType::Long(100), DataType::Long(30)]));
        assert!(result.contains(&vec![DataType::Long(101), DataType::Long(25)]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_and_inside_or() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let mut doc1 = BTreeMap::new();
        doc1.insert("db/id".to_string(), DataType::Long(100));
        doc1.insert("name".to_string(), DataType::String("alice".to_string()));
        doc1.insert("age".to_string(), DataType::Long(30));

        let mut doc2 = BTreeMap::new();
        doc2.insert("db/id".to_string(), DataType::Long(101));
        doc2.insert("name".to_string(), DataType::String("bob".to_string()));
        doc2.insert("age".to_string(), DataType::Long(25));

        let mut doc3 = BTreeMap::new();
        doc3.insert("db/id".to_string(), DataType::Long(102));
        doc3.insert("name".to_string(), DataType::String("charlie".to_string()));
        doc3.insert("age".to_string(), DataType::Long(35));

        node.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap();
        node.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap();
        node.execute_tx(vec![TxOp::Put(Document(doc3))]).await.unwrap();

        let query = Query {
            find: FindSpec::FindRel(vec![FindElement::Variable("?e".to_string())]),
            where_clauses: vec![WhereClause::Or(vec![
                OrBranch::And(vec![
                    WhereClause::Triple(TriplePattern {
                        entity: PatternElement::Variable("?e".to_string()),
                        attribute: PatternElement::Constant(kw("name")),
                        value: PatternElement::Constant(DataType::String("alice".to_string())),
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
                        value: PatternElement::Constant(DataType::String("charlie".to_string())),
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
        assert!(result.contains(&vec![DataType::Long(100)]));
        assert!(result.contains(&vec![DataType::Long(102)]));
        assert!(!result.contains(&vec![DataType::Long(101)]));
    }

    // TODO(triplox-5ox): Once cardinality/one override is implemented, update this test to use
    // the same entity in both transactions and verify only the latest value is returned.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_db_as_of_time_travel() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let mut doc1 = BTreeMap::new();
        doc1.insert("db/id".to_string(), DataType::Long(100));
        doc1.insert("name".to_string(), DataType::String("alice".to_string()));

        let tx_key1 = match node.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap() {
            TransactionResult::TxCommited(tx_key) => tx_key,
            _ => panic!("Tx1 should commit"),
        };

        let mut doc2 = BTreeMap::new();
        doc2.insert("db/id".to_string(), DataType::Long(101));
        doc2.insert("name".to_string(), DataType::String("bob".to_string()));

        let tx_key2 = match node.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap() {
            TransactionResult::TxCommited(tx_key) => tx_key,
            _ => panic!("Tx2 should commit"),
        };

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

        // db_as_of(tx_key1): should only see entity 100
        let db1 = node.db_as_of(tx_key1).await.unwrap();
        let result1 = db1.query(&query).await.unwrap();
        assert_eq!(result1.len(), 1);
        assert_eq!(result1[0], vec![DataType::Long(100), DataType::String("alice".to_string())]);

        // db_as_of(tx_key2): should see both entities
        let db2 = node.db_as_of(tx_key2).await.unwrap();
        let result2 = db2.query(&query).await.unwrap();
        assert_eq!(result2.len(), 2);
        assert!(result2.contains(&vec![DataType::Long(100), DataType::String("alice".to_string())]));
        assert!(result2.contains(&vec![DataType::Long(101), DataType::String("bob".to_string())]));
    }

    // TODO: once auto entity ID allocation (tempids) is implemented, this test
    // should be updated to use auto-allocated IDs instead of explicit db/id values.
    // Without the fix to skip already-indexed transactions on restart, auto-allocated
    // IDs would diverge because replaying transactions with an advanced next_t counter
    // produces different entity IDs, corrupting the index.
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
        let node = Node::local_node(&root_path).await.unwrap();
        define_test_schema(&node).await;

        let mut doc1 = BTreeMap::new();
        doc1.insert("db/id".to_string(), DataType::Long(100));
        doc1.insert("name".to_string(), DataType::String("alice".to_string()));

        let result = node.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        let db = node.db().await.unwrap();
        let results = db.query(&query).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], vec![DataType::Long(100), DataType::String("alice".to_string())]);

        node.close().await;

        // Second node: reopen at same path, verify data persisted, add more
        let node = Node::local_node(&root_path).await.unwrap();

        let db = node.db().await.unwrap();
        let results = db.query(&query).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], vec![DataType::Long(100), DataType::String("alice".to_string())]);

        let mut doc2 = BTreeMap::new();
        doc2.insert("db/id".to_string(), DataType::Long(101));
        doc2.insert("name".to_string(), DataType::String("bob".to_string()));

        let result = node.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        let db = node.db().await.unwrap();
        let results = db.query(&query).await.unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.contains(&vec![DataType::Long(100), DataType::String("alice".to_string())]));
        assert!(results.contains(&vec![DataType::Long(101), DataType::String("bob".to_string())]));

        node.close().await;
    }

    // The indexer's `latest_indexed_tx` field must be restored on restart so that
    // a TxWaiter for an already-indexed transaction returns immediately. Without
    // the fix, `await_tx` falls into the broadcast loop and hangs forever because
    // no new completions will be broadcast.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_local_node_restart_tx_waiter_for_already_indexed_tx() {
        let dir = tempfile::tempdir().unwrap();
        let root_path = dir.path().to_path_buf();

        // First node: bootstrap schema + insert data
        let node = Node::local_node(&root_path).await.unwrap();
        define_test_schema(&node).await;

        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(100));
        doc.insert("name".to_string(), DataType::String("alice".to_string()));

        let result = node.execute_tx(vec![TxOp::Put(Document(doc))]).await.unwrap();
        let tx_key = match result {
            TransactionResult::TxCommited(k) => k,
            _ => panic!("expected commit"),
        };

        node.close().await;

        // Second node: reopen — no new transactions
        let node = Node::local_node(&root_path).await.unwrap();

        // A waiter obtained after restart should resolve immediately for the
        // already-indexed tx_key. Without the fix this hangs forever.
        let waiter = node.indexer.read().await.tx_waiter();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            waiter.await_tx(tx_key),
        ).await;

        assert!(result.is_ok(), "tx_waiter should not timeout for an already-indexed tx");
        result.unwrap().expect("await_tx should succeed");

        node.close().await;
    }

    #[tokio::test]
    async fn test_db_as_of_pins_snapshot() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        // First transaction: insert alice
        let mut doc1 = BTreeMap::new();
        doc1.insert("db/id".to_string(), DataType::Long(100));
        doc1.insert("name".to_string(), DataType::String("alice".to_string()));
        let result1 = node.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap();
        let tx_key1 = match result1 {
            TransactionResult::TxCommited(tk) => tk,
            _ => panic!("Expected TxCommited"),
        };

        // Second transaction: insert bob
        let mut doc2 = BTreeMap::new();
        doc2.insert("db/id".to_string(), DataType::Long(200));
        doc2.insert("name".to_string(), DataType::String("bob".to_string()));
        node.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap();

        // db_as_of pinned to first tx should only see alice
        let db = node.db_as_of(tx_key1).await.unwrap();
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
        let results = db.query(&query).await.unwrap();
        assert_eq!(results.len(), 1, "basis-pinned DB should only see alice, got {:?}", results);
        assert_eq!(results[0], vec![DataType::Long(100), DataType::String("alice".to_string())]);

        // latest db should see both
        let db_latest = node.db().await.unwrap();
        let results_latest = db_latest.query(&query).await.unwrap();
        assert_eq!(results_latest.len(), 2);

        node.close().await;
    }

    #[tokio::test]
    async fn test_db_on_fresh_node_returns_sentinel_tx_key() {
        let node = Node::memory_node().await;

        let db = node.db().await.unwrap();
        let tx_key = db.tx_key();
        assert_eq!(tx_key.tx_id, 0);

        node.close().await;
    }

    /// Insert 3 people: Ivan (100, age=30), Bob (101, age=40), Dominic (102, age=50).
    async fn insert_three_people(node: &impl SubmitNode) {
        let people = vec![
            (100, "Ivan", 30),
            (101, "Bob", 40),
            (102, "Dominic", 50),
        ];
        for (id, name, age) in people {
            let mut doc = BTreeMap::new();
            doc.insert("db/id".to_string(), DataType::Long(id));
            doc.insert("name".to_string(), DataType::String(name.to_string()));
            doc.insert("age".to_string(), DataType::Long(age));
            node.execute_tx(vec![TxOp::Put(Document(doc))]).await.unwrap();
        }
    }

    fn predicate(left: Expr, op: BinaryOp, right: Expr) -> WhereClause {
        WhereClause::Predicate(Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }))
    }

    fn fn_expr(left: Expr, op: BinaryOp, right: Expr, output: &str) -> WhereClause {
        WhereClause::FnExpr(FnExpr {
            expr: Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }),
            output: output.to_string(),
        })
    }

    fn var(name: &str) -> Expr {
        Expr::Variable(name.to_string())
    }

    fn lit_long(n: i64) -> Expr {
        Expr::Literal(DataType::Long(n))
    }

    fn lit_string(s: &str) -> Expr {
        Expr::Literal(DataType::String(s.to_string()))
    }

    fn find_vars(names: &[&str]) -> FindSpec {
        FindSpec::FindRel(names.iter().map(|n| FindElement::Variable(n.to_string())).collect())
    }

    fn triple(entity: &str, attr: &str, value: &str) -> WhereClause {
        WhereClause::Triple(TriplePattern {
            entity: PatternElement::Variable(entity.to_string()),
            attribute: PatternElement::Constant(kw(attr)),
            value: PatternElement::Variable(value.to_string()),
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_predicate_lt() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        insert_three_people(&node).await;

        let query = Query {
            find: find_vars(&["?name"]),
            where_clauses: vec![
                triple("?e", "name", "?name"),
                triple("?e", "age", "?age"),
                predicate(var("?age"), BinaryOp::Lt, lit_long(50)),
            ],
        };

        let db = node.db().await.unwrap();
        let result = db.query(&query).await.unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&vec![DataType::String("Ivan".to_string())]));
        assert!(result.contains(&vec![DataType::String("Bob".to_string())]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_predicate_gte() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        insert_three_people(&node).await;

        let query = Query {
            find: find_vars(&["?name"]),
            where_clauses: vec![
                triple("?e", "name", "?name"),
                triple("?e", "age", "?age"),
                predicate(var("?age"), BinaryOp::GtEq, lit_long(50)),
            ],
        };

        let db = node.db().await.unwrap();
        let result = db.query(&query).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![DataType::String("Dominic".to_string())]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_predicate_eq_entity() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        insert_three_people(&node).await;

        let query = Query {
            find: find_vars(&["?name"]),
            where_clauses: vec![
                triple("?e", "name", "?name"),
                predicate(lit_long(100), BinaryOp::Eq, var("?e")),
            ],
        };

        let db = node.db().await.unwrap();
        let result = db.query(&query).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![DataType::String("Ivan".to_string())]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_predicate_eq_value() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        insert_three_people(&node).await;

        let query = Query {
            find: find_vars(&["?e"]),
            where_clauses: vec![
                triple("?e", "name", "?name"),
                predicate(lit_string("Ivan"), BinaryOp::Eq, var("?name")),
            ],
        };

        let db = node.db().await.unwrap();
        let result = db.query(&query).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![DataType::Long(100)]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_predicate_lte_two_vars() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        insert_three_people(&node).await;

        let query = Query {
            find: find_vars(&["?name1", "?name2"]),
            where_clauses: vec![
                triple("?e1", "name", "?name1"),
                triple("?e1", "age", "?age1"),
                triple("?e2", "name", "?name2"),
                triple("?e2", "age", "?age2"),
                predicate(var("?age1"), BinaryOp::LtEq, var("?age2")),
            ],
        };

        let db = node.db().await.unwrap();
        let result = db.query(&query).await.unwrap();
        // 3 people → 6 pairs where age1 <= age2:
        // (Ivan,Ivan), (Ivan,Bob), (Ivan,Dominic), (Bob,Bob), (Bob,Dominic), (Dominic,Dominic)
        assert_eq!(result.len(), 6);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_fn_div() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        insert_three_people(&node).await;

        let query = Query {
            find: find_vars(&["?name", "?half"]),
            where_clauses: vec![
                triple("?e", "name", "?name"),
                triple("?e", "age", "?age"),
                fn_expr(var("?age"), BinaryOp::Div, lit_long(2), "?half"),
            ],
        };

        let db = node.db().await.unwrap();
        let result = db.query(&query).await.unwrap();
        assert_eq!(result.len(), 3);
        assert!(result.contains(&vec![DataType::String("Ivan".to_string()), DataType::Long(15)]));
        assert!(result.contains(&vec![DataType::String("Bob".to_string()), DataType::Long(20)]));
        assert!(result.contains(&vec![DataType::String("Dominic".to_string()), DataType::Long(25)]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_fn_with_predicate() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        insert_three_people(&node).await;

        let query = Query {
            find: find_vars(&["?name", "?half"]),
            where_clauses: vec![
                triple("?e", "name", "?name"),
                triple("?e", "age", "?age"),
                fn_expr(var("?age"), BinaryOp::Div, lit_long(2), "?half"),
                predicate(var("?half"), BinaryOp::Gt, lit_long(20)),
            ],
        };

        let db = node.db().await.unwrap();
        let result = db.query(&query).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![DataType::String("Dominic".to_string()), DataType::Long(25)]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_fn_sub() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        insert_three_people(&node).await;

        let query = Query {
            find: find_vars(&["?name", "?result"]),
            where_clauses: vec![
                triple("?e", "name", "?name"),
                triple("?e", "age", "?age"),
                fn_expr(var("?age"), BinaryOp::Sub, lit_long(15), "?result"),
            ],
        };

        let db = node.db().await.unwrap();
        let result = db.query(&query).await.unwrap();
        assert_eq!(result.len(), 3);
        assert!(result.contains(&vec![DataType::String("Ivan".to_string()), DataType::Long(15)]));
        assert!(result.contains(&vec![DataType::String("Bob".to_string()), DataType::Long(25)]));
        assert!(result.contains(&vec![DataType::String("Dominic".to_string()), DataType::Long(35)]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_not_clause() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        insert_three_people(&node).await;

        let query = Query {
            find: find_vars(&["?name"]),
            where_clauses: vec![
                triple("?e", "name", "?name"),
                WhereClause::Not(vec![WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(kw("age")),
                    value: PatternElement::Constant(DataType::Long(50)),
                })]),
            ],
        };

        let db = node.db().await.unwrap();
        let result = db.query(&query).await.unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&vec![DataType::String("Ivan".to_string())]));
        assert!(result.contains(&vec![DataType::String("Bob".to_string())]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_keyword_in_value_position() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        // Define "sex" attribute (ID 55) with keyword value type
        let mut sex_attr = BTreeMap::new();
        sex_attr.insert("db/id".to_string(), DataType::Long(55));
        sex_attr.insert("db/ident".to_string(), DataType::Keyword(Keyword::plain("sex")));
        sex_attr.insert("db/valueType".to_string(), DataType::Keyword(Keyword::namespaced("db.type", "keyword")));
        sex_attr.insert("db/cardinality".to_string(), DataType::Keyword(Keyword::namespaced("db.cardinality", "one")));
        node.execute_tx(vec![TxOp::Put(Document(sex_attr))]).await.unwrap();

        let people = vec![
            (100, "Ivan", "male"),
            (101, "Ivana", "female"),
            (102, "Petr", "male"),
            (103, "Doris", "female"),
        ];
        for (id, name, sex) in people {
            let mut doc = BTreeMap::new();
            doc.insert("db/id".to_string(), DataType::Long(id));
            doc.insert("name".to_string(), DataType::String(name.to_string()));
            doc.insert("sex".to_string(), DataType::Keyword(Keyword::plain(sex)));
            node.execute_tx(vec![TxOp::Put(Document(doc))]).await.unwrap();
        }

        // find ?name where [?e :sex :male] [?e :name ?name]
        let query = Query {
            find: find_vars(&["?name"]),
            where_clauses: vec![
                WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(kw("sex")),
                    value: PatternElement::Constant(DataType::Keyword(Keyword::plain("male"))),
                }),
                WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(kw("name")),
                    value: PatternElement::Variable("?name".to_string()),
                }),
            ],
        };

        let db = node.db().await.unwrap();
        let result = db.query(&query).await.unwrap();

        // Only Ivan and Petr are male
        assert_eq!(result.len(), 2);
        assert!(result.contains(&vec![DataType::String("Ivan".to_string())]));
        assert!(result.contains(&vec![DataType::String("Petr".to_string())]));
        assert!(!result.contains(&vec![DataType::String("Ivana".to_string())]));
        assert!(!result.contains(&vec![DataType::String("Doris".to_string())]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_keyword_value_comparison_name_first() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let mut sex_attr = BTreeMap::new();
        sex_attr.insert("db/id".to_string(), DataType::Long(55));
        sex_attr.insert("db/ident".to_string(), DataType::Keyword(Keyword::plain("sex")));
        sex_attr.insert("db/valueType".to_string(), DataType::Keyword(Keyword::namespaced("db.type", "keyword")));
        sex_attr.insert("db/cardinality".to_string(), DataType::Keyword(Keyword::namespaced("db.cardinality", "one")));
        node.execute_tx(vec![TxOp::Put(Document(sex_attr))]).await.unwrap();

        let people = vec![
            (100, "Ivan", "male"),
            (101, "Petr", "male"),
            (102, "Doris", "female"),
            (103, "Jane", "female"),
        ];
        for (id, name, sex) in people {
            let mut doc = BTreeMap::new();
            doc.insert("db/id".to_string(), DataType::Long(id));
            doc.insert("name".to_string(), DataType::String(name.to_string()));
            doc.insert("sex".to_string(), DataType::Keyword(Keyword::plain(sex)));
            node.execute_tx(vec![TxOp::Put(Document(doc))]).await.unwrap();
        }

        // Same clause order as Clojure: name first (binds ?e), sex filter second
        let query = Query {
            find: find_vars(&["?name"]),
            where_clauses: vec![
                triple("?e", "name", "?name"),
                WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(kw("sex")),
                    value: PatternElement::Constant(DataType::Keyword(Keyword::plain("male"))),
                }),
            ],
        };

        let db = node.db().await.unwrap();
        let result = db.query(&query).await.unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.contains(&vec![DataType::String("Ivan".to_string())]));
        assert!(result.contains(&vec![DataType::String("Petr".to_string())]));
        assert!(!result.contains(&vec![DataType::String("Doris".to_string())]));
        assert!(!result.contains(&vec![DataType::String("Jane".to_string())]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_literal_entity_id_in_triple() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        // Define "last-name" attribute (ID 55)
        let mut attr = BTreeMap::new();
        attr.insert("db/id".to_string(), DataType::Long(55));
        attr.insert("db/ident".to_string(), DataType::Keyword(Keyword::plain("last-name")));
        attr.insert("db/valueType".to_string(), DataType::Keyword(Keyword::namespaced("db.type", "string")));
        attr.insert("db/cardinality".to_string(), DataType::Keyword(Keyword::namespaced("db.cardinality", "one")));
        node.execute_tx(vec![TxOp::Put(Document(attr))]).await.unwrap();

        // Insert entity 200 and entity 201 with last-names
        let mut doc1 = BTreeMap::new();
        doc1.insert("db/id".to_string(), DataType::Long(200));
        doc1.insert("last-name".to_string(), DataType::String("Ivannotov".to_string()));
        let mut doc2 = BTreeMap::new();
        doc2.insert("db/id".to_string(), DataType::Long(201));
        doc2.insert("last-name".to_string(), DataType::String("Bobnev".to_string()));
        node.execute_tx(vec![
            TxOp::Put(Document(doc1)),
            TxOp::Put(Document(doc2)),
        ]).await.unwrap();

        // find ?ln where [200 :last-name ?ln] — literal entity ID in entity position
        let query = Query {
            find: find_vars(&["?ln"]),
            where_clauses: vec![
                WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Constant(DataType::Long(200)),
                    attribute: PatternElement::Constant(kw("last-name")),
                    value: PatternElement::Variable("?ln".to_string()),
                }),
            ],
        };

        let db = node.db().await.unwrap();
        let result = db.query(&query).await.unwrap();

        // Should return exactly one row: "Ivannotov"
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![DataType::String("Ivannotov".to_string())]);
    }

    /// Failed transactions return TxAborted and the indexer continues processing.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_failed_tx_returns_aborted_and_indexer_continues() {
        let node = Node::memory_node().await;

        // Submit a tx with unknown attribute — should fail with TxAborted
        let mut bad_doc = BTreeMap::new();
        bad_doc.insert("db/id".to_string(), DataType::Long(100));
        bad_doc.insert("nonexistent/attr".to_string(), DataType::String("x".to_string()));

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            node.execute_tx(vec![TxOp::Put(Document(bad_doc))]),
        ).await.expect("Should not hang").expect("execute_tx should not return Err");

        match &result {
            TransactionResult::TxAborted(_, err) => {
                let msg = err.to_string();
                assert!(msg.contains("Unknown attribute"), "Expected 'Unknown attribute' error, got: {}", msg);
            },
            TransactionResult::TxCommited(_) => panic!("Expected TxAborted, got TxCommited"),
        }

        // Define schema and submit a valid tx — indexer should still be alive
        let result = node.execute_tx(test_schema_tx()).await.unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)),
                "Expected TxCommited for schema tx, got: {:?}", result);

        let mut good_doc = BTreeMap::new();
        good_doc.insert("db/id".to_string(), DataType::Long(200));
        good_doc.insert("name".to_string(), DataType::String("alice".to_string()));

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            node.execute_tx(vec![TxOp::Put(Document(good_doc))]),
        ).await.expect("Should not hang").expect("execute_tx should not return Err");

        assert!(matches!(result, TransactionResult::TxCommited(_)),
                "Expected TxCommited for valid tx, got: {:?}", result);
    }

    /// Cardinality-many attributes accumulate values without retracting old ones.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_cardinality_many_attribute() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        // Insert entity 100 with tags="rust"
        node.execute_tx(vec![TxOp::Add {
            entity_id: crate::ops::EntityId::new(100),
            attribute: crate::ops::Attribute("tags".to_string()),
            value: DataType::String("rust".to_string()),
        }]).await.unwrap();

        // Add another tag to the same entity — should NOT retract "rust"
        node.execute_tx(vec![TxOp::Add {
            entity_id: crate::ops::EntityId::new(100),
            attribute: crate::ops::Attribute("tags".to_string()),
            value: DataType::String("database".to_string()),
        }]).await.unwrap();

        // Query: find all tags for entity 100
        let query = Query {
            find: find_vars(&["?tag"]),
            where_clauses: vec![
                WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Constant(DataType::Long(100)),
                    attribute: PatternElement::Constant(kw("tags")),
                    value: PatternElement::Variable("?tag".to_string()),
                }),
            ],
        };

        let db = node.db().await.unwrap();
        let result = db.query(&query).await.unwrap();

        assert_eq!(result.len(), 2, "Expected both tags, got {:?}", result);
        assert!(result.contains(&vec![DataType::String("rust".to_string())]));
        assert!(result.contains(&vec![DataType::String("database".to_string())]));
    }
}
