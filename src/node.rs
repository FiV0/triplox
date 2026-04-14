use std::path::Path;
use std::sync::Arc;

use anyhow::Error;
use tokio::runtime::Handle;

use crate::clock;
use crate::file_log::FileLog;
use crate::indexer::{latest_tx_key_from_snapshot, Indexer};
use crate::log::{subscribe, TxLog, TxLogReader};
use crate::memory_log::MemoryLog;
use crate::ops::{Entid, QueryArg, TxOp};
use crate::query::{execute_query, validate_query, QueryResult};
use crate::schema::IdentMap;
use crate::slate::{in_memory_slate, local_slate, SlateComponents};
pub use crate::transaction::{TransactionResult, TxKey};
use edn::query::ParsedQuery;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

#[allow(async_fn_in_trait)]
pub trait SubmitNode {
    async fn submit_tx(&self, ops: Vec<TxOp>) -> Result<TxKey, Error>;
    async fn execute_tx(&self, ops: Vec<TxOp>) -> Result<TransactionResult, Error>;
}

pub trait IntoQuery {
    fn into_query(self) -> Result<ParsedQuery, Error>;
}

impl IntoQuery for ParsedQuery {
    fn into_query(self) -> Result<ParsedQuery, Error> {
        Ok(self)
    }
}

// TODO: consider using Cow or a lifetime on the trait to avoid cloning
impl IntoQuery for &ParsedQuery {
    fn into_query(self) -> Result<ParsedQuery, Error> {
        Ok(self.clone())
    }
}

impl IntoQuery for &str {
    fn into_query(self) -> Result<ParsedQuery, Error> {
        self.parse()
            .map_err(|e| anyhow::anyhow!("EDN parse error: {}", e))
    }
}

impl IntoQuery for String {
    fn into_query(self) -> Result<ParsedQuery, Error> {
        self.as_str().into_query()
    }
}

#[allow(async_fn_in_trait)]
pub trait Database {
    async fn query(&self, query: impl IntoQuery) -> Result<QueryResult, Error>;

    async fn query_with_args(
        &self,
        query: &ParsedQuery,
        args: &[QueryArg],
    ) -> Result<QueryResult, Error>;
}

#[allow(async_fn_in_trait)]
pub trait QueryNode {
    type DB: Database;
    async fn db(&self) -> Result<Self::DB, Error>;
    async fn db_as_of(&self, tx_key: TxKey) -> Result<Self::DB, Error>;
}

pub struct DB {
    snapshot: Arc<slatedb::DbSnapshot>,
    ident_map: IdentMap,
    handle: Handle,
    tx_key: TxKey,
    tx_eid: i64,
}

#[allow(unused)]
impl DB {
    pub fn new(
        snapshot: Arc<slatedb::DbSnapshot>,
        ident_map: IdentMap,
        handle: Handle,
        tx_key: TxKey,
        tx_eid: i64,
    ) -> Self {
        Self {
            snapshot,
            ident_map,
            handle,
            tx_key,
            tx_eid,
        }
    }

    /// Construct a DB from a snapshot by scanning EAV for TX_PARTITION entities to find the latest TxKey.
    pub async fn from_latest_snapshot(
        snapshot: Arc<slatedb::DbSnapshot>,
        ident_map: IdentMap,
        handle: Handle,
    ) -> Result<Self, Error> {
        let (tx_eid, tx_key) = crate::indexer::latest_tx_key_from_snapshot(&snapshot).await?;
        Ok(Self {
            snapshot,
            ident_map,
            handle,
            tx_key,
            tx_eid,
        })
    }

    pub fn tx_key(&self) -> &TxKey {
        &self.tx_key
    }

    pub fn entity(&self, _eid: Entid) {
        todo!()
    }
}

impl Database for DB {
    async fn query(&self, query: impl IntoQuery) -> Result<QueryResult, Error> {
        let parsed = query.into_query()?;
        self.query_with_args(&parsed, &[]).await
    }

    /// Execute a query against this database snapshot.
    /// Runs the sync join algorithm in a blocking task to avoid blocking the async runtime.
    async fn query_with_args(
        &self,
        query: &ParsedQuery,
        args: &[QueryArg],
    ) -> Result<QueryResult, Error> {
        validate_query(query, args)?;

        let snapshot = self.snapshot.clone();
        let handle = self.handle.clone();
        let ident_map = self.ident_map.clone();
        let query = query.clone();
        let args = args.to_vec();
        let as_of = self.tx_eid;

        tokio::task::spawn_blocking(move || {
            execute_query(&query, &args, snapshot, handle, &ident_map, as_of)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Query task failed: {}", e))?
    }
}

#[allow(unused)]
pub struct Node<L: TxLog> {
    log: Arc<RwLock<L>>,
    indexer: Arc<tokio::sync::RwLock<Indexer>>,
    slate: SlateComponents,
    subscription: CancellationToken,
}

impl Node<MemoryLog> {
    pub async fn memory_node() -> Self {
        let slate = in_memory_slate().await;
        let db = slate.db.clone();
        let metadata = crate::bootstrap::init_db(db.clone()).await;
        let indexer = Arc::new(tokio::sync::RwLock::new(Indexer::new(
            db.clone(),
            metadata,
            None,
        )));
        let log = Arc::new(RwLock::new(MemoryLog::new(Box::new(clock::SystemClock))));

        let subscription = subscribe(log.clone(), None, indexer.clone()).await;

        Node {
            log,
            indexer,
            slate,
            subscription,
        }
    }
}

impl Node<FileLog> {
    pub async fn local_node(root_path: &Path) -> Result<Self, Error> {
        std::fs::create_dir_all(root_path.join("db"))?;
        let db_path = root_path.join("db");
        let db_path_str = db_path
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("database path is not valid UTF-8: {:?}", db_path))?;
        let slate = local_slate(db_path_str).await;
        let db = slate.db.clone();
        let metadata = crate::bootstrap::init_db(db.clone()).await;

        // Determine the latest already-indexed tx_id so we skip replaying it
        // and restore the indexer's in-memory state for TxWaiter fast-path.
        let snapshot = Arc::new(db.snapshot().await?);
        let (_tx_eid, latest_indexed) = latest_tx_key_from_snapshot(&snapshot).await?;
        let latest_indexed_tx = if latest_indexed.tx_id > 0 {
            Some(latest_indexed)
        } else {
            None
        };

        let indexer = Arc::new(tokio::sync::RwLock::new(Indexer::new(
            db.clone(),
            metadata,
            latest_indexed_tx,
        )));
        let log = Arc::new(RwLock::new(FileLog::new(
            &root_path.join("log"),
            Box::new(clock::SystemClock),
        )?));

        let after_tx_id = latest_indexed_tx.map(|k| k.tx_id);

        // Read the last tx_key from the log before subscribing (for catch-up awaiting)
        let last_tx_key = {
            let log_reader = log.read().await;
            let records = log_reader.read_txs_after(after_tx_id, u16::MAX)?;
            records.last().map(|r| r.tx_key)
        };

        // Create a waiter for catch-up completion
        let waiter = match last_tx_key {
            Some(_) => Some(indexer.read().await.tx_waiter()),
            None => None,
        };

        let subscription = subscribe(log.clone(), after_tx_id, indexer.clone()).await;

        // Wait for catch-up to complete if there are un-indexed transactions
        if let Some((tx_key, waiter)) = last_tx_key.zip(waiter) {
            waiter.await_tx(tx_key).await?;
        }

        Ok(Node {
            log,
            indexer,
            slate,
            subscription,
        })
    }
}

impl<L: TxLog> Node<L> {
    pub async fn close(self) {
        self.subscription.cancel();
        self.slate.db.close().await.unwrap();
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
        let snapshot = self.slate.db.snapshot().await?;
        let ident_map = self
            .indexer
            .read()
            .await
            .metadata()
            .schema
            .ident_map
            .clone();
        let handle = Handle::current();
        DB::from_latest_snapshot(snapshot, ident_map, handle).await
    }
    // TODO we are currently not checking that snapshot contains TxKey
    async fn db_as_of(&self, tx_key: TxKey) -> Result<DB, Error> {
        let snapshot = self.slate.db.snapshot().await?;
        let ident_map = self
            .indexer
            .read()
            .await
            .metadata()
            .schema
            .ident_map
            .clone();
        let handle = Handle::current();
        let tx_eid = crate::indexer::tx_eid_for_tx_key(&snapshot, &tx_key).await?;
        Ok(DB::new(snapshot, ident_map, handle, tx_key, tx_eid))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::{DataType, EntityRef, TxOp};
    use crate::schema::test_schema_tx;
    use crate::transaction::TransactionResult;
    use edn::kw;
    use edn::Keyword;

    /// Define common test attributes (name, age, email, follows) through the standard tx path.
    async fn define_test_schema(node: &impl SubmitNode) {
        let result = node.execute_tx(test_schema_tx()).await.unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));
    }

    #[tokio::test]
    async fn test_submit_tx_async_indexing() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let tx_ops = vec![TxOp::Add {
            entity: "bob".into(),
            attribute: kw!(:name),
            value: "bob".into(),
        }];

        let waiter = node.indexer.read().await.tx_waiter();

        // submit_tx returns immediately with a TxKey
        let tx_key = node.submit_tx(tx_ops).await.unwrap();
        assert_eq!(tx_key.tx_id, 1);

        // Wait for indexer to process the transaction
        waiter
            .await_tx(tx_key)
            .await
            .expect("Transaction should be indexed");

        // Verify data is queryable after async indexing
        let db = node.db().await.unwrap();
        let result = db
            .query("[:find ?name :where [?e :name ?name]]")
            .await
            .unwrap();
        assert_eq!(result, vec![vec![DataType::String("bob".to_string())]]);
    }

    #[tokio::test]
    async fn test_execute_tx_with_add_triple() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let tx_ops = vec![TxOp::Add {
            entity: "e".into(),
            attribute: kw!(:email),
            value: "test@example.com".into(),
        }];

        let result = node.execute_tx(tx_ops).await.unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        let db = node.db().await.unwrap();
        let result = db
            .query(r#"[:find ?email :where [?e :email ?email]]"#)
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            vec![DataType::String("test@example.com".to_string())]
        );
    }

    // End-to-end query tests

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_single_pattern_var_const_var() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        node.execute_tx(vec![TxOp::Add {
            entity: "alice".into(),
            attribute: kw!(:name),
            value: "alice".into(),
        }])
        .await
        .unwrap();
        node.execute_tx(vec![TxOp::Add {
            entity: "bob".into(),
            attribute: kw!(:name),
            value: "bob".into(),
        }])
        .await
        .unwrap();

        let db = node.db().await.unwrap();
        let result = db
            .query("[:find ?name :where [?e :name ?name]]")
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.contains(&vec![DataType::String("alice".to_string())]));
        assert!(result.contains(&vec![DataType::String("bob".to_string())]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_single_pattern_var_const_const() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        node.execute_tx(vec![TxOp::Add {
            entity: "alice".into(),
            attribute: kw!(:name),
            value: "alice".into(),
        }])
        .await
        .unwrap();
        node.execute_tx(vec![TxOp::Add {
            entity: "bob".into(),
            attribute: kw!(:name),
            value: "bob".into(),
        }])
        .await
        .unwrap();

        let db = node.db().await.unwrap();
        let result = db
            .query(r#"[:find ?e :where [?e :name "alice"]]"#)
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_two_patterns_join() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        node.execute_tx(vec![TxOp::put(vec![
            (kw!(:name), "alice".into()),
            (kw!(:age), 30_i64.into()),
        ])])
        .await
        .unwrap();
        node.execute_tx(vec![TxOp::Add {
            entity: "bob".into(),
            attribute: kw!(:name),
            value: "bob".into(),
        }])
        .await
        .unwrap();

        let db = node.db().await.unwrap();
        let result = db
            .query("[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]")
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            vec![DataType::String("alice".to_string()), DataType::Long(30)]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_self_join_on_value_variable() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        node.execute_tx(vec![
            TxOp::Add {
                entity: "alice".into(),
                attribute: kw!(:name),
                value: "alice".into(),
            },
            TxOp::Add {
                entity: "bob".into(),
                attribute: kw!(:name),
                value: "bob".into(),
            },
        ])
        .await
        .unwrap();

        let db = node.db().await.unwrap();
        let result = db
            .query("[:find ?a ?b :where [?a :name ?x] [?b :name ?x]]")
            .await
            .unwrap();

        // Only same-entity pairs should match: (alice, alice) and (bob, bob).
        assert_eq!(result.len(), 2, "expected 2 self-pairs, got {:?}", result);
        for row in &result {
            assert_eq!(row.len(), 2);
            assert_eq!(row[0], row[1], "?a and ?b should be equal in {:?}", row);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_with_scalar_in_bindings() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        node.execute_tx(vec![
            TxOp::put(vec![
                (kw!(:db/id), DataType::String("ivan".to_string())),
                (kw!(:name), "Ivan".into()),
                (kw!(:email), "ivan@example.com".into()),
            ]),
            TxOp::put(vec![
                (kw!(:db/id), DataType::String("petr".to_string())),
                (kw!(:name), "Petr".into()),
                (kw!(:email), "petr@example.com".into()),
            ]),
        ])
        .await
        .unwrap();

        let db = node.db().await.unwrap();
        let parsed = edn::parse::parse_query(
            "[:find ?name ?email :in ?name :where [?e :name ?name] [?e :email ?email]]",
        )
        .unwrap();

        // Bind ?name to "Petr": only Petr's row should match.
        let result = db
            .query_with_args(&parsed, &[QueryArg::Scalar("Petr".into())])
            .await
            .unwrap();
        assert_eq!(
            result,
            vec![vec![
                DataType::String("Petr".to_string()),
                DataType::String("petr@example.com".to_string()),
            ]]
        );

        // Bind ?name to "Ivan": only Ivan's row.
        let result = db
            .query_with_args(&parsed, &[QueryArg::Scalar("Ivan".into())])
            .await
            .unwrap();
        assert_eq!(
            result,
            vec![vec![
                DataType::String("Ivan".to_string()),
                DataType::String("ivan@example.com".to_string()),
            ]]
        );

        // A name with no match yields no rows.
        let result = db
            .query_with_args(&parsed, &[QueryArg::Scalar("Bob".into())])
            .await
            .unwrap();
        assert!(result.is_empty());

        // Multiple scalar bindings: constrain on both name and email.
        let parsed = edn::parse::parse_query(
            "[:find ?e :in ?name ?email :where [?e :name ?name] [?e :email ?email]]",
        )
        .unwrap();
        let result = db
            .query_with_args(
                &parsed,
                &[
                    QueryArg::Scalar("Petr".into()),
                    QueryArg::Scalar("petr@example.com".into()),
                ],
            )
            .await
            .unwrap();
        assert_eq!(result.len(), 1);

        // Mismatched pair: no rows.
        let result = db
            .query_with_args(
                &parsed,
                &[
                    QueryArg::Scalar("Petr".into()),
                    QueryArg::Scalar("ivan@example.com".into()),
                ],
            )
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_with_variable_limit_in_binding() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        // Insert three named entities so LIMIT can actually truncate.
        node.execute_tx(vec![
            TxOp::put(vec![
                (kw!(:db/id), DataType::String("a".to_string())),
                (kw!(:name), "Alice".into()),
            ]),
            TxOp::put(vec![
                (kw!(:db/id), DataType::String("b".to_string())),
                (kw!(:name), "Bob".into()),
            ]),
            TxOp::put(vec![
                (kw!(:db/id), DataType::String("c".to_string())),
                (kw!(:name), "Carol".into()),
            ]),
        ])
        .await
        .unwrap();

        let db = node.db().await.unwrap();
        let parsed = edn::parse::parse_query(
            "[:find ?name :in ?limit :where [?e :name ?name] :order [?name :asc] :limit ?limit]",
        )
        .unwrap();

        // Variable limit resolved to 2.
        let result = db
            .query_with_args(&parsed, &[QueryArg::Scalar(DataType::Long(2))])
            .await
            .unwrap();
        assert_eq!(
            result,
            vec![
                vec![DataType::String("Alice".to_string())],
                vec![DataType::String("Bob".to_string())],
            ]
        );

        // Zero limit yields an empty result.
        let result = db
            .query_with_args(&parsed, &[QueryArg::Scalar(DataType::Long(0))])
            .await
            .unwrap();
        assert!(result.is_empty());

        // Non-Long binding for a variable limit is rejected.
        let err = db
            .query_with_args(&parsed, &[QueryArg::Scalar("two".into())])
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("must be bound to a Long"),
            "unexpected error: {}",
            err
        );

        // Negative limit is rejected.
        let err = db
            .query_with_args(&parsed, &[QueryArg::Scalar(DataType::Long(-1))])
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("non-negative"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_entity_value_join() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        node.execute_tx(vec![
            TxOp::put(vec![
                (kw!(:db/id), DataType::String("alice".to_string())),
                (kw!(:name), "alice".into()),
                (kw!(:follows), DataType::String("bob".to_string())),
            ]),
            TxOp::put(vec![
                (kw!(:db/id), DataType::String("bob".to_string())),
                (kw!(:name), "bob".into()),
            ]),
        ])
        .await
        .unwrap();

        // ?friend is in value position of :follows and entity position of :name

        let db = node.db().await.unwrap();
        let result = db
            .query("[:find ?name :where [?e :follows ?friend] [?friend :name ?name]]")
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![DataType::String("bob".to_string())]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_or_clause_basic() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        node.execute_tx(vec![TxOp::Add {
            entity: "alice".into(),
            attribute: kw!(:name),
            value: "alice".into(),
        }])
        .await
        .unwrap();
        node.execute_tx(vec![TxOp::Add {
            entity: "bob".into(),
            attribute: kw!(:name),
            value: "bob".into(),
        }])
        .await
        .unwrap();
        node.execute_tx(vec![TxOp::Add {
            entity: "charlie".into(),
            attribute: kw!(:name),
            value: "charlie".into(),
        }])
        .await
        .unwrap();

        let db = node.db().await.unwrap();
        let result = db
            .query(
                r#"[:find ?name :where (or [?e :name "alice"] [?e :name "bob"]) [?e :name ?name]]"#,
            )
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.contains(&vec![DataType::String("alice".to_string())]));
        assert!(result.contains(&vec![DataType::String("bob".to_string())]));
        assert!(!result.contains(&vec![DataType::String("charlie".to_string())]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_or_clause_with_additional_join() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        node.execute_tx(vec![TxOp::put(vec![
            (kw!(:name), "alice".into()),
            (kw!(:age), 30_i64.into()),
        ])])
        .await
        .unwrap();
        node.execute_tx(vec![TxOp::put(vec![
            (kw!(:name), "bob".into()),
            (kw!(:age), 25_i64.into()),
        ])])
        .await
        .unwrap();
        node.execute_tx(vec![TxOp::put(vec![
            (kw!(:name), "charlie".into()),
            (kw!(:age), 35_i64.into()),
        ])])
        .await
        .unwrap();

        let db = node.db().await.unwrap();
        let result = db.query(r#"[:find ?name ?age :where (or [?e :name "alice"] [?e :name "bob"]) [?e :name ?name] [?e :age ?age]]"#).await.unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.contains(&vec![
            DataType::String("alice".to_string()),
            DataType::Long(30)
        ]));
        assert!(result.contains(&vec![
            DataType::String("bob".to_string()),
            DataType::Long(25)
        ]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_and_inside_or() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        node.execute_tx(vec![TxOp::put(vec![
            (kw!(:name), "alice".into()),
            (kw!(:age), 30_i64.into()),
        ])])
        .await
        .unwrap();
        node.execute_tx(vec![TxOp::put(vec![
            (kw!(:name), "bob".into()),
            (kw!(:age), 25_i64.into()),
        ])])
        .await
        .unwrap();
        node.execute_tx(vec![TxOp::put(vec![
            (kw!(:name), "charlie".into()),
            (kw!(:age), 35_i64.into()),
        ])])
        .await
        .unwrap();

        let db = node.db().await.unwrap();
        let result = db.query(r#"[:find ?name :where (or (and [?e :name "alice"] [?e :age 30]) (and [?e :name "charlie"] [?e :age 35])) [?e :name ?name]]"#).await.unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.contains(&vec![DataType::String("alice".to_string())]));
        assert!(result.contains(&vec![DataType::String("charlie".to_string())]));
        assert!(!result.contains(&vec![DataType::String("bob".to_string())]));
    }

    // TODO(#86): Once cardinality/one override is implemented, update this test to use
    // the same entity in both transactions and verify only the latest value is returned.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_db_as_of_time_travel() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let tx_key1 = match node
            .execute_tx(vec![TxOp::Add {
                entity: "alice".into(),
                attribute: kw!(:name),
                value: "alice".into(),
            }])
            .await
            .unwrap()
        {
            TransactionResult::TxCommited(tx_key) => tx_key,
            _ => panic!("Tx1 should commit"),
        };

        let tx_key2 = match node
            .execute_tx(vec![TxOp::Add {
                entity: "bob".into(),
                attribute: kw!(:name),
                value: "bob".into(),
            }])
            .await
            .unwrap()
        {
            TransactionResult::TxCommited(tx_key) => tx_key,
            _ => panic!("Tx2 should commit"),
        };

        // db_as_of(tx_key1): should only see alice
        let db1 = node.db_as_of(tx_key1).await.unwrap();
        let result1 = db1
            .query("[:find ?name :where [?e :name ?name]]")
            .await
            .unwrap();
        assert_eq!(result1.len(), 1);
        assert_eq!(result1[0], vec![DataType::String("alice".to_string())]);

        // db_as_of(tx_key2): should see both
        let db2 = node.db_as_of(tx_key2).await.unwrap();
        let result2 = db2
            .query("[:find ?name :where [?e :name ?name]]")
            .await
            .unwrap();
        assert_eq!(result2.len(), 2);
        assert!(result2.contains(&vec![DataType::String("alice".to_string())]));
        assert!(result2.contains(&vec![DataType::String("bob".to_string())]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_local_node_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let root_path = dir.path().to_path_buf();

        // First node: insert data
        let node = Node::local_node(&root_path).await.unwrap();
        define_test_schema(&node).await;

        let result = node
            .execute_tx(vec![TxOp::Add {
                entity: "alice".into(),
                attribute: kw!(:name),
                value: "alice".into(),
            }])
            .await
            .unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        let db = node.db().await.unwrap();
        let results = db
            .query("[:find ?name :where [?e :name ?name]]")
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], vec![DataType::String("alice".to_string())]);

        node.close().await;

        // Second node: reopen at same path, verify data persisted, add more
        let node = Node::local_node(&root_path).await.unwrap();

        let db = node.db().await.unwrap();
        let results = db
            .query("[:find ?name :where [?e :name ?name]]")
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], vec![DataType::String("alice".to_string())]);

        let result = node
            .execute_tx(vec![TxOp::Add {
                entity: "bob".into(),
                attribute: kw!(:name),
                value: "bob".into(),
            }])
            .await
            .unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        let db = node.db().await.unwrap();
        let results = db
            .query("[:find ?name :where [?e :name ?name]]")
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.contains(&vec![DataType::String("alice".to_string())]));
        assert!(results.contains(&vec![DataType::String("bob".to_string())]));

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

        let result = node
            .execute_tx(vec![TxOp::Add {
                entity: "alice".into(),
                attribute: kw!(:name),
                value: "alice".into(),
            }])
            .await
            .unwrap();
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
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(2), waiter.await_tx(tx_key)).await;

        assert!(
            result.is_ok(),
            "tx_waiter should not timeout for an already-indexed tx"
        );
        result.unwrap().expect("await_tx should succeed");

        node.close().await;
    }

    #[tokio::test]
    async fn test_db_as_of_pins_snapshot() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        // First transaction: insert alice
        let result1 = node
            .execute_tx(vec![TxOp::Add {
                entity: "alice".into(),
                attribute: kw!(:name),
                value: "alice".into(),
            }])
            .await
            .unwrap();
        let tx_key1 = match result1 {
            TransactionResult::TxCommited(tk) => tk,
            _ => panic!("Expected TxCommited"),
        };

        // Second transaction: insert bob
        node.execute_tx(vec![TxOp::Add {
            entity: "bob".into(),
            attribute: kw!(:name),
            value: "bob".into(),
        }])
        .await
        .unwrap();

        // db_as_of pinned to first tx should only see alice
        let db = node.db_as_of(tx_key1).await.unwrap();
        let results = db
            .query("[:find ?name :where [?e :name ?name]]")
            .await
            .unwrap();
        assert_eq!(
            results.len(),
            1,
            "basis-pinned DB should only see alice, got {:?}",
            results
        );
        assert_eq!(results[0], vec![DataType::String("alice".to_string())]);

        // latest db should see both
        let db_latest = node.db().await.unwrap();
        let results_latest = db_latest
            .query("[:find ?name :where [?e :name ?name]]")
            .await
            .unwrap();
        assert_eq!(results_latest.len(), 2);

        node.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upsert_with_resolved_entity_id() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        // Insert entity with auto-assigned ID
        node.execute_tx(vec![TxOp::put(vec![
            (kw!(:name), "alice".into()),
            (kw!(:age), 30_i64.into()),
        ])])
        .await
        .unwrap();

        // Discover the auto-assigned entity ID
        let db = node.db().await.unwrap();
        let result = db
            .query(r#"[:find ?e :where [?e :name "alice"]]"#)
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        let entity_id = match &result[0][0] {
            DataType::Long(id) => *id,
            other => panic!("Expected Long entity ID, got {:?}", other),
        };

        // Upsert: update age using the discovered entity ID
        node.execute_tx(vec![TxOp::Add {
            entity: entity_id.into(),
            attribute: kw!(:age),
            value: 31_i64.into(),
        }])
        .await
        .unwrap();

        // Verify: alice should now have age 31 (cardinality-one retracted 30)
        let db = node.db().await.unwrap();
        let result = db
            .query("[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]")
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            vec![DataType::String("alice".to_string()), DataType::Long(31)]
        );

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

    /// Insert 3 people: Ivan (age=30), Bob (age=40), Dominic (age=50) with auto-assigned IDs.
    async fn insert_three_people(node: &impl SubmitNode) {
        let people: Vec<(&str, i64)> = vec![("Ivan", 30), ("Bob", 40), ("Dominic", 50)];
        for (name, age) in people {
            node.execute_tx(vec![TxOp::put(vec![
                (kw!(:name), name.into()),
                (kw!(:age), age.into()),
            ])])
            .await
            .unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_predicate_lt() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        insert_three_people(&node).await;

        let db = node.db().await.unwrap();
        let result = db
            .query("[:find ?name :where [?e :name ?name] [?e :age ?age] [(< ?age 50)]]")
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&vec![DataType::String("Ivan".to_string())]));
        assert!(result.contains(&vec![DataType::String("Bob".to_string())]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_predicate_gte() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        insert_three_people(&node).await;

        let db = node.db().await.unwrap();
        let result = db
            .query("[:find ?name :where [?e :name ?name] [?e :age ?age] [(>= ?age 50)]]")
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![DataType::String("Dominic".to_string())]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_predicate_eq_entity() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        insert_three_people(&node).await;

        let db = node.db().await.unwrap();
        let result = db
            .query("[:find ?name :where [?e :name ?name] [?e :age ?age] [(= 30 ?age)]]")
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![DataType::String("Ivan".to_string())]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_predicate_eq_value() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        insert_three_people(&node).await;

        let db = node.db().await.unwrap();
        let result = db
            .query(r#"[:find ?e :where [?e :name ?name] [(= "Ivan" ?name)]]"#)
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_predicate_lte_two_vars() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        insert_three_people(&node).await;

        let db = node.db().await.unwrap();
        let result = db.query("[:find ?name1 ?name2 :where [?e1 :name ?name1] [?e1 :age ?age1] [?e2 :name ?name2] [?e2 :age ?age2] [(<= ?age1 ?age2)]]").await.unwrap();
        // 3 people → 6 pairs where age1 <= age2:
        // (Ivan,Ivan), (Ivan,Bob), (Ivan,Dominic), (Bob,Bob), (Bob,Dominic), (Dominic,Dominic)
        assert_eq!(result.len(), 6);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_fn_div() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        insert_three_people(&node).await;

        let db = node.db().await.unwrap();
        let result = db
            .query("[:find ?name ?half :where [?e :name ?name] [?e :age ?age] [(/ ?age 2) ?half]]")
            .await
            .unwrap();
        assert_eq!(result.len(), 3);
        assert!(result.contains(&vec![
            DataType::String("Ivan".to_string()),
            DataType::Long(15)
        ]));
        assert!(result.contains(&vec![
            DataType::String("Bob".to_string()),
            DataType::Long(20)
        ]));
        assert!(result.contains(&vec![
            DataType::String("Dominic".to_string()),
            DataType::Long(25)
        ]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_fn_with_predicate() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        insert_three_people(&node).await;

        let db = node.db().await.unwrap();
        let result = db.query("[:find ?name ?half :where [?e :name ?name] [?e :age ?age] [(/ ?age 2) ?half] [(> ?half 20)]]").await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            vec![DataType::String("Dominic".to_string()), DataType::Long(25)]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_fn_sub() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        insert_three_people(&node).await;

        let db = node.db().await.unwrap();
        let result = db.query("[:find ?name ?result :where [?e :name ?name] [?e :age ?age] [(- ?age 15) ?result]]").await.unwrap();
        assert_eq!(result.len(), 3);
        assert!(result.contains(&vec![
            DataType::String("Ivan".to_string()),
            DataType::Long(15)
        ]));
        assert!(result.contains(&vec![
            DataType::String("Bob".to_string()),
            DataType::Long(25)
        ]));
        assert!(result.contains(&vec![
            DataType::String("Dominic".to_string()),
            DataType::Long(35)
        ]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_not_clause() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        insert_three_people(&node).await;

        let db = node.db().await.unwrap();
        let result = db
            .query("[:find ?name :where [?e :name ?name] (not [?e :age 50])]")
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&vec![DataType::String("Ivan".to_string())]));
        assert!(result.contains(&vec![DataType::String("Bob".to_string())]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_keyword_in_value_position() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        // Define "sex" attribute with keyword value type
        node.execute_tx(vec![TxOp::put(vec![
            (kw!(:db/ident), DataType::Keyword(kw!(:sex)).into()),
            (
                kw!(:db/valueType),
                DataType::Keyword(kw!(:db.type/keyword)).into(),
            ),
            (
                kw!(:db/cardinality),
                DataType::Keyword(kw!(:db.cardinality/one)).into(),
            ),
        ])])
        .await
        .unwrap();

        let people = vec![
            ("Ivan", "male"),
            ("Ivana", "female"),
            ("Petr", "male"),
            ("Doris", "female"),
        ];
        for (name, sex) in people {
            node.execute_tx(vec![TxOp::put(vec![
                (kw!(:name), name.into()),
                (kw!(:sex), DataType::Keyword(Keyword::plain(sex)).into()),
            ])])
            .await
            .unwrap();
        }

        // find ?name where [?e :sex :male] [?e :name ?name]

        let db = node.db().await.unwrap();
        let result = db
            .query("[:find ?name :where [?e :sex :male] [?e :name ?name]]")
            .await
            .unwrap();

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

        node.execute_tx(vec![TxOp::put(vec![
            (kw!(:db/ident), DataType::Keyword(kw!(:sex)).into()),
            (
                kw!(:db/valueType),
                DataType::Keyword(kw!(:db.type/keyword)).into(),
            ),
            (
                kw!(:db/cardinality),
                DataType::Keyword(kw!(:db.cardinality/one)).into(),
            ),
        ])])
        .await
        .unwrap();

        let people = vec![
            ("Ivan", "male"),
            ("Petr", "male"),
            ("Doris", "female"),
            ("Jane", "female"),
        ];
        for (name, sex) in people {
            node.execute_tx(vec![TxOp::put(vec![
                (kw!(:name), name.into()),
                (kw!(:sex), DataType::Keyword(Keyword::plain(sex)).into()),
            ])])
            .await
            .unwrap();
        }

        // Same clause order as Clojure: name first (binds ?e), sex filter second

        let db = node.db().await.unwrap();
        let result = db
            .query("[:find ?name :where [?e :name ?name] [?e :sex :male]]")
            .await
            .unwrap();

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

        // Define "last-name" attribute
        node.execute_tx(vec![TxOp::put(vec![
            (kw!(:db/ident), DataType::Keyword(kw!(:last-name)).into()),
            (
                kw!(:db/valueType),
                DataType::Keyword(kw!(:db.type/string)).into(),
            ),
            (
                kw!(:db/cardinality),
                DataType::Keyword(kw!(:db.cardinality/one)).into(),
            ),
        ])])
        .await
        .unwrap();

        // Insert two entities with last-names
        node.execute_tx(vec![
            TxOp::Add {
                entity: "ivannotov".into(),
                attribute: kw!(:last-name),
                value: "Ivannotov".into(),
            },
            TxOp::Add {
                entity: "bobnev".into(),
                attribute: kw!(:last-name),
                value: "Bobnev".into(),
            },
        ])
        .await
        .unwrap();

        // Discover the entity ID for "Ivannotov"
        let db = node.db().await.unwrap();
        let ids = db
            .query(r#"[:find ?e :where [?e :last-name "Ivannotov"]]"#)
            .await
            .unwrap();
        assert_eq!(ids.len(), 1);
        let entity_id = match &ids[0][0] {
            DataType::Long(id) => *id,
            other => panic!("Expected Long entity ID, got {:?}", other),
        };

        // Use literal entity ID in entity position of a query
        let result = db
            .query(format!("[:find ?ln :where [{entity_id} :last-name ?ln]]"))
            .await
            .unwrap();

        // Should return exactly one row: "Ivannotov"
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![DataType::String("Ivannotov".to_string())]);
    }

    /// Failed transactions return TxAborted and the indexer continues processing.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_failed_tx_returns_aborted_and_indexer_continues() {
        let node = Node::memory_node().await;

        // Submit a tx with unknown attribute — should fail with TxAborted
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            node.execute_tx(vec![TxOp::Add {
                entity: "e".into(),
                attribute: kw!(:nonexistent/attr),
                value: "x".into(),
            }]),
        )
        .await
        .expect("Should not hang")
        .expect("execute_tx should not return Err");

        match &result {
            TransactionResult::TxAborted(_, err) => {
                let msg = err.to_string();
                assert!(
                    msg.contains("Unknown attribute"),
                    "Expected 'Unknown attribute' error, got: {}",
                    msg
                );
            }
            TransactionResult::TxCommited(_) => panic!("Expected TxAborted, got TxCommited"),
        }

        // Define schema and submit a valid tx — indexer should still be alive
        let result = node.execute_tx(test_schema_tx()).await.unwrap();
        assert!(
            matches!(result, TransactionResult::TxCommited(_)),
            "Expected TxCommited for schema tx, got: {:?}",
            result
        );

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            node.execute_tx(vec![TxOp::Add {
                entity: "alice".into(),
                attribute: kw!(:name),
                value: "alice".into(),
            }]),
        )
        .await
        .expect("Should not hang")
        .expect("execute_tx should not return Err");

        assert!(
            matches!(result, TransactionResult::TxCommited(_)),
            "Expected TxCommited for valid tx, got: {:?}",
            result
        );
    }

    /// Cardinality-many attributes accumulate values without retracting old ones.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_cardinality_many_attribute() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        // Insert entity with tags="rust"
        node.execute_tx(vec![TxOp::Add {
            entity: "e".into(),
            attribute: kw!(:tags),
            value: "rust".into(),
        }])
        .await
        .unwrap();

        // Discover the auto-assigned entity ID
        let db = node.db().await.unwrap();
        let result = db
            .query(r#"[:find ?e :where [?e :tags "rust"]]"#)
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        let entity_id = match &result[0][0] {
            DataType::Long(id) => *id,
            other => panic!("Expected Long entity ID, got {:?}", other),
        };

        // Add another tag to the same entity — should NOT retract "rust"
        node.execute_tx(vec![TxOp::Add {
            entity: entity_id.into(),
            attribute: kw!(:tags),
            value: "database".into(),
        }])
        .await
        .unwrap();

        // Query: find all tags for the entity
        let db = node.db().await.unwrap();
        let result = db
            .query(format!("[:find ?tag :where [{entity_id} :tags ?tag]]"))
            .await
            .unwrap();

        assert_eq!(result.len(), 2, "Expected both tags, got {:?}", result);
        assert!(result.contains(&vec![DataType::String("rust".to_string())]));
        assert!(result.contains(&vec![DataType::String("database".to_string())]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_failed_tx_does_not_advance_counters() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        // tx1: valid insert — allocates first user entity
        let result1 = node
            .execute_tx(vec![TxOp::Add {
                entity: "alice".into(),
                attribute: kw!(:name),
                value: "alice".into(),
            }])
            .await
            .unwrap();
        assert!(matches!(result1, TransactionResult::TxCommited(_)));

        // tx2: insert with unknown attribute — should fail
        let result2 = node
            .execute_tx(vec![TxOp::Add {
                entity: "e".into(),
                attribute: kw!(:nonexistent_attr),
                value: "oops".into(),
            }])
            .await
            .unwrap();
        assert!(matches!(result2, TransactionResult::TxAborted(_, _)));

        // tx3: valid insert — should get the next contiguous entity ID
        let result3 = node
            .execute_tx(vec![TxOp::Add {
                entity: "bob".into(),
                attribute: kw!(:name),
                value: "bob".into(),
            }])
            .await
            .unwrap();
        assert!(matches!(result3, TransactionResult::TxCommited(_)));

        // Query all (entity, name) pairs and verify contiguous counter values
        let db = node.db().await.unwrap();
        let result = db
            .query("[:find ?e ?name :where [?e :name ?name]]")
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        let mut eids: Vec<i64> = result
            .iter()
            .map(|row| match &row[0] {
                DataType::Long(id) => *id,
                _ => panic!("Expected Long entity ID"),
            })
            .collect();
        eids.sort();

        // Counters 0 and 1 — no gap from the failed tx
        assert_eq!(crate::partition::extract_counter(eids[0]), 0);
        assert_eq!(crate::partition::extract_counter(eids[1]), 1);
    }

    // --- First-class transaction entity tests ---

    #[tokio::test(flavor = "multi_thread")]
    async fn test_committed_tx_entity() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let result = node
            .execute_tx(vec![TxOp::Add {
                entity: "alice".into(),
                attribute: kw!(:name),
                value: "alice".into(),
            }])
            .await
            .unwrap();
        let tx_key = match result {
            TransactionResult::TxCommited(k) => k,
            _ => panic!("Expected committed"),
        };

        // Query for the tx entity matching this tx_id, resolving the ref to its ident
        let db = node.db().await.unwrap();
        let query_str = format!(
            "[:find ?ident \
             :where [?tx :db/txId {}] [?tx :db/txResult ?r] [?r :db/ident ?ident]]",
            tx_key.tx_id
        );
        let result = db.query(query_str).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], DataType::Keyword(kw!(:db.tx/committed)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_aborted_tx_entity() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        // Submit a transaction with an unknown attribute to trigger abort
        let result = node
            .execute_tx(vec![TxOp::Add {
                entity: "e".into(),
                attribute: kw!(:nonexistent),
                value: "x".into(),
            }])
            .await
            .unwrap();
        let tx_key = match &result {
            TransactionResult::TxAborted(k, _) => *k,
            _ => panic!("Expected aborted, got {:?}", result),
        };

        // Query for the aborted tx entity, resolving the ref to its ident
        let db = node.db().await.unwrap();
        let query_str = format!(
            "[:find ?ident ?error \
             :where [?tx :db/txId {}] [?tx :db/txResult ?r] [?r :db/ident ?ident] [?tx :db.tx/error ?error]]",
            tx_key.tx_id
        );
        let result = db.query(query_str).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], DataType::Keyword(kw!(:db.tx/aborted)));
        if let DataType::String(s) = &result[0][1] {
            assert!(
                s.contains("nonexistent"),
                "Error should mention the unknown attribute, got: {}",
                s
            );
        } else {
            panic!("Expected String for error, got {:?}", result[0][1]);
        }
    }

    // --- Lookup ref tests ---

    #[tokio::test]
    async fn test_lookup_ref_entity_position() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        // Create an entity with a known email
        node.execute_tx(vec![TxOp::put(vec![
            (kw!(:name), "Alice".into()),
            (kw!(:email), "alice@example.com".into()),
        ])])
        .await
        .unwrap();

        // Use a lookup ref in entity position to add an attribute to the same entity
        let result = node
            .execute_tx(vec![TxOp::Add {
                entity: EntityRef::LookupRef(
                    kw!(:email),
                    DataType::String("alice@example.com".into()),
                ),
                attribute: kw!(:age),
                value: DataType::Long(30),
            }])
            .await
            .unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        // Verify both :name and :age are on the same entity
        let db = node.db().await.unwrap();
        let result = db
            .query("[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]")
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], DataType::String("Alice".into()));
        assert_eq!(result[0][1], DataType::Long(30));
    }

    #[tokio::test]
    async fn test_lookup_ref_value_position() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        // Create two entities
        node.execute_tx(vec![TxOp::put(vec![
            (kw!(:name), "Alice".into()),
            (kw!(:email), "alice@example.com".into()),
        ])])
        .await
        .unwrap();

        node.execute_tx(vec![TxOp::put(vec![
            (kw!(:name), "Bob".into()),
            (kw!(:email), "bob@example.com".into()),
        ])])
        .await
        .unwrap();

        // Bob follows Alice, using a lookup ref in value position for the :follows ref attr
        let result = node
            .execute_tx(vec![TxOp::Add {
                entity: EntityRef::LookupRef(
                    kw!(:email),
                    DataType::String("bob@example.com".into()),
                ),
                attribute: kw!(:follows),
                value: DataType::Vector(vec![
                    DataType::Keyword(kw!(:email)),
                    DataType::String("alice@example.com".into()),
                ]),
            }])
            .await
            .unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        // Verify the follow relationship
        let db = node.db().await.unwrap();
        let result = db
            .query("[:find ?follower ?followed :where [?e1 :name ?follower] [?e1 :follows ?e2] [?e2 :name ?followed]]")
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], DataType::String("Bob".into()));
        assert_eq!(result[0][1], DataType::String("Alice".into()));
    }

    #[tokio::test]
    async fn test_lookup_ref_in_put_db_id() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        // Create an entity with a known email
        node.execute_tx(vec![TxOp::put(vec![
            (kw!(:name), "Alice".into()),
            (kw!(:email), "alice@example.com".into()),
        ])])
        .await
        .unwrap();

        // Use a lookup ref as :db/id in a Put to update the same entity
        let result = node
            .execute_tx(vec![TxOp::Put(
                vec![
                    (
                        kw!(:db/id),
                        DataType::Vector(vec![
                            DataType::Keyword(kw!(:email)),
                            DataType::String("alice@example.com".into()),
                        ]),
                    ),
                    (kw!(:age), DataType::Long(30)),
                ]
                .into_iter()
                .collect(),
            )])
            .await
            .unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        // Verify :age was added to the same entity as :name
        let db = node.db().await.unwrap();
        let result = db
            .query("[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]")
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], DataType::String("Alice".into()));
        assert_eq!(result[0][1], DataType::Long(30));
    }

    #[tokio::test]
    async fn test_lookup_ref_not_found_errors() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        // Try a lookup ref for a non-existent entity
        let result = node
            .execute_tx(vec![TxOp::Add {
                entity: EntityRef::LookupRef(
                    kw!(:email),
                    DataType::String("nobody@example.com".into()),
                ),
                attribute: kw!(:name),
                value: "Ghost".into(),
            }])
            .await
            .unwrap();
        assert!(matches!(result, TransactionResult::TxAborted(_, _)));
    }
}
