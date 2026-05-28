use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Error;
use tokio::runtime::Handle;

use crate::clock;
use crate::error::TriploxError;
use crate::file_log::FileLog;
use crate::inc_query::plan_query;
use crate::incremental::{
    IncrementalQueryHandle, IncrementalQueryService, IncrementalQuerySubscription,
};
use crate::indexer::{latest_tx_basis_from_sdb, Indexer};
use crate::log::{subscribe, TxLog, TxLogReader, TxLogWriter};
use crate::memory_log::MemoryLog;
use crate::ops::{Entid, QueryArg, TxOp};
use crate::query::{execute_query, QueryResult};
use crate::query_validation::validate_query;
use crate::schema::{IdentMap, Schema};
use crate::slate::{in_memory_slate, local_slate, remote_slate, SlateComponents};
use edn::query::ParsedQuery;
use tokio_util::sync::CancellationToken;

pub use triplox_client::node::{
    collect_tx_ops, Database, IntoQuery, IntoTxOp, QueryNode, SubmitNode,
};
pub use triplox_client::transaction::{TransactionResult, TxBasis, TxKey};

const DB_AS_OF_INDEXING_TIMEOUT: Duration = Duration::from_secs(30);

pub struct DB<D = slatedb::Db, M = slatedb::Db>
where
    D: slatedb::DbReadOps + Send + Sync + 'static,
    M: slatedb::DbMetadataOps + Send + Sync + 'static,
{
    sdb: Arc<D>,
    ident_map: IdentMap,
    handle: Handle,
    tx_basis: TxBasis,
    range_stats: Arc<slatedb_estimates::RangeStats<M>>,
}

#[allow(unused)]
impl<D, M> DB<D, M>
where
    D: slatedb::DbReadOps + Send + Sync + 'static,
    M: slatedb::DbMetadataOps + Send + Sync + 'static,
{
    pub fn new(
        sdb: Arc<D>,
        ident_map: IdentMap,
        handle: Handle,
        tx_basis: TxBasis,
        range_stats: Arc<slatedb_estimates::RangeStats<M>>,
    ) -> Self {
        Self {
            sdb,
            ident_map,
            handle,
            tx_basis,
            range_stats,
        }
    }

    /// Construct a DB from a Db by scanning EAV for TX_PARTITION entities to find the latest TxBasis.
    pub async fn from_latest_sdb(
        sdb: Arc<D>,
        ident_map: IdentMap,
        handle: Handle,
        range_stats: Arc<slatedb_estimates::RangeStats<M>>,
    ) -> Result<Self, Error> {
        let tx_basis = latest_tx_basis_from_sdb(sdb.as_ref()).await?;
        Ok(Self {
            sdb,
            ident_map,
            handle,
            tx_basis,
            range_stats,
        })
    }

    pub fn tx_key(&self) -> &TxKey {
        &self.tx_basis.tx_key
    }

    pub fn tx_basis(&self) -> &TxBasis {
        &self.tx_basis
    }

    pub fn entity(&self, _eid: Entid) {
        todo!()
    }
}

impl<D, M> Database for DB<D, M>
where
    D: slatedb::DbReadOps + Send + Sync + 'static,
    M: slatedb::DbMetadataOps + Send + Sync + 'static,
{
    async fn query(&self, query: impl IntoQuery) -> Result<QueryResult, Error> {
        let parsed = query.into_query()?;
        self.query_with_args(&parsed, &[]).await
    }

    /// Execute a query against this database basis.
    /// Runs the sync join algorithm in a blocking task to avoid blocking the async runtime.
    async fn query_with_args(
        &self,
        query: &ParsedQuery,
        args: &[QueryArg],
    ) -> Result<QueryResult, Error> {
        validate_query(query, args)?;

        let sdb = self.sdb.clone();
        let handle = self.handle.clone();
        let ident_map = self.ident_map.clone();
        let query = query.clone();
        let args = args.to_vec();
        let as_of = self.tx_basis.tx_eid;
        let range_stats = self.range_stats.clone();

        tokio::task::spawn_blocking(move || {
            execute_query(&query, &args, sdb, handle, &ident_map, as_of, range_stats)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Query task failed: {}", e))?
    }
}

pub struct Node<L: TxLog> {
    log: Arc<L>,
    indexer: Arc<tokio::sync::RwLock<Indexer>>,
    pub(crate) slate: SlateComponents,
    subscription: CancellationToken,
    incremental: IncrementalQueryService,
}

pub(crate) trait InternalNode: Send + Sync + 'static {
    fn schema(&self) -> impl Future<Output = Schema> + Send + '_;
}

impl InternalNode for tokio::sync::RwLock<Indexer> {
    async fn schema(&self) -> Schema {
        self.read().await.metadata().schema.clone()
    }
}

impl<L: TxLog> InternalNode for Node<L> {
    async fn schema(&self) -> Schema {
        self.indexer.schema().await
    }
}

impl Node<MemoryLog> {
    pub async fn memory_node() -> Self {
        let slate = in_memory_slate().await;
        let metadata = crate::bootstrap::init_db(&slate).await.unwrap();
        let indexer = Arc::new(tokio::sync::RwLock::new(Indexer::new(
            slate.db.clone(),
            metadata,
            None,
        )));
        let log = Arc::new(MemoryLog::new(Box::new(clock::SystemClock)));

        let subscription = subscribe(log.clone(), None, indexer.clone()).await;
        let incremental = IncrementalQueryService::new(
            std::env::temp_dir().join(format!(
                "triplox-dbsp-incremental-{}",
                crate::util::random_string(10)
            )),
            Handle::current(),
            subscription.clone(),
            slate.object_path.clone(),
            slate.object_store.clone(),
        );

        Node {
            log,
            indexer,
            slate,
            subscription,
            incremental,
        }
    }
}

impl Node<FileLog> {
    /// Shared setup: given a slate and a path to the log file, bootstrap the
    /// database, create the indexer & FileLog, subscribe, and catch up.
    async fn from_slate_and_log(
        slate: SlateComponents,
        log_file: &Path,
        incremental_storage_path: PathBuf,
    ) -> Result<Self, Error> {
        let metadata = crate::bootstrap::init_db(&slate).await?;

        // Determine the latest already-indexed tx_id so we skip replaying it
        // and restore the indexer's in-memory state for TxWaiter fast-path.
        let latest_indexed = latest_tx_basis_from_sdb(slate.db.as_ref()).await?;
        // Bootstrap and the first FileLog transaction both currently use tx_id=0,
        // so we can't disambiguate them by tx_id alone. Use the entity ID instead:
        // only advance the log cursor past tx_id=0 once a tx entity beyond bootstrap
        // has been indexed; otherwise replay the log from the start so the first
        // user tx isn't skipped.
        let latest_indexed_tx = if latest_indexed.tx_eid > crate::bootstrap::BOOTSTRAP_TX_EID {
            Some(latest_indexed)
        } else {
            None
        };

        let indexer = Arc::new(tokio::sync::RwLock::new(Indexer::new(
            slate.db.clone(),
            metadata,
            latest_indexed_tx,
        )));
        let log = Arc::new(FileLog::new(log_file, Box::new(clock::SystemClock))?);

        let after_tx_id = latest_indexed_tx.map(|b| b.tx_key.tx_id);

        // Read the last tx_key from the log before subscribing (for catch-up awaiting)
        let records = log.read_txs_after(after_tx_id, u16::MAX).await?;
        let last_tx_key = records.last().map(|r| r.tx_key);

        // Create a waiter for catch-up completion
        let waiter = match last_tx_key {
            Some(_) => Some(indexer.read().await.tx_waiter()),
            None => None,
        };

        let subscription = subscribe(log.clone(), after_tx_id, indexer.clone()).await;
        let incremental = IncrementalQueryService::new(
            incremental_storage_path,
            Handle::current(),
            subscription.clone(),
            slate.object_path.clone(),
            slate.object_store.clone(),
        );

        // Wait for catch-up to complete if there are un-indexed transactions
        if let Some((tx_key, waiter)) = last_tx_key.zip(waiter) {
            let completion = waiter.await_tx(tx_key).await?;
            completion.result.map_err(|e| anyhow::anyhow!("{:#}", e))?;
        }

        Ok(Node {
            log,
            indexer,
            slate,
            subscription,
            incremental,
        })
    }

    pub async fn local_node(root_path: &Path) -> Result<Self, Error> {
        std::fs::create_dir_all(root_path.join("db"))?;
        let db_path = root_path.join("db");
        let slate = local_slate(&db_path).await;
        Self::from_slate_and_log(
            slate,
            &root_path.join("log"),
            root_path.join("dbsp-incremental"),
        )
        .await
    }

    pub async fn remote_node(
        log_path: &Path,
        endpoint: &str,
        bucket: &str,
        access_key: &str,
        secret_key: &str,
        region: &str,
    ) -> Result<Self, Error> {
        std::fs::create_dir_all(log_path)?;
        let cache_path = log_path.join("cache");
        let slate = remote_slate(
            endpoint,
            bucket,
            access_key,
            secret_key,
            region,
            &cache_path,
        )
        .await?;
        Self::from_slate_and_log(
            slate,
            &log_path.join("log"),
            log_path.join("dbsp-incremental"),
        )
        .await
    }
}

impl<L: TxLog> Node<L> {
    pub async fn close(self) {
        self.incremental.shutdown().await.unwrap();
        self.subscription.cancel();
        self.slate.db.close().await.unwrap();
    }

    async fn db_as_of_with_timeout(&self, basis: TxBasis, timeout: Duration) -> Result<DB, Error> {
        let waiter = self.indexer.read().await.tx_waiter();
        tokio::time::timeout(timeout, waiter.await_indexed(basis.tx_key))
            .await
            .map_err(|_| TriploxError::TxIndexingTimeout {
                tx_id: basis.tx_key.tx_id,
                timeout,
            })??;

        let ident_map = self
            .indexer
            .read()
            .await
            .metadata()
            .schema
            .ident_map
            .clone();
        let handle = Handle::current();
        let range_stats = self.slate.range_stats.clone();
        Ok(DB::new(
            self.slate.db.clone(),
            ident_map,
            handle,
            basis,
            range_stats,
        ))
    }

    pub(crate) async fn register_incremental_query(
        &self,
        query: ParsedQuery,
    ) -> Result<IncrementalQuerySubscription, Error> {
        let registration_gate = self.incremental.registration_gate();
        let _registration_guard = registration_gate.lock().await;
        let (basis, plan) = {
            let indexer = self.indexer.read().await;
            let basis = indexer.latest_tx_basis().ok_or_else(|| {
                // TODO(#278, #134): make initialized nodes always expose a latest indexed basis.
                anyhow::anyhow!("Indexer has no latest indexed transaction basis")
            })?;
            let plan = plan_query(&query, &indexer.metadata().schema)?;
            (basis, plan)
        };
        let subscription = self
            .incremental
            .register_query_snapshot(self.slate.db.as_ref(), plan, basis)
            .await?;
        self.incremental.start_cdc_once(self.indexer.clone());
        Ok(subscription)
    }

    pub(crate) async fn unregister_incremental_query(
        &self,
        handle: IncrementalQueryHandle,
    ) -> Result<(), Error> {
        self.incremental.unregister(handle).await
    }

    #[cfg(test)]
    async fn pause_next_incremental_registration_after_snapshot_for_test(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        self.incremental
            .pause_next_registration_after_snapshot_for_test()
            .await
    }
}

impl<L: TxLog> SubmitNode for Node<L> {
    async fn submit_tx<O: IntoTxOp>(&self, ops: Vec<O>) -> Result<TxKey, Error> {
        let ops = collect_tx_ops(ops)?;
        let serialized = bincode::serialize(&ops)?;
        Ok(self.log.append_tx(serialized).await)
    }

    async fn execute_tx<O: IntoTxOp>(&self, ops: Vec<O>) -> Result<TransactionResult, Error> {
        let ops = collect_tx_ops(ops)?;
        let serialized = bincode::serialize(&ops)?;

        let waiter = self.indexer.read().await.tx_waiter();

        let tx_key = self.log.append_tx(serialized).await;

        let completion = waiter.await_tx(tx_key).await?;
        let basis = completion.basis.ok_or_else(|| {
            anyhow::anyhow!("Indexer did not return TxBasis for tx {}", tx_key.tx_id)
        })?;
        match completion.result {
            Ok(()) => Ok(TransactionResult::TxCommited(basis)),
            Err(e) => Ok(TransactionResult::TxAborted(
                basis,
                anyhow::anyhow!("{:#}", e).into(),
            )),
        }
    }
}

impl<L: TxLog> QueryNode for Node<L> {
    type DB = DB;

    async fn db(&self) -> Result<DB, Error> {
        let ident_map = self
            .indexer
            .read()
            .await
            .metadata()
            .schema
            .ident_map
            .clone();
        let handle = Handle::current();
        let range_stats = self.slate.range_stats.clone();
        DB::from_latest_sdb(self.slate.db.clone(), ident_map, handle, range_stats).await
    }

    async fn db_as_of(&self, basis: TxBasis) -> Result<DB, Error> {
        self.db_as_of_with_timeout(basis, DB_AS_OF_INDEXING_TIMEOUT)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::clock::st_from_unix_epoch;
    use crate::error::TriploxError;
    use crate::ops::{DataType, EntityRef, TxOp};
    use crate::partition::{extract_partition, TX_PARTITION};
    use crate::schema::{
        test_schema_tx, unique_identity_schema_attribute, unique_value_schema_attribute,
    };
    use edn::kw;
    use edn::Keyword;
    use slatedb::config::{FlushOptions, FlushType};
    use triplox_client::transaction::TransactionResult;

    /// Define common test attributes (name, age, email, follows) through the standard tx path.
    async fn define_test_schema(node: &impl SubmitNode) {
        let result = node.execute_tx(test_schema_tx()).await.unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));
    }

    fn parse_query(input: &str) -> ParsedQuery {
        edn::parse::parse_query(input).expect("query should parse")
    }

    async fn flush_wal(node: &Node<MemoryLog>) {
        node.slate
            .db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::Wal,
            })
            .await
            .unwrap();
    }

    async fn recv_incremental_delta(
        subscription: &mut IncrementalQuerySubscription,
    ) -> crate::incremental::IncrementalQueryDelta {
        tokio::time::timeout(Duration::from_secs(5), subscription.deltas.recv())
            .await
            .expect("timed out waiting for incremental delta")
            .expect("subscription should be open")
    }

    async fn try_recv_incremental_delta(
        subscription: &mut IncrementalQuerySubscription,
    ) -> Option<crate::incremental::IncrementalQueryDelta> {
        tokio::time::timeout(Duration::from_millis(500), subscription.deltas.recv())
            .await
            .ok()
            .flatten()
    }

    fn sort_query_rows(rows: &mut [Vec<DataType>]) {
        rows.sort_by_key(|row| format!("{:?}", row));
    }

    fn integrate_delta(
        rows: &mut Vec<Vec<DataType>>,
        delta: crate::incremental::IncrementalQueryDelta,
    ) {
        for (row, weight) in delta.rows {
            match weight.cmp(&0) {
                std::cmp::Ordering::Greater => {
                    for _ in 0..weight {
                        rows.push(row.clone());
                    }
                }
                std::cmp::Ordering::Less => {
                    for _ in 0..(-weight) {
                        let index = rows
                            .iter()
                            .position(|existing| existing == &row)
                            .expect("negative delta should remove an existing row");
                        rows.remove(index);
                    }
                }
                std::cmp::Ordering::Equal => {}
            }
        }
        sort_query_rows(rows);
    }

    async fn execute_and_flush(node: &Node<MemoryLog>, tx_ops: Vec<TxOp>) -> TxBasis {
        let basis = match node.execute_tx(tx_ops).await.unwrap() {
            TransactionResult::TxCommited(basis) => basis,
            TransactionResult::TxAborted(_, err) => panic!("transaction aborted: {err}"),
        };
        flush_wal(node).await;
        basis
    }

    async fn assert_incremental_matches_db(
        node: &Node<MemoryLog>,
        subscription: &mut IncrementalQuerySubscription,
        rows: &mut Vec<Vec<DataType>>,
        basis: TxBasis,
        query: &str,
    ) {
        let db = node.db_as_of(basis).await.unwrap();
        let mut expected = db.query(query).await.unwrap();
        sort_query_rows(&mut expected);

        if rows != &expected {
            tokio::time::timeout(Duration::from_secs(5), async {
                while rows != &expected {
                    let delta = subscription
                        .deltas
                        .recv()
                        .await
                        .expect("subscription should be open");
                    integrate_delta(rows, delta);
                }
            })
            .await
            .expect("timed out waiting for incremental rows to match one-shot query");
        }

        assert_eq!(&expected, rows);
    }

    #[tokio::test]
    async fn test_register_incremental_query_installs_subscription() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        let expected_basis = *node.db().await.unwrap().tx_basis();

        let mut subscription = node
            .register_incremental_query(parse_query("[:find ?name :where [?e :name ?name]]"))
            .await
            .unwrap();

        assert_eq!(subscription.basis, expected_basis);
        assert!(matches!(
            subscription.deltas.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    #[ignore = "requires bootstrap latest_tx_basis to be visible through the indexer"]
    async fn test_incremental_schema_query_before_user_tx_observes_schema_changes() {
        let node = Node::memory_node().await;

        let mut subscription = node
            .register_incremental_query(parse_query("[:find ?ident :where [?e :db/ident ?ident]]"))
            .await
            .unwrap();
        let basis = match node.execute_tx(test_schema_tx()).await.unwrap() {
            TransactionResult::TxCommited(basis) => basis,
            TransactionResult::TxAborted(_, err) => panic!("transaction aborted: {err}"),
        };
        flush_wal(&node).await;

        let delta = recv_incremental_delta(&mut subscription).await;
        assert_eq!(delta.basis, Some(basis));
        let mut rows = delta.rows;
        rows.sort_by_key(|row| format!("{:?}", row));
        let mut expected = vec![
            (vec![DataType::Keyword(kw!(:age))], 1),
            (vec![DataType::Keyword(kw!(:email))], 1),
            (vec![DataType::Keyword(kw!(:follows))], 1),
            (vec![DataType::Keyword(kw!(:name))], 1),
            (vec![DataType::Keyword(kw!(:tags))], 1),
        ];
        expected.sort_by_key(|row| format!("{:?}", row));
        assert_eq!(rows, expected);
    }

    #[tokio::test]
    async fn test_register_incremental_query_after_existing_data_emits_no_initial_delta() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        let result = node
            .execute_tx(vec![TxOp::Add {
                entity: EntityRef::Id(100),
                attribute: kw!(:name),
                value: DataType::String("Alice".to_string()),
            }])
            .await
            .unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        let mut subscription = node
            .register_incremental_query(parse_query("[:find ?name :where [?e :name ?name]]"))
            .await
            .unwrap();

        assert!(matches!(
            subscription.deltas.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn test_primed_incremental_query_uses_existing_rows_for_future_delta() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        let result = node
            .execute_tx(vec![TxOp::Add {
                entity: EntityRef::Id(100),
                attribute: kw!(:name),
                value: DataType::String("Alice".to_string()),
            }])
            .await
            .unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        let mut subscription = node
            .register_incremental_query(parse_query(
                "[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]",
            ))
            .await
            .unwrap();
        let future_basis = match node
            .execute_tx(vec![TxOp::Add {
                entity: EntityRef::Id(100),
                attribute: kw!(:age),
                value: DataType::Long(30),
            }])
            .await
            .unwrap()
        {
            TransactionResult::TxCommited(basis) => basis,
            TransactionResult::TxAborted(_, err) => panic!("transaction aborted: {err}"),
        };
        flush_wal(&node).await;

        let delta = recv_incremental_delta(&mut subscription).await;
        assert_eq!(delta.basis, Some(future_basis));
        assert_eq!(
            delta.rows,
            vec![(
                vec![DataType::String("Alice".to_string()), DataType::Long(30)],
                1
            )]
        );
    }

    #[tokio::test]
    async fn test_incremental_cdc_emits_single_transaction_delta() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        flush_wal(&node).await;
        let mut subscription = node
            .register_incremental_query(parse_query("[:find ?name :where [?e :name ?name]]"))
            .await
            .unwrap();
        let basis = match node
            .execute_tx(vec![TxOp::Add {
                entity: EntityRef::Id(100),
                attribute: kw!(:name),
                value: DataType::String("Alice".to_string()),
            }])
            .await
            .unwrap()
        {
            TransactionResult::TxCommited(basis) => basis,
            TransactionResult::TxAborted(_, err) => panic!("transaction aborted: {err}"),
        };

        flush_wal(&node).await;

        let delta = recv_incremental_delta(&mut subscription).await;
        assert_eq!(delta.basis, Some(basis));
        assert_eq!(
            delta.rows,
            vec![(vec![DataType::String("Alice".to_string())], 1)]
        );
    }

    #[tokio::test]
    async fn test_incremental_registration_does_not_miss_tx_between_basis_and_insert() {
        let node = Arc::new(Node::memory_node().await);
        define_test_schema(node.as_ref()).await;
        flush_wal(node.as_ref()).await;
        let mut first_subscription = node
            .register_incremental_query(parse_query("[:find ?name :where [?e :name ?name]]"))
            .await
            .unwrap();

        let (registration_paused, release_registration) = node
            .pause_next_incremental_registration_after_snapshot_for_test()
            .await;
        let registering_node = node.clone();
        let registration = tokio::spawn(async move {
            registering_node
                .register_incremental_query(parse_query("[:find ?name :where [?e :name ?name]]"))
                .await
                .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(5), registration_paused)
            .await
            .expect("timed out waiting for registration pause")
            .expect("registration pause sender dropped");

        let basis = match node
            .execute_tx(vec![TxOp::Add {
                entity: EntityRef::Id(100),
                attribute: kw!(:name),
                value: DataType::String("Alice".to_string()),
            }])
            .await
            .unwrap()
        {
            TransactionResult::TxCommited(basis) => basis,
            TransactionResult::TxAborted(_, err) => panic!("transaction aborted: {err}"),
        };
        flush_wal(node.as_ref()).await;
        assert!(try_recv_incremental_delta(&mut first_subscription)
            .await
            .is_none());

        release_registration
            .send(())
            .expect("registration release receiver should be waiting");
        let mut second_subscription = registration.await.unwrap();

        let first_delta = recv_incremental_delta(&mut first_subscription).await;
        let second_delta = recv_incremental_delta(&mut second_subscription).await;
        assert_eq!(first_delta.basis, Some(basis));
        assert_eq!(second_delta.basis, Some(basis));
        assert_eq!(
            first_delta.rows,
            vec![(vec![DataType::String("Alice".to_string())], 1)]
        );
        assert_eq!(second_delta.rows, first_delta.rows);
    }

    #[tokio::test]
    async fn test_incremental_registration_basis_inside_wal_replays_after_basis_only() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        flush_wal(&node).await;
        let first_basis = match node
            .execute_tx(vec![TxOp::Add {
                entity: EntityRef::Id(100),
                attribute: kw!(:name),
                value: DataType::String("Alice".to_string()),
            }])
            .await
            .unwrap()
        {
            TransactionResult::TxCommited(basis) => basis,
            TransactionResult::TxAborted(_, err) => panic!("transaction aborted: {err}"),
        };
        let mut subscription = node
            .register_incremental_query(parse_query("[:find ?name :where [?e :name ?name]]"))
            .await
            .unwrap();
        assert_eq!(subscription.basis, first_basis);
        assert!(try_recv_incremental_delta(&mut subscription)
            .await
            .is_none());

        let second_basis = match node
            .execute_tx(vec![TxOp::Add {
                entity: EntityRef::Id(101),
                attribute: kw!(:name),
                value: DataType::String("Bob".to_string()),
            }])
            .await
            .unwrap()
        {
            TransactionResult::TxCommited(basis) => basis,
            TransactionResult::TxAborted(_, err) => panic!("transaction aborted: {err}"),
        };
        flush_wal(&node).await;

        let delta = recv_incremental_delta(&mut subscription).await;
        assert_eq!(delta.basis, Some(second_basis));
        assert_eq!(
            delta.rows,
            vec![(vec![DataType::String("Bob".to_string())], 1)]
        );
        assert!(try_recv_incremental_delta(&mut subscription)
            .await
            .is_none());
    }

    #[tokio::test]
    async fn test_incremental_cdc_groups_multi_entity_transaction() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        flush_wal(&node).await;
        let mut subscription = node
            .register_incremental_query(parse_query("[:find ?name :where [?e :name ?name]]"))
            .await
            .unwrap();
        let basis = match node
            .execute_tx(vec![
                TxOp::Add {
                    entity: EntityRef::Id(100),
                    attribute: kw!(:name),
                    value: DataType::String("Alice".to_string()),
                },
                TxOp::Add {
                    entity: EntityRef::Id(101),
                    attribute: kw!(:name),
                    value: DataType::String("Bob".to_string()),
                },
            ])
            .await
            .unwrap()
        {
            TransactionResult::TxCommited(basis) => basis,
            TransactionResult::TxAborted(_, err) => panic!("transaction aborted: {err}"),
        };

        flush_wal(&node).await;

        let mut delta = recv_incremental_delta(&mut subscription).await;
        delta.rows.sort_by_key(|row| format!("{:?}", row));
        assert_eq!(delta.basis, Some(basis));
        assert_eq!(
            delta.rows,
            vec![
                (vec![DataType::String("Alice".to_string())], 1),
                (vec![DataType::String("Bob".to_string())], 1),
            ]
        );
    }

    #[tokio::test]
    async fn test_incremental_cdc_cardinality_one_overwrite_emits_retract_and_assert() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        let result = node
            .execute_tx(vec![TxOp::Add {
                entity: EntityRef::Id(100),
                attribute: kw!(:name),
                value: DataType::String("Alice".to_string()),
            }])
            .await
            .unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));
        flush_wal(&node).await;
        let mut subscription = node
            .register_incremental_query(parse_query("[:find ?name :where [?e :name ?name]]"))
            .await
            .unwrap();
        let basis = match node
            .execute_tx(vec![TxOp::Add {
                entity: EntityRef::Id(100),
                attribute: kw!(:name),
                value: DataType::String("Bob".to_string()),
            }])
            .await
            .unwrap()
        {
            TransactionResult::TxCommited(basis) => basis,
            TransactionResult::TxAborted(_, err) => panic!("transaction aborted: {err}"),
        };

        flush_wal(&node).await;

        let mut delta = recv_incremental_delta(&mut subscription).await;
        delta.rows.sort_by_key(|row| format!("{:?}", row));
        assert_eq!(delta.basis, Some(basis));
        assert_eq!(
            delta.rows,
            vec![
                (vec![DataType::String("Alice".to_string())], -1),
                (vec![DataType::String("Bob".to_string())], 1),
            ]
        );
    }

    #[tokio::test]
    async fn test_incremental_entity_join_integrates_live_result() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        flush_wal(&node).await;
        let mut subscription = node
            .register_incremental_query(parse_query(
                "[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]",
            ))
            .await
            .unwrap();
        let mut rows = Vec::new();

        execute_and_flush(
            &node,
            vec![
                TxOp::Add {
                    entity: EntityRef::Id(100),
                    attribute: kw!(:name),
                    value: DataType::String("Alice".to_string()),
                },
                TxOp::Add {
                    entity: EntityRef::Id(100),
                    attribute: kw!(:age),
                    value: DataType::Long(30),
                },
            ],
        )
        .await;
        integrate_delta(&mut rows, recv_incremental_delta(&mut subscription).await);
        assert_eq!(
            rows,
            vec![vec![
                DataType::String("Alice".to_string()),
                DataType::Long(30)
            ]]
        );

        execute_and_flush(
            &node,
            vec![TxOp::Add {
                entity: EntityRef::Id(101),
                attribute: kw!(:age),
                value: DataType::Long(40),
            }],
        )
        .await;
        execute_and_flush(
            &node,
            vec![TxOp::Add {
                entity: EntityRef::Id(101),
                attribute: kw!(:name),
                value: DataType::String("Bob".to_string()),
            }],
        )
        .await;
        integrate_delta(&mut rows, recv_incremental_delta(&mut subscription).await);
        assert_eq!(
            rows,
            vec![
                vec![DataType::String("Alice".to_string()), DataType::Long(30)],
                vec![DataType::String("Bob".to_string()), DataType::Long(40)],
            ]
        );
    }

    #[tokio::test]
    async fn test_incremental_ref_value_and_three_pattern_chain() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        flush_wal(&node).await;
        let mut subscription = node
            .register_incremental_query(parse_query(
                "[:find ?name ?friend-name ?age :where [?e :name ?name] [?e :follows ?friend] [?friend :name ?friend-name] [?friend :age ?age]]",
            ))
            .await
            .unwrap();
        let mut rows = Vec::new();

        execute_and_flush(
            &node,
            vec![TxOp::Add {
                entity: EntityRef::Id(100),
                attribute: kw!(:name),
                value: DataType::String("Alice".to_string()),
            }],
        )
        .await;
        assert!(try_recv_incremental_delta(&mut subscription)
            .await
            .is_none());
        assert!(rows.is_empty());

        execute_and_flush(
            &node,
            vec![TxOp::Add {
                entity: EntityRef::Id(101),
                attribute: kw!(:name),
                value: DataType::String("Bob".to_string()),
            }],
        )
        .await;
        assert!(try_recv_incremental_delta(&mut subscription)
            .await
            .is_none());
        assert!(rows.is_empty());

        execute_and_flush(
            &node,
            vec![TxOp::Add {
                entity: EntityRef::Id(101),
                attribute: kw!(:age),
                value: DataType::Long(40),
            }],
        )
        .await;
        assert!(try_recv_incremental_delta(&mut subscription)
            .await
            .is_none());
        assert!(rows.is_empty());

        execute_and_flush(
            &node,
            vec![TxOp::Add {
                entity: EntityRef::Id(100),
                attribute: kw!(:follows),
                value: DataType::Long(101),
            }],
        )
        .await;
        integrate_delta(&mut rows, recv_incremental_delta(&mut subscription).await);

        assert_eq!(
            rows,
            vec![vec![
                DataType::String("Alice".to_string()),
                DataType::String("Bob".to_string()),
                DataType::Long(40),
            ]]
        );
    }

    #[tokio::test]
    async fn test_incremental_constants_and_cartesian_product() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        flush_wal(&node).await;
        let mut subscription = node
            .register_incremental_query(parse_query(
                r#"[:find ?name ?age :where [?e :name ?name] [?other :age ?age] [?e :name "Alice"]]"#,
            ))
            .await
            .unwrap();
        let mut rows = Vec::new();

        execute_and_flush(
            &node,
            vec![TxOp::Add {
                entity: EntityRef::Id(100),
                attribute: kw!(:name),
                value: DataType::String("Alice".to_string()),
            }],
        )
        .await;
        assert!(try_recv_incremental_delta(&mut subscription)
            .await
            .is_none());
        assert!(rows.is_empty());

        execute_and_flush(
            &node,
            vec![TxOp::Add {
                entity: EntityRef::Id(101),
                attribute: kw!(:age),
                value: DataType::Long(30),
            }],
        )
        .await;
        integrate_delta(&mut rows, recv_incremental_delta(&mut subscription).await);
        assert_eq!(
            rows,
            vec![vec![
                DataType::String("Alice".to_string()),
                DataType::Long(30)
            ]]
        );

        execute_and_flush(
            &node,
            vec![TxOp::Add {
                entity: EntityRef::Id(102),
                attribute: kw!(:age),
                value: DataType::Long(40),
            }],
        )
        .await;
        integrate_delta(&mut rows, recv_incremental_delta(&mut subscription).await);

        assert_eq!(
            rows,
            vec![
                vec![DataType::String("Alice".to_string()), DataType::Long(30)],
                vec![DataType::String("Alice".to_string()), DataType::Long(40)],
            ]
        );
    }

    #[tokio::test]
    async fn test_register_incremental_query_rejects_entity_placeholder() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let err = node
            .register_incremental_query(parse_query("[:find ?name :where [_ :name ?name]]"))
            .await
            .unwrap_err();

        assert!(
            err.to_string().contains("placeholders in entity position"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_register_incremental_query_rejects_value_placeholder() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let err = node
            .register_incremental_query(parse_query("[:find ?e :where [?e :name _]]"))
            .await
            .unwrap_err();

        assert!(
            err.to_string().contains("placeholders in value position"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_incremental_equivalence_entity_join() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        flush_wal(&node).await;
        let query = "[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]";
        let mut subscription = node
            .register_incremental_query(parse_query(query))
            .await
            .unwrap();
        let mut rows = Vec::new();

        let basis = execute_and_flush(
            &node,
            vec![TxOp::Add {
                entity: EntityRef::Id(100),
                attribute: kw!(:name),
                value: DataType::String("Alice".to_string()),
            }],
        )
        .await;
        assert_incremental_matches_db(&node, &mut subscription, &mut rows, basis, query).await;

        let basis = execute_and_flush(
            &node,
            vec![TxOp::Add {
                entity: EntityRef::Id(100),
                attribute: kw!(:age),
                value: DataType::Long(30),
            }],
        )
        .await;
        assert_incremental_matches_db(&node, &mut subscription, &mut rows, basis, query).await;

        let basis = execute_and_flush(
            &node,
            vec![
                TxOp::Add {
                    entity: EntityRef::Id(101),
                    attribute: kw!(:name),
                    value: DataType::String("Bob".to_string()),
                },
                TxOp::Add {
                    entity: EntityRef::Id(101),
                    attribute: kw!(:age),
                    value: DataType::Long(40),
                },
            ],
        )
        .await;
        assert_incremental_matches_db(&node, &mut subscription, &mut rows, basis, query).await;
    }

    #[tokio::test]
    async fn test_incremental_equivalence_cartesian_product() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        flush_wal(&node).await;
        let query = "[:find ?name ?age :where [?e :name ?name] [?other :age ?age]]";
        let mut subscription = node
            .register_incremental_query(parse_query(query))
            .await
            .unwrap();
        let mut rows = Vec::new();

        let basis = execute_and_flush(
            &node,
            vec![TxOp::Add {
                entity: EntityRef::Id(100),
                attribute: kw!(:name),
                value: DataType::String("Alice".to_string()),
            }],
        )
        .await;
        assert_incremental_matches_db(&node, &mut subscription, &mut rows, basis, query).await;

        let basis = execute_and_flush(
            &node,
            vec![TxOp::Add {
                entity: EntityRef::Id(101),
                attribute: kw!(:age),
                value: DataType::Long(30),
            }],
        )
        .await;
        assert_incremental_matches_db(&node, &mut subscription, &mut rows, basis, query).await;

        let basis = execute_and_flush(
            &node,
            vec![TxOp::Add {
                entity: EntityRef::Id(102),
                attribute: kw!(:name),
                value: DataType::String("Bob".to_string()),
            }],
        )
        .await;
        assert_incremental_matches_db(&node, &mut subscription, &mut rows, basis, query).await;
    }

    #[tokio::test]
    async fn test_register_incremental_query_rejects_unsupported_query() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let err = node
            .register_incremental_query(parse_query("[:find ?e :where [?e ?a ?v]]"))
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("Incremental query pattern attributes must be constant"));
    }

    #[tokio::test]
    async fn test_unregister_incremental_query_removes_subscription() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let mut subscription = node
            .register_incremental_query(parse_query("[:find ?name :where [?e :name ?name]]"))
            .await
            .unwrap();
        let handle = subscription.handle;

        node.unregister_incremental_query(handle).await.unwrap();

        assert!(subscription.deltas.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_unregister_incremental_query_rejects_duplicate_unregister() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let subscription = node
            .register_incremental_query(parse_query("[:find ?name :where [?e :name ?name]]"))
            .await
            .unwrap();
        let handle = subscription.handle;

        node.unregister_incremental_query(handle).await.unwrap();
        let err = node.unregister_incremental_query(handle).await.unwrap_err();

        assert!(err.to_string().contains("Unknown incremental query handle"));
    }

    #[tokio::test]
    async fn test_dropped_incremental_query_receiver_is_cleaned_up() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let subscription = node
            .register_incremental_query(parse_query("[:find ?name :where [?e :name ?name]]"))
            .await
            .unwrap();
        let handle = subscription.handle;
        drop(subscription);

        let err = node.unregister_incremental_query(handle).await.unwrap_err();
        assert!(err.to_string().contains("Unknown incremental query handle"));
    }

    #[tokio::test]
    async fn test_close_removes_incremental_query_storage() {
        let dir = tempfile::tempdir().unwrap();
        let node = Node::local_node(dir.path()).await.unwrap();
        define_test_schema(&node).await;
        let storage_path = dir.path().join("dbsp-incremental").join("query-1");

        let _subscription = node
            .register_incremental_query(parse_query("[:find ?name :where [?e :name ?name]]"))
            .await
            .unwrap();

        assert!(storage_path.exists());

        node.close().await;

        assert!(!storage_path.exists());
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
    async fn test_query_rejects_entity_placeholder() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let db = node.db().await.unwrap();
        let err = db
            .query("[:find ?name :where [_ :name ?name]]")
            .await
            .unwrap_err();

        assert!(
            err.to_string().contains("entity position"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_rejects_value_placeholder() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let db = node.db().await.unwrap();
        let err = db
            .query("[:find ?e :where [?e :name _]]")
            .await
            .unwrap_err();

        assert!(
            err.to_string().contains("value position"),
            "unexpected error: {}",
            err
        );
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
    async fn test_query_with_collection_in_bindings() {
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
            TxOp::put(vec![
                (kw!(:db/id), DataType::String("bob".to_string())),
                (kw!(:name), "Bob".into()),
                (kw!(:email), "bob@example.com".into()),
            ]),
        ])
        .await
        .unwrap();

        let db = node.db().await.unwrap();

        // Collection binding: match names in a set
        let parsed =
            edn::parse::parse_query("[:find ?name :in [?name ...] :where [?e :name ?name]]")
                .unwrap();

        let result = db
            .query_with_args(
                &parsed,
                &[QueryArg::Collection(vec!["Ivan".into(), "Petr".into()])],
            )
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        let names: HashSet<_> = result.iter().map(|row| row[0].clone()).collect();
        assert!(names.contains(&DataType::String("Ivan".to_string())));
        assert!(names.contains(&DataType::String("Petr".to_string())));

        // Collection with a value that doesn't match: only matching rows returned
        let result = db
            .query_with_args(
                &parsed,
                &[QueryArg::Collection(vec!["Ivan".into(), "Nobody".into()])],
            )
            .await
            .unwrap();
        assert_eq!(result, vec![vec![DataType::String("Ivan".to_string())]]);

        // Empty collection: no rows
        let result = db
            .query_with_args(&parsed, &[QueryArg::Collection(vec![])])
            .await
            .unwrap();
        assert!(result.is_empty());
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

        let basis1 = match node
            .execute_tx(vec![TxOp::Add {
                entity: "alice".into(),
                attribute: kw!(:name),
                value: "alice".into(),
            }])
            .await
            .unwrap()
        {
            TransactionResult::TxCommited(basis) => basis,
            _ => panic!("Tx1 should commit"),
        };

        let basis2 = match node
            .execute_tx(vec![TxOp::Add {
                entity: "bob".into(),
                attribute: kw!(:name),
                value: "bob".into(),
            }])
            .await
            .unwrap()
        {
            TransactionResult::TxCommited(basis) => basis,
            _ => panic!("Tx2 should commit"),
        };

        // db_as_of(basis1): should only see alice
        let db1 = node.db_as_of(basis1).await.unwrap();
        let result1 = db1
            .query("[:find ?name :where [?e :name ?name]]")
            .await
            .unwrap();
        assert_eq!(result1.len(), 1);
        assert_eq!(result1[0], vec![DataType::String("alice".to_string())]);

        // db_as_of(basis2): should see both
        let db2 = node.db_as_of(basis2).await.unwrap();
        let result2 = db2
            .query("[:find ?name :where [?e :name ?name]]")
            .await
            .unwrap();
        assert_eq!(result2.len(), 2);
        assert!(result2.contains(&vec![DataType::String("alice".to_string())]));
        assert!(result2.contains(&vec![DataType::String("bob".to_string())]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_db_as_of_aborted_tx_opens_basis() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let basis = match node
            .execute_tx(vec![TxOp::Add {
                entity: "e".into(),
                attribute: kw!(:nonexistent),
                value: "x".into(),
            }])
            .await
            .unwrap()
        {
            TransactionResult::TxAborted(basis, _) => basis,
            result => panic!("expected aborted tx, got {result:?}"),
        };

        let db = node.db_as_of(basis).await.unwrap();
        assert_eq!(*db.tx_basis(), basis);

        let query_str = format!(
            "[:find ?ident ?error \
             :where [?tx :db/txId {}] [?tx :db/txResult ?r] [?r :db/ident ?ident] [?tx :db/txError ?error]]",
            basis.tx_key.tx_id
        );
        let result = db.query(query_str).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], DataType::Keyword(kw!(:db.tx/aborted)));
        assert!(
            matches!(&result[0][1], DataType::String(s) if s.contains("nonexistent")),
            "expected abort error for unknown attribute, got {:?}",
            result[0][1]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_db_as_of_times_out_for_unindexed_tx() {
        let node = Node::memory_node().await;
        let basis = TxBasis {
            tx_key: TxKey {
                tx_id: 999,
                system_time: st_from_unix_epoch(999),
            },
            tx_eid: 999,
        };

        let err = match node
            .db_as_of_with_timeout(basis, Duration::from_millis(10))
            .await
        {
            Ok(_) => panic!("expected db_as_of timeout"),
            Err(err) => err,
        };
        assert!(
            matches!(
                err.downcast_ref::<TriploxError>(),
                Some(TriploxError::TxIndexingTimeout { tx_id: 999, .. })
            ),
            "expected TxIndexingTimeout, got {err:?}"
        );
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_local_node_bootstrap_only_restart_does_not_skip_first_log_tx() {
        let dir = tempfile::tempdir().unwrap();
        let root_path = dir.path().to_path_buf();

        let node = Node::local_node(&root_path).await.unwrap();
        node.close().await;

        let node = Node::local_node(&root_path).await.unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            node.execute_tx(test_schema_tx()),
        )
        .await
        .expect("first log transaction after bootstrap-only restart should not be skipped")
        .unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        let result = node
            .execute_tx(vec![TxOp::Add {
                entity: "alice".into(),
                attribute: kw!(:name),
                value: "alice".into(),
            }])
            .await
            .unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        node.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_local_node_restart_skips_already_indexed_first_log_tx() {
        let dir = tempfile::tempdir().unwrap();
        let root_path = dir.path().to_path_buf();

        let node = Node::local_node(&root_path).await.unwrap();
        define_test_schema(&node).await;
        node.close().await;

        let node = Node::local_node(&root_path).await.unwrap();
        let db = node.db().await.unwrap();
        let txs = db
            .query("[:find ?tx :where [?tx :db/txId 0]]")
            .await
            .unwrap();
        assert_eq!(txs.len(), 2);

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
        let basis = match result {
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
            waiter.await_tx(basis.tx_key),
        )
        .await;

        assert!(
            result.is_ok(),
            "tx_waiter should not timeout for an already-indexed tx"
        );
        result.unwrap().expect("await_tx should succeed");

        node.close().await;
    }

    #[tokio::test]
    async fn test_db_as_of_filters_by_basis() {
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
        let basis1 = match result1 {
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

        // db_as_of at the first tx basis should only see alice
        let db = node.db_as_of(basis1).await.unwrap();
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
    async fn test_db_on_fresh_node_returns_bootstrap_tx_key() {
        let node = Node::memory_node().await;

        let db = node.db().await.unwrap();
        let tx_key = db.tx_key();
        assert_eq!(tx_key.tx_id, 0);

        node.close().await;
    }

    #[tokio::test]
    async fn test_fresh_db_has_queryable_bootstrap_transaction_entity() {
        let node = Node::memory_node().await;

        let db = node.db().await.unwrap();
        let result = db
            .query(
                "[:find ?tx ?tx_id ?instant ?ident \
                 :where [?tx :db/txId ?tx_id] \
                        [?tx :db/txInstant ?instant] \
                        [?tx :db/txResult ?result] \
                        [?result :db/ident ?ident]]",
            )
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let tx_eid = match &result[0][0] {
            DataType::Long(id) => *id,
            other => panic!("Expected Long tx entity ID, got {:?}", other),
        };
        assert_eq!(extract_partition(tx_eid), TX_PARTITION);
        assert_eq!(result[0][1], DataType::Long(0));
        assert_eq!(result[0][2], DataType::Instant(st_from_unix_epoch(0)));
        assert_eq!(result[0][3], DataType::Keyword(kw!(:db.tx/committed)));

        node.close().await;
    }

    #[tokio::test]
    async fn test_first_submitted_tx_temporarily_shares_bootstrap_tx_id() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let db = node.db().await.unwrap();
        let result = db
            .query("[:find ?tx :where [?tx :db/txId 0]]")
            .await
            .unwrap();

        assert_eq!(result.len(), 2);

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
            (kw!(:db/ident), DataType::Keyword(kw!(:sex))),
            (kw!(:db/valueType), DataType::Keyword(kw!(:db.type/keyword))),
            (
                kw!(:db/cardinality),
                DataType::Keyword(kw!(:db.cardinality/one)),
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
                (kw!(:sex), DataType::Keyword(Keyword::plain(sex))),
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
            (kw!(:db/ident), DataType::Keyword(kw!(:sex))),
            (kw!(:db/valueType), DataType::Keyword(kw!(:db.type/keyword))),
            (
                kw!(:db/cardinality),
                DataType::Keyword(kw!(:db.cardinality/one)),
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
                (kw!(:sex), DataType::Keyword(Keyword::plain(sex))),
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
            (kw!(:db/ident), DataType::Keyword(kw!(:last-name))),
            (kw!(:db/valueType), DataType::Keyword(kw!(:db.type/string))),
            (
                kw!(:db/cardinality),
                DataType::Keyword(kw!(:db.cardinality/one)),
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
        let basis = match result {
            TransactionResult::TxCommited(k) => k,
            _ => panic!("Expected committed"),
        };

        // Query for the tx entity matching this tx_id, resolving the ref to its ident
        let db = node.db().await.unwrap();
        let query_str = format!(
            "[:find ?ident \
             :where [?tx :db/txId {}] [?tx :db/txResult ?r] [?r :db/ident ?ident]]",
            basis.tx_key.tx_id
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
        let basis = match &result {
            TransactionResult::TxAborted(k, _) => *k,
            _ => panic!("Expected aborted, got {:?}", result),
        };

        // Query for the aborted tx entity, resolving the ref to its ident
        let db = node.db().await.unwrap();
        let query_str = format!(
            "[:find ?ident ?error \
             :where [?tx :db/txId {}] [?tx :db/txResult ?r] [?r :db/ident ?ident] [?tx :db/txError ?error]]",
            basis.tx_key.tx_id
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
    async fn test_lookup_ref_batch_resolves_multiple_refs() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        node.execute_tx(vec![
            TxOp::put(vec![
                (kw!(:name), "Alice".into()),
                (kw!(:email), "alice@example.com".into()),
            ]),
            TxOp::put(vec![
                (kw!(:name), "Bob".into()),
                (kw!(:email), "bob@example.com".into()),
            ]),
        ])
        .await
        .unwrap();

        let result = node
            .execute_tx(vec![
                TxOp::Add {
                    entity: EntityRef::LookupRef(
                        kw!(:email),
                        DataType::String("bob@example.com".into()),
                    ),
                    attribute: kw!(:follows),
                    value: DataType::Vector(vec![
                        DataType::Keyword(kw!(:email)),
                        DataType::String("alice@example.com".into()),
                    ]),
                },
                TxOp::Add {
                    entity: EntityRef::LookupRef(
                        kw!(:email),
                        DataType::String("alice@example.com".into()),
                    ),
                    attribute: kw!(:age),
                    value: DataType::Long(30),
                },
            ])
            .await
            .unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        let db = node.db().await.unwrap();
        let result = db
            .query(
                r#"[:find ?follower ?followed ?age
                   :where [?e1 :name ?follower] [?e1 :follows ?e2] [?e2 :name ?followed] [?e2 :age ?age]]"#,
            )
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], DataType::String("Bob".into()));
        assert_eq!(result[0][1], DataType::String("Alice".into()));
        assert_eq!(result[0][2], DataType::Long(30));
    }

    #[tokio::test]
    async fn test_lookup_ref_batch_deduplicates_repeated_ref() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        node.execute_tx(vec![TxOp::put(vec![
            (kw!(:name), "Bob".into()),
            (kw!(:email), "bob@example.com".into()),
        ])])
        .await
        .unwrap();

        let bob_lookup = DataType::String("bob@example.com".into());
        let result = node
            .execute_tx(vec![TxOp::Add {
                entity: EntityRef::LookupRef(kw!(:email), bob_lookup.clone()),
                attribute: kw!(:follows),
                value: DataType::Vector(vec![DataType::Keyword(kw!(:email)), bob_lookup]),
            }])
            .await
            .unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        let db = node.db().await.unwrap();
        let result = db
            .query(
                r#"[:find ?follower ?followed
                   :where [?e1 :name ?follower] [?e1 :follows ?e2] [?e2 :name ?followed]]"#,
            )
            .await
            .unwrap();
        assert_eq!(
            result,
            vec![vec![
                DataType::String("Bob".into()),
                DataType::String("Bob".into())
            ]]
        );
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

    #[tokio::test]
    async fn test_unique_identity_tempid_upserts_existing_entity() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        node.execute_tx(vec![TxOp::put(vec![
            (kw!(:name), "Alice".into()),
            (kw!(:email), "alice@example.com".into()),
        ])])
        .await
        .unwrap();

        let result = node
            .execute_tx(vec![
                TxOp::Add {
                    entity: "alice-temp".into(),
                    attribute: kw!(:email),
                    value: "alice@example.com".into(),
                },
                TxOp::Add {
                    entity: "alice-temp".into(),
                    attribute: kw!(:age),
                    value: 31_i64.into(),
                },
            ])
            .await
            .unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        let db = node.db().await.unwrap();
        let result = db
            .query("[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]")
            .await
            .unwrap();
        assert_eq!(
            result,
            vec![vec![DataType::String("Alice".into()), DataType::Long(31)]]
        );
    }

    #[tokio::test]
    async fn test_two_tempids_same_new_identity_allocate_one_entity() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let result = node
            .execute_tx(vec![
                TxOp::Add {
                    entity: "t1".into(),
                    attribute: kw!(:email),
                    value: "shared@example.com".into(),
                },
                TxOp::Add {
                    entity: "t1".into(),
                    attribute: kw!(:name),
                    value: "Shared".into(),
                },
                TxOp::Add {
                    entity: "t2".into(),
                    attribute: kw!(:email),
                    value: "shared@example.com".into(),
                },
                TxOp::Add {
                    entity: "t2".into(),
                    attribute: kw!(:age),
                    value: 42_i64.into(),
                },
            ])
            .await
            .unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        let db = node.db().await.unwrap();
        let result = db
            .query("[:find ?e ?name ?age :where [?e :name ?name] [?e :age ?age]]")
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][1], DataType::String("Shared".into()));
        assert_eq!(result[0][2], DataType::Long(42));
    }

    #[tokio::test]
    async fn test_multistage_upsert_with_tempids_in_entity_and_value_position() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        assert!(matches!(
            node.execute_tx(vec![unique_identity_schema_attribute(kw!(:ref-id), "ref")])
                .await
                .unwrap(),
            TransactionResult::TxCommited(_)
        ));

        node.execute_tx(vec![TxOp::put(vec![
            (kw!(:name), "Alice".into()),
            (kw!(:email), "alice@example.com".into()),
        ])])
        .await
        .unwrap();
        let db = node.db().await.unwrap();
        let alice = match &db
            .query(r#"[:find ?e :where [?e :email "alice@example.com"]]"#)
            .await
            .unwrap()[0][0]
        {
            DataType::Long(id) => *id,
            other => panic!("Expected Long entity ID, got {:?}", other),
        };

        node.execute_tx(vec![TxOp::put(vec![
            (kw!(:name), "Bob".into()),
            (kw!(:ref-id), DataType::Long(alice)),
        ])])
        .await
        .unwrap();

        let result = node
            .execute_tx(vec![
                TxOp::Add {
                    entity: "alice-temp".into(),
                    attribute: kw!(:email),
                    value: "alice@example.com".into(),
                },
                TxOp::Add {
                    entity: "bob-temp".into(),
                    attribute: kw!(:ref-id),
                    value: DataType::String("alice-temp".into()),
                },
                TxOp::Add {
                    entity: "bob-temp".into(),
                    attribute: kw!(:age),
                    value: 7_i64.into(),
                },
            ])
            .await
            .unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        let db = node.db().await.unwrap();
        let result = db
            .query(r#"[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]"#)
            .await
            .unwrap();
        assert_eq!(
            result,
            vec![vec![DataType::String("Bob".into()), DataType::Long(7)]]
        );
    }

    #[tokio::test]
    async fn test_tempid_only_in_ref_value_position_aborts() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;

        let result = node
            .execute_tx(vec![
                TxOp::Add {
                    entity: "bob".into(),
                    attribute: kw!(:name),
                    value: "Bob".into(),
                },
                TxOp::Add {
                    entity: "bob".into(),
                    attribute: kw!(:follows),
                    value: DataType::String("alice".into()),
                },
            ])
            .await
            .unwrap();

        assert!(
            matches!(result, TransactionResult::TxAborted(_, _)),
            "tempids that appear only in value position must abort, got {:?}",
            result
        );
    }

    /// Cross-iteration EV→E resolution where the second-iteration store
    /// lookup misses entirely. alice-temp and bob-temp both resolve in iter 1
    /// via `:user/email`; the EV `(alice-temp :spouse bob-temp)` promotes to
    /// `UpsertE("alice-temp", :spouse, Long(bob))` for conflict detection.
    /// iter 2's `(:spouse, ref(bob))` lookup finds nothing — Mentat would
    /// route alice-temp to allocations and panic on the
    /// `tempids.contains_key` assert. Triplox's cumulative `resolved_tempids`
    /// recognizes the prior-generation resolution and routes the datom to the
    /// `resolved` population instead.
    #[tokio::test]
    async fn test_upsert_ev_cross_iteration_db_miss_resolves_via_prior_generation() {
        let node = Node::memory_node().await;
        node.execute_tx(vec![
            unique_identity_schema_attribute(kw!(:user/email), "string"),
            unique_identity_schema_attribute(kw!(:user/spouse), "ref"),
        ])
        .await
        .unwrap();

        node.execute_tx(vec![
            TxOp::put(vec![(kw!(:user/email), "alice".into())]),
            TxOp::put(vec![(kw!(:user/email), "bob".into())]),
        ])
        .await
        .unwrap();

        let result = node
            .execute_tx(vec![
                TxOp::Add {
                    entity: "alice-temp".into(),
                    attribute: kw!(:user/email),
                    value: "alice".into(),
                },
                TxOp::Add {
                    entity: "bob-temp".into(),
                    attribute: kw!(:user/email),
                    value: "bob".into(),
                },
                TxOp::Add {
                    entity: "alice-temp".into(),
                    attribute: kw!(:user/spouse),
                    value: DataType::String("bob-temp".into()),
                },
            ])
            .await
            .unwrap();
        assert!(
            matches!(result, TransactionResult::TxCommited(_)),
            "expected commit, got {:?}",
            result
        );

        let db = node.db().await.unwrap();
        let rows = db
            .query(
                r#"[:find ?ae ?be
                    :where [?ae :user/email "alice"]
                           [?be :user/email "bob"]
                           [?ae :user/spouse ?be]]"#,
            )
            .await
            .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "expected one (alice :spouse bob) row, got {:?}",
            rows
        );
    }

    /// Cross-iteration EV→E resolution where iter 2's store lookup binds the
    /// same tempid to a *different* entity than iter 1 picked. Pre-seed
    /// `(carol :spouse bob)` so the iter-2 `(:spouse, ref(bob))` lookup
    /// returns carol's entid for alice-temp; `record_resolutions` should see
    /// alice-temp → A in the cumulative map and alice-temp → C in the new
    /// map and abort with "Conflicting upserts".
    #[tokio::test]
    async fn test_upsert_ev_cross_iteration_conflict_rejects_tx() {
        let node = Node::memory_node().await;
        node.execute_tx(vec![
            unique_identity_schema_attribute(kw!(:user/email), "string"),
            unique_identity_schema_attribute(kw!(:user/spouse), "ref"),
        ])
        .await
        .unwrap();

        node.execute_tx(vec![
            TxOp::put(vec![
                (kw!(:db/id), DataType::String("alice".into())),
                (kw!(:user/email), "alice".into()),
            ]),
            TxOp::put(vec![
                (kw!(:db/id), DataType::String("bob".into())),
                (kw!(:user/email), "bob".into()),
            ]),
            TxOp::put(vec![
                (kw!(:db/id), DataType::String("carol".into())),
                (kw!(:user/email), "carol".into()),
                (kw!(:user/spouse), DataType::String("bob".into())),
            ]),
        ])
        .await
        .unwrap();

        let result = node
            .execute_tx(vec![
                TxOp::Add {
                    entity: "alice-temp".into(),
                    attribute: kw!(:user/email),
                    value: "alice".into(),
                },
                TxOp::Add {
                    entity: "bob-temp".into(),
                    attribute: kw!(:user/email),
                    value: "bob".into(),
                },
                TxOp::Add {
                    entity: "alice-temp".into(),
                    attribute: kw!(:user/spouse),
                    value: DataType::String("bob-temp".into()),
                },
            ])
            .await
            .unwrap();

        match result {
            TransactionResult::TxAborted(_, err) => {
                let msg = err.to_string();
                assert!(
                    msg.contains("Conflicting upserts"),
                    "expected 'Conflicting upserts', got: {}",
                    msg
                );
            }
            TransactionResult::TxCommited(_) => panic!("expected TxAborted, got TxCommited"),
        }
    }

    /// In-tx ownership transfer of a unique-identity ref attribute.
    ///
    /// The user retracts carol's `(:user/primary-friend, bob)` ownership and
    /// reasserts it on a new entity (resolved via `:user/email`) in the same
    /// transaction. validate_unique_constraints accounts for same-tx
    /// retractions and accepts this; the upsert resolver guard does not — it
    /// promotes the UpsertEV to UpsertE even when both tempids resolve, so
    /// the next-round VAE lookup sees carol (basis-t state) and emits
    /// "Conflicting upserts". The test asserts the user-intended success
    /// behavior; ignored until the layer disagreement is resolved.
    #[tokio::test]
    #[ignore]
    async fn test_in_tx_transfer_of_unique_identity_ref() {
        let node = Node::memory_node().await;
        node.execute_tx(vec![
            unique_identity_schema_attribute(kw!(:user/email), "string"),
            unique_identity_schema_attribute(kw!(:user/handle), "string"),
            unique_identity_schema_attribute(kw!(:user/primary-friend), "ref"),
        ])
        .await
        .unwrap();

        node.execute_tx(vec![
            TxOp::put(vec![
                (kw!(:db/id), DataType::String("alice".into())),
                (kw!(:user/email), DataType::String("alice".into())),
            ]),
            TxOp::put(vec![
                (kw!(:db/id), DataType::String("bob".into())),
                (kw!(:user/handle), DataType::String("bob".into())),
            ]),
            TxOp::put(vec![
                (kw!(:db/id), DataType::String("carol".into())),
                (kw!(:user/primary-friend), DataType::String("bob".into())),
            ]),
        ])
        .await
        .unwrap();

        let db = node.db().await.unwrap();
        let row = db
            .query(
                r#"[:find ?bob ?carol
                    :where [?bob :user/handle "bob"]
                           [?carol :user/primary-friend ?bob]]"#,
            )
            .await
            .unwrap();
        let (bob_eid, carol_eid) = match row.as_slice() {
            [r] => match r.as_slice() {
                [DataType::Long(b), DataType::Long(c)] => (*b, *c),
                other => panic!("Expected [Long, Long], got {:?}", other),
            },
            other => panic!("Expected exactly one row, got {:?}", other),
        };

        let result = node
            .execute_tx(vec![
                TxOp::Add {
                    entity: "u".into(),
                    attribute: kw!(:user/email),
                    value: "alice".into(),
                },
                TxOp::Add {
                    entity: "u".into(),
                    attribute: kw!(:user/primary-friend),
                    value: DataType::String("f".into()),
                },
                TxOp::Add {
                    entity: "f".into(),
                    attribute: kw!(:user/handle),
                    value: "bob".into(),
                },
                TxOp::Retract {
                    entity: EntityRef::Id(carol_eid),
                    attribute: kw!(:user/primary-friend),
                    value: DataType::Long(bob_eid),
                },
            ])
            .await
            .unwrap();
        assert!(
            matches!(result, TransactionResult::TxCommited(_)),
            "expected in-tx transfer to succeed, got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_unique_value_rejects_duplicate_and_lookup_ref() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        assert!(matches!(
            node.execute_tx(vec![unique_value_schema_attribute(kw!(:ssn), "string")])
                .await
                .unwrap(),
            TransactionResult::TxCommited(_)
        ));

        node.execute_tx(vec![TxOp::Add {
            entity: "p1".into(),
            attribute: kw!(:ssn),
            value: "123".into(),
        }])
        .await
        .unwrap();

        let duplicate = node
            .execute_tx(vec![TxOp::Add {
                entity: "p2".into(),
                attribute: kw!(:ssn),
                value: "123".into(),
            }])
            .await
            .unwrap();
        assert!(matches!(duplicate, TransactionResult::TxAborted(_, _)));

        let lookup_ref = node
            .execute_tx(vec![TxOp::Add {
                entity: EntityRef::LookupRef(kw!(:ssn), DataType::String("123".into())),
                attribute: kw!(:age),
                value: 1_i64.into(),
            }])
            .await
            .unwrap();
        assert!(matches!(lookup_ref, TransactionResult::TxAborted(_, _)));
    }

    /// Same-tx ownership transfer of a `:db.unique/value` attribute.
    ///
    /// Unlike `:db.unique/identity`, `:db.unique/value` does not participate
    /// in upsert resolution, so the resolver guard never fires. The
    /// transaction reaches `validate_unique_constraints`, which honors the
    /// in-tx retraction of the previous owner and accepts the reassertion.
    #[tokio::test]
    async fn test_in_tx_transfer_of_unique_value() {
        let node = Node::memory_node().await;
        define_test_schema(&node).await;
        node.execute_tx(vec![unique_value_schema_attribute(kw!(:ssn), "string")])
            .await
            .unwrap();

        node.execute_tx(vec![TxOp::Add {
            entity: "p1".into(),
            attribute: kw!(:ssn),
            value: "123".into(),
        }])
        .await
        .unwrap();

        let db = node.db().await.unwrap();
        let p1_eid = match &db
            .query(r#"[:find ?e :where [?e :ssn "123"]]"#)
            .await
            .unwrap()[0][0]
        {
            DataType::Long(id) => *id,
            other => panic!("Expected Long entity ID for p1, got {:?}", other),
        };

        let result = node
            .execute_tx(vec![
                TxOp::Retract {
                    entity: EntityRef::Id(p1_eid),
                    attribute: kw!(:ssn),
                    value: "123".into(),
                },
                TxOp::Add {
                    entity: "p2".into(),
                    attribute: kw!(:ssn),
                    value: "123".into(),
                },
            ])
            .await
            .unwrap();
        assert!(
            matches!(result, TransactionResult::TxCommited(_)),
            "expected in-tx transfer to succeed, got: {:?}",
            result
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_local_node_partition_counters_include_user_partition() {
        let dir = tempfile::tempdir().unwrap();
        let root_path = dir.path().to_path_buf();

        // Fresh local node — bootstrap only, no user transactions yet
        let node = Node::local_node(&root_path).await.unwrap();
        {
            let indexer = node.indexer.read().await;
            let pm = &indexer.metadata().partition_map;
            // All three partitions must be present even before any user data
            assert!(
                pm.contains_key(&crate::partition::USER_PARTITION),
                "USER_PARTITION should be in partition map after bootstrap"
            );
            assert_eq!(pm[&crate::partition::USER_PARTITION], 0);
            assert!(pm[&crate::partition::DB_PARTITION] > 0);
            assert_eq!(pm[&crate::partition::TX_PARTITION], 1);
        }

        // Insert a user entity, close, and reopen
        define_test_schema(&node).await;
        node.execute_tx(vec![TxOp::Add {
            entity: "alice".into(),
            attribute: kw!(:name),
            value: "alice".into(),
        }])
        .await
        .unwrap();
        node.close().await;

        // After restart, all partitions should be present with correct counters
        let node = Node::local_node(&root_path).await.unwrap();
        {
            let indexer = node.indexer.read().await;
            let pm = &indexer.metadata().partition_map;
            assert!(pm[&crate::partition::USER_PARTITION] > 0);
            assert!(pm[&crate::partition::DB_PARTITION] > 0);
            assert!(pm[&crate::partition::TX_PARTITION] > 0);
        }

        node.close().await;
    }
}
