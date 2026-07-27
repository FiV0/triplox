//! Writer-node incremental query service.

use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex as StdMutex};
use std::thread;

use crate::partition::tx_eid_from_tx_id;
use anyhow::{anyhow, Context, Result};
use dbsp::{utils::Tup2, ZWeight};
use slatedb::object_store::ObjectStore;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use triplox_client::transaction::TxKey;

use crate::inc_query::{plan_query, IncrementalQueryPlan};
use crate::incremental::cdc::{scan_current_triples, spawn_cdc_loop};
use crate::incremental::circuit::QueryCircuit;
use crate::indexer::Indexer;
use crate::ops::DataType;
use crate::slate::cdc::CdcCursor;
use edn::query::ParsedQuery;

pub(crate) mod cdc;
pub(crate) mod circuit;

const SUBSCRIPTION_CAPACITY: usize = 128;

pub(crate) type EncodedValue = Vec<u8>;
pub(crate) type EncodedRow = Vec<EncodedValue>;

#[derive(
    Clone,
    Debug,
    Default,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    size_of::SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    feldera_macros::IsNone,
)]
#[archive_attr(derive(Eq, PartialEq, Ord, PartialOrd))]
pub(crate) struct EncodedTriple {
    pub entity: EncodedValue,
    pub attribute: i64,
    pub value: EncodedValue,
}

pub(crate) type IncrementalQueryHandle = u64;

#[derive(Debug)]
pub(crate) struct IncrementalQuerySubscription {
    pub handle: IncrementalQueryHandle,
    pub tx_key: TxKey,
    pub deltas: mpsc::Receiver<IncrementalQueryDelta>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IncrementalQueryDelta {
    pub tx_key: TxKey,
    pub rows: Vec<(Vec<DataType>, isize)>,
}

type ServiceResult<T> = std::result::Result<T, String>;

enum IncrementalCommand {
    Register {
        plan: IncrementalQueryPlan,
        tx_key: TxKey,
        wal_cursor: CdcCursor,
        initial_triples: Vec<Tup2<EncodedTriple, ZWeight>>,
        response: oneshot::Sender<ServiceResult<IncrementalQuerySubscription>>,
    },
    Unregister {
        handle: IncrementalQueryHandle,
        response: oneshot::Sender<ServiceResult<()>>,
    },
    ApplyTriples {
        tx_key: TxKey,
        wal_seq: u64,
        triples: Vec<Tup2<EncodedTriple, ZWeight>>,
        response: oneshot::Sender<ServiceResult<()>>,
    },
    Shutdown {
        response: oneshot::Sender<ServiceResult<()>>,
    },
}

#[derive(Clone)]
pub(crate) struct IncrementalQueryService {
    commands: std_mpsc::Sender<IncrementalCommand>,
    cdc_object_path: String,
    cdc_object_store: Arc<dyn ObjectStore>,
    cancel: CancellationToken,
    cdc_task: Arc<StdMutex<Option<JoinHandle<Result<()>>>>>,
    registration_gate: Arc<Mutex<()>>,
}

// The two result levels in this service have two different meanings.
// The first level is about the communication of this service.
// The second level reports errors while processing a command.
impl IncrementalQueryService {
    pub(crate) fn new(
        storage_root: PathBuf,
        runtime: Handle,
        cancel: CancellationToken,
        cdc_object_path: String,
        cdc_object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        let cancel = cancel.child_token();
        let inner_cancel = cancel.clone();
        let (sender, receiver) = std_mpsc::channel();
        thread::Builder::new()
            .name("triplox-incremental-query".to_string())
            .spawn(move || {
                IncrementalQueryServiceInner::new(storage_root, runtime, inner_cancel).run(receiver)
            })
            .expect("incremental query service thread should start");

        Self {
            commands: sender,
            cdc_object_path,
            cdc_object_store,
            cancel,
            cdc_task: Arc::new(StdMutex::new(None)),
            registration_gate: Arc::new(Mutex::new(())),
        }
    }

    pub(crate) async fn register_query(
        &self,
        db: &slatedb::Db,
        query: ParsedQuery,
        indexer: Arc<RwLock<Indexer>>,
    ) -> Result<IncrementalQuerySubscription> {
        let _registration_guard = self.registration_gate.lock().await;
        let (tx_key, schema) = {
            let indexer = indexer.read().await;
            (indexer.latest_tx_key(), indexer.metadata().schema.clone())
        };
        let plan = plan_query(&query, &schema)?;
        let initial_triples =
            scan_current_triples(db, &plan, tx_eid_from_tx_id(tx_key.tx_id)).await?;
        let wal_cursor = CdcCursor {
            // TODO: This should likely be initialized to manifest.replay_after_wal_id + 1. See #337
            wal_id: 0,
            last_seq: db.status().durable_seq,
        };
        let subscription = self
            .register_prepared_query(plan, tx_key, wal_cursor, initial_triples)
            .await?;
        self.start_cdc_once(indexer);
        Ok(subscription)
    }

    fn start_cdc_once<N>(&self, node: Arc<N>)
    where
        N: crate::node::SchemaProvider,
    {
        let mut cdc_task = self.cdc_task.lock().unwrap();
        if cdc_task.is_some() {
            return;
        }

        let handle = spawn_cdc_loop(
            self.cdc_object_path.clone(),
            self.cdc_object_store.clone(),
            node,
            self.clone(),
            self.registration_gate.clone(),
            self.cancel.clone(),
        );
        *cdc_task = Some(handle);
    }

    pub(crate) async fn await_cdc_task(&self) -> Result<()> {
        let handle = self.cdc_task.lock().unwrap().take();
        if let Some(handle) = handle {
            let cdc_result = handle
                .await
                .context("Incremental query CDC task failed to join")?;
            cdc_result.context("Incremental query CDC loop failed")?;
        }
        Ok(())
    }

    pub(crate) async fn register_prepared_query(
        &self,
        plan: IncrementalQueryPlan,
        tx_key: TxKey,
        wal_cursor: CdcCursor,
        initial_triples: Vec<Tup2<EncodedTriple, ZWeight>>,
    ) -> Result<IncrementalQuerySubscription> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(IncrementalCommand::Register {
                plan,
                tx_key,
                wal_cursor,
                initial_triples,
                response,
            })
            .map_err(|_| anyhow!("Incremental query service stopped"))?;
        result
            .await
            .map_err(|_| anyhow!("Incremental query service stopped"))?
            .map_err(|err| anyhow!("{}", err))
    }

    pub(crate) async fn unregister(&self, handle: IncrementalQueryHandle) -> Result<()> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(IncrementalCommand::Unregister { handle, response })
            .map_err(|_| anyhow!("Incremental query service stopped"))?;
        result
            .await
            .map_err(|_| anyhow!("Incremental query service stopped"))?
            .map_err(|err| anyhow!("{}", err))
    }

    pub(crate) async fn apply_triples(
        &self,
        tx_key: TxKey,
        wal_seq: u64,
        triples: Vec<Tup2<EncodedTriple, ZWeight>>,
    ) -> Result<()> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(IncrementalCommand::ApplyTriples {
                tx_key,
                wal_seq,
                triples,
                response,
            })
            .map_err(|_| anyhow!("Incremental query service stopped"))?;
        result
            .await
            .map_err(|_| anyhow!("Incremental query service stopped"))?
            .map_err(|err| anyhow!("{}", err))
    }

    pub(crate) async fn shutdown(&self) -> Result<()> {
        self.cancel.cancel();
        let cdc_result = self.await_cdc_task().await;
        let (response, result) = oneshot::channel();
        let service_result = match self
            .commands
            .send(IncrementalCommand::Shutdown { response })
        {
            Ok(()) => result
                .await
                .map_err(|_| anyhow!("Incremental query service stopped"))
                .and_then(|result| result.map_err(|err| anyhow!("{}", err))),
            Err(_) => Err(anyhow!("Incremental query service stopped")),
        };

        match (cdc_result, service_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(err), Ok(())) | (Ok(()), Err(err)) => Err(err),
            (Err(cdc_err), Err(service_err)) => Err(anyhow!(
                "Incremental query CDC shutdown failed: {:#}; incremental query service shutdown failed: {:#}",
                cdc_err,
                service_err
            )),
        }
    }
}

enum DeltaDelivery {
    Delivered,
    Closed,
    Cancelled,
}

fn send_delta(
    runtime: &Handle,
    sender: &mpsc::Sender<IncrementalQueryDelta>,
    delta: IncrementalQueryDelta,
    cancel: &CancellationToken,
) -> DeltaDelivery {
    runtime.block_on(async {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => DeltaDelivery::Cancelled,
            result = sender.send(delta) => {
                match result {
                    Ok(()) => DeltaDelivery::Delivered,
                    Err(_) => DeltaDelivery::Closed,
                }
            }
        }
    })
}

struct RegisteredQuery {
    _plan: IncrementalQueryPlan,
    _circuit: QueryCircuit,
    sender: mpsc::Sender<IncrementalQueryDelta>,
    _tx_key: TxKey,
    _wal_cursor: CdcCursor,
}

struct IncrementalQueryServiceInner {
    next_query_id: u64,
    storage_root: PathBuf,
    queries: HashMap<IncrementalQueryHandle, RegisteredQuery>,
    runtime: Handle,
    cancel: CancellationToken,
}

impl IncrementalQueryServiceInner {
    fn new(storage_root: PathBuf, runtime: Handle, cancel: CancellationToken) -> Self {
        Self {
            next_query_id: 1,
            storage_root,
            queries: HashMap::new(),
            runtime,
            cancel,
        }
    }

    fn run(mut self, receiver: std_mpsc::Receiver<IncrementalCommand>) {
        while let Ok(command) = receiver.recv() {
            match command {
                IncrementalCommand::Register {
                    plan,
                    tx_key,
                    wal_cursor,
                    initial_triples,
                    response,
                } => {
                    let _ = response.send(self.register(plan, tx_key, wal_cursor, initial_triples));
                }
                IncrementalCommand::Unregister { handle, response } => {
                    let _ = response.send(self.unregister(handle));
                }
                IncrementalCommand::ApplyTriples {
                    tx_key,
                    wal_seq,
                    triples,
                    response,
                } => {
                    let _ = response.send(self.apply_triples(tx_key, wal_seq, triples));
                }
                IncrementalCommand::Shutdown { response } => {
                    let _ = response.send(self.remove_all_queries());
                    break;
                }
            }
        }
    }

    fn register(
        &mut self,
        plan: IncrementalQueryPlan,
        tx_key: TxKey,
        wal_cursor: CdcCursor,
        initial_triples: Vec<Tup2<EncodedTriple, ZWeight>>,
    ) -> ServiceResult<IncrementalQuerySubscription> {
        self.cleanup_closed_subscriptions()?;

        let handle = self.allocate_query_id();
        let mut circuit = QueryCircuit::build(plan.clone(), &self.query_storage_path(handle))
            .map_err(|err| format!("{:#}", err))?;
        // Priming is the circuit's first batch, so its delta is the whole query result.
        let priming_rows = circuit
            .apply(initial_triples)
            .map_err(|err| format!("{:#}", err))?;
        let (sender, receiver) = mpsc::channel(SUBSCRIPTION_CAPACITY);
        if !priming_rows.is_empty() {
            sender
                .try_send(IncrementalQueryDelta {
                    tx_key,
                    rows: priming_rows,
                })
                .map_err(|err| format!("Failed to enqueue priming result set: {}", err))?;
        }

        self.queries.insert(
            handle,
            RegisteredQuery {
                _plan: plan,
                _circuit: circuit,
                sender,
                _tx_key: tx_key,
                _wal_cursor: wal_cursor,
            },
        );

        Ok(IncrementalQuerySubscription {
            handle,
            tx_key,
            deltas: receiver,
        })
    }

    fn unregister(&mut self, handle: IncrementalQueryHandle) -> ServiceResult<()> {
        self.cleanup_closed_subscriptions()?;
        self.remove_query(handle)
    }

    fn apply_triples(
        &mut self,
        tx_key: TxKey,
        wal_seq: u64,
        triples: Vec<Tup2<EncodedTriple, ZWeight>>,
    ) -> ServiceResult<()> {
        self.cleanup_closed_subscriptions()?;
        let mut closed = Vec::new();
        let cancel = self.cancel.clone();

        for (id, query) in &mut self.queries {
            if tx_key.tx_id <= query._tx_key.tx_id {
                query._wal_cursor.last_seq = wal_seq;
                continue;
            }

            let rows = query
                ._circuit
                .apply(triples.clone())
                .map_err(|err| format!("{:#}", err))?;
            if rows.is_empty() {
                query._wal_cursor.last_seq = wal_seq;
                continue;
            }
            let delta = IncrementalQueryDelta { tx_key, rows };
            match send_delta(&self.runtime, &query.sender, delta, &cancel) {
                DeltaDelivery::Delivered => query._wal_cursor.last_seq = wal_seq,
                DeltaDelivery::Closed => closed.push(*id),
                DeltaDelivery::Cancelled => return Ok(()),
            }
        }

        for id in closed {
            self.remove_query(id)?;
        }
        Ok(())
    }

    fn allocate_query_id(&mut self) -> IncrementalQueryHandle {
        let id = self.next_query_id;
        self.next_query_id += 1;
        id
    }

    fn query_storage_path(&self, id: IncrementalQueryHandle) -> PathBuf {
        self.storage_root.join(format!("query-{}", id))
    }

    fn remove_query(&mut self, id: IncrementalQueryHandle) -> ServiceResult<()> {
        let query = self
            .queries
            .remove(&id)
            .ok_or_else(|| format!("Unknown incremental query handle: {:?}", id))?;
        drop(query);
        self.remove_query_storage(id)
    }

    fn remove_query_storage(&self, id: IncrementalQueryHandle) -> ServiceResult<()> {
        match std::fs::remove_dir_all(self.query_storage_path(id)) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
            Err(err) => Err(format!(
                "Failed to remove incremental query storage for {:?}: {}",
                id, err
            )),
        }
    }

    fn remove_all_queries(&mut self) -> ServiceResult<()> {
        let ids = self.queries.keys().copied().collect::<Vec<_>>();
        let mut errors = Vec::new();
        for id in ids {
            if let Err(err) = self.remove_query(id) {
                errors.push(err);
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    fn cleanup_closed_subscriptions(&mut self) -> ServiceResult<()> {
        let closed = self
            .queries
            .iter()
            .filter_map(|(id, query)| query.sender.is_closed().then_some(*id))
            .collect::<Vec<_>>();
        for id in closed {
            self.remove_query(id)?;
        }
        Ok(())
    }
}

impl Drop for IncrementalQueryServiceInner {
    fn drop(&mut self) {
        let _ = self.remove_all_queries();
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::{mpsc as std_mpsc, Arc};
    use std::time::Duration;

    use chrono::Utc;
    use dbsp::utils::Tup2;
    use edn::query::ToVariable;
    use slatedb::object_store::memory::InMemory;
    use triplox_client::transaction::TxKey;

    use super::*;
    use crate::codec::Encode;
    use crate::inc_query::{IncrementalQueryPlan, PatternPlan, PatternSlot, RelPlan, RelPlanKind};

    #[test]
    fn unregister_removes_query_storage() {
        let dir = tempfile::tempdir().unwrap();
        let runtime = test_runtime();
        let mut service = IncrementalQueryServiceInner::new(
            dir.path().to_path_buf(),
            runtime.handle().clone(),
            CancellationToken::new(),
        );
        let subscription = service
            .register(
                single_pattern_plan(),
                test_tx_key(),
                test_cursor(),
                initial_triples(),
            )
            .unwrap();
        let storage_path = dir.path().join("query-1");

        assert!(path_has_entries(&storage_path));

        service.unregister(subscription.handle).unwrap();

        assert!(!storage_path.exists());
    }

    #[test]
    fn dropped_receiver_cleanup_removes_query_storage() {
        let dir = tempfile::tempdir().unwrap();
        let runtime = test_runtime();
        let mut service = IncrementalQueryServiceInner::new(
            dir.path().to_path_buf(),
            runtime.handle().clone(),
            CancellationToken::new(),
        );
        let subscription = service
            .register(
                single_pattern_plan(),
                test_tx_key(),
                test_cursor(),
                initial_triples(),
            )
            .unwrap();
        let storage_path = dir.path().join("query-1");

        assert!(path_has_entries(&storage_path));
        let handle = subscription.handle;
        drop(subscription);

        let err = service.unregister(handle).unwrap_err();
        assert!(err.contains("Unknown incremental query handle"));
        assert!(!storage_path.exists());
    }

    #[test]
    fn register_enqueues_non_empty_priming_result_before_future_deltas() {
        let dir = tempfile::tempdir().unwrap();
        let runtime = test_runtime();
        let mut service = IncrementalQueryServiceInner::new(
            dir.path().to_path_buf(),
            runtime.handle().clone(),
            CancellationToken::new(),
        );
        let registration_basis = test_tx_key_with_tx_id(1);
        let future_basis = test_tx_key_with_tx_id(2);
        let mut subscription = service
            .register(
                single_pattern_plan(),
                registration_basis,
                test_cursor(),
                vec![name_triple(42, "Alice")],
            )
            .unwrap();

        service
            .apply_triples(future_basis, 2, vec![name_triple(43, "Bob")])
            .unwrap();

        assert_eq!(
            subscription.deltas.try_recv().unwrap(),
            IncrementalQueryDelta {
                tx_key: registration_basis,
                rows: vec![(vec![DataType::String("Alice".to_string())], 1)],
            }
        );
        assert_eq!(
            subscription.deltas.try_recv().unwrap(),
            IncrementalQueryDelta {
                tx_key: future_basis,
                rows: vec![(vec![DataType::String("Bob".to_string())], 1)],
            }
        );
    }

    #[test]
    fn register_skips_empty_priming_result() {
        let dir = tempfile::tempdir().unwrap();
        let runtime = test_runtime();
        let mut service = IncrementalQueryServiceInner::new(
            dir.path().to_path_buf(),
            runtime.handle().clone(),
            CancellationToken::new(),
        );
        let mut subscription = service
            .register(
                single_pattern_plan(),
                test_tx_key(),
                test_cursor(),
                Vec::new(),
            )
            .unwrap();

        assert!(matches!(
            subscription.deltas.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn apply_triples_skips_transactions_at_or_before_query_basis() {
        let dir = tempfile::tempdir().unwrap();
        let runtime = test_runtime();
        let mut service = IncrementalQueryServiceInner::new(
            dir.path().to_path_buf(),
            runtime.handle().clone(),
            CancellationToken::new(),
        );
        let old_basis = test_tx_key_with_tx_id(1);
        let new_basis = test_tx_key_with_tx_id(2);
        let mut old_subscription = service
            .register(
                single_pattern_plan(),
                old_basis,
                test_cursor(),
                vec![name_triple(42, "Alice")],
            )
            .unwrap();
        let mut new_subscription = service
            .register(
                single_pattern_plan(),
                new_basis,
                test_cursor(),
                vec![name_triple(42, "Alice"), name_triple(43, "Bob")],
            )
            .unwrap();

        // Drain priming deltas before testing application relative to each basis.
        old_subscription.deltas.try_recv().unwrap();
        new_subscription.deltas.try_recv().unwrap();

        service
            .apply_triples(new_basis, 2, vec![name_triple(43, "Bob")])
            .unwrap();

        assert_eq!(
            old_subscription.deltas.try_recv().unwrap().rows,
            vec![(vec![DataType::String("Bob".to_string())], 1)]
        );
        assert!(new_subscription.deltas.try_recv().is_err());
        assert_eq!(
            service
                .queries
                .get(&old_subscription.handle)
                .unwrap()
                ._wal_cursor
                .last_seq,
            2
        );
        assert_eq!(
            service
                .queries
                .get(&new_subscription.handle)
                .unwrap()
                ._wal_cursor
                .last_seq,
            2
        );
    }

    #[test]
    fn apply_triples_stops_waiting_on_full_subscription_when_cancelled() {
        let dir = tempfile::tempdir().unwrap();
        let runtime = test_runtime();
        let cancel = CancellationToken::new();
        let mut service = IncrementalQueryServiceInner::new(
            dir.path().to_path_buf(),
            runtime.handle().clone(),
            cancel.clone(),
        );
        let _subscription = service
            .register(
                single_pattern_plan(),
                test_tx_key(),
                test_cursor(),
                Vec::new(),
            )
            .unwrap();

        for seq in 1..=SUBSCRIPTION_CAPACITY {
            let name = format!("Alice {seq}");
            service
                .apply_triples(
                    test_tx_key_with_tx_id(seq as i64 + 1),
                    seq as u64,
                    vec![name_triple(seq as i64, &name)],
                )
                .unwrap();
        }

        let (done_tx, done_rx) = std_mpsc::channel();
        let handle = thread::spawn(move || {
            let name = format!("Alice {}", SUBSCRIPTION_CAPACITY + 1);
            let result = service.apply_triples(
                test_tx_key_with_tx_id(SUBSCRIPTION_CAPACITY as i64 + 2),
                (SUBSCRIPTION_CAPACITY + 1) as u64,
                vec![name_triple((SUBSCRIPTION_CAPACITY + 1) as i64, &name)],
            );
            done_tx.send(result).unwrap();
        });

        thread::sleep(Duration::from_millis(50));
        cancel.cancel();

        let result = done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("apply_triples should stop waiting after cancellation");
        assert!(result.is_ok());
        handle.join().unwrap();
    }

    #[tokio::test]
    async fn await_cdc_task_returns_inner_loop_error() {
        let dir = tempfile::tempdir().unwrap();
        let service = IncrementalQueryService::new(
            dir.path().to_path_buf(),
            Handle::current(),
            CancellationToken::new(),
            "/test_incremental_cdc_error".to_string(),
            Arc::new(InMemory::new()),
        );
        let handle: JoinHandle<Result<()>> = tokio::spawn(async { Err(anyhow!("cdc failed")) });
        *service.cdc_task.lock().unwrap() = Some(handle);

        let err = service.await_cdc_task().await.unwrap_err();
        let message = format!("{:#}", err);
        assert!(message.contains("Incremental query CDC loop failed"));
        assert!(message.contains("cdc failed"));

        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn await_cdc_task_returns_join_error() {
        let dir = tempfile::tempdir().unwrap();
        let service = IncrementalQueryService::new(
            dir.path().to_path_buf(),
            Handle::current(),
            CancellationToken::new(),
            "/test_incremental_cdc_join_error".to_string(),
            Arc::new(InMemory::new()),
        );
        let handle: JoinHandle<Result<()>> = tokio::spawn(async {
            panic!("cdc task panic");
            #[allow(unreachable_code)]
            Ok(())
        });
        *service.cdc_task.lock().unwrap() = Some(handle);

        let err = service.await_cdc_task().await.unwrap_err();
        let message = format!("{:#}", err);
        assert!(message.contains("Incremental query CDC task failed to join"));

        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_removes_queries_after_cdc_task_error() {
        let dir = tempfile::tempdir().unwrap();
        let service = IncrementalQueryService::new(
            dir.path().to_path_buf(),
            Handle::current(),
            CancellationToken::new(),
            "/test_incremental_shutdown_cdc_error".to_string(),
            Arc::new(InMemory::new()),
        );
        let _subscription = service
            .register_prepared_query(
                single_pattern_plan(),
                test_tx_key(),
                test_cursor(),
                initial_triples(),
            )
            .await
            .unwrap();
        let storage_path = dir.path().join("query-1");
        assert!(path_has_entries(&storage_path));

        let handle: JoinHandle<Result<()>> =
            tokio::spawn(async { Err(anyhow!("cdc shutdown failed")) });
        *service.cdc_task.lock().unwrap() = Some(handle);

        let err = service.shutdown().await.unwrap_err();
        let message = format!("{:#}", err);
        assert!(message.contains("Incremental query CDC loop failed"));
        assert!(message.contains("cdc shutdown failed"));
        assert!(!storage_path.exists());
    }

    // NOTE(coverage): this exercises the `registration_gate` mutual-exclusion
    // contract at the service layer only — it feeds `basis`/triples to
    // `register_prepared_query` directly and never drives the real
    // indexer -> WAL -> CDC -> register pipeline. The full no-missed-transaction
    // guarantee for a query registered against a *live* CDC loop also depends on
    // invariants outside this file: the indexer publishes `latest_indexed_tx`
    // only after issuing the slatedb write, both under the write lock (the write
    // is not awaited for durability), and `register_query` reads the basis under
    // the indexer read lock. Since CDC only ever reads transactions that were
    // already written, the basis observed under the read lock always covers the
    // CDC loop's position regardless of when the WAL flush happens. A
    // regression in that lock ordering would NOT be caught here; it needs a
    // node-level integration test that registers a second query after CDC has
    // advanced.
    #[tokio::test]
    async fn registration_gate_blocks_cdc_apply_until_query_is_registered() {
        let dir = tempfile::tempdir().unwrap();
        let service = IncrementalQueryService::new(
            dir.path().to_path_buf(),
            Handle::current(),
            CancellationToken::new(),
            "/test_incremental_registration_gate".to_string(),
            Arc::new(InMemory::new()),
        );
        let query_tx_key = test_tx_key_with_tx_id(1);
        let apply_tx_key = test_tx_key_with_tx_id(2);
        let mut first_subscription = service
            .register_prepared_query(
                single_pattern_plan(),
                query_tx_key,
                test_cursor(),
                Vec::new(),
            )
            .await
            .unwrap();

        let registration_guard = service.registration_gate.lock().await;
        let applying_service = service.clone();
        let apply = tokio::spawn(async move {
            let _registration_guard = applying_service.registration_gate.lock().await;
            applying_service
                .apply_triples(apply_tx_key, 2, vec![name_triple(43, "Bob")])
                .await
        });
        tokio::task::yield_now().await;
        assert!(first_subscription.deltas.try_recv().is_err());

        let mut second_subscription = service
            .register_prepared_query(
                single_pattern_plan(),
                query_tx_key,
                test_cursor(),
                Vec::new(),
            )
            .await
            .unwrap();
        assert!(second_subscription.deltas.try_recv().is_err());

        drop(registration_guard);
        apply.await.unwrap().unwrap();

        assert_eq!(
            first_subscription.deltas.recv().await.unwrap(),
            IncrementalQueryDelta {
                tx_key: apply_tx_key,
                rows: vec![(vec![DataType::String("Bob".to_string())], 1)],
            }
        );
        assert_eq!(
            second_subscription.deltas.recv().await.unwrap(),
            IncrementalQueryDelta {
                tx_key: apply_tx_key,
                rows: vec![(vec![DataType::String("Bob".to_string())], 1)],
            }
        );

        service.shutdown().await.unwrap();
    }

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Runtime::new().unwrap()
    }

    fn single_pattern_plan() -> IncrementalQueryPlan {
        let pattern = PatternPlan {
            attribute: 10,
            entity: PatternSlot::Variable("?e".to_var()),
            value: PatternSlot::Variable("?name".to_var()),
            pattern_vars: vec!["?e".to_var(), "?name".to_var()],
        };
        IncrementalQueryPlan {
            find_vars: vec!["?name".to_var()],
            variables: vec!["?e".to_var(), "?name".to_var()],
            where_plan: RelPlan {
                incoming_vars: None,
                output_vars: pattern.pattern_vars.clone(),
                kind: RelPlanKind::Pattern(pattern),
            },
        }
    }

    fn initial_triples() -> Vec<Tup2<EncodedTriple, ZWeight>> {
        vec![name_triple(42, "Alice")]
    }

    fn name_triple(entity: i64, name: &str) -> Tup2<EncodedTriple, ZWeight> {
        Tup2(
            EncodedTriple {
                entity: DataType::Long(entity).encode(),
                attribute: 10,
                value: DataType::String(name.to_string()).encode(),
            },
            1,
        )
    }

    fn test_tx_key() -> TxKey {
        test_tx_key_with_tx_id(1)
    }

    fn test_tx_key_with_tx_id(tx_id: i64) -> TxKey {
        TxKey {
            tx_id,
            system_time: Utc::now(),
        }
    }

    fn test_cursor() -> CdcCursor {
        CdcCursor {
            wal_id: 0,
            last_seq: 0,
        }
    }

    fn path_has_entries(path: &Path) -> bool {
        std::fs::read_dir(path)
            .unwrap()
            .next()
            .transpose()
            .unwrap()
            .is_some()
    }
}
