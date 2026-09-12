//! Writer-node incremental query service.

use std::collections::HashMap;
use std::io::ErrorKind;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use std::thread;
use std::time::Duration;

use crate::partition::tx_eid_from_tx_id;
use anyhow::{anyhow, Context, Error, Result};
use dbsp::{utils::Tup2, ZWeight};
use slatedb::object_store::ObjectStore;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;
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
pub(crate) mod subscription;

use subscription::{SubscriptionDeltas, SubscriptionLagged, Termination};
use worker::{Batch, Circuit, Completion, Control, Position, Worker};
mod worker;

const SUBSCRIPTION_CAPACITY: usize = 128;

#[derive(Clone, Copy)]
struct IncrementalQueryOptions {
    inbox_capacity: NonZeroUsize,
    max_concurrent_steps: NonZeroUsize,
    retire_timeout: Duration,
}

impl Default for IncrementalQueryOptions {
    fn default() -> Self {
        Self {
            inbox_capacity: NonZeroUsize::new(256).unwrap(),
            max_concurrent_steps: thread::available_parallelism().unwrap_or(NonZeroUsize::MIN).div_ceil(NonZeroUsize::new(2).unwrap()),
            retire_timeout: Duration::from_secs(10),
        }
    }
}

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
    pub deltas: SubscriptionDeltas,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IncrementalQueryDelta {
    pub tx_key: TxKey,
    pub rows: Vec<(Vec<DataType>, isize)>,
}

type ServiceResult<T> = Result<T>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum RetirementTimeout {
    #[error("Incremental query {handle} unregister timed out after {timeout:?}; cleanup continues in the background")]
    Unregister {
        handle: IncrementalQueryHandle,
        timeout: Duration,
    },
    #[error("Incremental query shutdown timed out after {timeout:?}; cleanup continues in the background")]
    Shutdown { timeout: Duration },
}

enum IncrementalCommand {
    Register {
        plan: Box<IncrementalQueryPlan>,
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
        batch: Arc<Batch>,
        response: oneshot::Sender<ServiceResult<()>>,
    },
    Shutdown {
        response: oneshot::Sender<ServiceResult<()>>,
    },
}

#[derive(Clone)]
pub(crate) struct IncrementalQueryService {
    commands: mpsc::UnboundedSender<IncrementalCommand>,
    cdc_object_path: String,
    cdc_object_store: Arc<dyn ObjectStore>,
    cancel: CancellationToken,
    cdc_task: Arc<StdMutex<Option<JoinHandle<Result<()>>>>>,
    registration_gate: Arc<Mutex<()>>,
    retire_timeout: Duration,
}

// The two result levels in this service have two different meanings.
// The first level is about the communication of this service.
// The second level reports errors while processing a command.
impl IncrementalQueryService {
    pub(crate) fn new(
        storage_root: PathBuf,
        cancel: CancellationToken,
        cdc_object_path: String,
        cdc_object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self::new_with_options(
            storage_root,
            cancel,
            cdc_object_path,
            cdc_object_store,
            IncrementalQueryOptions::default(),
        )
    }

    fn new_with_options(
        storage_root: PathBuf,
        cancel: CancellationToken,
        cdc_object_path: String,
        cdc_object_store: Arc<dyn ObjectStore>,
        options: IncrementalQueryOptions,
    ) -> Self {
        let cancel = cancel.child_token();
        let inner_cancel = cancel.clone();
        let (sender, receiver) = mpsc::unbounded_channel();
        thread::Builder::new()
            .name("triplox-incremental-query".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .thread_name("triplox-iq-worker")
                    .enable_all()
                    .build()
                    .expect("incremental query runtime should start");
                let inner = IncrementalQueryServiceInner::new(
                    storage_root,
                    runtime.handle().clone(),
                    inner_cancel,
                    options,
                );
                runtime.block_on(inner.run(receiver));
            })
            .expect("incremental query service thread should start");

        Self {
            commands: sender,
            cdc_object_path,
            cdc_object_store,
            cancel,
            cdc_task: Arc::new(StdMutex::new(None)),
            registration_gate: Arc::new(Mutex::new(())),
            retire_timeout: options.retire_timeout,
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
                plan: Box::new(plan),
                tx_key,
                wal_cursor,
                initial_triples,
                response,
            })
            .map_err(|_| anyhow!("Incremental query service stopped"))?;
        result.await.context("Incremental query service stopped")?
    }

    pub(crate) async fn unregister(&self, handle: IncrementalQueryHandle) -> Result<()> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(IncrementalCommand::Unregister { handle, response })
            .map_err(|_| anyhow!("Incremental query service stopped"))?;
        tokio::time::timeout(self.retire_timeout, result)
            .await
            .map_err(|_| RetirementTimeout::Unregister {
                handle,
                timeout: self.retire_timeout,
            })?
            .context("Incremental query service stopped")?
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
                batch: Arc::new(Batch {
                    tx_key,
                    wal_seq,
                    triples,
                }),
                response,
            })
            .map_err(|_| anyhow!("Incremental query service stopped"))?;
        result.await.context("Incremental query service stopped")?
    }

    pub(crate) async fn shutdown(&self) -> Result<()> {
        let deadline = tokio::time::Instant::now() + self.retire_timeout;
        self.cancel.cancel();
        let cdc_result = tokio::time::timeout_at(deadline, self.await_cdc_task())
            .await
            .map_err(|_| {
                RetirementTimeout::Shutdown {
                    timeout: self.retire_timeout,
                }
                .into()
            })
            .and_then(|result| result);
        // Request cleanup even when waiting for CDC used the entire deadline.
        let (response, result) = oneshot::channel();
        let service_result = match self
            .commands
            .send(IncrementalCommand::Shutdown { response })
        {
            Ok(()) => match tokio::time::timeout_at(deadline, result).await {
                Ok(result) => result
                    .context("Incremental query service stopped")
                    .and_then(|result| result),
                Err(_) => Err(RetirementTimeout::Shutdown {
                    timeout: self.retire_timeout,
                }
                .into()),
            },
            Err(_) => Err(anyhow!("Incremental query service stopped")),
        };

        match (cdc_result, service_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(err), Ok(())) | (Ok(()), Err(err)) => Err(err),
            (Err(cdc_err), Err(service_err)) => Err(cdc_err.context(format!(
                "Incremental query service shutdown failed: {service_err:#}"
            ))),
        }
    }
}

struct RegisteredQuery {
    inbox: mpsc::Sender<Arc<Batch>>,
    sender: mpsc::Sender<Result<IncrementalQueryDelta>>,
    control: Arc<Control>,
    basis: TxKey,
    routed: Position,
    // A caller wants to unregister the query. The dispatcher tells the worker to stop.
    // Once finished, the dispatcher sends the cleanup result to the original caller via this channel.
    unregister: Option<oneshot::Sender<ServiceResult<()>>>,
}

struct IncrementalQueryServiceInner {
    next_query_id: u64,
    storage_root: PathBuf,
    queries: HashMap<IncrementalQueryHandle, RegisteredQuery>,
    runtime: Handle,
    cancel: CancellationToken,
    options: IncrementalQueryOptions,
    steps: Arc<Semaphore>,
    completed: mpsc::UnboundedSender<Completion>,
    completions: mpsc::UnboundedReceiver<Completion>,
}

impl IncrementalQueryServiceInner {
    fn new(
        storage_root: PathBuf,
        runtime: Handle,
        cancel: CancellationToken,
        options: IncrementalQueryOptions,
    ) -> Self {
        let (completed, completions) = mpsc::unbounded_channel();
        Self {
            next_query_id: 1,
            storage_root,
            queries: HashMap::new(),
            runtime,
            cancel,
            options,
            steps: Arc::new(Semaphore::new(options.max_concurrent_steps.get())),
            completed,
            completions,
        }
    }

    fn retire(&mut self, completion: Completion) -> Result<()> {
        if let Some(query) = self.queries.remove(&completion.handle) {
            if let Some(response) = query.unregister {
                if let Err(cleanup) = response.send(completion.cleanup) {
                    return cleanup;
                }
                return Ok(());
            }
        }
        completion.cleanup
    }

    async fn run(mut self, mut receiver: mpsc::UnboundedReceiver<IncrementalCommand>) {
        let mut shutdown = None;
        loop {
            tokio::select! {
                completion = self.completions.recv(), if !self.queries.is_empty() => {
                    if let Some(completion) = completion {
                        if let Err(error) = self.retire(completion) { warn!("{error:#}"); }
                    }
                }
                command = receiver.recv() => match command {
                    Some(IncrementalCommand::Register { plan, tx_key, wal_cursor, initial_triples, response }) => {
                        let result = if self.cancel.is_cancelled() {
                            Err(anyhow!("Incremental query service stopped"))
                        } else {
                            self.register(*plan, tx_key, wal_cursor, initial_triples).await
                        };
                        if let Err(Ok(subscription)) = response.send(result) {
                            if let Some(query) = self.queries.get(&subscription.handle) {
                                query.control.terminate(None);
                            }
                        }
                    }
                    Some(IncrementalCommand::Unregister { handle, response }) => {
                        match self.queries.get_mut(&handle) {
                            Some(query) if !query.sender.is_closed() && !query.control.stop.is_cancelled() => {
                                query.control.terminate(None);
                                query.unregister = Some(response);
                            }
                            _ => { let _ = response.send(Err(anyhow!("Unknown incremental query handle: {handle:?}"))); }
                        }
                    }
                    Some(IncrementalCommand::ApplyTriples { batch, response }) => {
                        self.apply_triples(batch);
                        let _ = response.send(Ok(()));
                    }
                    Some(IncrementalCommand::Shutdown { response }) => { shutdown = Some(response); break; }
                    None => break,
                }
            }
        }
        receiver.close();
        self.cancel.cancel();
        for query in self.queries.values() {
            query.control.terminate(None);
        }
        let mut cleanup = Ok(());
        while !self.queries.is_empty() {
            if let Some(completion) = self.completions.recv().await {
                if let Err(error) = self.retire(completion) {
                    warn!("{error:#}");
                    cleanup = match cleanup {
                        Ok(()) => Err(error),
                        Err(previous) => Err(error.context(format!(
                            "An additional incremental query cleanup failed: {previous:#}"
                        ))),
                    };
                }
            }
        }
        if let Some(response) = shutdown {
            let _ = response.send(cleanup);
        }
    }

    fn allocate_query_id(&mut self) -> IncrementalQueryHandle {
        let id = self.next_query_id;
        self.next_query_id += 1;
        id
    }

    fn query_storage_path(&self, id: IncrementalQueryHandle) -> PathBuf {
        self.storage_root.join(format!("query-{id}"))
    }

    fn install<C: Circuit>(
        &mut self,
        handle: IncrementalQueryHandle,
        circuit: C,
        tx_key: TxKey,
        wal_cursor: CdcCursor,
        priming_rows: worker::Rows,
    ) -> IncrementalQuerySubscription {
        let (sender, receiver) = mpsc::channel(SUBSCRIPTION_CAPACITY);
        if !priming_rows.is_empty() {
            sender
                .try_send(Ok(IncrementalQueryDelta {
                    tx_key,
                    rows: priming_rows,
                }))
                .expect("new subscription queue has room for priming");
        }
        let (terminal, termination) = oneshot::channel();
        let control = Arc::new(Control::new(self.cancel.child_token(), terminal));
        let (inbox_sender, inbox) = mpsc::channel(self.options.inbox_capacity.get());
        let position = Position {
            tx_key,
            wal_seq: wal_cursor.last_seq,
        };
        let worker = Worker {
            circuit,
            storage_path: self.query_storage_path(handle),
            inbox,
            sender: sender.clone(),
            control: control.clone(),
            steps: self.steps.clone(),
        };
        let completed = self.completed.clone();
        self.runtime.spawn(async move {
            let cleanup = worker.run().await;
            let _ = completed.send(Completion { handle, cleanup });
        });
        self.queries.insert(
            handle,
            RegisteredQuery {
                inbox: inbox_sender,
                sender,
                control,
                basis: tx_key,
                routed: position,
                unregister: None,
            },
        );
        IncrementalQuerySubscription {
            handle,
            tx_key,
            deltas: SubscriptionDeltas::new(receiver, termination),
        }
    }

    async fn prepare_query<C: Circuit>(
        &self,
        handle: IncrementalQueryHandle,
        prepare: impl FnOnce(&std::path::Path) -> Result<(C, worker::Rows)> + Send + 'static,
    ) -> Result<(C, worker::Rows)> {
        let storage_path = self.query_storage_path(handle);
        let result = self
            .runtime
            .spawn_blocking(move || prepare(&storage_path))
            .await
            .context("Incremental query registration task panicked")
            .and_then(|result| result);
        let error = match result {
            Ok(prepared) => return Ok(prepared),
            Err(error) => error,
        };
        // The preparation task has finished unwinding before storage can be removed.
        let storage_path = self.query_storage_path(handle);
        let cleanup = self
            .runtime
            .spawn_blocking(move || match std::fs::remove_dir_all(storage_path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error),
            })
            .await
            .context("Incremental query registration cleanup task panicked")
            .and_then(|result| result.map_err(Error::from));
        Err(match cleanup {
            Ok(()) => error,
            Err(cleanup) => error.context(format!(
                "Incremental query registration cleanup failed: {cleanup:#}"
            )),
        })
    }

    async fn register(
        &mut self,
        plan: IncrementalQueryPlan,
        tx_key: TxKey,
        wal_cursor: CdcCursor,
        initial_triples: Vec<Tup2<EncodedTriple, ZWeight>>,
    ) -> ServiceResult<IncrementalQuerySubscription> {
        let handle = self.allocate_query_id();
        // Registration remains serialized, but DBSP construction and destruction stay off async threads.
        let (circuit, rows) = self
            .prepare_query(handle, move |storage_path| {
                let mut circuit = QueryCircuit::build(plan, storage_path)?;
                let rows = circuit.apply(initial_triples)?;
                Ok((circuit, rows))
            })
            .await?;
        Ok(self.install(handle, circuit, tx_key, wal_cursor, rows))
    }

    fn apply_triples(&mut self, batch: Arc<Batch>) {
        for query in self.queries.values_mut() {
            if query.control.stop.is_cancelled() {
                continue;
            }
            if query.sender.is_closed() {
                query.control.terminate(None);
                continue;
            }
            if batch.tx_key.tx_id <= query.basis.tx_id {
                continue;
            }
            match query.inbox.try_send(batch.clone()) {
                Ok(()) => {
                    query.routed = Position {
                        tx_key: batch.tx_key,
                        wal_seq: batch.wal_seq,
                    }
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    query
                        .control
                        .terminate(Some(Termination::Lagged(SubscriptionLagged {
                            tx_key: batch.tx_key,
                            capacity: self.options.inbox_capacity.get(),
                        })));
                }
                Err(mpsc::error::TrySendError::Closed(_)) => query.control.terminate(None),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use chrono::Utc;
    use dbsp::utils::Tup2;
    use edn::query::ToVariable;
    use slatedb::object_store::memory::InMemory;
    use triplox_client::transaction::TxKey;

    use super::*;
    use crate::codec::Encode;
    use crate::inc_query::test_support::{parse_query, test_schema, AGE_ATTR_ID, NAME_ATTR_ID};
    use crate::inc_query::{IncrementalQueryPlan, PatternPlan, PatternSlot, RelPlan, RelPlanKind};
    use crate::query::{FindPlan, Projection};

    #[track_caller]
    fn expect_delta(result: Result<IncrementalQueryDelta>) -> IncrementalQueryDelta {
        result.expect("expected subscription delta")
    }

    fn single_pattern_plan() -> IncrementalQueryPlan {
        let pattern = PatternPlan {
            attribute: 10,
            entity: PatternSlot::Variable("?e".to_var()),
            value: PatternSlot::Variable("?name".to_var()),
            pattern_vars: vec!["?e".to_var(), "?name".to_var()],
        };
        IncrementalQueryPlan {
            find_plan: FindPlan {
                group_key_indices: vec![1],
                projections: vec![Projection::GroupVar(0)],
                has_aggregates: false,
            },
            where_plan: RelPlan {
                incoming_vars: None,
                output_vars: pattern.pattern_vars.clone(),
                kind: RelPlanKind::Pattern(pattern),
            },
        }
    }

    fn aggregate_plan(query: &str) -> IncrementalQueryPlan {
        plan_query(&parse_query(query), &test_schema()).unwrap()
    }

    fn name_triple(entity: i64, name: &str) -> Tup2<EncodedTriple, ZWeight> {
        Tup2(
            EncodedTriple {
                entity: DataType::Long(entity).encode(),
                attribute: NAME_ATTR_ID,
                value: DataType::String(name.to_string()).encode(),
            },
            1,
        )
    }

    fn age_triple(entity: i64, age: i64) -> Tup2<EncodedTriple, ZWeight> {
        Tup2(
            EncodedTriple {
                entity: DataType::Long(entity).encode(),
                attribute: AGE_ATTR_ID,
                value: DataType::Long(age).encode(),
            },
            1,
        )
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

    fn service_at(dir: &Path) -> IncrementalQueryService {
        IncrementalQueryService::new(
            dir.to_path_buf(),
            CancellationToken::new(),
            "/test_circuit_runtime".to_owned(),
            Arc::new(InMemory::new()),
        )
    }

    async fn register(
        service: &IncrementalQueryService,
        plan: IncrementalQueryPlan,
        triples: Vec<Tup2<EncodedTriple, ZWeight>>,
    ) -> IncrementalQuerySubscription {
        service
            .register_prepared_query(plan, test_tx_key_with_tx_id(1), test_cursor(), triples)
            .await
            .unwrap()
    }

    async fn delta(
        subscription: &mut IncrementalQuerySubscription,
    ) -> Result<IncrementalQueryDelta> {
        tokio::time::timeout(Duration::from_secs(10), subscription.deltas.recv())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn unregister_removes_query_storage() {
        let dir = tempfile::tempdir().unwrap();
        let service = service_at(dir.path());
        let subscription = register(
            &service,
            single_pattern_plan(),
            vec![name_triple(42, "Alice")],
        )
        .await;
        assert!(path_has_entries(&dir.path().join("query-1")));
        service.unregister(subscription.handle).await.unwrap();
        assert!(!dir.path().join("query-1").exists());
        assert!(service.unregister(subscription.handle).await.is_err());
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn priming_precedes_live_deltas_and_empty_priming_is_omitted() {
        let dir = tempfile::tempdir().unwrap();
        let service = service_at(dir.path());
        let mut primed = register(
            &service,
            single_pattern_plan(),
            vec![name_triple(42, "Alice")],
        )
        .await;
        let mut empty = register(&service, single_pattern_plan(), vec![]).await;
        assert!(empty.deltas.try_recv().is_err());
        let tx_key = test_tx_key_with_tx_id(2);
        service
            .apply_triples(tx_key, 2, vec![name_triple(43, "Bob")])
            .await
            .unwrap();
        assert_eq!(
            delta(&mut primed).await.unwrap().rows,
            vec![(vec![DataType::String("Alice".into())], 1)]
        );
        assert_eq!(delta(&mut primed).await.unwrap().tx_key, tx_key);
        assert_eq!(delta(&mut empty).await.unwrap().tx_key, tx_key);
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn invalid_aggregate_priming_rejects_registration_and_cleans_storage() {
        let dir = tempfile::tempdir().unwrap();
        let service = service_at(dir.path());
        let error = service
            .register_prepared_query(
                aggregate_plan("[:find (sum ?name) :where [?e :name ?name]]"),
                test_tx_key_with_tx_id(1),
                test_cursor(),
                vec![name_triple(42, "Alice")],
            )
            .await
            .unwrap_err();
        assert!(error.downcast_ref::<circuit::AggregateError>().is_some());
        assert!(!dir.path().join("query-1").exists());
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn live_aggregate_error_removes_only_the_affected_subscription() {
        let dir = tempfile::tempdir().unwrap();
        let service = service_at(dir.path());
        let mut aggregate = register(
            &service,
            aggregate_plan("[:find (sum ?value) :where (or [?e :age ?value] [?e :name ?value])]"),
            vec![],
        )
        .await;
        let mut names = register(&service, single_pattern_plan(), vec![]).await;
        delta(&mut aggregate).await.unwrap();
        service
            .apply_triples(test_tx_key_with_tx_id(2), 2, vec![age_triple(42, 10)])
            .await
            .unwrap();
        service
            .apply_triples(test_tx_key_with_tx_id(3), 3, vec![name_triple(43, "Alice")])
            .await
            .unwrap();
        assert!(delta(&mut aggregate)
            .await
            .unwrap()
            .rows
            .contains(&(vec![DataType::Long(10)], 1)));
        assert!(delta(&mut aggregate)
            .await
            .unwrap_err()
            .downcast_ref::<circuit::AggregateError>()
            .is_some());
        assert!(aggregate.deltas.recv().await.is_none());
        delta(&mut names).await.unwrap();
        let tx_key = test_tx_key_with_tx_id(4);
        service
            .apply_triples(tx_key, 4, vec![name_triple(44, "Bob")])
            .await
            .unwrap();
        assert_eq!(delta(&mut names).await.unwrap().tx_key, tx_key);
        service.shutdown().await.unwrap();
        assert!(!dir.path().join("query-1").exists());
    }

    #[tokio::test]
    async fn history_before_registration_basis_does_not_fill_inbox() {
        let dir = tempfile::tempdir().unwrap();
        let service = IncrementalQueryService::new_with_options(
            dir.path().to_path_buf(),
            CancellationToken::new(),
            "/test_history".into(),
            Arc::new(InMemory::new()),
            IncrementalQueryOptions {
                inbox_capacity: NonZeroUsize::MIN,
                max_concurrent_steps: NonZeroUsize::MIN,
                ..IncrementalQueryOptions::default()
            },
        );
        let basis = test_tx_key_with_tx_id(1000);
        let mut query = service
            .register_prepared_query(single_pattern_plan(), basis, test_cursor(), vec![])
            .await
            .unwrap();
        for seq in 1..=1000 {
            service
                .apply_triples(
                    test_tx_key_with_tx_id(seq),
                    seq as u64,
                    vec![name_triple(seq, "old")],
                )
                .await
                .unwrap();
        }
        assert!(query.deltas.try_recv().is_err());
        let tx_key = test_tx_key_with_tx_id(1001);
        service
            .apply_triples(tx_key, 1001, vec![name_triple(1001, "new")])
            .await
            .unwrap();
        assert_eq!(delta(&mut query).await.unwrap().tx_key, tx_key);
        service.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn dropping_last_service_handle_cleans_up_queries() {
        let dir = tempfile::tempdir().unwrap();
        let service = service_at(dir.path());
        let mut query = register(&service, single_pattern_plan(), vec![]).await;
        drop(service);
        assert!(
            tokio::time::timeout(Duration::from_secs(5), query.deltas.recv())
                .await
                .unwrap()
                .is_none()
        );
        assert!(!dir.path().join("query-1").exists());
    }

    #[tokio::test]
    async fn abandoned_registration_response_retires_query() {
        let dir = tempfile::tempdir().unwrap();
        let service = service_at(dir.path());
        let (response, result) = oneshot::channel();
        drop(result);
        service
            .commands
            .send(IncrementalCommand::Register {
                plan: Box::new(single_pattern_plan()),
                tx_key: test_tx_key_with_tx_id(1),
                wal_cursor: test_cursor(),
                initial_triples: vec![],
                response,
            })
            .unwrap();
        service
            .apply_triples(test_tx_key_with_tx_id(2), 2, vec![])
            .await
            .unwrap();
        service.shutdown().await.unwrap();
        assert!(!dir.path().join("query-1").exists());
    }

    #[tokio::test]
    async fn await_cdc_task_returns_inner_loop_error() {
        let dir = tempfile::tempdir().unwrap();
        let service = IncrementalQueryService::new(
            dir.path().to_path_buf(),
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
            CancellationToken::new(),
            "/test_incremental_shutdown_cdc_error".to_string(),
            Arc::new(InMemory::new()),
        );
        let _subscription = service
            .register_prepared_query(
                single_pattern_plan(),
                test_tx_key_with_tx_id(1),
                test_cursor(),
                vec![name_triple(42, "Alice")],
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
            expect_delta(first_subscription.deltas.recv().await.unwrap()),
            IncrementalQueryDelta {
                tx_key: apply_tx_key,
                rows: vec![(vec![DataType::String("Bob".to_string())], 1)],
            }
        );
        assert_eq!(
            expect_delta(second_subscription.deltas.recv().await.unwrap()),
            IncrementalQueryDelta {
                tx_key: apply_tx_key,
                rows: vec![(vec![DataType::String("Bob".to_string())], 1)],
            }
        );

        service.shutdown().await.unwrap();
    }
}

#[cfg(test)]
mod runtime_tests;
