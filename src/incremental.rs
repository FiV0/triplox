//! Writer-node incremental query service.

use std::collections::{HashMap, HashSet};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex as StdMutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::partition::tx_eid_from_tx_id;
use anyhow::{anyhow, Context, Error, Result};
use dbsp::{utils::Tup2, ZWeight};
use slatedb::object_store::ObjectStore;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot, watch, Mutex, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use triplox_client::transaction::TxKey;

use crate::inc_query::{plan_query, IncrementalQueryPlan};
use crate::incremental::cdc::{scan_current_triples, spawn_cdc_loop};
use crate::incremental::circuit::QueryCircuit;
use crate::incremental::worker::{
    spawn_query_worker, QueryWorker, RetireOutcome, RetireReason, WorkerMessage,
};
use crate::indexer::Indexer;
use crate::ops::DataType;
use crate::slate::cdc::CdcCursor;
use edn::query::ParsedQuery;

pub(crate) mod cdc;
pub(crate) mod circuit;
pub(crate) mod worker;

const SUBSCRIPTION_CAPACITY: usize = 128;
// How many transactions a query may fall behind before its subscription is terminated.
// Batches are shared, so depth costs one `Arc` per queued transaction per query.
const QUERY_INBOX_CAPACITY: usize = 256;
// How many circuits may step at once. Kept well under the runtime's blocking pool.
const MAX_CONCURRENT_STEPS: usize = 4;
// How long shutdown waits for workers, and terminal errors wait for delivery.
const RETIRE_TIMEOUT: Duration = Duration::from_secs(10);
const RETIRE_POLL_INTERVAL: Duration = Duration::from_millis(10);
const DROP_TIMEOUT: Duration = Duration::from_secs(1);

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
    pub deltas: mpsc::Receiver<Result<IncrementalQueryDelta>>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IncrementalQueryDelta {
    pub tx_key: TxKey,
    pub rows: Vec<(Vec<DataType>, isize)>,
}

type ServiceResult<T> = Result<T>;

struct RegisterRequest {
    plan: IncrementalQueryPlan,
    tx_key: TxKey,
    wal_cursor: CdcCursor,
    initial_triples: Vec<Tup2<EncodedTriple, ZWeight>>,
}

enum IncrementalCommand {
    // Boxed because registration is rare while `ApplyTriples` crosses this channel once per
    // WAL transaction and would otherwise pay for the registration payload's size.
    Register {
        request: Box<RegisterRequest>,
        response: oneshot::Sender<ServiceResult<IncrementalQuerySubscription>>,
    },
    Unregister {
        handle: IncrementalQueryHandle,
        response: oneshot::Sender<ServiceResult<()>>,
    },
    ApplyTriples {
        tx_key: TxKey,
        wal_seq: u64,
        // Shared so fan-out to every registered query is a refcount bump, not a deep clone.
        triples: Arc<[Tup2<EncodedTriple, ZWeight>]>,
        response: oneshot::Sender<ServiceResult<()>>,
    },
    // Barrier: hands back one ack per live query, resolved once that worker has drained
    // everything queued ahead of it.
    Flush {
        response: oneshot::Sender<ServiceResult<Vec<oneshot::Receiver<()>>>>,
    },
    #[cfg(test)]
    InjectPanic {
        handle: IncrementalQueryHandle,
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
        config: IncrementalServiceConfig,
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
                IncrementalQueryServiceInner::new(storage_root, config, inner_cancel).run(receiver)
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
                request: Box::new(RegisterRequest {
                    plan,
                    tx_key,
                    wal_cursor,
                    initial_triples,
                }),
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
        result.await.context("Incremental query service stopped")?
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
                // Convert on the caller's task so the service thread only moves refcounts.
                triples: Arc::from(triples),
                response,
            })
            .map_err(|_| anyhow!("Incremental query service stopped"))?;
        result.await.context("Incremental query service stopped")?
    }

    /// Resolves once every query registered right now has stepped and delivered everything
    /// already routed to it. Routing is ordered, so a preceding `apply_triples` is covered.
    pub(crate) async fn flush(&self) -> Result<()> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(IncrementalCommand::Flush { response })
            .map_err(|_| anyhow!("Incremental query service stopped"))?;
        let acks = result
            .await
            .context("Incremental query service stopped")??;
        for ack in acks {
            // A query that retired before reaching the barrier just drops its ack.
            let _ = ack.await;
        }
        Ok(())
    }

    /// Makes one worker panic, so its isolation from the service can be tested for real.
    #[cfg(test)]
    pub(crate) async fn inject_panic(&self, handle: IncrementalQueryHandle) -> Result<()> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(IncrementalCommand::InjectPanic { handle, response })
            .map_err(|_| anyhow!("Incremental query service stopped"))?;
        result.await.context("Incremental query service stopped")?
    }

    pub(crate) async fn shutdown(&self) -> Result<()> {
        self.cancel.cancel();
        let cdc_result = self.await_cdc_task().await;
        let (response, result) = oneshot::channel();
        let service_result = match self
            .commands
            .send(IncrementalCommand::Shutdown { response })
        {
            Ok(()) => match result.await {
                Ok(result) => result,
                Err(err) => Err(err).context("Incremental query service stopped"),
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
    inbox: mpsc::Sender<WorkerMessage>,
    routed: u64,
    delivered: watch::Receiver<u64>,
    terminate: Option<oneshot::Sender<RetireReason>>,
    // Weak so the router can spot a dropped receiver without keeping the subscription open:
    // a strong clone here would delay end-of-stream until the next command reaped it.
    subscriber: mpsc::WeakSender<Result<IncrementalQueryDelta>>,
    tx_key: TxKey,
    wal_cursor: CdcCursor,
}

struct PendingUnregister {
    response: oneshot::Sender<ServiceResult<()>>,
    known: bool,
}

/// Sizing for the service. The defaults are the production values; tests shrink them to
/// force lag and contention deterministically.
#[derive(Clone, Copy, Debug)]
pub(crate) struct IncrementalServiceConfig {
    pub subscription_capacity: usize,
    pub inbox_capacity: usize,
    pub max_concurrent_steps: usize,
    pub retire_timeout: Duration,
}

impl Default for IncrementalServiceConfig {
    fn default() -> Self {
        Self {
            subscription_capacity: SUBSCRIPTION_CAPACITY,
            inbox_capacity: QUERY_INBOX_CAPACITY,
            max_concurrent_steps: MAX_CONCURRENT_STEPS,
            retire_timeout: RETIRE_TIMEOUT,
        }
    }
}

// Routes transactions to per-query workers and owns the registry. Nothing here waits on a
// circuit step or a subscriber, so one slow query cannot hold up the others.
struct IncrementalQueryServiceInner {
    next_query_id: u64,
    storage_root: PathBuf,
    queries: HashMap<IncrementalQueryHandle, RegisteredQuery>,
    // Retired here but not yet acked by their worker, so their storage may still exist.
    retiring: HashSet<IncrementalQueryHandle>,
    pending_unregister: HashMap<IncrementalQueryHandle, PendingUnregister>,
    // Owned rather than borrowed from the node, so circuit work cannot starve request
    // serving and workers can still retire while the node's runtime is going away.
    // `None` only while dropping.
    runtime: Option<Runtime>,
    steps: Arc<Semaphore>,
    config: IncrementalServiceConfig,
    // Deliberately not the command channel: workers holding a command sender would keep it
    // alive for ever, and `run` would never see it disconnect.
    retirements: std_mpsc::Sender<RetireOutcome>,
    retired: std_mpsc::Receiver<RetireOutcome>,
    cancel: CancellationToken,
}

impl IncrementalQueryServiceInner {
    fn new(
        storage_root: PathBuf,
        config: IncrementalServiceConfig,
        cancel: CancellationToken,
    ) -> Self {
        // Worker tasks are almost pure `await` - the circuit steps go to the blocking pool -
        // so a couple of async threads is plenty.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(config.max_concurrent_steps + 4)
            .thread_name("triplox-iq-worker")
            .enable_all()
            .build()
            .expect("incremental query runtime should start");
        let (retirements, retired) = std_mpsc::channel();

        Self {
            next_query_id: 1,
            storage_root,
            queries: HashMap::new(),
            retiring: HashSet::new(),
            pending_unregister: HashMap::new(),
            runtime: Some(runtime),
            steps: Arc::new(Semaphore::new(config.max_concurrent_steps)),
            config,
            retirements,
            retired,
            cancel,
        }
    }

    fn allocate_query_id(&mut self) -> IncrementalQueryHandle {
        let id = self.next_query_id;
        self.next_query_id += 1;
        id
    }

    fn query_storage_path(&self, id: IncrementalQueryHandle) -> PathBuf {
        self.storage_root.join(format!("query-{}", id))
    }

    // Drops a query from the registry and tells its worker to stop. The worker still owns the
    // circuit, so the storage directory only goes once it acks.
    fn retire(&mut self, id: IncrementalQueryHandle, reason: RetireReason) -> bool {
        let Some(query) = self.queries.remove(&id) else {
            return false;
        };
        if let Some(terminate) = query.terminate {
            let _ = terminate.send(reason);
        }
        self.retiring.insert(id);
        true
    }

    fn apply_retirement(&mut self, outcome: RetireOutcome) -> ServiceResult<()> {
        self.retiring.remove(&outcome.id);
        self.queries.remove(&outcome.id);
        if let Some(pending) = self.pending_unregister.remove(&outcome.id) {
            let result = outcome.storage.and_then(|()| {
                if pending.known {
                    Ok(())
                } else {
                    Err(anyhow!(
                        "Unknown incremental query handle: {:?}",
                        outcome.id
                    ))
                }
            });
            if let Err(Err(error)) = pending.response.send(result) {
                warn!("{}", error);
            }
            return Ok(());
        }
        outcome.storage
    }

    // Applies whatever retirements have already arrived, and retires queries whose subscriber
    // went away. Never blocks.
    fn reap(&mut self) {
        while let Ok(outcome) = self.retired.try_recv() {
            if let Err(error) = self.apply_retirement(outcome) {
                warn!("{}", error);
            }
        }

        // `upgrade` fails once the worker has dropped its own sender, which means it is
        // already retiring and its outcome is on the way.
        let gone = self
            .queries
            .iter()
            .filter_map(|(id, query)| {
                let closed = query
                    .subscriber
                    .upgrade()
                    .is_some_and(|subscriber| subscriber.is_closed());
                closed.then_some(*id)
            })
            .collect::<Vec<_>>();
        for id in gone {
            self.retire(id, RetireReason::SubscriberGone);
        }
    }

    // Shutdown waits for teardown; ordinary unregister leaves the router running.
    fn await_retirements(&mut self) -> ServiceResult<()> {
        let deadline = Instant::now() + self.config.retire_timeout;
        let mut failure: Option<Error> = None;

        while !self.retiring.is_empty() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match self.retired.recv_timeout(remaining) {
                Ok(outcome) => {
                    if let Err(error) = self.apply_retirement(outcome) {
                        failure.get_or_insert(error);
                    }
                }
                // The service holds a sender, so this only fires on timeout.
                Err(_) => break,
            }
        }

        // A worker wedged inside a circuit step cannot be interrupted - DBSP exposes no
        // external kill - so leave its storage rather than unlinking under a live circuit.
        for id in &self.retiring {
            warn!(
                "Incremental query {} did not retire within {:?}; leaving {} behind",
                id,
                self.config.retire_timeout,
                self.query_storage_path(*id).display()
            );
        }
        for (id, pending) in self.pending_unregister.drain() {
            let _ = pending.response.send(Err(anyhow!(
                "Incremental query {id} did not retire before shutdown timed out"
            )));
        }

        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn run(mut self, receiver: std_mpsc::Receiver<IncrementalCommand>) {
        loop {
            self.reap();
            // Retirements use a separate channel so workers cannot keep commands alive.
            let command = match receiver.recv_timeout(RETIRE_POLL_INTERVAL) {
                Ok(command) => command,
                Err(std_mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
            };
            self.reap();
            match command {
                IncrementalCommand::Register { request, response } => {
                    let RegisterRequest {
                        plan,
                        tx_key,
                        wal_cursor,
                        initial_triples,
                    } = *request;
                    let _ = response.send(self.register(plan, tx_key, wal_cursor, initial_triples));
                }
                IncrementalCommand::Unregister { handle, response } => {
                    self.unregister(handle, response);
                }
                IncrementalCommand::ApplyTriples {
                    tx_key,
                    wal_seq,
                    triples,
                    response,
                } => {
                    let _ = response.send(self.apply_triples(tx_key, wal_seq, triples));
                }
                IncrementalCommand::Flush { response } => {
                    let _ = response.send(self.flush());
                }
                #[cfg(test)]
                IncrementalCommand::InjectPanic { handle, response } => {
                    let result = match self.queries.get(&handle) {
                        Some(query) => query
                            .inbox
                            .try_send(WorkerMessage::Panic)
                            .map_err(|err| anyhow!("Failed to inject worker panic: {}", err)),
                        None => Err(anyhow!("Unknown incremental query handle: {:?}", handle)),
                    };
                    let _ = response.send(result);
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
        let handle = self.allocate_query_id();
        let storage_path = self.query_storage_path(handle);
        let mut circuit = match QueryCircuit::build(plan, &storage_path) {
            Ok(circuit) => circuit,
            Err(err) => return Err(self.cleanup_failed_registration(handle, err)),
        };
        // Priming is the circuit's first batch, so its delta is the whole query result.
        let priming_rows = match circuit.apply(initial_triples) {
            Ok(rows) => rows,
            Err(err) => {
                drop(circuit);
                return Err(self.cleanup_failed_registration(handle, err));
            }
        };

        let (subscriber, receiver) = mpsc::channel(self.config.subscription_capacity);
        if !priming_rows.is_empty() {
            if let Err(err) = subscriber.try_send(Ok(IncrementalQueryDelta {
                tx_key,
                rows: priming_rows,
            })) {
                drop(circuit);
                let err = anyhow!("Failed to enqueue priming result set: {}", err);
                return Err(self.cleanup_failed_registration(handle, err));
            }
        }

        let (inbox, inbox_receiver) = mpsc::channel(self.config.inbox_capacity);
        let (delivered, delivery_progress) = watch::channel(0);
        let (terminate, terminate_receiver) = oneshot::channel();
        spawn_query_worker(
            self.runtime
                .as_ref()
                .expect("the runtime is only taken while dropping")
                .handle(),
            QueryWorker {
                id: handle,
                circuit: Some(circuit),
                storage_path,
                inbox: inbox_receiver,
                delivered,
                // Only the worker and supervisor keep the subscription open.
                subscriber: subscriber.clone(),
                terminate: terminate_receiver,
                retirements: self.retirements.clone(),
                steps: self.steps.clone(),
                cancel: self.cancel.clone(),
                retire_timeout: self.config.retire_timeout,
            },
        );

        self.queries.insert(
            handle,
            RegisteredQuery {
                inbox,
                routed: 0,
                delivered: delivery_progress,
                terminate: Some(terminate),
                subscriber: subscriber.downgrade(),
                tx_key,
                wal_cursor,
            },
        );

        Ok(IncrementalQuerySubscription {
            handle,
            tx_key,
            deltas: receiver,
        })
    }

    fn unregister(
        &mut self,
        handle: IncrementalQueryHandle,
        response: oneshot::Sender<ServiceResult<()>>,
    ) {
        if self.pending_unregister.contains_key(&handle)
            || (!self.queries.contains_key(&handle) && !self.retiring.contains(&handle))
        {
            let _ = response.send(Err(anyhow!(
                "Unknown incremental query handle: {:?}",
                handle
            )));
            return;
        }
        let known = self.queries.get(&handle).is_some_and(|query| {
            query
                .subscriber
                .upgrade()
                .is_some_and(|subscriber| !subscriber.is_closed())
        });
        self.retire(handle, RetireReason::Unregistered);
        self.pending_unregister
            .insert(handle, PendingUnregister { response, known });
    }

    fn apply_triples(
        &mut self,
        tx_key: TxKey,
        wal_seq: u64,
        triples: Arc<[Tup2<EncodedTriple, ZWeight>]>,
    ) -> ServiceResult<()> {
        let mut lagged = Vec::new();

        for (id, query) in &mut self.queries {
            // The CDC loop replays the WAL from the start, so skipping here rather than in the
            // worker keeps replayed history from filling inboxes and terminating fresh queries.
            if tx_key.tx_id <= query.tx_key.tx_id {
                query.wal_cursor.last_seq = wal_seq;
                continue;
            }
            let work = WorkerMessage::Apply {
                tx_key,
                triples: triples.clone(),
            };
            match query.inbox.try_send(work) {
                Ok(()) => {
                    query.routed += 1;
                    query.wal_cursor.last_seq = wal_seq;
                }
                // Full means the query is behind; closed means its worker already stopped.
                Err(_) => lagged.push(*id),
            }
        }

        let capacity = self.config.inbox_capacity;
        for id in lagged {
            self.retire(id, RetireReason::Lagged { capacity });
        }
        Ok(())
    }

    // Hands back one ack per live query, resolved when that worker has drained everything
    // queued ahead of the barrier.
    fn flush(&mut self) -> ServiceResult<Vec<oneshot::Receiver<()>>> {
        let mut acks = Vec::new();
        for query in self.queries.values() {
            let (ack, wait) = oneshot::channel();
            let target = query.routed;
            let mut delivered = query.delivered.clone();
            // A barrier must not consume inbox capacity or terminate a busy query.
            self.runtime.as_ref().unwrap().spawn(async move {
                while *delivered.borrow_and_update() < target {
                    if delivered.changed().await.is_err() {
                        return;
                    }
                }
                let _ = ack.send(());
            });
            acks.push(wait);
        }
        Ok(acks)
    }

    fn cleanup_failed_registration(&self, id: IncrementalQueryHandle, error: Error) -> Error {
        match remove_query_storage(&self.query_storage_path(id)) {
            Ok(()) => error,
            Err(cleanup_error) => error.context(format!(
                "Incremental query registration cleanup failed: {cleanup_error:#}"
            )),
        }
    }

    fn remove_all_queries(&mut self) -> ServiceResult<()> {
        self.cancel.cancel();
        for id in self.queries.keys().copied().collect::<Vec<_>>() {
            self.retire(id, RetireReason::Shutdown);
        }
        self.await_retirements()
    }
}

impl Drop for IncrementalQueryServiceInner {
    fn drop(&mut self) {
        self.config.retire_timeout = self.config.retire_timeout.min(DROP_TIMEOUT);
        let deadline = Instant::now() + self.config.retire_timeout;
        if let Err(error) = self.remove_all_queries() {
            warn!("{}", error);
        }
        // Bounded, because a worker wedged in a circuit step would otherwise block for ever.
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(deadline.saturating_duration_since(Instant::now()));
        }
    }
}

fn remove_query_storage(path: &Path) -> Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| {
            format!(
                "Failed to remove incremental query storage at {}",
                path.display()
            )
        }),
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
    use crate::inc_query::test_support::{parse_query, test_schema, AGE_ATTR_ID, NAME_ATTR_ID};
    use crate::inc_query::{IncrementalQueryPlan, PatternPlan, PatternSlot, RelPlan, RelPlanKind};
    use crate::query::{FindPlan, Projection};

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Runtime::new().unwrap()
    }

    #[track_caller]
    fn expect_delta(result: Result<IncrementalQueryDelta>) -> IncrementalQueryDelta {
        result.expect("expected subscription delta")
    }

    #[track_caller]
    fn expect_error(result: Result<IncrementalQueryDelta>) -> anyhow::Error {
        result.expect_err("expected subscription error")
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

    fn test_service(dir: &Path, config: IncrementalServiceConfig) -> IncrementalQueryService {
        IncrementalQueryService::new(
            dir.to_path_buf(),
            config,
            CancellationToken::new(),
            format!("/test_incremental_{}", crate::util::random_string(8)),
            Arc::new(InMemory::new()),
        )
    }

    async fn register(
        service: &IncrementalQueryService,
        plan: IncrementalQueryPlan,
        tx_key: TxKey,
        initial_triples: Vec<Tup2<EncodedTriple, ZWeight>>,
    ) -> Result<IncrementalQuerySubscription> {
        service
            .register_prepared_query(plan, tx_key, test_cursor(), initial_triples)
            .await
    }

    #[tokio::test]
    async fn unregister_removes_query_storage() {
        let dir = tempfile::tempdir().unwrap();
        let service = test_service(dir.path(), IncrementalServiceConfig::default());
        let subscription = register(
            &service,
            single_pattern_plan(),
            test_tx_key_with_tx_id(1),
            vec![name_triple(42, "Alice")],
        )
        .await
        .unwrap();
        let storage_path = dir.path().join("query-1");

        assert!(path_has_entries(&storage_path));

        service.unregister(subscription.handle).await.unwrap();

        assert!(!storage_path.exists());
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn dropped_receiver_cleanup_removes_query_storage() {
        let dir = tempfile::tempdir().unwrap();
        let service = test_service(dir.path(), IncrementalServiceConfig::default());
        let subscription = register(
            &service,
            single_pattern_plan(),
            test_tx_key_with_tx_id(1),
            vec![name_triple(42, "Alice")],
        )
        .await
        .unwrap();
        let storage_path = dir.path().join("query-1");

        assert!(path_has_entries(&storage_path));
        let handle = subscription.handle;
        drop(subscription);

        let err = service.unregister(handle).await.unwrap_err();
        assert!(err.to_string().contains("Unknown incremental query handle"));
        assert!(!storage_path.exists());
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn register_enqueues_non_empty_priming_result_before_future_deltas() {
        let dir = tempfile::tempdir().unwrap();
        let service = test_service(dir.path(), IncrementalServiceConfig::default());
        let registration_basis = test_tx_key_with_tx_id(1);
        let future_basis = test_tx_key_with_tx_id(2);
        let mut subscription = register(
            &service,
            single_pattern_plan(),
            registration_basis,
            vec![name_triple(42, "Alice")],
        )
        .await
        .unwrap();

        service
            .apply_triples(future_basis, 2, vec![name_triple(43, "Bob")])
            .await
            .unwrap();
        service.flush().await.unwrap();

        assert_eq!(
            expect_delta(subscription.deltas.try_recv().unwrap()),
            IncrementalQueryDelta {
                tx_key: registration_basis,
                rows: vec![(vec![DataType::String("Alice".to_string())], 1)],
            }
        );
        assert_eq!(
            expect_delta(subscription.deltas.try_recv().unwrap()),
            IncrementalQueryDelta {
                tx_key: future_basis,
                rows: vec![(vec![DataType::String("Bob".to_string())], 1)],
            }
        );
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn register_skips_empty_priming_result() {
        let dir = tempfile::tempdir().unwrap();
        let service = test_service(dir.path(), IncrementalServiceConfig::default());
        let mut subscription = register(
            &service,
            single_pattern_plan(),
            test_tx_key_with_tx_id(1),
            Vec::new(),
        )
        .await
        .unwrap();

        assert!(matches!(
            subscription.deltas.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn invalid_aggregate_priming_rejects_registration_and_cleans_storage() {
        let dir = tempfile::tempdir().unwrap();
        let service = test_service(dir.path(), IncrementalServiceConfig::default());

        let err = register(
            &service,
            aggregate_plan("[:find (sum ?name) :where [?e :name ?name]]"),
            test_tx_key_with_tx_id(1),
            vec![name_triple(42, "Alice")],
        )
        .await
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("sum: cannot aggregate non-numeric value"));
        assert!(err.downcast_ref::<circuit::AggregateError>().is_some());
        // Priming failed before a worker existed, so nothing is registered under that handle.
        assert!(service.unregister(1).await.is_err());
        assert!(!dir.path().join("query-1").exists());
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn live_aggregate_error_removes_only_the_affected_subscription() {
        let dir = tempfile::tempdir().unwrap();
        let service = test_service(dir.path(), IncrementalServiceConfig::default());
        let mut aggregate_subscription = register(
            &service,
            aggregate_plan(
                "[:find (sum ?value)
                  :where (or [?e :age ?value] [?e :name ?value])]",
            ),
            test_tx_key_with_tx_id(1),
            Vec::new(),
        )
        .await
        .unwrap();
        let mut names_subscription = register(
            &service,
            single_pattern_plan(),
            test_tx_key_with_tx_id(1),
            Vec::new(),
        )
        .await
        .unwrap();
        // An aggregate over an empty snapshot still primes with a row, so drain it first.
        aggregate_subscription.deltas.try_recv().unwrap();

        service
            .apply_triples(test_tx_key_with_tx_id(2), 2, vec![age_triple(42, 10)])
            .await
            .unwrap();
        service.flush().await.unwrap();
        service
            .apply_triples(test_tx_key_with_tx_id(3), 3, vec![name_triple(43, "Alice")])
            .await
            .unwrap();
        service.flush().await.unwrap();

        assert_eq!(
            expect_delta(names_subscription.deltas.try_recv().unwrap()).rows,
            vec![(vec![DataType::String("Alice".to_string())], 1)]
        );
        let prior_delta = expect_delta(aggregate_subscription.deltas.try_recv().unwrap());
        assert!(prior_delta.rows.contains(&(vec![DataType::Long(10)], 1)));
        let error = expect_error(aggregate_subscription.deltas.recv().await.unwrap());
        assert_eq!(error.to_string(), "sum: cannot aggregate non-numeric value");
        assert!(error.downcast_ref::<circuit::AggregateError>().is_some());
        assert!(aggregate_subscription.deltas.recv().await.is_none());
        assert!(service
            .unregister(aggregate_subscription.handle)
            .await
            .is_err());

        service
            .apply_triples(test_tx_key_with_tx_id(4), 4, vec![name_triple(44, "Bob")])
            .await
            .unwrap();
        service.flush().await.unwrap();
        assert_eq!(
            expect_delta(names_subscription.deltas.try_recv().unwrap()).rows,
            vec![(vec![DataType::String("Bob".to_string())], 1)]
        );
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn apply_triples_skips_transactions_at_or_before_query_basis() {
        let dir = tempfile::tempdir().unwrap();
        let service = test_service(dir.path(), IncrementalServiceConfig::default());
        let old_basis = test_tx_key_with_tx_id(1);
        let new_basis = test_tx_key_with_tx_id(2);
        let mut old_subscription = register(
            &service,
            single_pattern_plan(),
            old_basis,
            vec![name_triple(42, "Alice")],
        )
        .await
        .unwrap();
        let mut new_subscription = register(
            &service,
            single_pattern_plan(),
            new_basis,
            vec![name_triple(42, "Alice"), name_triple(43, "Bob")],
        )
        .await
        .unwrap();

        // Drain priming deltas before testing application relative to each basis.
        old_subscription.deltas.try_recv().unwrap();
        new_subscription.deltas.try_recv().unwrap();

        service
            .apply_triples(new_basis, 2, vec![name_triple(43, "Bob")])
            .await
            .unwrap();
        service.flush().await.unwrap();

        assert_eq!(
            expect_delta(old_subscription.deltas.try_recv().unwrap()).rows,
            vec![(vec![DataType::String("Bob".to_string())], 1)]
        );
        // The flush barrier passed through the second query too, so an empty inbox here means
        // the transaction was skipped at the router rather than merely still in flight.
        assert!(new_subscription.deltas.try_recv().is_err());
        service.shutdown().await.unwrap();
    }

    // Replaces the old "apply_triples blocks until cancelled" test: fanning out no longer
    // waits on any subscriber, which is the point of the whole change.
    #[tokio::test]
    async fn apply_triples_returns_while_a_subscriber_is_full() {
        let dir = tempfile::tempdir().unwrap();
        let config = IncrementalServiceConfig {
            subscription_capacity: 1,
            inbox_capacity: 8,
            ..IncrementalServiceConfig::default()
        };
        let service = test_service(dir.path(), config);
        let _wedged = register(
            &service,
            single_pattern_plan(),
            test_tx_key_with_tx_id(1),
            Vec::new(),
        )
        .await
        .unwrap();

        // Nobody reads `_wedged`, so its worker parks on the first send and its inbox fills.
        for seq in 1..=config.inbox_capacity {
            let name = format!("Alice {seq}");
            let applied = tokio::time::timeout(
                Duration::from_secs(5),
                service.apply_triples(
                    test_tx_key_with_tx_id(seq as i64 + 1),
                    seq as u64,
                    vec![name_triple(seq as i64, &name)],
                ),
            )
            .await
            .expect("fan-out must not wait on a full subscriber");
            applied.unwrap();
        }

        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_is_bounded_when_a_worker_is_parked_on_a_full_subscriber() {
        let dir = tempfile::tempdir().unwrap();
        let config = IncrementalServiceConfig {
            subscription_capacity: 1,
            inbox_capacity: 4,
            ..IncrementalServiceConfig::default()
        };
        let service = test_service(dir.path(), config);
        let _wedged = register(
            &service,
            single_pattern_plan(),
            test_tx_key_with_tx_id(1),
            Vec::new(),
        )
        .await
        .unwrap();

        for seq in 1..=config.inbox_capacity {
            let name = format!("Alice {seq}");
            service
                .apply_triples(
                    test_tx_key_with_tx_id(seq as i64 + 1),
                    seq as u64,
                    vec![name_triple(seq as i64, &name)],
                )
                .await
                .unwrap();
        }

        tokio::time::timeout(Duration::from_secs(10), service.shutdown())
            .await
            .expect("shutdown must not wait on a parked worker")
            .unwrap();
        assert!(!dir.path().join("query-1").exists());
    }

    // Bound delivery waits so a stalled worker fails the test instead of hanging it.
    async fn recv_delta(
        subscription: &mut IncrementalQuerySubscription,
    ) -> Result<IncrementalQueryDelta> {
        tokio::time::timeout(Duration::from_secs(10), subscription.deltas.recv())
            .await
            .expect("a delta should arrive")
            .expect("the subscription should still be open")
    }

    fn pause_worker(
        inner: &IncrementalQueryServiceInner,
        handle: IncrementalQueryHandle,
    ) -> oneshot::Sender<()> {
        let (started, wait) = oneshot::channel();
        let (resume, paused) = oneshot::channel();
        inner.queries[&handle]
            .inbox
            .try_send(WorkerMessage::Pause {
                started,
                resume: paused,
            })
            .unwrap();
        test_runtime().block_on(async {
            tokio::time::timeout(Duration::from_secs(5), wait)
                .await
                .unwrap()
                .unwrap();
        });
        resume
    }

    #[test]
    fn unregister_waits_for_its_worker_without_blocking_the_router() {
        let dir = tempfile::tempdir().unwrap();
        let mut inner = IncrementalQueryServiceInner::new(
            dir.path().to_path_buf(),
            IncrementalServiceConfig::default(),
            CancellationToken::new(),
        );
        let basis = test_tx_key_with_tx_id(1);
        let paused = inner
            .register(single_pattern_plan(), basis, test_cursor(), Vec::new())
            .unwrap();
        let mut healthy = inner
            .register(single_pattern_plan(), basis, test_cursor(), Vec::new())
            .unwrap();
        let resume = pause_worker(&inner, paused.handle);
        let (commands, receiver) = std_mpsc::channel();
        let router = thread::spawn(move || inner.run(receiver));

        test_runtime().block_on(async {
            let (response, mut unregistered) = oneshot::channel();
            commands
                .send(IncrementalCommand::Unregister {
                    handle: paused.handle,
                    response,
                })
                .unwrap();
            let (response, applied) = oneshot::channel();
            commands
                .send(IncrementalCommand::ApplyTriples {
                    tx_key: test_tx_key_with_tx_id(2),
                    wal_seq: 2,
                    triples: Arc::from(vec![name_triple(42, "Alice")]),
                    response,
                })
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), applied)
                .await
                .expect("unregister must leave the router responsive")
                .unwrap()
                .unwrap();
            assert_eq!(expect_delta(recv_delta(&mut healthy).await).tx_key.tx_id, 2);
            assert!(matches!(
                unregistered.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            assert!(dir.path().join("query-1").exists());

            resume.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(5), unregistered)
                .await
                .expect("retirement must be reaped without another command")
                .unwrap()
                .unwrap();
            assert!(!dir.path().join("query-1").exists());
        });

        drop(commands);
        router.join().unwrap();
        assert!(!dir.path().join("query-2").exists());
    }

    #[test]
    fn flush_does_not_overflow_a_full_worker_inbox() {
        let dir = tempfile::tempdir().unwrap();
        let mut inner = IncrementalQueryServiceInner::new(
            dir.path().to_path_buf(),
            IncrementalServiceConfig {
                inbox_capacity: 2,
                ..IncrementalServiceConfig::default()
            },
            CancellationToken::new(),
        );
        let mut subscription = inner
            .register(
                single_pattern_plan(),
                test_tx_key_with_tx_id(1),
                test_cursor(),
                Vec::new(),
            )
            .unwrap();
        let resume = pause_worker(&inner, subscription.handle);
        for seq in 2..=3 {
            inner
                .apply_triples(
                    test_tx_key_with_tx_id(seq),
                    seq as u64,
                    Arc::from(vec![name_triple(seq, "Alice")]),
                )
                .unwrap();
        }
        assert_eq!(inner.queries[&subscription.handle].inbox.capacity(), 0);
        let mut ack = inner.flush().unwrap().pop().unwrap();
        assert!(inner.queries.contains_key(&subscription.handle));
        assert!(matches!(
            ack.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        resume.send(()).unwrap();
        test_runtime().block_on(async {
            tokio::time::timeout(Duration::from_secs(5), ack)
                .await
                .unwrap()
                .unwrap();
            for seq in 2..=3 {
                assert_eq!(
                    expect_delta(recv_delta(&mut subscription).await)
                        .tx_key
                        .tx_id,
                    seq
                );
            }
        });
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deltas_arrive_in_wal_order_for_a_slow_subscriber() {
        let dir = tempfile::tempdir().unwrap();
        let config = IncrementalServiceConfig {
            // Small enough that the worker parks on the subscriber between transactions.
            subscription_capacity: 2,
            inbox_capacity: 64,
            ..IncrementalServiceConfig::default()
        };
        let service = test_service(dir.path(), config);
        let mut subscription = register(
            &service,
            single_pattern_plan(),
            test_tx_key_with_tx_id(1),
            Vec::new(),
        )
        .await
        .unwrap();

        let applied = 24;
        for seq in 1..=applied {
            let name = format!("Alice {seq}");
            service
                .apply_triples(
                    test_tx_key_with_tx_id(seq as i64 + 1),
                    seq as u64,
                    vec![name_triple(seq as i64, &name)],
                )
                .await
                .unwrap();
        }

        let mut seen = Vec::new();
        while seen.len() < applied {
            let delta = expect_delta(recv_delta(&mut subscription).await);
            seen.push(delta.tx_key.tx_id);
            // Drain slowly, so the worker keeps parking on a full subscriber channel.
            tokio::task::yield_now().await;
        }

        let mut ordered = seen.clone();
        ordered.sort_unstable();
        assert_eq!(seen, ordered, "deltas must arrive in WAL order");
        assert_eq!(seen, (2..=applied as i64 + 1).collect::<Vec<_>>());
        service.shutdown().await.unwrap();
    }

    #[test]
    fn lag_error_timeout_closes_the_subscription_and_cleans_storage() {
        let dir = tempfile::tempdir().unwrap();
        let mut inner = IncrementalQueryServiceInner::new(
            dir.path().to_path_buf(),
            IncrementalServiceConfig {
                subscription_capacity: 1,
                inbox_capacity: 1,
                retire_timeout: Duration::from_millis(50),
                ..IncrementalServiceConfig::default()
            },
            CancellationToken::new(),
        );
        let mut subscription = inner
            .register(
                single_pattern_plan(),
                test_tx_key_with_tx_id(1),
                test_cursor(),
                vec![name_triple(1, "Priming")],
            )
            .unwrap();
        let resume = pause_worker(&inner, subscription.handle);
        for seq in 2..=3 {
            inner
                .apply_triples(
                    test_tx_key_with_tx_id(seq),
                    seq as u64,
                    Arc::from(vec![name_triple(seq, "Alice")]),
                )
                .unwrap();
        }
        resume.send(()).unwrap();
        let outcome = inner.retired.recv_timeout(Duration::from_secs(5)).unwrap();
        inner.apply_retirement(outcome).unwrap();
        assert!(!dir.path().join("query-1").exists());
        assert_eq!(
            expect_delta(subscription.deltas.try_recv().unwrap())
                .tx_key
                .tx_id,
            1
        );
        test_runtime().block_on(async {
            assert!(
                tokio::time::timeout(Duration::from_secs(5), subscription.deltas.recv())
                    .await
                    .unwrap()
                    .is_none()
            );
        });
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_slow_query_does_not_stall_a_fast_one() {
        let dir = tempfile::tempdir().unwrap();
        let config = IncrementalServiceConfig {
            subscription_capacity: 1,
            inbox_capacity: 32,
            ..IncrementalServiceConfig::default()
        };
        let service = test_service(dir.path(), config);
        let basis = test_tx_key_with_tx_id(1);
        // Nobody ever reads `_wedged`, so its worker parks on the very first delivery.
        let _wedged = register(&service, single_pattern_plan(), basis, Vec::new())
            .await
            .unwrap();
        let mut healthy = register(&service, single_pattern_plan(), basis, Vec::new())
            .await
            .unwrap();

        let applied = 8;
        for seq in 1..=applied {
            let name = format!("Alice {seq}");
            service
                .apply_triples(
                    test_tx_key_with_tx_id(seq as i64 + 1),
                    seq as u64,
                    vec![name_triple(seq as i64, &name)],
                )
                .await
                .unwrap();
        }

        // The old lock-step service could not pass this: the wedged subscriber held the
        // single service thread and no other query made progress.
        for seq in 1..=applied {
            let delta = expect_delta(recv_delta(&mut healthy).await);
            assert_eq!(delta.tx_key.tx_id, seq as i64 + 1);
        }
        service.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn inbox_overflow_terminates_only_the_lagging_query() {
        let dir = tempfile::tempdir().unwrap();
        let config = IncrementalServiceConfig {
            subscription_capacity: 1,
            inbox_capacity: 2,
            ..IncrementalServiceConfig::default()
        };
        let service = test_service(dir.path(), config);
        let basis = test_tx_key_with_tx_id(1);
        let mut lagging = register(&service, single_pattern_plan(), basis, Vec::new())
            .await
            .unwrap();
        let mut healthy = register(&service, single_pattern_plan(), basis, Vec::new())
            .await
            .unwrap();

        // Far more than `lagging` can buffer while nobody reads it.
        let applied = 12;
        for seq in 1..=applied {
            let name = format!("Alice {seq}");
            service
                .apply_triples(
                    test_tx_key_with_tx_id(seq as i64 + 1),
                    seq as u64,
                    vec![name_triple(seq as i64, &name)],
                )
                .await
                .unwrap();
            // Drain `healthy` as it goes - on this deliberately tiny config it would
            // otherwise fall behind too, and both queries would be terminated.
            let delta = expect_delta(recv_delta(&mut healthy).await);
            assert_eq!(delta.tx_key.tx_id, seq as i64 + 1);
        }

        // The lagging subscription ends with a terminal error rather than silently gapping.
        let mut delivered = 0;
        let error = loop {
            match tokio::time::timeout(Duration::from_secs(10), lagging.deltas.recv())
                .await
                .expect("the lagging subscription should resolve")
            {
                Some(Ok(_)) => delivered += 1,
                Some(Err(error)) => break error,
                None => panic!(
                    "lagging subscription closed after {delivered} deltas without a \
                     terminal error"
                ),
            }
        };
        assert!(
            error.to_string().contains("fell behind"),
            "unexpected terminal error: {error}"
        );
        assert!(error
            .downcast_ref::<crate::incremental::worker::SubscriptionLagError>()
            .is_some());
        assert!(delivered < applied, "the lagging query should have gapped");
        assert!(lagging.deltas.recv().await.is_none());
        assert!(service.unregister(lagging.handle).await.is_err());
        assert!(!dir.path().join("query-1").exists());

        // The healthy neighbour is untouched and still receiving.
        service
            .apply_triples(
                test_tx_key_with_tx_id(applied as i64 + 2),
                applied as u64 + 1,
                vec![name_triple(99, "Zoe")],
            )
            .await
            .unwrap();
        let delta = expect_delta(recv_delta(&mut healthy).await);
        assert_eq!(
            delta.rows,
            vec![(vec![DataType::String("Zoe".to_string())], 1)]
        );
        service.shutdown().await.unwrap();
    }

    // The CDC loop replays the WAL from the beginning, so a query registered on a node with
    // history sees a burst of transactions at or below its basis. Those must be skipped at
    // the router; routing them would fill the inbox and terminate a healthy new query.
    #[tokio::test(flavor = "multi_thread")]
    async fn replayed_history_does_not_terminate_a_fresh_query() {
        let dir = tempfile::tempdir().unwrap();
        let config = IncrementalServiceConfig {
            subscription_capacity: 1,
            inbox_capacity: 2,
            ..IncrementalServiceConfig::default()
        };
        let service = test_service(dir.path(), config);
        let basis = test_tx_key_with_tx_id(500);
        let mut subscription = register(&service, single_pattern_plan(), basis, Vec::new())
            .await
            .unwrap();

        // Replay far more history than the inbox could ever hold.
        for tx_id in 1..=100 {
            let name = format!("Historic {tx_id}");
            service
                .apply_triples(
                    test_tx_key_with_tx_id(tx_id),
                    tx_id as u64,
                    vec![name_triple(tx_id, &name)],
                )
                .await
                .unwrap();
        }
        service.flush().await.unwrap();

        // Still alive, and still able to receive a transaction after its basis.
        let live = test_tx_key_with_tx_id(501);
        service
            .apply_triples(live, 501, vec![name_triple(1000, "Live")])
            .await
            .unwrap();
        let delta = expect_delta(recv_delta(&mut subscription).await);
        assert_eq!(delta.tx_key, live);
        assert_eq!(
            delta.rows,
            vec![(vec![DataType::String("Live".to_string())], 1)]
        );
        service.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn worker_panic_retires_only_that_query() {
        let dir = tempfile::tempdir().unwrap();
        let service = test_service(dir.path(), IncrementalServiceConfig::default());
        let basis = test_tx_key_with_tx_id(1);
        let mut panicking = register(&service, single_pattern_plan(), basis, Vec::new())
            .await
            .unwrap();
        let mut healthy = register(&service, single_pattern_plan(), basis, Vec::new())
            .await
            .unwrap();

        service.inject_panic(panicking.handle).await.unwrap();

        let error = expect_error(recv_delta(&mut panicking).await);
        assert!(
            error.to_string().contains("panicked"),
            "unexpected terminal error: {error}"
        );
        assert!(panicking.deltas.recv().await.is_none());
        // Whether this reports the handle as unknown depends on whether the retirement was
        // already reaped, but either way it waits for the teardown to finish.
        let _ = service.unregister(panicking.handle).await;
        assert!(!dir.path().join("query-1").exists());

        // The service survived, the neighbour still works, and new queries still register.
        service
            .apply_triples(test_tx_key_with_tx_id(2), 2, vec![name_triple(42, "Alice")])
            .await
            .unwrap();
        let delta = expect_delta(recv_delta(&mut healthy).await);
        assert_eq!(
            delta.rows,
            vec![(vec![DataType::String("Alice".to_string())], 1)]
        );
        let _fresh = register(
            &service,
            single_pattern_plan(),
            test_tx_key_with_tx_id(2),
            Vec::new(),
        )
        .await
        .unwrap();
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn await_cdc_task_returns_inner_loop_error() {
        let dir = tempfile::tempdir().unwrap();
        let service = IncrementalQueryService::new(
            dir.path().to_path_buf(),
            IncrementalServiceConfig::default(),
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
            IncrementalServiceConfig::default(),
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
            IncrementalServiceConfig::default(),
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
            IncrementalServiceConfig::default(),
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
