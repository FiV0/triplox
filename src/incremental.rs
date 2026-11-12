//! Writer-node incremental query service.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use dbsp::{PoolConfig, RuntimePool};
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use triplox_client::transaction::TxKey;

use crate::config::IncrementalConfig;
use crate::inc_query::{plan_query, IncrementalQueryPlan};
use crate::incremental::cdc::{initial_scans, spawn_cdc_loop};
use crate::incremental::circuit::QueryCircuit;
use crate::indexer::Indexer;
use crate::ops::DataType;
use crate::slate::SlateComponents;
use edn::query::ParsedQuery;

pub(crate) mod cdc;
pub(crate) mod circuit;
pub(crate) mod subscription;

use subscription::{SubscriptionDeltas, SubscriptionLagged, Termination};
use worker::{Batch, Build, Circuit, Completion, SnapshotCircuit, StopSignal, Worker};
mod worker;

const SUBSCRIPTION_OUTPUT_CAPACITY: usize = 128;

#[derive(Clone, Copy)]
struct IncrementalQueryOptions {
    config: IncrementalConfig,
    inbox_capacity: NonZeroUsize,
    retire_timeout: Duration,
}

impl IncrementalQueryOptions {
    fn for_config(config: IncrementalConfig) -> Self {
        Self {
            config,
            inbox_capacity: NonZeroUsize::new(256).unwrap(),
            retire_timeout: Duration::from_secs(10),
        }
    }
}

impl Default for IncrementalQueryOptions {
    fn default() -> Self {
        Self::for_config(IncrementalConfig::default())
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
    feldera_macros::OrdRepr,
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
        // The box here is purely an optimization to keep the enum stack size small.
        plan: Box<IncrementalQueryPlan>,
        response: oneshot::Sender<Result<IncrementalQuerySubscription>>,
    },
    Unregister {
        handle: IncrementalQueryHandle,
        response: oneshot::Sender<Result<()>>,
    },
    Shutdown {
        response: oneshot::Sender<Result<()>>,
    },
}

// A batch from the CDC task. The dispatcher answers once it has routed the batch.
pub(crate) struct CdcBatch {
    batch: Arc<Batch>,
    routed: oneshot::Sender<()>,
}

// The service owns the CDC task, so the dispatcher stops once the commands channel closes.
pub(crate) struct IncrementalQueryService {
    commands: mpsc::UnboundedSender<IncrementalCommand>,
    indexer: Arc<RwLock<Indexer>>,
    retire_timeout: Duration,
    cdc_cancel: CancellationToken,
    // TODO: remove the option
    cdc_task: Option<JoinHandle<Result<()>>>,
}

// Starts on a blocking thread, as the pool spawns its DBSP worker and merger threads.
async fn start_pool(config: IncrementalConfig) -> Result<RuntimePool> {
    tokio::task::spawn_blocking(move || {
        RuntimePool::start(
            PoolConfig::with_threads(config.threads.get())
                .with_merger_threads(config.merger_threads.get().into())
                .with_max_rss_bytes(*circuit::MAX_RSS_BYTES)
                .with_cache_mib(config.cache_mib.get()),
        )
    })
    .await
    .context("Incremental runtime pool startup task panicked")?
    .context("Incremental runtime pool failed to start")
}

async fn join_cdc(task: Option<JoinHandle<Result<()>>>) -> Result<()> {
    let Some(task) = task else {
        return Ok(());
    };
    task.await
        .context("Incremental query CDC task failed to join")?
        .context("Incremental query CDC loop failed")
}

fn shutdown_result(cdc_result: Result<()>, cleanup: Result<()>) -> Result<()> {
    match (cdc_result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(cdc_error), Err(cleanup_error)) => Err(cdc_error.context(format!(
            "Incremental query service shutdown failed: {cleanup_error:#}"
        ))),
    }
}

// The two result levels in this service have two different meanings.
// The first level is about the communication of this service.
// The second level reports errors while processing a command.
impl IncrementalQueryService {
    // Tests call this without CDC and send batches on the returned sender themselves.
    async fn new_with_batch_sender(
        storage_root: PathBuf,
        cancel: &CancellationToken,
        db: Arc<slatedb::Db>,
        indexer: Arc<RwLock<Indexer>>,
        options: IncrementalQueryOptions,
    ) -> Result<(Self, mpsc::UnboundedSender<CdcBatch>)> {
        let pool = start_pool(options.config).await?;
        let (commands, command_receiver) = mpsc::unbounded_channel();
        let (batches, batch_receiver) = mpsc::unbounded_channel();
        let inner = IncrementalQueryServiceInner::new(
            pool,
            storage_root,
            cancel.child_token(),
            options,
            db,
            indexer.clone(),
        );
        tokio::spawn(inner.run(command_receiver, batch_receiver));

        let service = Self {
            commands,
            indexer,
            retire_timeout: options.retire_timeout,
            cdc_cancel: cancel.child_token(),
            cdc_task: None,
        };
        Ok((service, batches))
    }

    // The CDC task starts with the service and reads the WAL from its beginning.
    pub(crate) async fn new(
        storage_root: PathBuf,
        cancel: CancellationToken,
        slate: &SlateComponents,
        indexer: Arc<RwLock<Indexer>>,
        cdc_poll_interval: Duration,
        config: IncrementalConfig,
    ) -> Result<Self> {
        let (mut service, batches) = Self::new_with_batch_sender(
            storage_root,
            &cancel,
            slate.db.clone(),
            indexer.clone(),
            IncrementalQueryOptions::for_config(config),
        )
        .await?;
        service.cdc_task = Some(spawn_cdc_loop(
            slate.object_path.clone(),
            slate.object_store.clone(),
            indexer,
            batches,
            service.cdc_cancel.clone(),
            cdc_poll_interval,
        ));
        Ok(service)
    }

    // The plan uses the schema at call time and the dispatcher reads the basis later.
    // This is correct because an attribute id does not change after the schema assigns it.
    pub(crate) async fn register_query(
        &self,
        query: ParsedQuery,
    ) -> Result<IncrementalQuerySubscription> {
        let schema = self.indexer.read().await.metadata().schema.clone();
        let plan = plan_query(&query, &schema)?;
        let (response, result) = oneshot::channel();
        self.commands
            .send(IncrementalCommand::Register {
                plan: Box::new(plan),
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

    // Stops CDC before the dispatcher stops the workers.
    pub(crate) async fn shutdown(mut self) -> Result<()> {
        self.cdc_cancel.cancel();
        let cdc_task = self.cdc_task.take();
        let commands = self.commands.clone();
        // A task, so a shutdown that times out still finishes in the background.
        let shutdown = tokio::spawn(async move {
            let cdc_result = join_cdc(cdc_task).await;
            let (response, result) = oneshot::channel();
            let cleanup = match commands.send(IncrementalCommand::Shutdown { response }) {
                Ok(()) => result
                    .await
                    .context("Incremental query service stopped")
                    .and_then(|cleanup| cleanup),
                Err(_) => Err(anyhow!("Incremental query service stopped")),
            };
            shutdown_result(cdc_result, cleanup)
        });
        tokio::time::timeout(self.retire_timeout, shutdown)
            .await
            .map_err(|_| RetirementTimeout::Shutdown {
                timeout: self.retire_timeout,
            })?
            .context("Incremental query shutdown task panicked")?
    }
}

// A service dropped without shutdown still stops its CDC task.
impl Drop for IncrementalQueryService {
    fn drop(&mut self) {
        self.cdc_cancel.cancel();
    }
}

struct RegisteredQuery {
    inbox: mpsc::Sender<Arc<Batch>>,
    sender: mpsc::Sender<IncrementalQueryDelta>,
    stop: Arc<StopSignal>,
    basis: TxKey,
    // A caller wants to unregister the query. The dispatcher tells the worker to stop.
    // Once finished, the dispatcher sends the cleanup result to the original caller via this channel.
    unregister: Option<oneshot::Sender<Result<()>>>,
}

type Registration = oneshot::Sender<Result<IncrementalQuerySubscription>>;

struct IncrementalQueryServiceInner {
    pool: RuntimePool,
    next_query_id: u64,
    storage_root: PathBuf,
    queries: HashMap<IncrementalQueryHandle, RegisteredQuery>,
    cancel: CancellationToken,
    options: IncrementalQueryOptions,
    // Workers prime their circuits, so they need the db and the indexer.
    db: Arc<slatedb::Db>,
    indexer: Arc<RwLock<Indexer>>,
    // Sending end of worker retirement notifications. Each worker gets its own copy of this channel.
    completed: mpsc::UnboundedSender<Completion>,
    // Receiving end of worker retirement notifications. The service reads this.
    completions: mpsc::UnboundedReceiver<Completion>,
}

impl IncrementalQueryServiceInner {
    fn new(
        pool: RuntimePool,
        storage_root: PathBuf,
        cancel: CancellationToken,
        options: IncrementalQueryOptions,
        db: Arc<slatedb::Db>,
        indexer: Arc<RwLock<Indexer>>,
    ) -> Self {
        let (completed, completions) = mpsc::unbounded_channel();
        Self {
            pool,
            next_query_id: 1,
            storage_root,
            queries: HashMap::new(),
            cancel,
            options,
            db,
            indexer,
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

    async fn run(
        mut self,
        mut commands: mpsc::UnboundedReceiver<IncrementalCommand>,
        mut batches: mpsc::UnboundedReceiver<CdcBatch>,
    ) {
        let mut shutdown = None;
        let mut cleanup = Ok(());
        let mut cdc_running = true;
        loop {
            tokio::select! {
                completion = self.completions.recv(), if !self.queries.is_empty() => {
                    if let Some(completion) = completion {
                        if let Err(error) = self.retire(completion) {
                            warn!("{error:#}");
                            // A cancel of the node stops the workers, so shutdown reports their cleanup.
                            if self.cancel.is_cancelled() && cleanup.is_ok() {
                                cleanup = Err(error);
                            }
                        }
                    }
                }
                batch = batches.recv(), if cdc_running => match batch {
                    Some(CdcBatch { batch, routed }) => {
                        self.apply_batch(batch);
                        let _ = routed.send(());
                    }
                    // The commands channel, not CDC, decides when the dispatcher stops.
                    None => cdc_running = false,
                },
                command = commands.recv() => match command {
                    Some(IncrementalCommand::Register { plan, response }) => {
                        // No batch is routed between this read and the registry insert, so no lock with CDC is needed.
                        let basis = self.indexer.read().await.latest_tx_key();
                        self.start_registration(*plan, basis, response).await;
                    }
                    Some(IncrementalCommand::Unregister { handle, response }) => {
                        match self.queries.get_mut(&handle) {
                            Some(query) if !query.sender.is_closed() && !query.stop.token.is_cancelled() => {
                                query.stop.terminate(None);
                                query.unregister = Some(response);
                            }
                            _ => { let _ = response.send(Err(anyhow!("Unknown incremental query handle: {handle:?}"))); }
                        }
                    }
                    Some(IncrementalCommand::Shutdown { response }) => { shutdown = Some(response); break; }
                    None => break,
                }
            }
        }
        commands.close();
        batches.close();
        self.cancel.cancel();
        // A build cannot be interrupted, so a worker that builds retires after its build.
        for query in self.queries.values() {
            query.stop.terminate(None);
        }
        while !self.queries.is_empty() {
            if let Some(completion) = self.completions.recv().await {
                if let Err(error) = self.retire(completion) {
                    warn!("{error:#}");
                    if cleanup.is_ok() {
                        cleanup = Err(error);
                    }
                }
            }
        }
        let pool = self.pool.clone();
        let result = tokio::task::spawn_blocking(move || {
            pool.shutdown()
                .map_err(|_| anyhow!("Incremental runtime pool shutdown panicked"))
        })
        .await
        .context("Incremental runtime pool shutdown task panicked")
        .and_then(|result| result);
        if cleanup.is_ok() {
            cleanup = result;
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

    // Registers the query and starts its worker. The worker primes before it reads queued batches.
    fn start_query<C: Circuit>(
        &mut self,
        basis: TxKey,
        build: Build<C>,
        response: Registration,
    ) -> IncrementalQueryHandle {
        let handle = self.allocate_query_id();
        // The worker sends output rows to the subscription on this channel.
        let (sender, receiver) = mpsc::channel(SUBSCRIPTION_OUTPUT_CAPACITY);
        let (terminal, termination) = oneshot::channel();
        let stop = Arc::new(StopSignal::new(self.cancel.child_token(), terminal));
        let (inbox_sender, inbox) = mpsc::channel(self.options.inbox_capacity.get());
        let subscription = IncrementalQuerySubscription {
            handle,
            tx_key: basis,
            deltas: SubscriptionDeltas::new(receiver, termination),
        };
        let worker = Worker {
            build,
            storage_path: self.query_storage_path(handle),
            inbox,
            sender: sender.clone(),
            stop: stop.clone(),
        };
        let completed = self.completed.clone();
        tokio::spawn(async move {
            let cleanup = worker.run(subscription, response).await;
            let _ = completed.send(Completion { handle, cleanup });
        });
        self.queries.insert(
            handle,
            RegisteredQuery {
                inbox: inbox_sender,
                sender,
                stop,
                basis,
                unregister: None,
            },
        );
        handle
    }

    async fn start_registration(
        &mut self,
        plan: IncrementalQueryPlan,
        tx_key: TxKey,
        response: Registration,
    ) {
        if self.cancel.is_cancelled() {
            let _ = response.send(Err(anyhow!("Incremental query service stopped")));
            return;
        }
        let pool = self.pool.clone();
        let db = self.db.clone();
        let scans = initial_scans(&plan);
        self.start_query(
            tx_key,
            Box::new(move |storage_path| {
                Ok(SnapshotCircuit {
                    circuit: QueryCircuit::build(plan, storage_path, pool)?,
                    db,
                    scans,
                })
            }),
            response,
        );
    }

    fn apply_batch(&self, batch: Arc<Batch>) {
        for query in self.queries.values() {
            if query.stop.token.is_cancelled() {
                continue;
            }
            if query.sender.is_closed() {
                query.stop.terminate(None);
                continue;
            }
            let Some(batch) = batch.after(query.basis.tx_id) else {
                continue;
            };
            match query.inbox.try_send(batch.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    query
                        .stop
                        .terminate(Some(Termination::Lagged(SubscriptionLagged {
                            tx_key: batch.tx_key,
                            capacity: self.options.inbox_capacity.get(),
                        })));
                }
                Err(mpsc::error::TrySendError::Closed(_)) => query.stop.terminate(None),
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
    use dbsp::ZWeight;
    use edn::query::ToVariable;
    use triplox_client::transaction::TxKey;

    use super::*;
    use crate::codec::Encode;
    use crate::inc_query::test_support::{parse_query, test_schema, AGE_ATTR_ID, NAME_ATTR_ID};
    use crate::inc_query::{IncrementalQueryPlan, PatternPlan, PatternSlot, RelPlan, RelPlanKind};
    use crate::partition::tx_eid_from_tx_id;
    use crate::query::{FindPlan, Projection};
    use worker::test_support::single_batch;

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

    fn path_has_entries(path: &Path) -> bool {
        std::fs::read_dir(path)
            .unwrap()
            .next()
            .transpose()
            .unwrap()
            .is_some()
    }

    pub(super) fn small_pool_config() -> IncrementalConfig {
        IncrementalConfig {
            threads: NonZeroUsize::new(2).unwrap(),
            merger_threads: std::num::NonZeroU16::MIN,
            cache_mib: NonZeroUsize::new(16).unwrap(),
        }
    }

    // An in-memory slate whose names are asserted in transaction 1.
    async fn slate_with_names(names: &[(i64, &str)]) -> SlateComponents {
        let slate = crate::slate::in_memory_slate().await;
        let datoms = names
            .iter()
            .map(|&(entity, name)| crate::ops::Datom {
                entity,
                attribute: edn::kw!(:name),
                value: DataType::String(name.to_string()),
                op: crate::ops::DatomOp::Assert,
            })
            .collect::<Vec<_>>();
        // SlateDB rejects an empty write batch.
        if !datoms.is_empty() {
            cdc::test_support::write_scan_datoms(&slate.db, tx_eid_from_tx_id(1), &datoms).await;
        }
        slate
    }

    // The dispatcher reads the basis from the indexer, so a new indexer sets the basis of later registrations.
    async fn set_basis(service: &IncrementalQueryService, db: Arc<slatedb::Db>, basis_tx_id: i64) {
        *service.indexer.write().await =
            cdc::test_support::indexer_at(db, test_tx_key_with_tx_id(basis_tx_id));
    }

    type Batches = mpsc::UnboundedSender<CdcBatch>;

    // The service has no CDC task, so only the tests route batches, through the returned sender.
    // The registrations of this service have basis 1 until `set_basis` changes it.
    async fn service_on(
        slate: SlateComponents,
        dir: &Path,
        options: IncrementalQueryOptions,
    ) -> (IncrementalQueryService, Batches) {
        let indexer = cdc::test_support::indexer_at(slate.db.clone(), test_tx_key_with_tx_id(1));
        IncrementalQueryService::new_with_batch_sender(
            dir.to_path_buf(),
            &CancellationToken::new(),
            slate.db.clone(),
            Arc::new(RwLock::new(indexer)),
            options,
        )
        .await
        .unwrap()
    }

    async fn service_with_names(
        dir: &Path,
        names: &[(i64, &str)],
    ) -> (IncrementalQueryService, Batches) {
        service_on(
            slate_with_names(names).await,
            dir,
            IncrementalQueryOptions::default(),
        )
        .await
    }

    async fn service_at(dir: &Path) -> (IncrementalQueryService, Batches) {
        service_with_names(dir, &[]).await
    }

    // Sends a planned query, as `register_query` does after planning.
    async fn register_plan(
        service: &IncrementalQueryService,
        plan: IncrementalQueryPlan,
    ) -> Result<IncrementalQuerySubscription> {
        let (response, result) = oneshot::channel();
        service
            .commands
            .send(IncrementalCommand::Register {
                plan: Box::new(plan),
                response,
            })
            .unwrap();
        result.await.unwrap()
    }

    async fn register(
        service: &IncrementalQueryService,
        plan: IncrementalQueryPlan,
    ) -> IncrementalQuerySubscription {
        register_plan(service, plan).await.unwrap()
    }

    // Routes a batch, as the CDC task does.
    pub(super) async fn apply_batch(batches: &Batches, batch: Batch) -> Result<()> {
        let (routed, result) = oneshot::channel();
        batches
            .send(CdcBatch {
                batch: Arc::new(batch),
                routed,
            })
            .map_err(|_| anyhow!("Incremental query service stopped"))?;
        result.await.context("Incremental query service stopped")
    }

    async fn register_inline(
        inner: &mut IncrementalQueryServiceInner,
        plan: IncrementalQueryPlan,
        tx_key: TxKey,
    ) -> IncrementalQuerySubscription {
        let (response, result) = oneshot::channel();
        inner.start_registration(plan, tx_key, response).await;
        result.await.unwrap().unwrap()
    }

    async fn next_delta(
        subscription: &mut IncrementalQuerySubscription,
    ) -> Result<IncrementalQueryDelta> {
        tokio::time::timeout(Duration::from_secs(10), subscription.deltas.recv())
            .await
            .unwrap()
            .unwrap()
    }

    // Skips the empty deltas that only report progress.
    async fn delta(
        subscription: &mut IncrementalQuerySubscription,
    ) -> Result<IncrementalQueryDelta> {
        loop {
            match next_delta(subscription).await {
                Ok(delta) if delta.rows.is_empty() => continue,
                result => return result,
            }
        }
    }

    #[tokio::test]
    async fn batches_without_output_send_empty_deltas() {
        let dir = tempfile::tempdir().unwrap();
        let (service, batches) = service_at(dir.path()).await;
        let mut query = register(
            &service,
            aggregate_plan("[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]"),
        )
        .await;
        // A batch without the query's attributes skips the step.
        let unmatched = test_tx_key_with_tx_id(2);
        apply_batch(&batches, single_batch(unmatched, vec![]))
            .await
            .unwrap();
        let progress = next_delta(&mut query).await.unwrap();
        assert_eq!(progress.tx_key, unmatched);
        assert!(progress.rows.is_empty());
        // A matching batch that joins with nothing steps without output.
        let unchanged = test_tx_key_with_tx_id(3);
        apply_batch(&batches, single_batch(unchanged, vec![age_triple(42, 30)]))
            .await
            .unwrap();
        let progress = next_delta(&mut query).await.unwrap();
        assert_eq!(progress.tx_key, unchanged);
        assert!(progress.rows.is_empty());
        let changed = test_tx_key_with_tx_id(4);
        apply_batch(
            &batches,
            single_batch(changed, vec![name_triple(42, "Alice")]),
        )
        .await
        .unwrap();
        let delta = next_delta(&mut query).await.unwrap();
        assert_eq!(delta.tx_key, changed);
        assert_eq!(
            delta.rows,
            vec![(
                vec![DataType::String("Alice".to_string()), DataType::Long(30)],
                1
            )]
        );
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn pooled_queries_share_workers_and_retire_independently() {
        let dir = tempfile::tempdir().unwrap();
        let slate = slate_with_names(&[(42, "Alice")]).await;
        let mut inner = IncrementalQueryServiceInner::new(
            start_pool(small_pool_config()).await.unwrap(),
            dir.path().into(),
            CancellationToken::new(),
            IncrementalQueryOptions {
                config: small_pool_config(),
                ..Default::default()
            },
            slate.db.clone(),
            cdc::test_support::test_indexer(slate.db.clone()),
        );
        let basis = test_tx_key_with_tx_id(1);
        let mut first = register_inline(&mut inner, single_pattern_plan(), basis).await;
        let mut second = register_inline(&mut inner, single_pattern_plan(), basis).await;
        let pool = inner.pool.clone();
        assert_eq!(pool.stats().registered_circuits, 2);
        assert_eq!(pool.stats().foreground_threads, 2);
        assert_eq!(pool.stats().merger_threads, 1);
        assert_eq!(
            delta(&mut first).await.unwrap().rows,
            delta(&mut second).await.unwrap().rows
        );
        inner.queries[&first.handle].stop.terminate(None);
        let completion = tokio::time::timeout(Duration::from_secs(5), inner.completions.recv())
            .await
            .unwrap()
            .unwrap();
        inner.retire(completion).unwrap();
        assert_eq!(pool.stats().registered_circuits, 1);
        assert!(!inner.query_storage_path(first.handle).exists());
        let mut old = name_triple(42, "Alice");
        old.1 = -1;
        inner.apply_batch(Arc::new(single_batch(
            test_tx_key_with_tx_id(2),
            vec![old, name_triple(42, "Bob")],
        )));
        let rows = delta(&mut second).await.unwrap().rows;
        assert!(rows.contains(&(vec![DataType::String("Alice".into())], -1)));
        assert!(rows.contains(&(vec![DataType::String("Bob".into())], 1)));
        inner.queries[&second.handle].stop.terminate(None);
        let completion = tokio::time::timeout(Duration::from_secs(5), inner.completions.recv())
            .await
            .unwrap()
            .unwrap();
        inner.retire(completion).unwrap();
        assert_eq!(pool.stats().registered_circuits, 0);
        assert!(!inner.query_storage_path(second.handle).exists());
        tokio::task::spawn_blocking(move || pool.shutdown().unwrap())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn unregister_removes_query_storage() {
        let dir = tempfile::tempdir().unwrap();
        let (service, _batches) = service_with_names(dir.path(), &[(42, "Alice")]).await;
        let subscription = register(&service, single_pattern_plan()).await;
        assert!(path_has_entries(&dir.path().join("query-1")));
        service.unregister(subscription.handle).await.unwrap();
        assert!(!dir.path().join("query-1").exists());
        assert!(service.unregister(subscription.handle).await.is_err());
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn priming_precedes_live_deltas_and_empty_priming_is_omitted() {
        let dir = tempfile::tempdir().unwrap();
        let slate = slate_with_names(&[(42, "Alice")]).await;
        let db = slate.db.clone();
        let (service, batches) =
            service_on(slate, dir.path(), IncrementalQueryOptions::default()).await;
        let mut primed = register(&service, single_pattern_plan()).await;
        // Alice comes after this basis, so the priming result is empty.
        set_basis(&service, db, 0).await;
        let mut empty = register(&service, single_pattern_plan()).await;
        assert!(empty.deltas.try_recv().is_err());
        let tx_key = test_tx_key_with_tx_id(2);
        apply_batch(&batches, single_batch(tx_key, vec![name_triple(43, "Bob")]))
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
        let (service, _batches) = service_with_names(dir.path(), &[(42, "Alice")]).await;
        let error = register_plan(
            &service,
            aggregate_plan("[:find (sum ?name) :where [?e :name ?name]]"),
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
        let (service, batches) = service_at(dir.path()).await;
        let mut aggregate = register(
            &service,
            aggregate_plan("[:find (sum ?value) :where (or [?e :age ?value] [?e :name ?value])]"),
        )
        .await;
        let mut names = register(&service, single_pattern_plan()).await;
        delta(&mut aggregate).await.unwrap();
        apply_batch(
            &batches,
            single_batch(test_tx_key_with_tx_id(2), vec![age_triple(42, 10)]),
        )
        .await
        .unwrap();
        apply_batch(
            &batches,
            single_batch(test_tx_key_with_tx_id(3), vec![name_triple(43, "Alice")]),
        )
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
        apply_batch(&batches, single_batch(tx_key, vec![name_triple(44, "Bob")]))
            .await
            .unwrap();
        assert_eq!(delta(&mut names).await.unwrap().tx_key, tx_key);
        service.shutdown().await.unwrap();
        assert!(!dir.path().join("query-1").exists());
    }

    #[tokio::test]
    async fn history_before_registration_basis_does_not_fill_inbox() {
        let dir = tempfile::tempdir().unwrap();
        let slate = crate::slate::in_memory_slate().await;
        let db = slate.db.clone();
        let (service, batches) = service_on(
            slate,
            dir.path(),
            IncrementalQueryOptions {
                inbox_capacity: NonZeroUsize::MIN,
                ..IncrementalQueryOptions::default()
            },
        )
        .await;
        set_basis(&service, db, 1000).await;
        let mut query = register(&service, single_pattern_plan()).await;
        for seq in 1..=1000 {
            apply_batch(
                &batches,
                single_batch(test_tx_key_with_tx_id(seq), vec![name_triple(seq, "old")]),
            )
            .await
            .unwrap();
        }
        assert!(query.deltas.try_recv().is_err());
        let tx_key = test_tx_key_with_tx_id(1001);
        apply_batch(
            &batches,
            single_batch(tx_key, vec![name_triple(1001, "new")]),
        )
        .await
        .unwrap();
        assert_eq!(delta(&mut query).await.unwrap().tx_key, tx_key);
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn batch_straddling_basis_applies_only_later_transactions() {
        let dir = tempfile::tempdir().unwrap();
        let slate = crate::slate::in_memory_slate().await;
        let db = slate.db.clone();
        let (service, batches) =
            service_on(slate, dir.path(), IncrementalQueryOptions::default()).await;
        set_basis(&service, db.clone(), 0).await;
        let mut before = register(&service, single_pattern_plan()).await;
        set_basis(&service, db, 2).await;
        let mut inside = register(&service, single_pattern_plan()).await;
        let tx_key = test_tx_key_with_tx_id(3);
        apply_batch(
            &batches,
            Batch::new(
                tx_key,
                vec![
                    name_triple(1, "Alice"),
                    name_triple(2, "Bob"),
                    name_triple(3, "Cara"),
                ],
                vec![1, 2, 3],
            ),
        )
        .await
        .unwrap();

        let mut whole = delta(&mut before).await.unwrap();
        whole.rows.sort_by_key(|row| format!("{:?}", row));
        assert_eq!(
            whole,
            IncrementalQueryDelta {
                tx_key,
                rows: vec![
                    (vec![DataType::String("Alice".into())], 1),
                    (vec![DataType::String("Bob".into())], 1),
                    (vec![DataType::String("Cara".into())], 1),
                ],
            }
        );
        assert_eq!(
            delta(&mut inside).await.unwrap(),
            IncrementalQueryDelta {
                tx_key,
                rows: vec![(vec![DataType::String("Cara".into())], 1)],
            }
        );
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn dropping_last_service_handle_cleans_up_queries() {
        let dir = tempfile::tempdir().unwrap();
        let (service, _batches) = service_at(dir.path()).await;
        let mut query = register(&service, single_pattern_plan()).await;
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
    async fn dropping_service_stops_cdc() {
        let dir = tempfile::tempdir().unwrap();
        let (service, _batches) = service_at(dir.path()).await;
        let cdc_cancel = service.cdc_cancel.clone();
        drop(service);
        assert!(cdc_cancel.is_cancelled());
    }

    #[tokio::test]
    async fn dispatcher_serves_commands_after_cdc_exits() {
        let dir = tempfile::tempdir().unwrap();
        let (service, batches) = service_at(dir.path()).await;
        drop(batches);
        let subscription = register(&service, single_pattern_plan()).await;
        service.unregister(subscription.handle).await.unwrap();
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn abandoned_registration_response_retires_query() {
        let dir = tempfile::tempdir().unwrap();
        let (service, batches) = service_at(dir.path()).await;
        let (response, result) = oneshot::channel();
        drop(result);
        service
            .commands
            .send(IncrementalCommand::Register {
                plan: Box::new(single_pattern_plan()),
                response,
            })
            .unwrap();
        apply_batch(&batches, single_batch(test_tx_key_with_tx_id(2), vec![]))
            .await
            .unwrap();
        service.shutdown().await.unwrap();
        assert!(!dir.path().join("query-1").exists());
    }
}

#[cfg(test)]
mod runtime_tests;
