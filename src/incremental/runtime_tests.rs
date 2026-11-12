use super::tests::{apply_batch, small_pool_config};
use super::worker::test_support::single_batch;
use super::*;
use dbsp::utils::Tup2;
use std::future::Future;
use std::sync::mpsc as sync_mpsc;
use std::time::Duration;
use tempfile::TempDir;

// Longest wait for an event that must occur.
const WAIT: Duration = Duration::from_secs(5);
// Retirement timeout for the tests in which the timeout must expire.
const SHORT_RETIRE_TIMEOUT: Duration = Duration::from_millis(50);

async fn within<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(WAIT, future).await.expect("timed out")
}

// The faults and delays of one test query. The default query has none.
#[derive(Default)]
struct ProbeConfig {
    // The build waits until this channel gets a message or closes.
    build_gate: Option<sync_mpsc::Receiver<()>>,
    build_panic: bool,
    prime_panic: bool,
    priming: worker::Rows,
    // Each apply waits until this channel gets a message or closes.
    apply_gate: Option<mpsc::UnboundedReceiver<()>>,
    apply_panic: bool,
}

struct Probe {
    config: ProbeConfig,
    steps: mpsc::UnboundedSender<i64>,
    storage: PathBuf,
    dropped: sync_mpsc::Sender<bool>,
}

impl Circuit for Probe {
    async fn prime(&mut self, _basis: TxKey) -> Result<worker::Rows> {
        assert!(!self.config.prime_panic, "injected priming panic");
        Ok(std::mem::take(&mut self.config.priming))
    }

    fn matches(&self, _batch: &Batch) -> bool {
        true
    }

    async fn apply(&mut self, batch: &Batch) -> Result<worker::Rows> {
        // Test batches are numbered by their offset from the bootstrap tx.
        let seq = batch.tx_key.tx_id - crate::bootstrap::BOOTSTRAP_TX_KEY.tx_id;
        let _ = self.steps.send(seq);
        if let Some(gate) = &mut self.config.apply_gate {
            let _ = gate.recv().await;
        }
        assert!(!self.config.apply_panic, "injected circuit panic");
        Ok(vec![(vec![DataType::Long(seq)], 1)])
    }
}

impl Drop for Probe {
    fn drop(&mut self) {
        let _ = self.dropped.send(self.storage.exists());
    }
}

fn assert_dropped(drops: Vec<sync_mpsc::Receiver<bool>>) {
    for dropped in drops {
        assert!(
            dropped
                .try_recv()
                .expect("circuit should have been dropped"),
            "storage removed before circuit destruction"
        );
    }
}

// A batch of one transaction, `seq` transactions after the bootstrap tx.
fn batch(seq: i64) -> Batch {
    let mut tx_key = *crate::bootstrap::BOOTSTRAP_TX_KEY;
    tx_key.tx_id += seq;
    single_batch(
        tx_key,
        vec![Tup2(
            EncodedTriple {
                entity: vec![],
                attribute: 1,
                value: vec![],
            },
            1,
        )],
    )
}

async fn started(steps: &mut mpsc::UnboundedReceiver<i64>) -> i64 {
    within(steps.recv()).await.unwrap()
}

async fn next(query: &mut IncrementalQuerySubscription) -> Result<IncrementalQueryDelta> {
    within(query.deltas.recv()).await.unwrap()
}

// A registration whose build and priming can still be in progress.
struct Pending {
    handle: IncrementalQueryHandle,
    registration: oneshot::Receiver<Result<IncrementalQuerySubscription>>,
    storage_path: PathBuf,
    // The sequence number of each batch that the probe started to apply.
    steps: mpsc::UnboundedReceiver<i64>,
}

impl Pending {
    async fn result(&mut self) -> Result<IncrementalQuerySubscription> {
        within(&mut self.registration).await.unwrap()
    }
}

// The dispatcher task of a started fixture. No CDC task exists, so only `apply` routes batches.
struct Dispatcher {
    batches: mpsc::UnboundedSender<CdcBatch>,
    task: JoinHandle<()>,
    _storage: TempDir,
}

impl Dispatcher {
    async fn apply(&self, seq: i64) -> TxKey {
        let batch = batch(seq);
        let tx_key = batch.tx_key;
        within(apply_batch(&self.batches, batch)).await.unwrap();
        tx_key
    }

    // The task also asserts that each circuit was dropped before its storage was removed.
    // Borrows so the storage stays alive for the assertions that follow.
    async fn join(&mut self) {
        within(&mut self.task).await.unwrap();
    }
}

struct Fixture {
    inner: IncrementalQueryServiceInner,
    drops: Vec<sync_mpsc::Receiver<bool>>,
    _storage: TempDir,
}

impl Fixture {
    async fn new(capacity: usize) -> Self {
        let storage = tempfile::tempdir().unwrap();
        let slate = crate::slate::in_memory_slate().await;
        let inner = IncrementalQueryServiceInner::new(
            start_pool(small_pool_config()).await.unwrap(),
            storage.path().to_path_buf(),
            CancellationToken::new(),
            IncrementalQueryOptions {
                inbox_capacity: NonZeroUsize::new(capacity).unwrap(),
                ..IncrementalQueryOptions::default()
            },
            slate.db.clone(),
            cdc::test_support::test_indexer(slate.db.clone()),
        );
        Self {
            inner,
            drops: vec![],
            _storage: storage,
        }
    }

    // Starts a registration whose build creates the storage and then the probe.
    fn register(&mut self, mut config: ProbeConfig) -> Pending {
        let (dropped, observed) = sync_mpsc::channel();
        // A build that panics makes no probe, so no drop is reported.
        if !config.build_panic {
            self.drops.push(observed);
        }
        let (steps_sender, steps) = mpsc::unbounded_channel();
        let (response, registration) = oneshot::channel();
        let handle = self.inner.start_query(
            *crate::bootstrap::BOOTSTRAP_TX_KEY,
            Box::new(move |storage| {
                std::fs::create_dir(storage)?;
                // The build runs on a blocking thread, so waiting here models a slow build.
                if let Some(gate) = config.build_gate.take() {
                    let _ = gate.recv();
                }
                assert!(!config.build_panic, "injected construction panic");
                Ok(Probe {
                    config,
                    steps: steps_sender,
                    storage: storage.to_path_buf(),
                    dropped,
                })
            }),
            response,
        );
        Pending {
            handle,
            registration,
            storage_path: self.inner.query_storage_path(handle),
            steps,
        }
    }

    async fn query_with(
        &mut self,
        config: ProbeConfig,
    ) -> (IncrementalQuerySubscription, mpsc::UnboundedReceiver<i64>) {
        let mut pending = self.register(config);
        (pending.result().await.unwrap(), pending.steps)
    }

    async fn query(&mut self) -> (IncrementalQuerySubscription, mpsc::UnboundedReceiver<i64>) {
        self.query_with(ProbeConfig::default()).await
    }

    // Registers a query whose every apply waits for a message on the returned sender.
    async fn gated_query(
        &mut self,
    ) -> (
        IncrementalQuerySubscription,
        mpsc::UnboundedReceiver<i64>,
        mpsc::UnboundedSender<()>,
    ) {
        let (release, gate) = mpsc::unbounded_channel();
        let (query, steps) = self
            .query_with(ProbeConfig {
                apply_gate: Some(gate),
                ..ProbeConfig::default()
            })
            .await;
        (query, steps, release)
    }

    // Starts a registration whose build waits for a message on the returned sender.
    fn gated_build(&mut self, priming: worker::Rows) -> (sync_mpsc::Sender<()>, Pending) {
        let (release, gate) = sync_mpsc::channel();
        let pending = self.register(ProbeConfig {
            build_gate: Some(gate),
            priming,
            ..ProbeConfig::default()
        });
        (release, pending)
    }

    fn apply(&self, seq: i64) -> TxKey {
        let batch = Arc::new(batch(seq));
        let tx_key = batch.tx_key;
        self.inner.apply_batch(batch);
        tx_key
    }

    fn stop(&self, handle: IncrementalQueryHandle) -> Arc<StopSignal> {
        self.inner.queries[&handle].stop.clone()
    }

    async fn retire(&mut self) -> IncrementalQueryHandle {
        let completion = within(self.inner.completions.recv()).await.unwrap();
        let handle = completion.handle;
        self.inner.retire(completion).unwrap();
        assert!(!self.inner.query_storage_path(handle).exists());
        handle
    }

    async fn finish(mut self) {
        for query in self.inner.queries.values() {
            query.stop.terminate(None);
        }
        while !self.inner.queries.is_empty() {
            self.retire().await;
        }
        assert_dropped(self.drops);
    }

    // Runs the dispatcher as a task, so the tests use the service as a caller does.
    fn start(self) -> (IncrementalQueryService, Dispatcher) {
        let (commands, command_receiver) = mpsc::unbounded_channel();
        let (batches, batch_receiver) = mpsc::unbounded_channel();
        let service = IncrementalQueryService {
            commands,
            indexer: self.inner.indexer.clone(),
            retire_timeout: WAIT,
            cdc_cancel: CancellationToken::new(),
            cdc_task: None,
        };
        let task = tokio::spawn(async move {
            self.inner.run(command_receiver, batch_receiver).await;
            assert_dropped(self.drops);
        });
        let dispatcher = Dispatcher {
            batches,
            task,
            _storage: self._storage,
        };
        (service, dispatcher)
    }
}

#[tokio::test]
async fn slow_query_preserves_fifo_while_other_query_advances() {
    let mut fixture = Fixture::new(8).await;
    let (mut slow, mut slow_steps, release) = fixture.gated_query().await;
    let (mut fast, mut fast_steps) = fixture.query().await;
    let first = fixture.apply(1);
    assert_eq!(started(&mut slow_steps).await, 1);
    assert_eq!(started(&mut fast_steps).await, 1);
    assert_eq!(next(&mut fast).await.unwrap().tx_key, first);
    let second = fixture.apply(2);
    let third = fixture.apply(3);
    assert_eq!(started(&mut fast_steps).await, 2);
    assert_eq!(started(&mut fast_steps).await, 3);
    assert_eq!(next(&mut fast).await.unwrap().tx_key, second);
    assert_eq!(next(&mut fast).await.unwrap().tx_key, third);
    assert!(slow_steps.try_recv().is_err());
    for (seq, tx_key) in [(1, first), (2, second), (3, third)] {
        release.send(()).unwrap();
        assert_eq!(next(&mut slow).await.unwrap().tx_key, tx_key);
        if seq < 3 {
            assert_eq!(started(&mut slow_steps).await, seq + 1);
        }
    }
    fixture.finish().await;
}

#[tokio::test]
async fn service_does_not_throttle_steps() {
    let mut fixture = Fixture::new(4).await;
    let mut queries = vec![];
    for _ in 0..3 {
        queries.push(fixture.gated_query().await);
    }
    fixture.apply(1);
    // Every step starts before any is released; limiting concurrency is the pool's job.
    for (_, steps, _) in &mut queries {
        assert_eq!(started(steps).await, 1);
    }
    for (query, _, release) in &mut queries {
        release.send(()).unwrap();
        next(query).await.unwrap();
    }
    fixture.finish().await;
}

#[tokio::test]
async fn overflow_suppresses_in_flight_result_and_discards_pending_work() {
    let mut fixture = Fixture::new(1).await;
    let (mut slow, mut slow_steps, release) = fixture.gated_query().await;
    let (mut healthy, _) = fixture.query().await;
    fixture.apply(1);
    started(&mut slow_steps).await;
    next(&mut healthy).await.unwrap();
    fixture.apply(2);
    next(&mut healthy).await.unwrap();
    let overflow = fixture.apply(3);
    assert_eq!(next(&mut healthy).await.unwrap().tx_key, overflow);
    let error = next(&mut slow).await.unwrap_err();
    let lagged = error.downcast_ref::<SubscriptionLagged>().unwrap();
    assert_eq!(lagged.tx_key, overflow);
    assert_eq!(lagged.capacity, 1);
    assert!(fixture.inner.query_storage_path(slow.handle).exists());
    assert!(fixture.inner.completions.try_recv().is_err());
    release.send(()).unwrap();
    assert_eq!(fixture.retire().await, slow.handle);
    assert!(slow_steps.try_recv().is_err());
    assert!(slow.deltas.recv().await.is_none());
    let fourth = fixture.apply(4);
    assert_eq!(next(&mut healthy).await.unwrap().tx_key, fourth);
    fixture.finish().await;
}

#[tokio::test]
async fn full_output_pauses_only_its_query_and_input_overflow_delivers_error() {
    let mut fixture = Fixture::new(1).await;
    let (mut slow, mut slow_steps, release) = fixture.gated_query().await;
    let (mut healthy, _) = fixture.query().await;
    for seq in 1..=SUBSCRIPTION_OUTPUT_CAPACITY as i64 + 1 {
        fixture.apply(seq);
        assert_eq!(started(&mut slow_steps).await, seq);
        release.send(()).unwrap();
        next(&mut healthy).await.unwrap();
    }
    fixture.apply(1000);
    next(&mut healthy).await.unwrap();
    let overflow = fixture.apply(1001);
    assert_eq!(next(&mut healthy).await.unwrap().tx_key, overflow);
    assert!(next(&mut slow)
        .await
        .unwrap_err()
        .downcast_ref::<SubscriptionLagged>()
        .is_some());
    assert_eq!(fixture.retire().await, slow.handle);
    fixture.finish().await;
}

#[tokio::test]
async fn full_output_can_resume_without_termination() {
    let mut fixture = Fixture::new(4).await;
    let (mut query, mut steps) = fixture.query().await;
    for seq in 1..=SUBSCRIPTION_OUTPUT_CAPACITY as i64 + 1 {
        fixture.apply(seq);
        assert_eq!(started(&mut steps).await, seq);
    }
    for seq in 1..=SUBSCRIPTION_OUTPUT_CAPACITY as i64 + 1 {
        assert_eq!(
            next(&mut query).await.unwrap().rows,
            vec![(vec![DataType::Long(seq)], 1)]
        );
    }
    assert!(!fixture.stop(query.handle).token.is_cancelled());
    fixture.finish().await;
}

#[tokio::test]
async fn panic_retires_only_affected_circuit() {
    let mut fixture = Fixture::new(4).await;
    let (mut failing, _) = fixture
        .query_with(ProbeConfig {
            apply_panic: true,
            ..ProbeConfig::default()
        })
        .await;
    let (mut healthy, _) = fixture.query().await;
    fixture.apply(1);
    assert!(next(&mut failing)
        .await
        .unwrap_err()
        .to_string()
        .contains("panicked"));
    next(&mut healthy).await.unwrap();
    assert_eq!(fixture.retire().await, failing.handle);
    let tx_key = fixture.apply(2);
    assert_eq!(next(&mut healthy).await.unwrap().tx_key, tx_key);
    fixture.finish().await;
}

#[tokio::test]
async fn disconnect_retires_without_another_transaction() {
    let mut fixture = Fixture::new(1).await;
    let (query, _) = fixture.query().await;
    let handle = query.handle;
    drop(query);
    assert_eq!(fixture.retire().await, handle);
    fixture.finish().await;
}

#[tokio::test]
async fn unregister_waits_for_apply_without_blocking_dispatch() {
    let mut fixture = Fixture::new(4).await;
    let (query, mut steps, release) = fixture.gated_query().await;
    let (mut healthy, _) = fixture.query().await;
    fixture.apply(1);
    started(&mut steps).await;
    next(&mut healthy).await.unwrap();
    let stop = fixture.stop(query.handle);
    let storage_path = fixture.inner.query_storage_path(query.handle);
    let (service, mut dispatcher) = fixture.start();
    // The test sends the command itself, so it does not wait for the answer.
    let (response, mut result) = oneshot::channel();
    service
        .commands
        .send(IncrementalCommand::Unregister {
            handle: query.handle,
            response,
        })
        .unwrap();
    within(stop.token.cancelled()).await;
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(storage_path.exists());
    let tx_key = dispatcher.apply(2).await;
    assert_eq!(next(&mut healthy).await.unwrap().tx_key, tx_key);
    release.send(()).unwrap();
    within(result).await.unwrap().unwrap();
    assert!(!storage_path.exists());
    drop(service);
    dispatcher.join().await;
}

#[tokio::test]
async fn shutdown_waits_for_in_flight_apply_and_removes_storage() {
    let mut fixture = Fixture::new(4).await;
    let (mut query, mut steps, release) = fixture.gated_query().await;
    fixture.apply(1);
    started(&mut steps).await;
    let stop = fixture.stop(query.handle);
    let storage_path = fixture.inner.query_storage_path(query.handle);
    let (service, mut dispatcher) = fixture.start();
    let (response, mut result) = oneshot::channel();
    service
        .commands
        .send(IncrementalCommand::Shutdown { response })
        .unwrap();
    within(stop.token.cancelled()).await;
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(storage_path.exists());
    release.send(()).unwrap();
    within(result).await.unwrap().unwrap();
    dispatcher.join().await;
    assert!(query.deltas.recv().await.is_none());
    assert!(!storage_path.exists());
}

#[tokio::test]
async fn unregister_timeout_preserves_storage_and_other_queries_keep_progressing() {
    let mut fixture = Fixture::new(4).await;
    let (mut query, mut steps, release) = fixture.gated_query().await;
    let (mut healthy, _) = fixture.query().await;
    fixture.apply(1);
    started(&mut steps).await;
    next(&mut healthy).await.unwrap();
    let storage_path = fixture.inner.query_storage_path(query.handle);
    let stop = fixture.stop(query.handle);
    let (mut service, mut dispatcher) = fixture.start();
    service.retire_timeout = SHORT_RETIRE_TIMEOUT;

    let error = within(service.unregister(query.handle)).await.unwrap_err();
    assert!(matches!(error.downcast_ref::<RetirementTimeout>(),
        Some(RetirementTimeout::Unregister { handle, .. }) if *handle == query.handle));
    assert!(stop.token.is_cancelled());
    assert!(storage_path.exists());
    let tx_key = dispatcher.apply(2).await;
    assert_eq!(next(&mut healthy).await.unwrap().tx_key, tx_key);

    release.send(()).unwrap();
    drop(service);
    dispatcher.join().await;
    assert!(!storage_path.exists());
    assert!(query.deltas.recv().await.is_none());
    assert!(steps.try_recv().is_err());
}

#[tokio::test]
async fn shutdown_timeout_preserves_storage_until_running_apply_finishes() {
    let mut fixture = Fixture::new(4).await;
    let (mut query, mut steps, release) = fixture.gated_query().await;
    fixture.apply(1);
    started(&mut steps).await;
    let storage_path = fixture.inner.query_storage_path(query.handle);
    let (mut service, mut dispatcher) = fixture.start();
    service.retire_timeout = SHORT_RETIRE_TIMEOUT;

    let error = within(service.shutdown()).await.unwrap_err();
    assert!(matches!(
        error.downcast_ref::<RetirementTimeout>(),
        Some(RetirementTimeout::Shutdown { .. })
    ));
    assert!(!dispatcher.task.is_finished());
    assert!(storage_path.exists());
    release.send(()).unwrap();
    dispatcher.join().await;
    assert!(!storage_path.exists());
    assert!(query.deltas.recv().await.is_none());
}

#[tokio::test]
async fn shutdown_stops_cdc_before_workers() {
    let mut fixture = Fixture::new(4).await;
    let (mut query, _) = fixture.query().await;
    let storage_path = fixture.inner.query_storage_path(query.handle);
    let (release, gate) = oneshot::channel::<()>();
    let (mut service, mut dispatcher) = fixture.start();
    service.retire_timeout = SHORT_RETIRE_TIMEOUT;
    service.cdc_task = Some(tokio::spawn(async move {
        let _ = gate.await;
        Ok(())
    }));

    let error = within(service.shutdown()).await.unwrap_err();
    assert!(matches!(
        error.downcast_ref::<RetirementTimeout>(),
        Some(RetirementTimeout::Shutdown { .. })
    ));
    // Shutdown still waits for CDC, so the dispatcher keeps the worker and its storage.
    assert!(!dispatcher.task.is_finished());
    assert!(storage_path.exists());
    release.send(()).unwrap();
    dispatcher.join().await;
    assert!(!storage_path.exists());
    assert!(query.deltas.recv().await.is_none());
}

#[tokio::test]
async fn shutdown_reports_cdc_error_and_cleanup_failure() {
    let mut fixture = Fixture::new(4).await;
    let (query, _) = fixture.query().await;
    let storage_path = fixture.inner.query_storage_path(query.handle);
    std::fs::remove_dir(&storage_path).unwrap();
    std::fs::write(&storage_path, "not a directory").unwrap();
    let (mut service, mut dispatcher) = fixture.start();
    service.cdc_task = Some(tokio::spawn(async { Err(anyhow!("cdc failed")) }));

    let message = format!("{:#}", service.shutdown().await.unwrap_err());
    assert!(
        message.contains("Incremental query CDC loop failed"),
        "{message}"
    );
    assert!(message.contains("cdc failed"), "{message}");
    assert!(
        message.contains("Failed to remove incremental query storage"),
        "{message}"
    );
    dispatcher.join().await;
}

#[tokio::test]
async fn shutdown_reports_cdc_task_panic() {
    let fixture = Fixture::new(4).await;
    let (mut service, mut dispatcher) = fixture.start();
    service.cdc_task = Some(tokio::spawn(async {
        panic!("cdc task panic");
        #[allow(unreachable_code)]
        Ok(())
    }));

    let message = format!("{:#}", service.shutdown().await.unwrap_err());
    assert!(
        message.contains("Incremental query CDC task failed to join"),
        "{message}"
    );
    dispatcher.join().await;
}

#[tokio::test]
async fn unregister_cleanup_failure_does_not_stop_dispatcher() {
    let mut fixture = Fixture::new(4).await;
    let (mut query, _) = fixture.query().await;
    let (mut healthy, _) = fixture.query().await;
    let storage_path = fixture.inner.query_storage_path(query.handle);
    std::fs::remove_dir(&storage_path).unwrap();
    std::fs::write(&storage_path, "not a directory").unwrap();
    let (service, mut dispatcher) = fixture.start();
    let error = service.unregister(query.handle).await.unwrap_err();
    assert!(error.downcast_ref::<std::io::Error>().is_some());
    assert!(error
        .to_string()
        .contains("Failed to remove incremental query storage"));
    // EOF requires retirement to remove the registry's remaining output sender.
    assert!(within(query.deltas.recv()).await.is_none());
    let tx_key = dispatcher.apply(1).await;
    assert_eq!(next(&mut healthy).await.unwrap().tx_key, tx_key);
    service.shutdown().await.unwrap();
    dispatcher.join().await;
}

#[tokio::test]
async fn unregister_is_not_blocked_by_registration() {
    let mut fixture = Fixture::new(4).await;
    let (query, _) = fixture.query().await;
    let (release, mut pending) = fixture.gated_build(vec![]);
    let (service, mut dispatcher) = fixture.start();
    // The build is still blocked, so this returns only if the dispatcher keeps serving.
    service.unregister(query.handle).await.unwrap();
    release.send(()).unwrap();
    let mut registered = pending.result().await.unwrap();
    let tx_key = dispatcher.apply(1).await;
    assert_eq!(next(&mut registered).await.unwrap().tx_key, tx_key);
    service.shutdown().await.unwrap();
    dispatcher.join().await;
}

#[tokio::test]
async fn shutdown_waits_for_pending_build_and_rejects_it() {
    let mut fixture = Fixture::new(4).await;
    let (release, mut pending) = fixture.gated_build(vec![]);
    let stop = fixture.stop(pending.handle);
    let (service, mut dispatcher) = fixture.start();
    let shutdown = tokio::spawn(service.shutdown());
    // The dispatcher stops the query when it starts the shutdown.
    within(stop.token.cancelled()).await;
    // A build cannot be interrupted, so shutdown waits for it.
    assert!(!shutdown.is_finished());
    release.send(()).unwrap();
    within(shutdown).await.unwrap().unwrap();
    // The worker stopped before priming, so the response channel closed.
    assert!(within(&mut pending.registration).await.is_err());
    assert!(!pending.storage_path.exists());
    dispatcher.join().await;
}

#[tokio::test]
async fn registration_panics_remove_storage_after_unwinding() {
    let mut fixture = Fixture::new(1).await;
    let build_panic = ProbeConfig {
        build_panic: true,
        ..ProbeConfig::default()
    };
    let prime_panic = ProbeConfig {
        prime_panic: true,
        ..ProbeConfig::default()
    };
    for config in [build_panic, prime_panic] {
        let mut pending = fixture.register(config);
        let error = pending.result().await.unwrap_err();
        assert!(error.to_string().contains("panicked"), "{error:#}");
        // The registration fails only after its storage is gone.
        assert!(!pending.storage_path.exists());
        assert_eq!(fixture.retire().await, pending.handle);
    }
    let (mut healthy, _) = fixture.query().await;
    let tx_key = fixture.apply(1);
    assert_eq!(next(&mut healthy).await.unwrap().tx_key, tx_key);
    fixture.finish().await;
}

#[tokio::test]
async fn batches_routed_during_build_follow_the_priming_delta() {
    let mut fixture = Fixture::new(2).await;
    let (release, mut pending) = fixture.gated_build(vec![(vec![DataType::Long(0)], 1)]);
    let first = fixture.apply(1);
    let second = fixture.apply(2);
    assert!(!fixture.stop(pending.handle).token.is_cancelled());
    release.send(()).unwrap();
    let mut query = pending.result().await.unwrap();
    assert_eq!(
        next(&mut query).await.unwrap(),
        IncrementalQueryDelta {
            tx_key: *crate::bootstrap::BOOTSTRAP_TX_KEY,
            rows: vec![(vec![DataType::Long(0)], 1)],
        }
    );
    assert_eq!(next(&mut query).await.unwrap().tx_key, first);
    assert_eq!(next(&mut query).await.unwrap().tx_key, second);
    fixture.finish().await;
}

#[tokio::test]
async fn inbox_overflow_during_build_fails_registration_with_lag() {
    let mut fixture = Fixture::new(1).await;
    let (release, mut pending) = fixture.gated_build(vec![]);
    fixture.apply(1);
    assert!(!fixture.stop(pending.handle).token.is_cancelled());
    let overflow = fixture.apply(2);
    release.send(()).unwrap();
    let error = pending.result().await.unwrap_err();
    let lagged = error.downcast_ref::<SubscriptionLagged>().unwrap();
    assert_eq!(lagged.tx_key, overflow);
    assert_eq!(lagged.capacity, 1);
    // The registration fails only after its storage is gone.
    assert!(!pending.storage_path.exists());
    assert_eq!(fixture.retire().await, pending.handle);
    fixture.finish().await;
}

#[tokio::test]
async fn stop_after_lag_during_build_keeps_the_lag() {
    let mut fixture = Fixture::new(1).await;
    let (release, mut pending) = fixture.gated_build(vec![]);
    fixture.apply(1);
    let overflow = fixture.apply(2);
    // A shutdown stops the query again before the worker reads the lag.
    fixture.stop(pending.handle).terminate(None);
    release.send(()).unwrap();
    let error = pending.result().await.unwrap_err();
    assert_eq!(
        error.downcast_ref::<SubscriptionLagged>().unwrap().tx_key,
        overflow
    );
    assert_eq!(fixture.retire().await, pending.handle);
    fixture.finish().await;
}
