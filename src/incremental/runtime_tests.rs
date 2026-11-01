use super::*;
use dbsp::utils::Tup2;
use std::sync::mpsc as sync_mpsc;
use std::time::Duration;
use tempfile::TempDir;

struct Probe {
    started: mpsc::UnboundedSender<i64>,
    release: Option<mpsc::UnboundedReceiver<()>>,
    panic: bool,
    prime_panic: bool,
    priming: worker::Rows,
    storage: PathBuf,
    dropped: sync_mpsc::Sender<bool>,
}

impl Circuit for Probe {
    async fn prime(&mut self, _basis: TxKey) -> Result<worker::Rows> {
        assert!(!self.prime_panic, "injected priming panic");
        Ok(std::mem::take(&mut self.priming))
    }

    fn matches(&self, _batch: &Batch) -> bool {
        true
    }

    async fn apply(&mut self, batch: &Batch) -> Result<worker::Rows> {
        let seq = batch.triples()[0].0.attribute;
        let _ = self.started.send(seq);
        if let Some(release) = &mut self.release {
            let _ = release.recv().await;
        }
        assert!(!self.panic, "injected circuit panic");
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

struct Fixture {
    inner: IncrementalQueryServiceInner,
    drops: Vec<sync_mpsc::Receiver<bool>>,
    _storage: TempDir,
}

impl Fixture {
    async fn new(capacity: usize) -> Self {
        let storage = tempfile::tempdir().unwrap();
        let inner = IncrementalQueryServiceInner::new(
            storage.path().to_path_buf(),
            CancellationToken::new(),
            IncrementalQueryOptions {
                inbox_capacity: NonZeroUsize::new(capacity).unwrap(),
                ..IncrementalQueryOptions::default()
            },
            crate::slate::in_memory_slate().await.db,
        );
        Self {
            inner,
            drops: vec![],
            _storage: storage,
        }
    }

    // Starts a registration whose build creates the storage and then the probe.
    fn register(
        &mut self,
        gate: Option<sync_mpsc::Receiver<()>>,
        priming: worker::Rows,
        probe: impl FnOnce(PathBuf, sync_mpsc::Sender<bool>) -> Probe + Send + 'static,
    ) -> (
        oneshot::Receiver<Result<IncrementalQuerySubscription>>,
        PathBuf,
    ) {
        let (dropped, observed) = sync_mpsc::channel();
        self.drops.push(observed);
        let (response, registration) = oneshot::channel();
        let handle = self.inner.start_query(
            *crate::bootstrap::BOOTSTRAP_TX_KEY,
            Box::new(move |storage| {
                std::fs::create_dir(storage)?;
                // The build runs on a blocking thread, so waiting here models a slow build.
                if let Some(gate) = gate {
                    let _ = gate.recv();
                }
                let mut probe = probe(storage.to_path_buf(), dropped);
                probe.priming = priming;
                Ok(probe)
            }),
            response,
        );
        (registration, self.inner.query_storage_path(handle))
    }

    async fn query(
        &mut self,
        release: Option<mpsc::UnboundedReceiver<()>>,
        panic: bool,
    ) -> (IncrementalQuerySubscription, mpsc::UnboundedReceiver<i64>) {
        let (started, events) = mpsc::unbounded_channel();
        let (registration, _) = self.register(None, vec![], move |storage, dropped| Probe {
            started,
            release,
            panic,
            prime_panic: false,
            priming: vec![],
            storage,
            dropped,
        });
        (registration.await.unwrap().unwrap(), events)
    }

    // Starts a registration whose build blocks until `release` is sent.
    fn gated_build(
        &mut self,
        priming: worker::Rows,
    ) -> (
        sync_mpsc::Sender<()>,
        oneshot::Receiver<Result<IncrementalQuerySubscription>>,
        PathBuf,
    ) {
        let (release, gate) = sync_mpsc::channel::<()>();
        let (registration, storage_path) =
            self.register(Some(gate), priming, |storage, dropped| {
                let (started, _) = mpsc::unbounded_channel();
                Probe {
                    started,
                    release: None,
                    panic: false,
                    prime_panic: false,
                    priming: vec![],
                    storage,
                    dropped,
                }
            });
        (release, registration, storage_path)
    }

    fn batch(seq: i64) -> Arc<Batch> {
        let mut tx_key = *crate::bootstrap::BOOTSTRAP_TX_KEY;
        tx_key.tx_id += seq;
        Arc::new(Batch::single(
            tx_key,
            vec![Tup2(
                EncodedTriple {
                    entity: vec![],
                    attribute: seq,
                    value: vec![],
                },
                1,
            )],
        ))
    }

    fn apply(&mut self, seq: i64) -> TxKey {
        let batch = Self::batch(seq);
        self.inner.apply_batch(batch.clone());
        batch.tx_key
    }

    async fn retire(&mut self) -> IncrementalQueryHandle {
        let completion =
            tokio::time::timeout(Duration::from_secs(5), self.inner.completions.recv())
                .await
                .unwrap()
                .unwrap();
        let handle = completion.handle;
        self.inner.retire(completion).unwrap();
        assert!(!self.inner.query_storage_path(handle).exists());
        handle
    }

    async fn finish(mut self) {
        for query in self.inner.queries.values() {
            query.control.terminate(None);
        }
        while !self.inner.queries.is_empty() {
            self.retire().await;
        }
        assert_dropped(self.drops);
    }

    async fn start(self) -> (IncrementalQueryService, JoinHandle<()>, TempDir) {
        let (commands, receiver) = mpsc::unbounded_channel();
        let service = IncrementalQueryService {
            commands,
            indexer: cdc::test_support::test_indexer(self.inner.db.clone()),
            registration_gate: Arc::new(Mutex::new(())),
            retire_timeout: Duration::from_millis(50),
        };
        let dispatcher = tokio::spawn(async move {
            self.inner.run(receiver).await;
            assert_dropped(self.drops);
        });
        (service, dispatcher, self._storage)
    }
}

async fn started(events: &mut mpsc::UnboundedReceiver<i64>) -> i64 {
    tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap()
}

async fn next(query: &mut IncrementalQuerySubscription) -> Result<IncrementalQueryDelta> {
    tokio::time::timeout(Duration::from_secs(5), query.deltas.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn slow_query_preserves_fifo_while_other_query_advances() {
    let mut fixture = Fixture::new(8).await;
    let (release, gate) = mpsc::unbounded_channel();
    let (mut slow, mut slow_steps) = fixture.query(Some(gate), false).await;
    let (mut fast, mut fast_steps) = fixture.query(None, false).await;
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
        let (release, gate) = mpsc::unbounded_channel();
        let (query, steps) = fixture.query(Some(gate), false).await;
        queries.push((release, query, steps));
    }
    fixture.apply(1);
    // Every step starts before any is released; limiting concurrency is the pool's job.
    for (_, _, steps) in &mut queries {
        assert_eq!(started(steps).await, 1);
    }
    for (release, query, _) in &mut queries {
        release.send(()).unwrap();
        next(query).await.unwrap();
    }
    fixture.finish().await;
}

#[tokio::test]
async fn overflow_suppresses_in_flight_result_and_discards_pending_work() {
    let mut fixture = Fixture::new(1).await;
    let (release, gate) = mpsc::unbounded_channel();
    let (mut slow, mut slow_steps) = fixture.query(Some(gate), false).await;
    let (mut healthy, _) = fixture.query(None, false).await;
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
    let (release, gate) = mpsc::unbounded_channel();
    let (mut slow, mut slow_steps) = fixture.query(Some(gate), false).await;
    let (mut healthy, _) = fixture.query(None, false).await;
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
    let (mut query, mut steps) = fixture.query(None, false).await;
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
    assert!(!fixture.inner.queries[&query.handle]
        .control
        .stop
        .is_cancelled());
    fixture.finish().await;
}

#[tokio::test]
async fn panic_retires_only_affected_circuit() {
    let mut fixture = Fixture::new(4).await;
    let (mut failing, _) = fixture.query(None, true).await;
    let (mut healthy, _) = fixture.query(None, false).await;
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
    let (query, _) = fixture.query(None, false).await;
    let handle = query.handle;
    drop(query);
    assert_eq!(fixture.retire().await, handle);
    fixture.finish().await;
}

#[tokio::test]
async fn unregister_waits_for_apply_without_blocking_dispatch() {
    let mut fixture = Fixture::new(4).await;
    let (release, gate) = mpsc::unbounded_channel();
    let (query, mut steps) = fixture.query(Some(gate), false).await;
    let (mut healthy, _) = fixture.query(None, false).await;
    fixture.apply(1);
    started(&mut steps).await;
    next(&mut healthy).await.unwrap();
    let control = fixture.inner.queries[&query.handle].control.clone();
    let storage_path = fixture.inner.query_storage_path(query.handle);
    let (commands, receiver) = mpsc::unbounded_channel();
    let dispatcher = tokio::spawn(fixture.inner.run(receiver));
    let (response, mut result) = oneshot::channel();
    commands
        .send(IncrementalCommand::Unregister {
            handle: query.handle,
            response,
        })
        .unwrap();
    control.stop.cancelled().await;
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(storage_path.exists());
    let mut tx_key = *crate::bootstrap::BOOTSTRAP_TX_KEY;
    tx_key.tx_id += 2;
    let (response, applied) = oneshot::channel();
    commands
        .send(IncrementalCommand::ApplyBatch {
            batch: Arc::new(Batch::single(
                tx_key,
                vec![Tup2(
                    EncodedTriple {
                        entity: vec![],
                        attribute: 2,
                        value: vec![],
                    },
                    1,
                )],
            )),
            response,
        })
        .unwrap();
    applied.await.unwrap();
    assert_eq!(next(&mut healthy).await.unwrap().tx_key, tx_key);
    release.send(()).unwrap();
    result.await.unwrap().unwrap();
    assert!(!storage_path.exists());
    drop(commands);
    dispatcher.await.unwrap();
    assert_dropped(fixture.drops);
}

#[tokio::test]
async fn shutdown_waits_for_in_flight_apply_and_removes_storage() {
    let mut fixture = Fixture::new(4).await;
    let (release, gate) = mpsc::unbounded_channel();
    let (mut query, mut steps) = fixture.query(Some(gate), false).await;
    fixture.apply(1);
    started(&mut steps).await;
    let control = fixture.inner.queries[&query.handle].control.clone();
    let storage_path = fixture.inner.query_storage_path(query.handle);
    let (commands, receiver) = mpsc::unbounded_channel();
    let dispatcher = tokio::spawn(fixture.inner.run(receiver));
    let (response, mut result) = oneshot::channel();
    commands
        .send(IncrementalCommand::Shutdown { response })
        .unwrap();
    control.stop.cancelled().await;
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(storage_path.exists());
    release.send(()).unwrap();
    result.await.unwrap().unwrap();
    dispatcher.await.unwrap();
    assert_dropped(fixture.drops);
    assert!(query.deltas.recv().await.is_none());
    assert!(!storage_path.exists());
}

#[tokio::test]
async fn unregister_timeout_preserves_storage_and_other_queries_keep_progressing() {
    let mut fixture = Fixture::new(4).await;
    let (release, gate) = mpsc::unbounded_channel();
    let (mut query, mut steps) = fixture.query(Some(gate), false).await;
    let (mut healthy, _) = fixture.query(None, false).await;
    fixture.apply(1);
    started(&mut steps).await;
    next(&mut healthy).await.unwrap();
    let storage_path = fixture.inner.query_storage_path(query.handle);
    let control = fixture.inner.queries[&query.handle].control.clone();
    let (service, dispatcher, _storage) = fixture.start().await;

    let error = tokio::time::timeout(Duration::from_secs(5), service.unregister(query.handle))
        .await
        .unwrap()
        .unwrap_err();
    assert!(matches!(error.downcast_ref::<RetirementTimeout>(),
        Some(RetirementTimeout::Unregister { handle, .. }) if *handle == query.handle));
    assert!(control.stop.is_cancelled());
    assert!(storage_path.exists());
    let mut tx_key = *crate::bootstrap::BOOTSTRAP_TX_KEY;
    tx_key.tx_id += 2;
    service
        .apply_batch(Batch::single(
            tx_key,
            vec![Tup2(
                EncodedTriple {
                    entity: vec![],
                    attribute: 2,
                    value: vec![],
                },
                1,
            )],
        ))
        .await
        .unwrap();
    assert_eq!(next(&mut healthy).await.unwrap().tx_key, tx_key);

    release.send(()).unwrap();
    drop(service);
    tokio::time::timeout(Duration::from_secs(5), dispatcher)
        .await
        .unwrap()
        .unwrap();
    assert!(!storage_path.exists());
    assert!(query.deltas.recv().await.is_none());
    assert!(steps.try_recv().is_err());
}

#[tokio::test]
async fn shutdown_timeout_preserves_storage_until_running_apply_finishes() {
    let mut fixture = Fixture::new(4).await;
    let (release, gate) = mpsc::unbounded_channel();
    let (mut query, mut steps) = fixture.query(Some(gate), false).await;
    fixture.apply(1);
    started(&mut steps).await;
    let storage_path = fixture.inner.query_storage_path(query.handle);
    let (service, dispatcher, _storage) = fixture.start().await;

    let error = tokio::time::timeout(Duration::from_secs(5), service.shutdown())
        .await
        .unwrap()
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<RetirementTimeout>(),
        Some(RetirementTimeout::Shutdown { .. })
    ));
    assert!(!dispatcher.is_finished());
    assert!(storage_path.exists());
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), dispatcher)
        .await
        .unwrap()
        .unwrap();
    assert!(!storage_path.exists());
    assert!(query.deltas.recv().await.is_none());
}

#[tokio::test]
async fn shutdown_stops_cdc_before_workers() {
    let mut fixture = Fixture::new(4).await;
    let (mut query, _) = fixture.query(None, false).await;
    let storage_path = fixture.inner.query_storage_path(query.handle);
    let (release, gate) = oneshot::channel::<()>();
    fixture.inner.cdc_task = Some(tokio::spawn(async move {
        let _ = gate.await;
        Ok(())
    }));
    let (service, dispatcher, _storage) = fixture.start().await;

    let error = tokio::time::timeout(Duration::from_secs(5), service.shutdown())
        .await
        .unwrap()
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<RetirementTimeout>(),
        Some(RetirementTimeout::Shutdown { .. })
    ));
    // The dispatcher still waits for CDC, so the worker keeps its storage.
    assert!(!dispatcher.is_finished());
    assert!(storage_path.exists());
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), dispatcher)
        .await
        .unwrap()
        .unwrap();
    assert!(!storage_path.exists());
    assert!(query.deltas.recv().await.is_none());
}

#[tokio::test]
async fn shutdown_reports_cdc_error_and_cleanup_failure() {
    let mut fixture = Fixture::new(4).await;
    let (query, _) = fixture.query(None, false).await;
    let storage_path = fixture.inner.query_storage_path(query.handle);
    std::fs::remove_dir(&storage_path).unwrap();
    std::fs::write(&storage_path, "not a directory").unwrap();
    fixture.inner.cdc_task = Some(tokio::spawn(async { Err(anyhow!("cdc failed")) }));
    let (mut service, dispatcher, _storage) = fixture.start().await;
    service.retire_timeout = Duration::from_secs(5);

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
    tokio::time::timeout(Duration::from_secs(5), dispatcher)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn shutdown_reports_cdc_task_panic() {
    let mut fixture = Fixture::new(4).await;
    fixture.inner.cdc_task = Some(tokio::spawn(async {
        panic!("cdc task panic");
        #[allow(unreachable_code)]
        Ok(())
    }));
    let (mut service, dispatcher, _storage) = fixture.start().await;
    service.retire_timeout = Duration::from_secs(5);

    let message = format!("{:#}", service.shutdown().await.unwrap_err());
    assert!(
        message.contains("Incremental query CDC task failed to join"),
        "{message}"
    );
    tokio::time::timeout(Duration::from_secs(5), dispatcher)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn unregister_cleanup_failure_does_not_stop_dispatcher() {
    let mut fixture = Fixture::new(4).await;
    let (mut query, _) = fixture.query(None, false).await;
    let (mut healthy, _) = fixture.query(None, false).await;
    let storage_path = fixture.inner.query_storage_path(query.handle);
    std::fs::remove_dir(&storage_path).unwrap();
    std::fs::write(&storage_path, "not a directory").unwrap();
    let (mut service, dispatcher, _storage) = fixture.start().await;
    service.retire_timeout = Duration::from_secs(5);
    let error = service.unregister(query.handle).await.unwrap_err();
    assert!(error.downcast_ref::<std::io::Error>().is_some());
    assert!(error
        .to_string()
        .contains("Failed to remove incremental query storage"));
    // EOF requires retirement to remove the registry's remaining output sender.
    assert!(
        tokio::time::timeout(Duration::from_secs(5), query.deltas.recv())
            .await
            .unwrap()
            .is_none()
    );
    let mut tx_key = *crate::bootstrap::BOOTSTRAP_TX_KEY;
    tx_key.tx_id += 1;
    tokio::time::timeout(
        Duration::from_secs(5),
        service.apply_batch(Batch::single(
            tx_key,
            vec![Tup2(
                EncodedTriple {
                    entity: vec![],
                    attribute: 1,
                    value: vec![],
                },
                1,
            )],
        )),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(next(&mut healthy).await.unwrap().tx_key, tx_key);
    service.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), dispatcher)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn unregister_is_not_blocked_by_registration() {
    let mut fixture = Fixture::new(4).await;
    let (query, _) = fixture.query(None, false).await;
    let (release, registration, _) = fixture.gated_build(vec![]);
    let (mut service, dispatcher, _storage) = fixture.start().await;
    service.retire_timeout = Duration::from_secs(5);
    // The build is still blocked, so this returns only if the dispatcher keeps serving.
    service.unregister(query.handle).await.unwrap();
    release.send(()).unwrap();
    let mut registered = tokio::time::timeout(Duration::from_secs(5), registration)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let batch = Fixture::batch(1);
    service
        .apply_batch(Batch::single(batch.tx_key, batch.triples().to_vec()))
        .await
        .unwrap();
    assert_eq!(next(&mut registered).await.unwrap().tx_key, batch.tx_key);
    service.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), dispatcher)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn shutdown_waits_for_pending_build_and_rejects_it() {
    let mut fixture = Fixture::new(4).await;
    let (release, registration, storage_path) = fixture.gated_build(vec![]);
    let (mut service, dispatcher, _storage) = fixture.start().await;
    service.retire_timeout = Duration::from_secs(5);
    let shutdown = tokio::spawn(async move { service.shutdown().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    // A build cannot be interrupted, so shutdown waits for it.
    assert!(!shutdown.is_finished());
    release.send(()).unwrap();
    shutdown.await.unwrap().unwrap();
    // The worker stopped before it took Prime, so the response channel closed.
    assert!(registration.await.is_err());
    assert!(!storage_path.exists());
    tokio::time::timeout(Duration::from_secs(5), dispatcher)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn registration_panics_remove_storage_after_unwinding() {
    let mut fixture = Fixture::new(1).await;
    for panic_during_build in [true, false] {
        let (dropped, observed) = sync_mpsc::channel();
        let (response, registration) = oneshot::channel();
        let handle = fixture.inner.start_query(
            *crate::bootstrap::BOOTSTRAP_TX_KEY,
            Box::new(move |storage| {
                std::fs::create_dir(storage)?;
                assert!(!panic_during_build, "injected construction panic");
                let (started, _) = mpsc::unbounded_channel();
                Ok(Probe {
                    started,
                    release: None,
                    panic: false,
                    prime_panic: true,
                    priming: vec![],
                    storage: storage.to_path_buf(),
                    dropped,
                })
            }),
            response,
        );
        let error = registration.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("panicked"), "{error:#}");
        // The registration fails only after its storage is gone.
        assert!(!fixture.inner.query_storage_path(handle).exists());
        assert_eq!(fixture.retire().await, handle);
        if !panic_during_build {
            assert_dropped(vec![observed]);
        }
    }
    let (mut healthy, _) = fixture.query(None, false).await;
    let tx_key = fixture.apply(1);
    assert_eq!(next(&mut healthy).await.unwrap().tx_key, tx_key);
    fixture.finish().await;
}

#[tokio::test]
async fn batches_routed_during_build_follow_the_priming_delta() {
    let mut fixture = Fixture::new(4).await;
    let (release, registration, _) = fixture.gated_build(vec![(vec![DataType::Long(0)], 1)]);
    let first = fixture.apply(1);
    let second = fixture.apply(2);
    release.send(()).unwrap();
    let mut query = tokio::time::timeout(Duration::from_secs(5), registration)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
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
async fn inbox_overflow_during_build_returns_lagged_subscription() {
    let mut fixture = Fixture::new(1).await;
    let (release, registration, _) = fixture.gated_build(vec![]);
    // Prime fills the inbox while the build blocks.
    let overflow = fixture.apply(1);
    release.send(()).unwrap();
    let mut query = tokio::time::timeout(Duration::from_secs(5), registration)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let error = next(&mut query).await.unwrap_err();
    let lagged = error.downcast_ref::<SubscriptionLagged>().unwrap();
    assert_eq!(lagged.tx_key, overflow);
    assert_eq!(lagged.capacity, 1);
    assert_eq!(fixture.retire().await, query.handle);
    assert!(query.deltas.recv().await.is_none());
    fixture.finish().await;
}
