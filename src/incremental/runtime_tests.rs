use super::*;
use std::sync::mpsc as sync_mpsc;
use std::time::Duration;
use tempfile::TempDir;

struct Probe {
    started: mpsc::UnboundedSender<i64>,
    release: Option<mpsc::UnboundedReceiver<()>>,
    panic: bool,
    storage: PathBuf,
    dropped: sync_mpsc::Sender<bool>,
}

impl Circuit for Probe {
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
    fn new(capacity: usize) -> Self {
        let storage = tempfile::tempdir().unwrap();
        let inner = IncrementalQueryServiceInner::new(
            storage.path().to_path_buf(),
            CancellationToken::new(),
            IncrementalQueryOptions {
                inbox_capacity: NonZeroUsize::new(capacity).unwrap(),
                ..IncrementalQueryOptions::default()
            },
        );
        Self {
            inner,
            drops: vec![],
            _storage: storage,
        }
    }

    fn query(
        &mut self,
        release: Option<mpsc::UnboundedReceiver<()>>,
        panic: bool,
    ) -> (IncrementalQuerySubscription, mpsc::UnboundedReceiver<i64>) {
        let handle = self.inner.allocate_query_id();
        let storage = self.inner.query_storage_path(handle);
        std::fs::create_dir(&storage).unwrap();
        let (started, events) = mpsc::unbounded_channel();
        let (dropped, observed) = sync_mpsc::channel();
        self.drops.push(observed);
        let probe = Probe {
            started,
            release,
            panic,
            storage,
            dropped,
        };
        let subscription =
            self.inner
                .install(handle, probe, *crate::bootstrap::BOOTSTRAP_TX_KEY, vec![]);
        (subscription, events)
    }

    // Starts a registration whose build blocks until `release` is sent.
    fn gated_build(
        &mut self,
    ) -> (
        sync_mpsc::Sender<()>,
        oneshot::Receiver<Result<IncrementalQuerySubscription>>,
        PathBuf,
    ) {
        let handle = self.inner.allocate_query_id();
        let storage_path = self.inner.query_storage_path(handle);
        let (release, gate) = sync_mpsc::channel::<()>();
        let (dropped, observed) = sync_mpsc::channel();
        self.drops.push(observed);
        let (response, registration) = oneshot::channel();
        self.inner.spawn_build(
            handle,
            *crate::bootstrap::BOOTSTRAP_TX_KEY,
            response,
            move |storage| {
                std::fs::create_dir(storage)?;
                // The build runs on a blocking thread, so waiting here models a slow priming.
                let _ = gate.recv();
                let (started, _) = mpsc::unbounded_channel();
                let probe = Probe {
                    started,
                    release: None,
                    panic: false,
                    storage: storage.to_path_buf(),
                    dropped,
                };
                Ok((probe, vec![]))
            },
        );
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

    fn start(self) -> (IncrementalQueryService, JoinHandle<()>, TempDir) {
        let (commands, receiver) = mpsc::unbounded_channel();
        let service = IncrementalQueryService {
            commands,
            cdc_object_path: "/test_retirement".into(),
            cdc_object_store: Arc::new(slatedb::object_store::memory::InMemory::new()),
            cdc_poll_interval: Duration::from_millis(10),
            cancel: self.inner.cancel.clone(),
            cdc_task: Arc::new(StdMutex::new(None)),
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
    let mut fixture = Fixture::new(8);
    let (release, gate) = mpsc::unbounded_channel();
    let (mut slow, mut slow_steps) = fixture.query(Some(gate), false);
    let (mut fast, mut fast_steps) = fixture.query(None, false);
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
    let mut fixture = Fixture::new(4);
    let mut queries = vec![];
    for _ in 0..3 {
        let (release, gate) = mpsc::unbounded_channel();
        let (query, steps) = fixture.query(Some(gate), false);
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
    let mut fixture = Fixture::new(1);
    let (release, gate) = mpsc::unbounded_channel();
    let (mut slow, mut slow_steps) = fixture.query(Some(gate), false);
    let (mut healthy, _) = fixture.query(None, false);
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
    let mut fixture = Fixture::new(1);
    let (release, gate) = mpsc::unbounded_channel();
    let (mut slow, mut slow_steps) = fixture.query(Some(gate), false);
    let (mut healthy, _) = fixture.query(None, false);
    for seq in 1..=SUBSCRIPTION_CAPACITY as i64 + 1 {
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
    let mut fixture = Fixture::new(4);
    let (mut query, mut steps) = fixture.query(None, false);
    for seq in 1..=SUBSCRIPTION_CAPACITY as i64 + 1 {
        fixture.apply(seq);
        assert_eq!(started(&mut steps).await, seq);
    }
    for seq in 1..=SUBSCRIPTION_CAPACITY as i64 + 1 {
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
    let mut fixture = Fixture::new(4);
    let (mut failing, _) = fixture.query(None, true);
    let (mut healthy, _) = fixture.query(None, false);
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
    let mut fixture = Fixture::new(1);
    let (query, _) = fixture.query(None, false);
    let handle = query.handle;
    drop(query);
    assert_eq!(fixture.retire().await, handle);
    fixture.finish().await;
}

#[tokio::test]
async fn unregister_waits_for_apply_without_blocking_dispatch() {
    let mut fixture = Fixture::new(4);
    let (release, gate) = mpsc::unbounded_channel();
    let (query, mut steps) = fixture.query(Some(gate), false);
    let (mut healthy, _) = fixture.query(None, false);
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
    let mut fixture = Fixture::new(4);
    let (release, gate) = mpsc::unbounded_channel();
    let (mut query, mut steps) = fixture.query(Some(gate), false);
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
    let mut fixture = Fixture::new(4);
    let (release, gate) = mpsc::unbounded_channel();
    let (mut query, mut steps) = fixture.query(Some(gate), false);
    let (mut healthy, _) = fixture.query(None, false);
    fixture.apply(1);
    started(&mut steps).await;
    next(&mut healthy).await.unwrap();
    let storage_path = fixture.inner.query_storage_path(query.handle);
    let control = fixture.inner.queries[&query.handle].control.clone();
    let (service, dispatcher, _storage) = fixture.start();

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
    let mut fixture = Fixture::new(4);
    let (release, gate) = mpsc::unbounded_channel();
    let (mut query, mut steps) = fixture.query(Some(gate), false);
    fixture.apply(1);
    started(&mut steps).await;
    let storage_path = fixture.inner.query_storage_path(query.handle);
    let (service, dispatcher, _storage) = fixture.start();

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
async fn shutdown_timeout_includes_cdc_and_still_requests_cleanup() {
    let mut fixture = Fixture::new(4);
    let (query, _) = fixture.query(None, false);
    let storage_path = fixture.inner.query_storage_path(query.handle);
    let (service, dispatcher, _storage) = fixture.start();
    let (release, gate) = oneshot::channel();
    let (finished, completion) = oneshot::channel();
    *service.cdc_task.lock().unwrap() = Some(tokio::spawn(async move {
        let _ = gate.await;
        let _ = finished.send(());
        Ok(())
    }));

    let error = tokio::time::timeout(Duration::from_secs(5), service.shutdown())
        .await
        .unwrap()
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<RetirementTimeout>(),
        Some(RetirementTimeout::Shutdown { .. })
    ));
    // The dispatcher must finish even though CDC has not returned.
    tokio::time::timeout(Duration::from_secs(5), dispatcher)
        .await
        .unwrap()
        .unwrap();
    assert!(!storage_path.exists());
    release.send(()).unwrap();
    completion.await.unwrap();
}

#[tokio::test]
async fn shutdown_reports_cleanup_failure_while_waiting_for_cdc() {
    let mut fixture = Fixture::new(4);
    let (mut query, _) = fixture.query(None, false);
    let storage_path = fixture.inner.query_storage_path(query.handle);
    std::fs::remove_dir(&storage_path).unwrap();
    std::fs::write(&storage_path, "not a directory").unwrap();
    let (mut service, dispatcher, _storage) = fixture.start();
    service.retire_timeout = Duration::from_secs(5);
    *service.cdc_task.lock().unwrap() = Some(tokio::spawn(async move {
        // Hold shutdown in the CDC wait until the dispatcher has retired the query.
        assert!(query.deltas.recv().await.is_none());
        Ok(())
    }));

    let error = service.shutdown().await.unwrap_err();
    assert!(error.downcast_ref::<std::io::Error>().is_some());
    assert!(error
        .to_string()
        .contains("Failed to remove incremental query storage"));
    tokio::time::timeout(Duration::from_secs(5), dispatcher)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn unregister_cleanup_failure_does_not_stop_dispatcher() {
    let mut fixture = Fixture::new(4);
    let (mut query, _) = fixture.query(None, false);
    let (mut healthy, _) = fixture.query(None, false);
    let storage_path = fixture.inner.query_storage_path(query.handle);
    std::fs::remove_dir(&storage_path).unwrap();
    std::fs::write(&storage_path, "not a directory").unwrap();
    let (mut service, dispatcher, _storage) = fixture.start();
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
    let mut fixture = Fixture::new(4);
    let (query, _) = fixture.query(None, false);
    let (release, registration, _) = fixture.gated_build();
    let (mut service, dispatcher, _storage) = fixture.start();
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
    let mut fixture = Fixture::new(4);
    let (release, registration, storage_path) = fixture.gated_build();
    let (mut service, dispatcher, _storage) = fixture.start();
    service.retire_timeout = Duration::from_secs(5);
    let shutdown = tokio::spawn(async move { service.shutdown().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    // A build cannot be interrupted, so shutdown waits for it.
    assert!(!shutdown.is_finished());
    release.send(()).unwrap();
    shutdown.await.unwrap().unwrap();
    let error = registration.await.unwrap().unwrap_err();
    assert!(error
        .to_string()
        .contains("Incremental query service stopped"));
    assert!(!storage_path.exists());
    tokio::time::timeout(Duration::from_secs(5), dispatcher)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn registration_panics_remove_storage_after_unwinding() {
    let mut fixture = Fixture::new(1);
    for panic_during_build in [true, false] {
        let handle = fixture.inner.allocate_query_id();
        let (dropped, observed) = sync_mpsc::channel();
        let error = prepare_query(fixture.inner.query_storage_path(handle), move |storage| {
            std::fs::create_dir(storage)?;
            assert!(!panic_during_build, "injected construction panic");
            let (started, _) = mpsc::unbounded_channel();
            let mut probe = Probe {
                started,
                release: None,
                panic: true,
                storage: storage.to_path_buf(),
                dropped,
            };
            let rows = futures::executor::block_on(probe.apply(&Fixture::batch(1)))?;
            Ok((probe, rows))
        })
        .await
        .err()
        .expect("registration should fail");
        assert!(error
            .downcast_ref::<tokio::task::JoinError>()
            .unwrap()
            .is_panic());
        if !panic_during_build {
            assert_dropped(vec![observed]);
        }
        assert!(!fixture.inner.query_storage_path(handle).exists());
    }
    let (mut healthy, _) = fixture.query(None, false);
    let tx_key = fixture.apply(1);
    assert_eq!(next(&mut healthy).await.unwrap().tx_key, tx_key);
    fixture.finish().await;
}
