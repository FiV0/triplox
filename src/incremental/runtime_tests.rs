use super::*;
use std::sync::mpsc as sync_mpsc;
use std::time::Duration;
use tempfile::TempDir;

struct Probe {
    started: mpsc::UnboundedSender<i64>,
    release: Option<sync_mpsc::Receiver<()>>,
    panic: bool,
    storage: PathBuf,
}

impl Circuit for Probe {
    fn apply(&mut self, triples: Vec<Tup2<EncodedTriple, ZWeight>>) -> Result<worker::Rows> {
        let seq = triples[0].0.attribute;
        let _ = self.started.send(seq);
        if let Some(release) = &self.release {
            let _ = release.recv();
        }
        assert!(!self.panic, "injected circuit panic");
        Ok(vec![(vec![DataType::Long(seq)], 1)])
    }
}

impl Drop for Probe {
    fn drop(&mut self) {
        assert!(
            self.storage.exists(),
            "storage removed before circuit destruction"
        );
    }
}

struct Fixture {
    inner: IncrementalQueryServiceInner,
    _storage: TempDir,
}

impl Fixture {
    fn new(capacity: usize, steps: usize) -> Self {
        let storage = tempfile::tempdir().unwrap();
        let inner = IncrementalQueryServiceInner::new(
            storage.path().to_path_buf(),
            Handle::current(),
            CancellationToken::new(),
            IncrementalQueryOptions {
                inbox_capacity: NonZeroUsize::new(capacity).unwrap(),
                max_concurrent_steps: NonZeroUsize::new(steps).unwrap(),
                ..IncrementalQueryOptions::default()
            },
        );
        Self {
            inner,
            _storage: storage,
        }
    }

    fn query(
        &mut self,
        release: Option<sync_mpsc::Receiver<()>>,
        panic: bool,
    ) -> (IncrementalQuerySubscription, mpsc::UnboundedReceiver<i64>) {
        let handle = self.inner.allocate_query_id();
        let storage = self.inner.query_storage_path(handle);
        std::fs::create_dir(&storage).unwrap();
        let (started, events) = mpsc::unbounded_channel();
        let probe = Probe {
            started,
            release,
            panic,
            storage,
        };
        let subscription = self.inner.install(
            handle,
            probe,
            *crate::bootstrap::BOOTSTRAP_TX_KEY,
            CdcCursor::default(),
            vec![],
        );
        (subscription, events)
    }

    fn apply(&mut self, seq: i64) -> TxKey {
        let mut tx_key = *crate::bootstrap::BOOTSTRAP_TX_KEY;
        tx_key.tx_id += seq;
        self.inner.apply_triples(Arc::new(Batch {
            tx_key,
            wal_seq: seq as u64,
            triples: vec![Tup2(
                EncodedTriple {
                    entity: vec![],
                    attribute: seq,
                    value: vec![],
                },
                1,
            )],
        }));
        tx_key
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
    }

    fn start(self) -> (IncrementalQueryService, JoinHandle<()>, TempDir) {
        let (commands, receiver) = mpsc::unbounded_channel();
        let service = IncrementalQueryService {
            commands,
            cdc_object_path: "/test_retirement".into(),
            cdc_object_store: Arc::new(slatedb::object_store::memory::InMemory::new()),
            cancel: self.inner.cancel.clone(),
            cdc_task: Arc::new(StdMutex::new(None)),
            registration_gate: Arc::new(Mutex::new(())),
            retire_timeout: Duration::from_millis(50),
        };
        let dispatcher = tokio::spawn(self.inner.run(receiver));
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
    let mut fixture = Fixture::new(8, 2);
    let (release, gate) = sync_mpsc::channel();
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
async fn shared_semaphore_limits_simultaneous_applies() {
    let mut fixture = Fixture::new(4, 1);
    let (release_a, gate_a) = sync_mpsc::channel();
    let (release_b, gate_b) = sync_mpsc::channel();
    let (mut a, mut steps_a) = fixture.query(Some(gate_a), false);
    let (mut b, mut steps_b) = fixture.query(Some(gate_b), false);
    fixture.apply(1);
    let a_first =
        tokio::select! { _ = started(&mut steps_a) => true, _ = started(&mut steps_b) => false };
    assert_eq!(fixture.inner.steps.available_permits(), 0);
    if a_first {
        assert!(steps_b.try_recv().is_err());
        release_a.send(()).unwrap();
        assert_eq!(started(&mut steps_b).await, 1);
        release_b.send(()).unwrap();
    } else {
        assert!(steps_a.try_recv().is_err());
        release_b.send(()).unwrap();
        assert_eq!(started(&mut steps_a).await, 1);
        release_a.send(()).unwrap();
    }
    next(&mut a).await.unwrap();
    next(&mut b).await.unwrap();
    fixture.finish().await;
}

#[tokio::test]
async fn overflow_suppresses_in_flight_result_and_discards_pending_work() {
    let mut fixture = Fixture::new(1, 2);
    let (release, gate) = sync_mpsc::channel();
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
async fn full_output_releases_permit_and_input_overflow_delivers_error() {
    let mut fixture = Fixture::new(1, 1);
    let (release, gate) = sync_mpsc::channel();
    let (mut slow, mut slow_steps) = fixture.query(Some(gate), false);
    let (mut healthy, _) = fixture.query(None, false);
    for seq in 1..=SUBSCRIPTION_CAPACITY as i64 + 1 {
        fixture.apply(seq);
        assert_eq!(started(&mut slow_steps).await, seq);
        release.send(()).unwrap();
        // With one permit, this also proves output delivery does not retain the permit.
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
    let mut fixture = Fixture::new(4, 1);
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
    let mut fixture = Fixture::new(4, 1);
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
    let mut fixture = Fixture::new(1, 1);
    let (query, _) = fixture.query(None, false);
    let handle = query.handle;
    drop(query);
    assert_eq!(fixture.retire().await, handle);
    fixture.finish().await;
}

#[tokio::test]
async fn cancellation_while_waiting_for_permit_does_not_apply() {
    let mut fixture = Fixture::new(2, 1);
    let permit = fixture.inner.steps.clone().acquire_owned().await.unwrap();
    let (query, mut steps) = fixture.query(None, false);
    fixture.apply(1);
    fixture.inner.queries[&query.handle].control.terminate(None);
    drop(permit);
    assert_eq!(fixture.retire().await, query.handle);
    assert!(steps.try_recv().is_err());
    fixture.finish().await;
}

#[tokio::test]
async fn unregister_waits_for_apply_without_blocking_dispatch() {
    let mut fixture = Fixture::new(4, 2);
    let (release, gate) = sync_mpsc::channel();
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
        .send(IncrementalCommand::ApplyTriples {
            batch: Arc::new(Batch {
                tx_key,
                wal_seq: 2,
                triples: vec![Tup2(
                    EncodedTriple {
                        entity: vec![],
                        attribute: 2,
                        value: vec![],
                    },
                    1,
                )],
            }),
            response,
        })
        .unwrap();
    applied.await.unwrap().unwrap();
    assert_eq!(next(&mut healthy).await.unwrap().tx_key, tx_key);
    release.send(()).unwrap();
    result.await.unwrap().unwrap();
    assert!(!storage_path.exists());
    drop(commands);
    dispatcher.await.unwrap();
}

#[tokio::test]
async fn shutdown_waits_for_in_flight_apply_and_removes_storage() {
    let mut fixture = Fixture::new(4, 1);
    let (release, gate) = sync_mpsc::channel();
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
    assert!(query.deltas.recv().await.is_none());
    assert!(!storage_path.exists());
}

#[tokio::test]
async fn unregister_timeout_preserves_storage_and_other_queries_keep_progressing() {
    let mut fixture = Fixture::new(4, 2);
    let (release, gate) = sync_mpsc::channel();
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
        .apply_triples(
            tx_key,
            2,
            vec![Tup2(
                EncodedTriple {
                    entity: vec![],
                    attribute: 2,
                    value: vec![],
                },
                1,
            )],
        )
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
    let mut fixture = Fixture::new(4, 1);
    let (release, gate) = sync_mpsc::channel();
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
    let mut fixture = Fixture::new(4, 1);
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
async fn cleanup_failure_does_not_fail_other_dispatches() {
    let mut fixture = Fixture::new(4, 1);
    let (query, _) = fixture.query(None, false);
    let (mut healthy, _) = fixture.query(None, false);
    let storage_path = fixture.inner.query_storage_path(query.handle);
    std::fs::remove_dir(&storage_path).unwrap();
    std::fs::write(&storage_path, "not a directory").unwrap();
    drop(query);
    let completion = tokio::time::timeout(Duration::from_secs(5), fixture.inner.completions.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(fixture.inner.retire(completion).is_err());
    let tx_key = fixture.apply(1);
    assert_eq!(next(&mut healthy).await.unwrap().tx_key, tx_key);
    fixture.finish().await;
}

#[tokio::test]
async fn registration_panics_remove_storage_after_unwinding() {
    let mut fixture = Fixture::new(1, 1);
    for panic_during_build in [true, false] {
        let handle = fixture.inner.allocate_query_id();
        let error = fixture
            .inner
            .prepare_query(handle, move |storage| {
                std::fs::create_dir(storage)?;
                assert!(!panic_during_build, "injected construction panic");
                let (started, _) = mpsc::unbounded_channel();
                let mut probe = Probe {
                    started,
                    release: None,
                    panic: true,
                    storage: storage.to_path_buf(),
                };
                let rows = probe.apply(vec![Tup2(
                    EncodedTriple {
                        entity: vec![],
                        attribute: 1,
                        value: vec![],
                    },
                    1,
                )])?;
                Ok((probe, rows))
            })
            .await
            .err()
            .expect("registration should fail");
        assert!(error
            .downcast_ref::<tokio::task::JoinError>()
            .unwrap()
            .is_panic());
        assert!(!fixture.inner.query_storage_path(handle).exists());
    }
    let (mut healthy, _) = fixture.query(None, false);
    let tx_key = fixture.apply(1);
    assert_eq!(next(&mut healthy).await.unwrap().tx_key, tx_key);
    fixture.finish().await;
}
