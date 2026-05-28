//! Writer-node incremental query service.

use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex as StdMutex};
use std::thread;

use anyhow::{anyhow, Result};
use dbsp::{utils::Tup2, ZWeight};
use slatedb::object_store::ObjectStore;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use triplox_client::transaction::TxBasis;

use crate::inc_query::IncrementalQueryPlan;
use crate::incremental::cdc::{scan_current_triples, spawn_cdc_loop};
use crate::incremental::circuit::QueryCircuit;
use crate::ops::DataType;
use crate::slate::cdc::CdcCursor;

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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct IncrementalQueryHandle {
    id: IncrementalQueryId,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct IncrementalQueryId(u64);

#[derive(Debug)]
pub(crate) struct IncrementalQuerySubscription {
    pub handle: IncrementalQueryHandle,
    pub basis: TxBasis,
    pub deltas: mpsc::Receiver<IncrementalQueryDelta>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IncrementalQueryDelta {
    pub basis: Option<TxBasis>,
    pub wal_seq: u64,
    pub rows: Vec<(Vec<DataType>, isize)>,
}

#[derive(Clone)]
pub(crate) struct IncrementalQueryService {
    commands: std_mpsc::Sender<IncrementalCommand>,
    cdc_path: String,
    cdc_object_store: Arc<dyn ObjectStore>,
    cancel: CancellationToken,
    cdc_started: Arc<AtomicBool>,
    cdc_task: Arc<StdMutex<Option<JoinHandle<()>>>>,
    registration_gate: Arc<Mutex<()>>,
    #[cfg(test)]
    registration_pause: Arc<Mutex<Option<IncrementalRegistrationPause>>>,
}

impl IncrementalQueryService {
    pub(crate) fn new_with_cancel(
        storage_root: PathBuf,
        runtime: Handle,
        cancel: CancellationToken,
        cdc_path: String,
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
            cdc_path,
            cdc_object_store,
            cancel,
            cdc_started: Arc::new(AtomicBool::new(false)),
            cdc_task: Arc::new(StdMutex::new(None)),
            registration_gate: Arc::new(Mutex::new(())),
            #[cfg(test)]
            registration_pause: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn registration_gate(&self) -> Arc<Mutex<()>> {
        self.registration_gate.clone()
    }

    pub(crate) async fn register_query_snapshot(
        &self,
        db: &slatedb::Db,
        plan: IncrementalQueryPlan,
        basis: TxBasis,
    ) -> Result<IncrementalQuerySubscription> {
        let initial_triples = scan_current_triples(db, &plan, basis.tx_eid).await?;
        #[cfg(test)]
        self.pause_registration_after_snapshot_for_test().await;
        let wal_cursor = CdcCursor {
            wal_id: 0,
            last_seq: db.status().durable_seq,
        };
        self.register(plan, basis, wal_cursor, initial_triples)
            .await
    }

    pub(crate) fn start_cdc_once<N>(&self, node: Arc<N>)
    where
        N: crate::node::InternalNode,
    {
        if self
            .cdc_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let handle = spawn_cdc_loop(
                self.cdc_path.clone(),
                self.cdc_object_store.clone(),
                node,
                self.clone(),
                self.registration_gate.clone(),
                self.cancel.clone(),
            );
            *self.cdc_task.lock().unwrap() = Some(handle);
        }
    }

    pub(crate) async fn await_cdc_task(&self) {
        let handle = self.cdc_task.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.await.unwrap();
        }
    }

    pub(crate) async fn register(
        &self,
        plan: IncrementalQueryPlan,
        basis: TxBasis,
        wal_cursor: CdcCursor,
        initial_triples: Vec<Tup2<EncodedTriple, ZWeight>>,
    ) -> Result<IncrementalQuerySubscription> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(IncrementalCommand::Register {
                plan,
                basis,
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
        basis: Option<TxBasis>,
        wal_seq: u64,
        triples: Vec<Tup2<EncodedTriple, ZWeight>>,
    ) -> Result<()> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(IncrementalCommand::ApplyTriples {
                basis,
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
        self.await_cdc_task().await;
        let (response, result) = oneshot::channel();
        self.commands
            .send(IncrementalCommand::Shutdown { response })
            .map_err(|_| anyhow!("Incremental query service stopped"))?;
        result
            .await
            .map_err(|_| anyhow!("Incremental query service stopped"))?
            .map_err(|err| anyhow!("{}", err))
    }

    #[cfg(test)]
    pub(crate) async fn pause_next_registration_after_snapshot_for_test(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (reached, reached_rx) = tokio::sync::oneshot::channel();
        let (release, release_rx) = tokio::sync::oneshot::channel();
        *self.registration_pause.lock().await = Some(IncrementalRegistrationPause {
            reached,
            release: release_rx,
        });
        (reached_rx, release)
    }

    #[cfg(test)]
    async fn pause_registration_after_snapshot_for_test(&self) {
        let pause = self.registration_pause.lock().await.take();
        if let Some(IncrementalRegistrationPause { reached, release }) = pause {
            let _ = reached.send(());
            let _ = release.await;
        }
    }
}

#[cfg(test)]
struct IncrementalRegistrationPause {
    reached: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

enum IncrementalCommand {
    Register {
        plan: IncrementalQueryPlan,
        basis: TxBasis,
        wal_cursor: CdcCursor,
        initial_triples: Vec<Tup2<EncodedTriple, ZWeight>>,
        response: oneshot::Sender<ServiceResult<IncrementalQuerySubscription>>,
    },
    Unregister {
        handle: IncrementalQueryHandle,
        response: oneshot::Sender<ServiceResult<()>>,
    },
    ApplyTriples {
        basis: Option<TxBasis>,
        wal_seq: u64,
        triples: Vec<Tup2<EncodedTriple, ZWeight>>,
        response: oneshot::Sender<ServiceResult<()>>,
    },
    Shutdown {
        response: oneshot::Sender<ServiceResult<()>>,
    },
}

type ServiceResult<T> = std::result::Result<T, String>;

struct IncrementalQueryServiceInner {
    next_query_id: u64,
    storage_root: PathBuf,
    queries: HashMap<IncrementalQueryId, RegisteredQuery>,
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
                    basis,
                    wal_cursor,
                    initial_triples,
                    response,
                } => {
                    let _ = response.send(self.register(plan, basis, wal_cursor, initial_triples));
                }
                IncrementalCommand::Unregister { handle, response } => {
                    let _ = response.send(self.unregister(handle));
                }
                IncrementalCommand::ApplyTriples {
                    basis,
                    wal_seq,
                    triples,
                    response,
                } => {
                    let _ = response.send(self.apply_triples(basis, wal_seq, triples));
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
        basis: TxBasis,
        wal_cursor: CdcCursor,
        initial_triples: Vec<Tup2<EncodedTriple, ZWeight>>,
    ) -> ServiceResult<IncrementalQuerySubscription> {
        self.cleanup_closed_subscriptions()?;

        let handle = IncrementalQueryHandle {
            id: self.allocate_query_id(),
        };
        let mut circuit = QueryCircuit::build(plan.clone(), &self.query_storage_path(handle.id))
            .map_err(|err| format!("{:#}", err))?;
        circuit
            .prime(initial_triples)
            .map_err(|err| format!("{:#}", err))?;
        let (sender, receiver) = mpsc::channel(SUBSCRIPTION_CAPACITY);

        self.queries.insert(
            handle.id,
            RegisteredQuery {
                _plan: plan,
                _circuit: circuit,
                sender,
                _basis: basis,
                _wal_cursor: wal_cursor,
            },
        );

        Ok(IncrementalQuerySubscription {
            handle,
            basis,
            deltas: receiver,
        })
    }

    fn unregister(&mut self, handle: IncrementalQueryHandle) -> ServiceResult<()> {
        self.cleanup_closed_subscriptions()?;
        self.remove_query(handle.id)
    }

    fn apply_triples(
        &mut self,
        basis: Option<TxBasis>,
        wal_seq: u64,
        triples: Vec<Tup2<EncodedTriple, ZWeight>>,
    ) -> ServiceResult<()> {
        self.cleanup_closed_subscriptions()?;
        let mut closed = Vec::new();
        let cancel = self.cancel.clone();

        for (id, query) in &mut self.queries {
            if basis.is_none() && wal_seq <= query._wal_cursor.last_seq {
                continue;
            }
            if basis.is_some_and(|basis| basis.tx_key.tx_id <= query._basis.tx_key.tx_id) {
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
            let delta = IncrementalQueryDelta {
                basis,
                wal_seq,
                rows,
            };
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

    fn allocate_query_id(&mut self) -> IncrementalQueryId {
        let id = IncrementalQueryId(self.next_query_id);
        self.next_query_id += 1;
        id
    }

    fn query_storage_path(&self, id: IncrementalQueryId) -> PathBuf {
        self.storage_root.join(format!("query-{}", id.0))
    }

    fn remove_query(&mut self, id: IncrementalQueryId) -> ServiceResult<()> {
        let query = self
            .queries
            .remove(&id)
            .ok_or_else(|| format!("Unknown incremental query handle: {:?}", id))?;
        drop(query);
        self.remove_query_storage(id)
    }

    fn remove_query_storage(&self, id: IncrementalQueryId) -> ServiceResult<()> {
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
    _basis: TxBasis,
    _wal_cursor: CdcCursor,
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::mpsc as std_mpsc;
    use std::time::Duration;

    use chrono::Utc;
    use dbsp::utils::Tup2;
    use edn::query::ToVariable;
    use triplox_client::transaction::TxKey;

    use super::*;
    use crate::codec::Encode;
    use crate::inc_query::{IncrementalQueryPlan, PatternPlan, PatternSlot};

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
                test_basis(),
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
                test_basis(),
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
    fn apply_triples_skips_transactions_at_or_before_query_basis() {
        let dir = tempfile::tempdir().unwrap();
        let runtime = test_runtime();
        let mut service = IncrementalQueryServiceInner::new(
            dir.path().to_path_buf(),
            runtime.handle().clone(),
            CancellationToken::new(),
        );
        let old_basis = test_basis_with_tx_id(1);
        let new_basis = test_basis_with_tx_id(2);
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

        service
            .apply_triples(Some(new_basis), 2, vec![name_triple(43, "Bob")])
            .unwrap();

        assert_eq!(
            old_subscription.deltas.try_recv().unwrap().rows,
            vec![(vec![DataType::String("Bob".to_string())], 1)]
        );
        assert!(new_subscription.deltas.try_recv().is_err());
        assert_eq!(
            service
                .queries
                .get(&old_subscription.handle.id)
                .unwrap()
                ._wal_cursor
                .last_seq,
            2
        );
        assert_eq!(
            service
                .queries
                .get(&new_subscription.handle.id)
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
                test_basis(),
                test_cursor(),
                Vec::new(),
            )
            .unwrap();

        for seq in 1..=SUBSCRIPTION_CAPACITY {
            let name = format!("Alice {seq}");
            service
                .apply_triples(None, seq as u64, vec![name_triple(seq as i64, &name)])
                .unwrap();
        }

        let (done_tx, done_rx) = std_mpsc::channel();
        let handle = thread::spawn(move || {
            let name = format!("Alice {}", SUBSCRIPTION_CAPACITY + 1);
            let result = service.apply_triples(
                None,
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

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Runtime::new().unwrap()
    }

    fn single_pattern_plan() -> IncrementalQueryPlan {
        let pattern = PatternPlan {
            attribute: 10,
            entity: PatternSlot::Variable("?e".to_var()),
            value: PatternSlot::Variable("?name".to_var()),
            output_vars: vec!["?e".to_var(), "?name".to_var()],
        };
        IncrementalQueryPlan {
            find_vars: vec!["?name".to_var()],
            variables: vec!["?e".to_var(), "?name".to_var()],
            joins: vec![],
            patterns: vec![pattern],
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

    fn test_basis() -> TxBasis {
        test_basis_with_tx_id(1)
    }

    fn test_basis_with_tx_id(tx_id: i64) -> TxBasis {
        TxBasis {
            tx_key: TxKey {
                tx_id,
                system_time: Utc::now(),
            },
            tx_eid: tx_id + 1,
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
