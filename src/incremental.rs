//! Writer-node incremental query service.

use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::mpsc as std_mpsc;
use std::thread;

use anyhow::{anyhow, Error, Result};
use dbsp::circuit::{
    CircuitConfig, CircuitStorageConfig, StorageCacheConfig, StorageConfig, StorageOptions,
};
use dbsp::{utils::Tup2, DBSPHandle, OutputHandle, Runtime, ZSetHandle, ZWeight};
use tokio::sync::{mpsc, oneshot};
use triplox_client::transaction::TxBasis;

use crate::inc_query::IncrementalQueryPlan;
use crate::incremental::circuit::{decode_output_rows, query_find_stream, RowZSet};
use crate::ops::DataType;
use crate::slate::cdc::CdcCursor;

pub(crate) mod cdc;
pub(crate) mod circuit;

const SUBSCRIPTION_CAPACITY: usize = 128;

pub(crate) type EncodedValue = Vec<u8>;
pub(crate) type EncodedRow = Vec<EncodedValue>;

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
}

impl IncrementalQueryService {
    pub(crate) fn new(storage_root: PathBuf) -> Self {
        let (sender, receiver) = std_mpsc::channel();
        thread::Builder::new()
            .name("triplox-incremental-query".to_string())
            .spawn(move || IncrementalQueryServiceInner::new(storage_root).run(receiver))
            .expect("incremental query service thread should start");

        Self { commands: sender }
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

    #[cfg(test)]
    pub(crate) async fn active_query_count(&self) -> usize {
        let (response, result) = oneshot::channel();
        self.commands
            .send(IncrementalCommand::ActiveQueryCount { response })
            .expect("incremental query service should be running");
        result
            .await
            .expect("incremental query service should respond")
    }
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
    #[cfg(test)]
    ActiveQueryCount { response: oneshot::Sender<usize> },
}

type ServiceResult<T> = std::result::Result<T, String>;

struct IncrementalQueryServiceInner {
    next_query_id: u64,
    storage_root: PathBuf,
    queries: HashMap<IncrementalQueryId, RegisteredQuery>,
}

impl IncrementalQueryServiceInner {
    fn new(storage_root: PathBuf) -> Self {
        Self {
            next_query_id: 1,
            storage_root,
            queries: HashMap::new(),
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
                #[cfg(test)]
                IncrementalCommand::ActiveQueryCount { response } => {
                    let _ = response.send(self.active_query_count());
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
            if query.sender.blocking_send(delta).is_err() {
                closed.push(*id);
            } else {
                query._wal_cursor.last_seq = wal_seq;
            }
        }

        for id in closed {
            self.remove_query(id)?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn active_query_count(&mut self) -> usize {
        self.cleanup_closed_subscriptions()
            .expect("closed subscription cleanup should succeed");
        self.queries.len()
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

struct RegisteredQuery {
    _plan: IncrementalQueryPlan,
    _circuit: QueryCircuit,
    sender: mpsc::Sender<IncrementalQueryDelta>,
    _basis: TxBasis,
    _wal_cursor: CdcCursor,
}

struct QueryCircuit {
    _circuit: DBSPHandle,
    _input: ZSetHandle<EncodedTriple>,
    _output: OutputHandle<RowZSet>,
}

impl QueryCircuit {
    fn build(plan: IncrementalQueryPlan, storage_path: &Path) -> Result<Self> {
        let config = storage_circuit_config(storage_path)?;
        let (circuit, (input, output)) = Runtime::init_circuit(config, move |circuit| {
            let (input, handle) = circuit.add_input_zset::<EncodedTriple>();
            let rows = query_find_stream(&input, plan);
            Ok((handle, rows.output()))
        })
        .map_err(Error::from)?;

        Ok(Self {
            _circuit: circuit,
            _input: input,
            _output: output,
        })
    }

    fn prime(&mut self, mut initial_triples: Vec<Tup2<EncodedTriple, ZWeight>>) -> Result<()> {
        self._input.append(&mut initial_triples);
        self._circuit.transaction().map_err(Error::from)?;
        let _ = self._output.consolidate();
        Ok(())
    }

    fn apply(
        &mut self,
        mut triples: Vec<Tup2<EncodedTriple, ZWeight>>,
    ) -> Result<Vec<(Vec<DataType>, isize)>> {
        self._input.append(&mut triples);
        self._circuit.transaction().map_err(Error::from)?;
        decode_output_rows(&self._output.consolidate())
    }
}

fn storage_circuit_config(storage_path: &Path) -> Result<CircuitConfig> {
    if storage_path.exists() {
        std::fs::remove_dir_all(storage_path)?;
    }
    std::fs::create_dir_all(storage_path)?;
    let storage = CircuitStorageConfig::for_config(
        StorageConfig {
            path: storage_path.to_string_lossy().into_owned(),
            cache: StorageCacheConfig::default(),
        },
        StorageOptions {
            min_storage_bytes: Some(0),
            ..StorageOptions::default()
        },
    )
    .map_err(Error::from)?;

    Ok(CircuitConfig::with_workers(1).with_storage(Some(storage)))
}

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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use chrono::Utc;
    use dbsp::utils::Tup2;
    use edn::query::ToVariable;
    use triplox_client::transaction::TxKey;

    use super::*;
    use crate::codec::Encode;
    use crate::inc_query::{IncrementalQueryPlan, PatternPlan, PatternSlot};

    #[test]
    fn query_circuit_uses_file_backed_storage_root() {
        let dir = tempfile::tempdir().unwrap();
        let storage_path = dir.path().join("query-1");
        std::fs::create_dir_all(&storage_path).unwrap();
        std::fs::write(storage_path.join("stale"), b"stale").unwrap();

        let mut circuit = QueryCircuit::build(single_pattern_plan(), &storage_path).unwrap();
        circuit
            .prime(vec![Tup2(
                EncodedTriple {
                    entity: DataType::Long(42).encode(),
                    attribute: 10,
                    value: DataType::String("Alice".to_string()).encode(),
                },
                1,
            )])
            .unwrap();

        assert!(storage_path.exists());
        assert!(!storage_path.join("stale").exists());
        assert!(path_has_entries(&storage_path));
    }

    #[test]
    fn unregister_removes_query_storage() {
        let dir = tempfile::tempdir().unwrap();
        let mut service = IncrementalQueryServiceInner::new(dir.path().to_path_buf());
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
        let mut service = IncrementalQueryServiceInner::new(dir.path().to_path_buf());
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
        drop(subscription);

        assert_eq!(service.active_query_count(), 0);
        assert!(!storage_path.exists());
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
        vec![Tup2(
            EncodedTriple {
                entity: DataType::Long(42).encode(),
                attribute: 10,
                value: DataType::String("Alice".to_string()).encode(),
            },
            1,
        )]
    }

    fn test_basis() -> TxBasis {
        TxBasis {
            tx_key: TxKey {
                tx_id: 1,
                system_time: Utc::now(),
            },
            tx_eid: 2,
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
