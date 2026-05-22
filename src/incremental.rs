//! Writer-node incremental query service.

use std::collections::HashMap;
use std::sync::mpsc as std_mpsc;
use std::thread;

use anyhow::{anyhow, Error, Result};
use dbsp::{utils::Tup2, CircuitHandle, OutputHandle, RootCircuit, ZSetHandle, ZWeight};
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

pub(crate) struct IncrementalQueryService {
    commands: std_mpsc::Sender<IncrementalCommand>,
}

impl IncrementalQueryService {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = std_mpsc::channel();
        thread::Builder::new()
            .name("triplox-incremental-query".to_string())
            .spawn(move || IncrementalQueryServiceInner::new().run(receiver))
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
    queries: HashMap<IncrementalQueryId, RegisteredQuery>,
}

impl IncrementalQueryServiceInner {
    fn new() -> Self {
        Self {
            next_query_id: 1,
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
        self.cleanup_closed_subscriptions();

        let circuit = QueryCircuit::build(plan.clone()).map_err(|err| format!("{:#}", err))?;
        circuit
            .prime(initial_triples)
            .map_err(|err| format!("{:#}", err))?;
        let (sender, receiver) = mpsc::channel(SUBSCRIPTION_CAPACITY);
        let handle = IncrementalQueryHandle {
            id: self.allocate_query_id(),
        };

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
        self.cleanup_closed_subscriptions();
        self.queries
            .remove(&handle.id)
            .map(|_| ())
            .ok_or_else(|| format!("Unknown incremental query handle: {:?}", handle.id))
    }

    fn apply_triples(
        &mut self,
        basis: Option<TxBasis>,
        wal_seq: u64,
        triples: Vec<Tup2<EncodedTriple, ZWeight>>,
    ) -> ServiceResult<()> {
        self.cleanup_closed_subscriptions();
        let mut closed = Vec::new();

        for (id, query) in &mut self.queries {
            let rows = query
                ._circuit
                .apply(triples.clone())
                .map_err(|err| format!("{:#}", err))?;
            if rows.is_empty() {
                continue;
            }
            let delta = IncrementalQueryDelta {
                basis,
                wal_seq,
                rows,
            };
            if query.sender.blocking_send(delta).is_err() {
                closed.push(*id);
            }
        }

        for id in closed {
            self.queries.remove(&id);
        }
        Ok(())
    }

    #[cfg(test)]
    fn active_query_count(&mut self) -> usize {
        self.cleanup_closed_subscriptions();
        self.queries.len()
    }

    fn allocate_query_id(&mut self) -> IncrementalQueryId {
        let id = IncrementalQueryId(self.next_query_id);
        self.next_query_id += 1;
        id
    }

    fn cleanup_closed_subscriptions(&mut self) {
        self.queries.retain(|_, query| !query.sender.is_closed());
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
    _circuit: CircuitHandle,
    _input: ZSetHandle<EncodedTriple>,
    _output: OutputHandle<RowZSet>,
}

impl QueryCircuit {
    fn build(plan: IncrementalQueryPlan) -> Result<Self> {
        let (circuit, (input, output)) = RootCircuit::build(|circuit| {
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

    fn prime(&self, mut initial_triples: Vec<Tup2<EncodedTriple, ZWeight>>) -> Result<()> {
        self._input.append(&mut initial_triples);
        self._circuit.transaction().map_err(Error::from)?;
        let _ = self._output.consolidate();
        Ok(())
    }

    fn apply(
        &self,
        mut triples: Vec<Tup2<EncodedTriple, ZWeight>>,
    ) -> Result<Vec<(Vec<DataType>, isize)>> {
        self._input.append(&mut triples);
        self._circuit.transaction().map_err(Error::from)?;
        decode_output_rows(&self._output.consolidate())
    }
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
