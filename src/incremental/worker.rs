//! Per-query circuit driver tasks.
//!
//! One worker task owns one `QueryCircuit`. The router fans a transaction into every
//! query's inbox and never blocks on a circuit step or a subscriber send, so a slow or
//! wedged query cannot stall the other queries or the CDC loop.

use std::path::PathBuf;
use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Error, Result};
use dbsp::{utils::Tup2, ZWeight};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, watch, Semaphore};
use tokio::task::spawn_blocking;
use tokio_util::sync::CancellationToken;
use triplox_client::transaction::TxKey;

use crate::incremental::circuit::QueryCircuit;
use crate::incremental::{
    remove_query_storage, EncodedTriple, IncrementalQueryDelta, IncrementalQueryHandle,
};
use crate::ops::DataType;

#[derive(Debug, thiserror::Error)]
#[error("Incremental query fell behind by more than {capacity} pending transactions")]
pub(in crate::incremental) struct SubscriptionLagError {
    pub capacity: usize,
}

/// Work routed to one query, in WAL order.
pub(in crate::incremental) enum WorkerMessage {
    Apply {
        tx_key: TxKey,
        triples: Arc<[Tup2<EncodedTriple, ZWeight>]>,
    },
    /// Injects a worker panic, so the supervisor's isolation can be tested for real.
    #[cfg(test)]
    Panic,
    #[cfg(test)]
    Pause {
        started: oneshot::Sender<()>,
        resume: oneshot::Receiver<()>,
    },
}

/// Why a worker stopped. Only the first two reach the subscriber as a terminal error.
pub(in crate::incremental) enum RetireReason {
    CircuitFailed(Error),
    Lagged { capacity: usize },
    Unregistered,
    SubscriberGone,
    Shutdown,
}

impl RetireReason {
    fn into_subscriber_error(self) -> Option<Error> {
        match self {
            RetireReason::CircuitFailed(error) => Some(error),
            RetireReason::Lagged { capacity } => Some(SubscriptionLagError { capacity }.into()),
            RetireReason::Unregistered | RetireReason::SubscriberGone | RetireReason::Shutdown => {
                None
            }
        }
    }
}

/// Reported back to the router once the circuit is gone and its storage is cleaned up.
pub(in crate::incremental) struct RetireOutcome {
    pub id: IncrementalQueryHandle,
    pub storage: Result<()>,
}

pub(in crate::incremental) struct QueryWorker {
    pub id: IncrementalQueryHandle,
    /// Only `None` while the circuit is on loan to the blocking pool for a step.
    pub circuit: Option<QueryCircuit>,
    pub storage_path: PathBuf,
    pub inbox: mpsc::Receiver<WorkerMessage>,
    pub delivered: watch::Sender<u64>,
    pub subscriber: mpsc::Sender<Result<IncrementalQueryDelta>>,
    pub terminate: oneshot::Receiver<RetireReason>,
    pub retirements: std_mpsc::Sender<RetireOutcome>,
    pub steps: Arc<Semaphore>,
    pub cancel: CancellationToken,
    pub retire_timeout: Duration,
}

// Drops the circuit and its storage off the async runtime, then acks the router.
async fn finish_retirement(
    id: IncrementalQueryHandle,
    circuit: Option<QueryCircuit>,
    storage_path: PathBuf,
    retirements: std_mpsc::Sender<RetireOutcome>,
) {
    // Dropping a circuit joins its DBSP threads, so it must not run on the async runtime,
    // and the directory can only go once those threads are gone.
    let storage = spawn_blocking(move || {
        drop(circuit);
        remove_query_storage(&storage_path)
    })
    .await
    .unwrap_or_else(|err| Err(anyhow!("Incremental query teardown task failed: {err}")));

    let _ = retirements.send(RetireOutcome { id, storage });
}

/// Spawns the worker under a supervisor so a panic still retires the query and cleans up
/// instead of taking the service down with it.
pub(in crate::incremental) fn spawn_query_worker(runtime: &Handle, worker: QueryWorker) {
    let id = worker.id;
    let storage_path = worker.storage_path.clone();
    let subscriber = worker.subscriber.clone();
    let retirements = worker.retirements.clone();
    let retire_timeout = worker.retire_timeout;
    let cancel = worker.cancel.clone();

    runtime.spawn(async move {
        if tokio::spawn(worker.run()).await.is_ok() {
            return;
        }
        // The worker panicked, so its circuit was already dropped while unwinding; only the
        // subscriber notice, the storage directory and the ack are left to do.
        let error = anyhow!("Incremental query circuit worker panicked");
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {},
            _ = tokio::time::timeout(retire_timeout, subscriber.send(Err(error))) => {},
        }
        drop(subscriber);
        finish_retirement(id, None, storage_path, retirements).await;
    });
}

impl QueryWorker {
    async fn run(mut self) {
        let reason = self.drive().await;
        self.retire(reason).await;
    }

    // Consumes the inbox in order until something ends the subscription.
    async fn drive(&mut self) -> RetireReason {
        loop {
            // Control arms are biased ahead of the inbox so termination is never queued
            // behind pending work. They all leave the loop, so deltas cannot be reordered.
            let message = tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return RetireReason::Shutdown,
                reason = &mut self.terminate => {
                    return reason.unwrap_or(RetireReason::Unregistered);
                }
                _ = self.subscriber.closed() => return RetireReason::SubscriberGone,
                message = self.inbox.recv() => match message {
                    Some(message) => message,
                    None => return RetireReason::Unregistered,
                },
            };

            match message {
                #[cfg(test)]
                WorkerMessage::Panic => panic!("injected incremental query worker panic"),
                #[cfg(test)]
                WorkerMessage::Pause { started, resume } => {
                    let _ = started.send(());
                    let _ = resume.await;
                }
                WorkerMessage::Apply { tx_key, triples } => {
                    if let Err(reason) = self.step(tx_key, triples).await {
                        return reason;
                    }
                    self.delivered.send_modify(|count| *count += 1);
                }
            }
        }
    }

    async fn step(
        &mut self,
        tx_key: TxKey,
        triples: Arc<[Tup2<EncodedTriple, ZWeight>]>,
    ) -> Result<(), RetireReason> {
        let rows = self.apply(triples).await?;
        if rows.is_empty() {
            return Ok(());
        }

        // Parking here is the normal way a slow subscriber shows up, so the control arms have
        // to stay live: without them a wedged send would ignore lag termination and
        // unregister until the retire deadline.
        let delta = IncrementalQueryDelta { tx_key, rows };
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => Err(RetireReason::Shutdown),
            reason = &mut self.terminate => Err(reason.unwrap_or(RetireReason::Unregistered)),
            result = self.subscriber.send(Ok(delta)) => result
                .map_err(|_| RetireReason::SubscriberGone),
        }
    }

    // Steps the circuit on the blocking pool; the semaphore caps how many circuits do that
    // at once. `block_in_place` is not usable here because it panics on the current-thread
    // runtimes that much of the test suite uses.
    async fn apply(
        &mut self,
        triples: Arc<[Tup2<EncodedTriple, ZWeight>]>,
    ) -> Result<Vec<(Vec<DataType>, isize)>, RetireReason> {
        // Waiting for a permit is bounded by the other circuits, but stay interruptible so a
        // queue of pending steps cannot delay termination.
        let permit = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => return Err(RetireReason::Shutdown),
            reason = &mut self.terminate => {
                return Err(reason.unwrap_or(RetireReason::Unregistered));
            }
            permit = self.steps.clone().acquire_owned() => {
                permit.map_err(|_| RetireReason::Shutdown)?
            }
        };

        let mut circuit = self
            .circuit
            .take()
            .expect("the circuit is only on loan during a step");
        let (circuit, rows) = spawn_blocking(move || {
            // DBSP's `append` steals the buffer, so the circuit needs its own copy.
            let rows = circuit.apply(triples.to_vec());
            (circuit, rows)
        })
        .await
        .expect("a panicking circuit step propagates to the worker's supervisor");
        drop(permit);
        self.circuit = Some(circuit);

        rows.map_err(RetireReason::CircuitFailed)
    }

    async fn retire(self, reason: RetireReason) {
        let QueryWorker {
            id,
            circuit,
            storage_path,
            subscriber,
            retirements,
            retire_timeout,
            cancel,
            ..
        } = self;

        if let Some(error) = reason.into_subscriber_error() {
            // A lagging query is precisely the one whose channel is full, so bound the wait
            // rather than parking here forever; the subscriber then just sees a clean end.
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {},
                _ = tokio::time::timeout(retire_timeout, subscriber.send(Err(error))) => {},
            }
        }
        drop(subscriber);
        finish_retirement(id, circuit, storage_path, retirements).await;
    }
}
