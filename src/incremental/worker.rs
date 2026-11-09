use std::future::Future;
use std::io::ErrorKind;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Context, Result};
use dbsp::{utils::Tup2, ZWeight};
use futures::FutureExt;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use triplox_client::transaction::TxKey;

use super::cdc::{scan_current_triples, InitialScan};
use super::circuit::QueryCircuit;
use super::subscription::Termination;
use super::{
    EncodedTriple, IncrementalQueryDelta, IncrementalQueryHandle, IncrementalQuerySubscription,
    Registration,
};
use crate::ops::DataType;
use crate::partition::tx_eid_from_tx_id;

pub(super) type Rows = Vec<(Vec<DataType>, isize)>;

pub(super) struct Batch {
    /// Key of the last transaction in the batch.
    pub tx_key: TxKey,
    /// Sorted by attribute, so each attribute's triples are contiguous.
    triples: Vec<Tup2<EncodedTriple, ZWeight>>,
    /// Transaction id of each triple, parallel to `triples`.
    tx_ids: Vec<i64>,
    /// The smallest of `tx_ids`, so a batch entirely after a basis is shared without a scan.
    first_tx_id: i64,
}

impl Batch {
    pub fn new(
        tx_key: TxKey,
        triples: Vec<Tup2<EncodedTriple, ZWeight>>,
        tx_ids: Vec<i64>,
    ) -> Self {
        let first_tx_id = tx_ids.iter().copied().min().unwrap_or(tx_key.tx_id);
        let mut rows = triples.into_iter().zip(tx_ids).collect::<Vec<_>>();
        rows.sort_unstable_by_key(|(Tup2(triple, _), _)| triple.attribute);
        let (triples, tx_ids) = rows.into_iter().unzip();
        Self {
            tx_key,
            triples,
            tx_ids,
            first_tx_id,
        }
    }

    pub fn triples(&self) -> &[Tup2<EncodedTriple, ZWeight>] {
        &self.triples
    }

    pub fn attribute_triples(&self, attribute: i64) -> &[Tup2<EncodedTriple, ZWeight>] {
        let start = self
            .triples
            .partition_point(|Tup2(triple, _)| triple.attribute < attribute);
        let end = start
            + self.triples[start..]
                .partition_point(|Tup2(triple, _)| triple.attribute <= attribute);
        &self.triples[start..end]
    }

    /// The transactions of this batch after `basis`, or `None` if there are none.
    pub fn after(self: &Arc<Self>, basis: i64) -> Option<Arc<Self>> {
        if self.tx_key.tx_id <= basis {
            return None;
        }
        if self.first_tx_id > basis {
            return Some(self.clone());
        }
        let (triples, tx_ids) = self
            .triples
            .iter()
            .zip(&self.tx_ids)
            .filter(|(_, &tx_id)| tx_id > basis)
            .map(|(triple, &tx_id)| (triple.clone(), tx_id))
            .unzip();
        Some(Arc::new(Self::new(self.tx_key, triples, tx_ids)))
    }
}

pub(super) trait Circuit: Send + 'static {
    fn prime(&mut self, basis: TxKey) -> impl Future<Output = Result<Rows>> + Send;
    fn matches(&self, batch: &Batch) -> bool;
    fn apply(&mut self, batch: &Batch) -> impl Future<Output = Result<Rows>> + Send;
}

// A query circuit that primes from a snapshot scan of SlateDB.
pub(super) struct SnapshotCircuit {
    pub circuit: QueryCircuit,
    pub db: Arc<slatedb::Db>,
    pub scans: Vec<InitialScan>,
}

impl Circuit for SnapshotCircuit {
    async fn prime(&mut self, basis: TxKey) -> Result<Rows> {
        let triples = scan_current_triples(
            self.db.as_ref(),
            &self.scans,
            tx_eid_from_tx_id(basis.tx_id),
        )
        .await?;
        self.circuit.prime(triples).await
    }

    fn matches(&self, batch: &Batch) -> bool {
        self.circuit.matches(batch)
    }

    fn apply(&mut self, batch: &Batch) -> impl Future<Output = Result<Rows>> + Send {
        self.circuit.apply(batch)
    }
}

// Stops a query once and sends the first stop reason to its subscription.
pub(super) struct StopSignal {
    pub token: CancellationToken,
    terminal: Mutex<Option<oneshot::Sender<Termination>>>,
}

impl StopSignal {
    pub fn new(token: CancellationToken, terminal: oneshot::Sender<Termination>) -> Self {
        Self {
            token,
            terminal: Mutex::new(Some(terminal)),
        }
    }

    // Sends the reason before it cancels, so a worker that sees the stop can read the reason.
    pub fn terminate(&self, reason: Option<Termination>) {
        let mut terminal = self.terminal.lock().unwrap();
        if let (Some(sender), Some(reason)) = (terminal.take(), reason) {
            let _ = sender.send(reason);
        }
        self.token.cancel();
    }
}

pub(super) struct Completion {
    pub handle: IncrementalQueryHandle,
    pub cleanup: Result<()>,
}

pub(super) type Build<C> = Box<dyn FnOnce(&Path) -> Result<C> + Send>;

pub(super) struct Worker<C> {
    pub build: Build<C>,
    pub storage_path: PathBuf,
    pub inbox: mpsc::Receiver<Arc<Batch>>,
    pub sender: mpsc::Sender<IncrementalQueryDelta>,
    pub stop: Arc<StopSignal>,
}

// Primes the circuit, answers the registration, and then applies batches until the query stops.
async fn serve<C: Circuit>(
    circuit: &mut C,
    basis: TxKey,
    registration: &mut Option<(IncrementalQuerySubscription, Registration)>,
    inbox: &mut mpsc::Receiver<Arc<Batch>>,
    sender: &mpsc::Sender<IncrementalQueryDelta>,
    stop: &StopSignal,
) -> Result<()> {
    if stop.token.is_cancelled() {
        return Ok(());
    }
    let rows = circuit.prime(basis).await?;
    if stop.token.is_cancelled() {
        return Ok(());
    }
    if !rows.is_empty() {
        sender
            .try_send(IncrementalQueryDelta {
                tx_key: basis,
                rows,
            })
            .expect("new subscription queue has room for priming");
    }
    let (subscription, response) = registration.take().expect("priming registration");
    if response.send(Ok(subscription)).is_err() {
        return Ok(());
    }
    loop {
        let batch = tokio::select! {
            biased;
            _ = stop.token.cancelled() => break,
            _ = sender.closed() => break,
            batch = inbox.recv() => match batch { Some(batch) => batch, None => break },
        };
        if !circuit.matches(&batch) {
            continue;
        }
        // A started step always finishes; stopping takes effect between steps.
        let rows = circuit.apply(&batch).await?;
        if stop.token.is_cancelled() {
            break;
        }
        if rows.is_empty() {
            continue;
        }
        tokio::select! {
            biased;
            _ = stop.token.cancelled() => break,
            result = sender.send(IncrementalQueryDelta { tx_key: batch.tx_key, rows }) => {
                if result.is_err() { break; }
            }
        }
    }
    Ok(())
}

impl<C: Circuit> Worker<C> {
    pub async fn run(
        self,
        subscription: IncrementalQuerySubscription,
        response: Registration,
    ) -> Result<()> {
        let Self {
            build,
            storage_path,
            mut inbox,
            sender,
            stop,
        } = self;
        let basis = subscription.tx_key;
        let mut registration = Some((subscription, response));
        let build_path = storage_path.clone();
        // A panic in the build becomes a join error, so only serving needs catch_unwind.
        let built = tokio::task::spawn_blocking(move || build(&build_path))
            .await
            .context("Incremental query build task panicked")
            .and_then(|built| built);
        let (circuit, error) = match built {
            Ok(mut circuit) => {
                let served = serve(
                    &mut circuit,
                    basis,
                    &mut registration,
                    &mut inbox,
                    &sender,
                    &stop,
                );
                // The circuit outlives a panic, so the cleanup still destroys it on a blocking thread.
                let error = match AssertUnwindSafe(served).catch_unwind().await {
                    Ok(result) => result.err(),
                    Err(_) => Some(anyhow!("Incremental query worker panicked")),
                };
                (Some(circuit), error)
            }
            Err(error) => (None, Some(error)),
        };
        // The registration is still open if the worker stopped before priming finished.
        let failure = match (registration, error) {
            // A build or serving error fails the registration with that error.
            (Some((_, response)), Some(error)) => Some((response, error)),
            // A stop without an error fails the registration only if it was a lag.
            (Some((mut subscription, response)), None) => match subscription.deltas.try_recv() {
                // A lag fails the registration with the lag error, which waits in the subscription.
                Ok(Err(lag)) => Some((response, lag)),
                // Other stops close the response channel, so the registration fails.
                _ => None,
            },
            // The subscriber already holds the subscription, so an error reaches it as the termination.
            (None, error) => {
                stop.terminate(error.map(Termination::Failed));
                None
            }
        };
        stop.terminate(None);
        drop(inbox);
        drop(sender);
        // Circuit destruction waits on the pool, so it runs on a blocking thread before storage removal.
        let cleanup = tokio::task::spawn_blocking(move || {
            drop(circuit);
            match std::fs::remove_dir_all(&storage_path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error).with_context(|| {
                    format!(
                        "Failed to remove incremental query storage {}",
                        storage_path.display()
                    )
                }),
            }
        })
        .await
        .context("Incremental query cleanup task panicked")
        .and_then(|cleanup| cleanup);
        // A registration that did not get its subscription returns only after the storage is gone.
        if let Some((response, error)) = failure {
            let _ = response.send(Err(error));
        }
        cleanup
    }
}

#[cfg(test)]
pub(super) mod test_support {
    use super::*;

    /// A batch holding a single transaction.
    pub(crate) fn single_batch(tx_key: TxKey, triples: Vec<Tup2<EncodedTriple, ZWeight>>) -> Batch {
        let tx_ids = vec![tx_key.tx_id; triples.len()];
        Batch::new(tx_key, triples, tx_ids)
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::single_batch;
    use super::*;

    fn triple(attribute: i64, entity: u8) -> Tup2<EncodedTriple, ZWeight> {
        Tup2(
            EncodedTriple {
                entity: vec![entity],
                attribute,
                value: vec![],
            },
            1,
        )
    }

    fn entities(batch: &Batch, attribute: i64) -> Vec<u8> {
        let mut entities = batch
            .attribute_triples(attribute)
            .iter()
            .map(|Tup2(triple, _)| triple.entity[0])
            .collect::<Vec<_>>();
        entities.sort_unstable();
        entities
    }

    #[test]
    fn batch_returns_each_attributes_triples() {
        let batch = single_batch(
            *crate::bootstrap::BOOTSTRAP_TX_KEY,
            vec![triple(3, 1), triple(1, 2), triple(3, 3), triple(2, 4)],
        );

        assert_eq!(entities(&batch, 1), vec![2]);
        assert_eq!(entities(&batch, 2), vec![4]);
        assert_eq!(entities(&batch, 3), vec![1, 3]);
        assert!(entities(&batch, 0).is_empty());
        assert!(entities(&batch, 4).is_empty());
    }

    #[test]
    fn batch_after_keeps_only_later_transactions() {
        let mut tx_key = *crate::bootstrap::BOOTSTRAP_TX_KEY;
        tx_key.tx_id = 3;
        let batch = Arc::new(Batch::new(
            tx_key,
            vec![triple(2, 1), triple(1, 2), triple(2, 3), triple(1, 4)],
            vec![1, 2, 3, 3],
        ));

        assert!(Arc::ptr_eq(&batch.after(0).unwrap(), &batch));
        assert!(batch.after(3).is_none());
        let after_first = batch.after(1).unwrap();
        assert_eq!(after_first.tx_key, tx_key);
        assert_eq!(entities(&after_first, 1), vec![2, 4]);
        assert_eq!(entities(&after_first, 2), vec![3]);
        let after_second = batch.after(2).unwrap();
        assert_eq!(entities(&after_second, 1), vec![4]);
        assert_eq!(entities(&after_second, 2), vec![3]);
    }
}
