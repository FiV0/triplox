use std::io::ErrorKind;
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Context, Result};
use dbsp::{utils::Tup2, ZWeight};
use futures::FutureExt;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio_util::sync::CancellationToken;
use triplox_client::transaction::TxKey;

use super::circuit::QueryCircuit;
use super::subscription::Termination;
use super::{EncodedTriple, IncrementalQueryDelta, IncrementalQueryHandle};
use crate::ops::DataType;

pub(super) type Rows = Vec<(Vec<DataType>, isize)>;

pub(super) trait Circuit: Send + 'static {
    fn apply(&mut self, triples: Vec<Tup2<EncodedTriple, ZWeight>>) -> Result<Rows>;
}

impl Circuit for QueryCircuit {
    fn apply(&mut self, triples: Vec<Tup2<EncodedTriple, ZWeight>>) -> Result<Rows> {
        self.apply(triples)
    }
}

pub(super) struct Batch {
    /// Key of the last transaction in the batch.
    pub tx_key: TxKey,
    pub triples: Vec<Tup2<EncodedTriple, ZWeight>>,
    /// `(tx_id, start)` of each transaction's triples, strictly increasing in both.
    txs: Vec<(i64, usize)>,
}

impl Batch {
    pub fn new(
        tx_key: TxKey,
        triples: Vec<Tup2<EncodedTriple, ZWeight>>,
        txs: Vec<(i64, usize)>,
    ) -> Self {
        debug_assert!(txs.is_sorted_by(|a, b| a.0 < b.0 && a.1 < b.1));
        Self {
            tx_key,
            triples,
            txs,
        }
    }

    /// A batch holding a single transaction.
    #[cfg(test)]
    pub fn single(tx_key: TxKey, triples: Vec<Tup2<EncodedTriple, ZWeight>>) -> Self {
        Self::new(tx_key, triples, vec![(tx_key.tx_id, 0)])
    }

    /// The transactions of this batch after `basis`, or `None` if there are none.
    pub fn after(self: &Arc<Self>, basis: i64) -> Option<Arc<Self>> {
        if self.tx_key.tx_id <= basis {
            return None;
        }
        let first = self.txs.partition_point(|&(tx_id, _)| tx_id <= basis);
        if first == 0 {
            return Some(self.clone());
        }
        let &(_, start) = self.txs.get(first)?;
        let txs = self.txs[first..]
            .iter()
            .map(|&(tx_id, offset)| (tx_id, offset - start))
            .collect();
        Some(Arc::new(Self::new(
            self.tx_key,
            self.triples[start..].to_vec(),
            txs,
        )))
    }
}

pub(super) struct Control {
    pub stop: CancellationToken,
    terminal: Mutex<Option<oneshot::Sender<Termination>>>,
}

impl Control {
    pub fn new(stop: CancellationToken, terminal: oneshot::Sender<Termination>) -> Self {
        Self {
            stop,
            terminal: Mutex::new(Some(terminal)),
        }
    }

    pub fn terminate(&self, reason: Option<Termination>) {
        let mut terminal = self.terminal.lock().unwrap();
        self.stop.cancel();
        if let Some(sender) = terminal.take() {
            if let Some(reason) = reason {
                let _ = sender.send(reason);
            }
        }
    }
}

pub(super) struct Completion {
    pub handle: IncrementalQueryHandle,
    pub cleanup: Result<()>,
}

pub(super) struct Worker<C> {
    pub circuit: C,
    pub storage_path: PathBuf,
    pub inbox: mpsc::Receiver<Arc<Batch>>,
    pub sender: mpsc::Sender<IncrementalQueryDelta>,
    pub control: Arc<Control>,
    pub steps: Arc<Semaphore>,
}

impl<C: Circuit> Worker<C> {
    pub async fn run(self) -> Result<()> {
        let Self {
            circuit,
            storage_path,
            mut inbox,
            sender,
            control,
            steps,
        } = self;
        // Only blocking jobs touch the circuit, including destruction after a panic.
        let circuit = Arc::new(Mutex::new(Some(circuit)));
        let execution = async {
            loop {
                let batch = tokio::select! {
                    biased;
                    _ = control.stop.cancelled() => break,
                    _ = sender.closed() => break,
                    batch = inbox.recv() => match batch { Some(batch) => batch, None => break },
                };
                let permit = tokio::select! {
                    biased;
                    _ = control.stop.cancelled() => break,
                    _ = sender.closed() => break,
                    permit = steps.clone().acquire_owned() => permit?,
                };
                let tx_key = batch.tx_key;
                let job_circuit = circuit.clone();
                let stop = control.stop.clone();
                let rows = tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    if stop.is_cancelled() {
                        return Ok(None);
                    }
                    job_circuit
                        .lock()
                        .unwrap()
                        .as_mut()
                        .unwrap()
                        .apply(batch.triples.clone())
                        .map(Some)
                })
                .await
                .context("Incremental query apply task panicked")??;
                let Some(rows) = rows else { break };
                if control.stop.is_cancelled() {
                    break;
                }
                if rows.is_empty() {
                    continue;
                }
                tokio::select! {
                    biased;
                    _ = control.stop.cancelled() => break,
                    result = sender.send(IncrementalQueryDelta { tx_key, rows }) => {
                        if result.is_err() { break; }
                    }
                }
            }
            Ok::<_, anyhow::Error>(())
        };
        let result = AssertUnwindSafe(execution).catch_unwind().await;
        let error = match result {
            Ok(result) => result.err(),
            Err(_) => Some(anyhow!("Incremental query worker panicked")),
        };
        control.terminate(error.map(Termination::Failed));
        drop(inbox);
        drop(sender);
        tokio::task::spawn_blocking(move || {
            let circuit = circuit
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take();
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
        .context("Incremental query cleanup task panicked")?
    }
}

#[cfg(test)]
mod tests {
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

    fn entities(batch: &Batch) -> Vec<u8> {
        let mut entities = batch
            .triples
            .iter()
            .map(|Tup2(triple, _)| triple.entity[0])
            .collect::<Vec<_>>();
        entities.sort_unstable();
        entities
    }

    #[test]
    fn batch_after_keeps_only_later_transactions() {
        let mut tx_key = *crate::bootstrap::BOOTSTRAP_TX_KEY;
        tx_key.tx_id = 3;
        let batch = Arc::new(Batch::new(
            tx_key,
            vec![triple(2, 1), triple(1, 2), triple(2, 3), triple(1, 4)],
            vec![(1, 0), (2, 1), (3, 2)],
        ));

        assert!(Arc::ptr_eq(&batch.after(0).unwrap(), &batch));
        assert!(batch.after(3).is_none());
        let after_first = batch.after(1).unwrap();
        assert_eq!(after_first.tx_key, tx_key);
        assert_eq!(entities(&after_first), vec![2, 3, 4]);
        let after_second = batch.after(2).unwrap();
        assert_eq!(entities(&after_second), vec![3, 4]);
    }
}
