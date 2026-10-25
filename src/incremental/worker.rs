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

    /// A batch holding a single transaction.
    #[cfg(test)]
    pub fn single(tx_key: TxKey, triples: Vec<Tup2<EncodedTriple, ZWeight>>) -> Self {
        let tx_ids = vec![tx_key.tx_id; triples.len()];
        Self::new(tx_key, triples, tx_ids)
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
    fn matches(&self, batch: &Batch) -> bool;
    fn apply(&mut self, batch: &Batch) -> Result<Rows>;
}

impl Circuit for QueryCircuit {
    fn matches(&self, batch: &Batch) -> bool {
        self.matches(batch)
    }

    fn apply(&mut self, batch: &Batch) -> Result<Rows> {
        self.apply(batch)
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
        // Only blocking jobs step or destroy the circuit, including after a panic.
        let circuit = Arc::new(Mutex::new(Some(circuit)));
        let execution = async {
            loop {
                let batch = tokio::select! {
                    biased;
                    _ = control.stop.cancelled() => break,
                    _ = sender.closed() => break,
                    batch = inbox.recv() => match batch { Some(batch) => batch, None => break },
                };
                if !circuit.lock().unwrap().as_ref().unwrap().matches(&batch) {
                    continue;
                }
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
                        .apply(&batch)
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

    struct SelectiveCircuit {
        checked: mpsc::UnboundedSender<i64>,
    }

    impl Circuit for SelectiveCircuit {
        fn matches(&self, batch: &Batch) -> bool {
            let attribute = batch.triples()[0].0.attribute;
            self.checked.send(attribute).unwrap();
            attribute == 2
        }

        fn apply(&mut self, batch: &Batch) -> Result<Rows> {
            assert_eq!(batch.triples()[0].0.attribute, 2);
            Ok(vec![(vec![DataType::Long(2)], 1)])
        }
    }

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

    #[tokio::test]
    async fn unrelated_batch_skips_execution_permit() {
        let storage = tempfile::tempdir().unwrap();
        let storage_path = storage.path().join("query");
        std::fs::create_dir(&storage_path).unwrap();
        let steps = Arc::new(Semaphore::new(1));
        let permit = steps.clone().acquire_owned().await.unwrap();
        let (checked, mut checks) = mpsc::unbounded_channel();
        let (inbox_sender, inbox) = mpsc::channel(2);
        let (sender, mut deltas) = mpsc::channel(1);
        let (terminal, _termination) = oneshot::channel();
        let control = Arc::new(Control::new(CancellationToken::new(), terminal));
        let worker = Worker {
            circuit: SelectiveCircuit { checked },
            storage_path: storage_path.clone(),
            inbox,
            sender,
            control: control.clone(),
            steps,
        };
        let task = tokio::spawn(worker.run());
        let tx_key = *crate::bootstrap::BOOTSTRAP_TX_KEY;
        for attribute in [1, 2] {
            inbox_sender
                .send(Arc::new(Batch::single(tx_key, vec![triple(attribute, 1)])))
                .await
                .unwrap();
        }

        // Both batches are checked while the only execution permit is held.
        for attribute in [1, 2] {
            let checked = tokio::time::timeout(std::time::Duration::from_secs(5), checks.recv())
                .await
                .unwrap();
            assert_eq!(checked, Some(attribute));
        }
        assert!(matches!(
            deltas.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        drop(permit);
        let delta = tokio::time::timeout(std::time::Duration::from_secs(5), deltas.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delta.tx_key, tx_key);
        assert_eq!(delta.rows, vec![(vec![DataType::Long(2)], 1)]);

        control.terminate(None);
        task.await.unwrap().unwrap();
        assert!(!storage_path.exists());
    }

    #[test]
    fn batch_returns_each_attributes_triples() {
        let batch = Batch::single(
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
