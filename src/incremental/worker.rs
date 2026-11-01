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

use super::circuit::QueryCircuit;
use super::subscription::Termination;
use super::{
    EncodedTriple, IncrementalQueryDelta, IncrementalQueryHandle, IncrementalQuerySubscription,
    Registration,
};
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
    fn prime(
        &mut self,
        triples: Vec<Tup2<EncodedTriple, ZWeight>>,
    ) -> impl Future<Output = Result<Rows>> + Send;
    fn matches(&self, batch: &Batch) -> bool;
    fn apply(&mut self, batch: &Batch) -> impl Future<Output = Result<Rows>> + Send;
}

impl Circuit for QueryCircuit {
    fn prime(
        &mut self,
        triples: Vec<Tup2<EncodedTriple, ZWeight>>,
    ) -> impl Future<Output = Result<Rows>> + Send {
        self.prime(triples)
    }

    fn matches(&self, batch: &Batch) -> bool {
        self.matches(batch)
    }

    fn apply(&mut self, batch: &Batch) -> impl Future<Output = Result<Rows>> + Send {
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

pub(super) enum Message {
    Prime {
        basis: TxKey,
        triples: Vec<Tup2<EncodedTriple, ZWeight>>,
        subscription: IncrementalQuerySubscription,
        response: Registration,
    },
    Apply(Arc<Batch>),
}

pub(super) type Build<C> = Box<dyn FnOnce(&Path) -> Result<C> + Send>;

pub(super) struct Worker<C> {
    pub build: Build<C>,
    pub storage_path: PathBuf,
    pub inbox: mpsc::Receiver<Message>,
    pub sender: mpsc::Sender<IncrementalQueryDelta>,
    pub control: Arc<Control>,
}

impl<C: Circuit> Worker<C> {
    pub async fn run(self) -> Result<()> {
        let Self {
            build,
            storage_path,
            mut inbox,
            sender,
            control,
        } = self;
        let mut circuit = None;
        // The registration that priming answers, while priming runs.
        let mut registration = None;
        let execution = async {
            let build_path = storage_path.clone();
            let built = tokio::task::spawn_blocking(move || build(&build_path))
                .await
                .context("Incremental query build task panicked")??;
            let circuit = circuit.insert(built);
            loop {
                let message = tokio::select! {
                    biased;
                    _ = control.stop.cancelled() => break,
                    _ = sender.closed() => break,
                    message = inbox.recv() => match message { Some(message) => message, None => break },
                };
                let batch = match message {
                    Message::Prime {
                        basis,
                        triples,
                        subscription,
                        response,
                    } => {
                        registration = Some(response);
                        let rows = circuit.prime(triples).await?;
                        if control.stop.is_cancelled() {
                            return Err(anyhow!("Incremental query service stopped"));
                        }
                        if !rows.is_empty() {
                            sender
                                .try_send(IncrementalQueryDelta {
                                    tx_key: basis,
                                    rows,
                                })
                                .expect("new subscription queue has room for priming");
                        }
                        let response = registration.take().expect("priming registration");
                        if response.send(Ok(subscription)).is_err() {
                            break;
                        }
                        continue;
                    }
                    Message::Apply(batch) => batch,
                };
                if !circuit.matches(&batch) {
                    continue;
                }
                // A started step always finishes; stopping takes effect between steps.
                let rows = circuit.apply(&batch).await?;
                if control.stop.is_cancelled() {
                    break;
                }
                if rows.is_empty() {
                    continue;
                }
                tokio::select! {
                    biased;
                    _ = control.stop.cancelled() => break,
                    result = sender.send(IncrementalQueryDelta { tx_key: batch.tx_key, rows }) => {
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
        // A failure before the subscription exists fails the registration.
        let rejection = match (error, registration) {
            (Some(error), Some(response)) => Some((response, error)),
            (Some(error), None) => match inbox.try_recv() {
                Ok(Message::Prime { response, .. }) => Some((response, error)),
                _ => {
                    control.terminate(Some(Termination::Failed(error)));
                    None
                }
            },
            (None, _) => None,
        };
        control.terminate(None);
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
        // A failed registration returns only after its storage is gone.
        if let Some((response, error)) = rejection {
            let _ = response.send(Err(error));
        }
        cleanup
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
