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
    pub tx_key: TxKey,
    pub wal_seq: u64,
    pub triples: Vec<Tup2<EncodedTriple, ZWeight>>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct Position {
    pub tx_key: TxKey,
    pub wal_seq: u64,
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
    pub sender: mpsc::Sender<Result<IncrementalQueryDelta>>,
    pub control: Arc<Control>,
    pub steps: Arc<Semaphore>,
    pub applied: Arc<Mutex<Position>>,
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
            applied,
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
                let position = Position {
                    tx_key: batch.tx_key,
                    wal_seq: batch.wal_seq,
                };
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
                *applied.lock().unwrap() = position;
                if control.stop.is_cancelled() {
                    break;
                }
                if rows.is_empty() {
                    continue;
                }
                tokio::select! {
                    biased;
                    _ = control.stop.cancelled() => break,
                    result = sender.send(Ok(IncrementalQueryDelta { tx_key: position.tx_key, rows })) => {
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
