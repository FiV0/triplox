use std::future::Future;
use std::io::ErrorKind;
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Context, Result};
use dbsp::{utils::Tup2, ZWeight};
use futures::FutureExt;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use triplox_client::transaction::TxKey;

use super::circuit::QueryCircuit;
use super::subscription::Termination;
use super::{EncodedTriple, IncrementalQueryDelta, IncrementalQueryHandle};
use crate::ops::DataType;

pub(super) type Rows = Vec<(Vec<DataType>, isize)>;

pub(super) trait Circuit: Send + 'static {
    fn apply(
        &mut self,
        triples: Vec<Tup2<EncodedTriple, ZWeight>>,
    ) -> impl Future<Output = Result<Rows>> + Send;
}

impl Circuit for QueryCircuit {
    fn apply(
        &mut self,
        triples: Vec<Tup2<EncodedTriple, ZWeight>>,
    ) -> impl Future<Output = Result<Rows>> + Send {
        self.apply(triples)
    }
}

pub(super) struct Batch {
    pub tx_key: TxKey,
    pub triples: Vec<Tup2<EncodedTriple, ZWeight>>,
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
}

impl<C: Circuit> Worker<C> {
    pub async fn run(self) -> Result<()> {
        let Self {
            mut circuit,
            storage_path,
            mut inbox,
            sender,
            control,
        } = self;
        let execution = async {
            loop {
                let batch = tokio::select! {
                    biased;
                    _ = control.stop.cancelled() => break,
                    _ = sender.closed() => break,
                    batch = inbox.recv() => match batch { Some(batch) => batch, None => break },
                };
                // A started step always finishes; stopping takes effect between steps.
                let rows = circuit.apply(batch.triples.clone()).await?;
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
        control.terminate(error.map(Termination::Failed));
        drop(inbox);
        drop(sender);
        // Circuit destruction waits on the pool, so it runs on a blocking thread before storage removal.
        tokio::task::spawn_blocking(move || {
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
