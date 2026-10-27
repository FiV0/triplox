use anyhow::{anyhow, Context, Result};
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

/// A dedicated `tpx-indexer` thread running a current-thread runtime, so indexing never queues
/// behind tasks on the shared runtime. The indexer and SlateDB's batch-writer share this thread,
/// so a commit is a local task switch. A current-thread runtime only runs inside `block_on`, which
/// is why the thread is spawned by hand rather than via `new_multi_thread().worker_threads(1)`.
pub(crate) struct IndexerRuntime {
    handle: Handle,
    stop: CancellationToken,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl IndexerRuntime {
    pub(crate) async fn start() -> Result<Self> {
        let stop = CancellationToken::new();
        let thread_stop = stop.clone();
        // Stops the thread if startup fails or this future is dropped before handing it over.
        let startup_guard = stop.clone().drop_guard();
        let (started, handle) = oneshot::channel();
        let thread = std::thread::Builder::new()
            .name("tpx-indexer".to_owned())
            .spawn(move || {
                // S3 I/O has its own runtime, but SlateDB spawns read tasks on the caller's runtime,
                // so an object store or log doing network I/O here must not panic for lack of a driver.
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = started.send(Err(error));
                        return;
                    }
                };
                let _ = started.send(Ok(runtime.handle().clone()));
                runtime.block_on(thread_stop.cancelled());
            })
            .context("Failed to spawn the indexer thread")?;
        let handle = handle
            .await
            .context("Indexer thread exited during startup")?
            .context("Failed to build the indexer runtime")?;
        startup_guard.disarm();
        Ok(Self {
            handle,
            stop,
            thread: Some(thread),
        })
    }

    pub(crate) fn handle(&self) -> &Handle {
        &self.handle
    }

    // Drops the tasks still on the runtime and joins the thread.
    pub(crate) async fn shutdown(mut self) -> Result<()> {
        self.stop.cancel();
        let Some(thread) = self.thread.take() else {
            return Ok(());
        };
        tokio::task::spawn_blocking(move || thread.join())
            .await
            .context("Indexer thread join task panicked")?
            .map_err(|_| anyhow!("Indexer thread panicked"))
    }
}

impl Drop for IndexerRuntime {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn tasks_run_on_the_indexer_thread() {
        let runtime = IndexerRuntime::start().await.unwrap();
        let name = runtime
            .handle()
            .spawn(async { std::thread::current().name().map(str::to_owned) })
            .await
            .unwrap();
        assert_eq!(name.as_deref(), Some("tpx-indexer"));
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn drop_stops_the_runtime_and_its_tasks() {
        let runtime = IndexerRuntime::start().await.unwrap();
        let (sender, dropped) = oneshot::channel::<()>();
        runtime.handle().spawn(async move {
            let _sender = sender;
            std::future::pending::<()>().await
        });
        drop(runtime);
        let result = tokio::time::timeout(Duration::from_secs(5), dropped)
            .await
            .expect("indexer runtime kept running after drop");
        assert!(result.is_err());
    }
}
