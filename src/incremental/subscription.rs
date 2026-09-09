use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::{Error, Result};
use tokio::sync::{mpsc, oneshot};
use triplox_client::transaction::TxKey;

use super::IncrementalQueryDelta;

#[derive(Debug, thiserror::Error)]
#[error("Incremental query fell behind at transaction {tx_key:?}: input capacity {capacity} exceeded; subscribe again")]
pub(crate) struct SubscriptionLagged {
    pub tx_key: TxKey,
    pub capacity: usize,
}

#[derive(Debug)]
pub(crate) enum Termination {
    Lagged(SubscriptionLagged),
    Failed(Error),
}

/// Keeps terminal errors deliverable even when the result queue is full.
#[derive(Debug)]
pub(crate) struct SubscriptionDeltas {
    receiver: mpsc::Receiver<Result<IncrementalQueryDelta>>,
    termination: Option<oneshot::Receiver<Termination>>,
    error: Option<Error>,
    finished: bool,
}

impl SubscriptionDeltas {
    pub(crate) fn new(
        receiver: mpsc::Receiver<Result<IncrementalQueryDelta>>,
        termination: oneshot::Receiver<Termination>,
    ) -> Self {
        Self {
            receiver,
            termination: Some(termination),
            error: None,
            finished: false,
        }
    }

    fn finish(&mut self, error: Error) -> Poll<Option<Result<IncrementalQueryDelta>>> {
        self.finished = true;
        self.receiver.close();
        while self.receiver.try_recv().is_ok() {}
        Poll::Ready(Some(Err(error)))
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<IncrementalQueryDelta>>> {
        if self.finished {
            return Poll::Ready(None);
        }
        if let Some(termination) = &mut self.termination {
            if let Poll::Ready(result) = Pin::new(termination).poll(cx) {
                self.termination = None;
                match result {
                    Ok(Termination::Lagged(error)) => return self.finish(error.into()),
                    Ok(Termination::Failed(error)) => self.error = Some(error),
                    Err(_) => {}
                }
            }
        }
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(Err(error))) => self.finish(error),
            Poll::Ready(Some(delta)) => Poll::Ready(Some(delta)),
            other => {
                if let Some(error) = self.error.take() {
                    return self.finish(error);
                }
                if matches!(other, Poll::Ready(None)) && self.termination.is_none() {
                    self.finished = true;
                    Poll::Ready(None)
                } else {
                    Poll::Pending
                }
            }
        }
    }

    pub(crate) async fn recv(&mut self) -> Option<Result<IncrementalQueryDelta>> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    pub(crate) fn try_recv(
        &mut self,
    ) -> Result<Result<IncrementalQueryDelta>, mpsc::error::TryRecvError> {
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        match self.poll_recv(&mut cx) {
            Poll::Ready(Some(delta)) => Ok(delta),
            Poll::Ready(None) => Err(mpsc::error::TryRecvError::Disconnected),
            Poll::Pending => Err(mpsc::error::TryRecvError::Empty),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::BOOTSTRAP_TX_KEY;

    #[tokio::test]
    async fn lag_error_bypasses_full_result_queue() {
        let (sender, receiver) = mpsc::channel(1);
        let (terminal, termination) = oneshot::channel();
        let mut deltas = SubscriptionDeltas::new(receiver, termination);
        sender
            .send(Ok(IncrementalQueryDelta {
                tx_key: *BOOTSTRAP_TX_KEY,
                rows: vec![],
            }))
            .await
            .unwrap();
        terminal
            .send(Termination::Lagged(SubscriptionLagged {
                tx_key: *BOOTSTRAP_TX_KEY,
                capacity: 1,
            }))
            .unwrap();
        let error = deltas.recv().await.unwrap().unwrap_err();
        assert!(error.downcast_ref::<SubscriptionLagged>().is_some());
        assert!(sender.is_closed());
        assert!(deltas.recv().await.is_none());
    }

    #[tokio::test]
    async fn query_error_follows_successful_results() {
        let (sender, receiver) = mpsc::channel(1);
        let (terminal, termination) = oneshot::channel();
        let mut deltas = SubscriptionDeltas::new(receiver, termination);
        sender
            .send(Ok(IncrementalQueryDelta {
                tx_key: *BOOTSTRAP_TX_KEY,
                rows: vec![],
            }))
            .await
            .unwrap();
        terminal
            .send(Termination::Failed(
                std::io::Error::other("query failed").into(),
            ))
            .unwrap();
        assert!(deltas.recv().await.unwrap().is_ok());
        let error = deltas.recv().await.unwrap().unwrap_err();
        assert!(error.downcast_ref::<std::io::Error>().is_some());
        assert!(deltas.recv().await.is_none());
    }
}
