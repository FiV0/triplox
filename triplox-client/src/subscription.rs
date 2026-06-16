//! Live incremental query subscription over a streaming HTTP/2 response.
//!
//! [`ClientNode::subscribe`](crate::ClientNode::subscribe) returns a
//! [`Subscription`], a `Stream` of [`Delta`]s decoded from the bare,
//! self-delimiting msgpack frames the server streams. Dropping the subscription
//! cancels the HTTP/2 stream, which the server observes as a teardown signal.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::{anyhow, bail, Error, Result};
use bytes::{Buf, Bytes, BytesMut};
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::codec::{Decoder, FramedRead};
use tokio_util::io::StreamReader;

use crate::msgpack_codec::{subscription_frame_from_value, ErrorResponseBody, SubscriptionFrame};
use crate::ops::DataType;
use crate::protocol::DEFAULT_MAX_MESSAGE_SIZE;
use crate::transaction::TxKey;

type ByteStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>>;
type FrameStream = FramedRead<StreamReader<ByteStream, Bytes>, MsgpackFrameDecoder>;

const SUBSCRIPTION_QUEUE_CAPACITY: usize = 128;

/// A single transaction's z-set changes for a subscribed query.
#[derive(Debug, Clone, PartialEq)]
pub struct Delta {
    /// The transaction key that produced this delta.
    pub tx_key: TxKey,
    /// `(values, weight)` rows; `weight` is the raw signed multiplicity.
    pub rows: Vec<(Vec<DataType>, i64)>,
}

/// A live incremental query subscription.
///
/// Implements `Stream<Item = Result<Delta>>`; use `StreamExt::next().await` or
/// stream combinators. Dropping it cancels the HTTP/2 stream and unsubscribes.
pub struct Subscription {
    tx_key: TxKey,
    deltas: mpsc::Receiver<Result<Delta>>,
    reader: JoinHandle<()>,
}

fn error_frame_to_error(err: ErrorResponseBody) -> Error {
    anyhow!("subscription error (code {}): {}", err.code, err.message)
}

async fn read_deltas(mut frames: FrameStream, sender: mpsc::Sender<Result<Delta>>) {
    while let Some(frame) = frames.next().await {
        let (item, terminal) = match frame {
            Ok(SubscriptionFrame::Delta { tx_key, rows }) => (Ok(Delta { tx_key, rows }), false),
            Ok(SubscriptionFrame::Error(err)) => (Err(error_frame_to_error(err)), true),
            Ok(SubscriptionFrame::Open { .. }) => {
                (Err(anyhow!("unexpected open frame mid-stream")), true)
            }
            Err(err) => (Err(err), true),
        };

        if sender.send(item).await.is_err() || terminal {
            break;
        }
    }
}

impl Subscription {
    /// The registration tx_key. Deltas describe transactions strictly after it.
    pub fn tx_key(&self) -> TxKey {
        self.tx_key
    }

    /// Wrap a streaming subscription response: frame its body and read the
    /// leading `open` frame, returning the ready-to-poll subscription.
    pub(crate) async fn connect(resp: reqwest::Response) -> Result<Self> {
        let byte_stream = resp
            .bytes_stream()
            .map(|chunk| chunk.map_err(io::Error::other));
        Self::from_byte_stream(byte_stream).await
    }

    async fn from_byte_stream<S>(stream: S) -> Result<Self>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + 'static,
    {
        let reader = StreamReader::new(Box::pin(stream) as ByteStream);
        let mut frames = FramedRead::new(reader, MsgpackFrameDecoder::default());
        let tx_key = match frames.next().await {
            Some(Ok(SubscriptionFrame::Open { tx_key, .. })) => tx_key,
            Some(Ok(SubscriptionFrame::Error(err))) => return Err(error_frame_to_error(err)),
            Some(Ok(other)) => bail!("expected open frame, got {other:?}"),
            Some(Err(err)) => return Err(err),
            None => bail!("subscription stream closed before the open frame"),
        };
        let (sender, deltas) = mpsc::channel(SUBSCRIPTION_QUEUE_CAPACITY);
        let reader = tokio::spawn(read_deltas(frames, sender));
        Ok(Subscription {
            tx_key,
            deltas,
            reader,
        })
    }
}

impl Stream for Subscription {
    type Item = Result<Delta>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.deltas.poll_recv(cx)
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

/// Frames a byte stream of bare, self-delimiting msgpack values into
/// [`SubscriptionFrame`]s. Returns `Ok(None)` for an incomplete frame (need more
/// bytes); a corrupt or oversized frame is an error.
pub(crate) struct MsgpackFrameDecoder {
    max_frame_size: usize,
}

impl Default for MsgpackFrameDecoder {
    fn default() -> Self {
        Self {
            max_frame_size: DEFAULT_MAX_MESSAGE_SIZE as usize,
        }
    }
}

/// `true` when the decode error is a truncated value (need more bytes) rather
/// than a corrupt one.
fn needs_more_data(err: &rmpv::decode::Error) -> bool {
    match err {
        rmpv::decode::Error::InvalidMarkerRead(e) | rmpv::decode::Error::InvalidDataRead(e) => {
            e.kind() == io::ErrorKind::UnexpectedEof
        }
        rmpv::decode::Error::DepthLimitExceeded => false,
    }
}

impl Decoder for MsgpackFrameDecoder {
    type Item = SubscriptionFrame;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        if src.is_empty() {
            return Ok(None);
        }
        let mut cursor: &[u8] = &src[..];
        let remaining_before = cursor.len();
        match rmpv::decode::read_value(&mut cursor) {
            Ok(value) => {
                let consumed = remaining_before - cursor.len();
                let frame = subscription_frame_from_value(value)?;
                src.advance(consumed);
                Ok(Some(frame))
            }
            Err(err) if needs_more_data(&err) => {
                if src.len() > self.max_frame_size {
                    bail!(
                        "subscription frame exceeds maximum size of {} bytes",
                        self.max_frame_size
                    );
                }
                Ok(None)
            }
            Err(err) => Err(anyhow!("msgpack frame decode error: {err}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msgpack_codec::encode_subscription_frame;
    use crate::protocol::ColumnDescription;
    use crate::transaction::TxKey;
    use chrono::{TimeZone, Utc};
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, LazyLock};
    use tokio::time::{sleep, timeout, Duration};

    static SAMPLE_TX_KEY: LazyLock<TxKey> = LazyLock::new(|| TxKey {
        tx_id: 3,
        system_time: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
    });

    fn open_bytes() -> Vec<u8> {
        encode_subscription_frame(&SubscriptionFrame::Open {
            tx_key: *SAMPLE_TX_KEY,
            columns: vec![ColumnDescription {
                name: "n".to_string(),
                data_type: 255,
                members: None,
            }],
        })
        .unwrap()
    }

    fn delta_bytes(name: &str) -> Vec<u8> {
        encode_subscription_frame(&SubscriptionFrame::Delta {
            tx_key: *SAMPLE_TX_KEY,
            rows: vec![(vec![DataType::String(name.to_string())], 1)],
        })
        .unwrap()
    }

    fn error_bytes() -> Vec<u8> {
        encode_subscription_frame(&SubscriptionFrame::Error(ErrorResponseBody {
            severity: b'F',
            code: 4000,
            message: "boom".to_string(),
            detail: None,
            hint: None,
        }))
        .unwrap()
    }

    fn unknown_bytes() -> Vec<u8> {
        // {"kind": "heartbeat"} — an unsupported frame kind the client must reject.
        let mut buf = Vec::new();
        rmp::encode::write_map_len(&mut buf, 1).unwrap();
        rmp::encode::write_str(&mut buf, "kind").unwrap();
        rmp::encode::write_str(&mut buf, "heartbeat").unwrap();
        buf
    }

    #[test]
    fn decoder_needs_more_then_completes() {
        let bytes = open_bytes();
        let mut decoder = MsgpackFrameDecoder::default();
        let mut buf = BytesMut::from(&bytes[..bytes.len() - 1]);
        assert!(decoder.decode(&mut buf).unwrap().is_none(), "truncated");
        buf.extend_from_slice(&bytes[bytes.len() - 1..]);
        let frame = decoder.decode(&mut buf).unwrap().expect("complete frame");
        assert!(matches!(frame, SubscriptionFrame::Open { .. }));
        assert!(buf.is_empty());
    }

    #[test]
    fn decoder_rejects_non_map_frame() {
        // A complete msgpack value that is not a frame map is a protocol error.
        let mut v = Vec::new();
        rmp::encode::write_uint(&mut v, 5).unwrap();
        let mut buf = BytesMut::from(&v[..]);
        assert!(MsgpackFrameDecoder::default().decode(&mut buf).is_err());
    }

    #[test]
    fn decoder_rejects_oversize_frame() {
        // str8 header declaring 100 bytes; an incomplete frame past the cap errors.
        let mut buf = BytesMut::from(&[0xd9u8, 100, 0x00, 0x00][..]);
        let mut decoder = MsgpackFrameDecoder { max_frame_size: 3 };
        assert!(decoder.decode(&mut buf).is_err());
    }

    #[tokio::test]
    async fn subscription_surfaces_unknown_frame_kind_error() {
        let mut payload = Vec::new();
        payload.extend(open_bytes());
        payload.extend(unknown_bytes());
        let stream =
            futures::stream::once(async move { Ok::<Bytes, io::Error>(Bytes::from(payload)) });

        let mut sub = Subscription::from_byte_stream(stream).await.unwrap();
        assert_eq!(sub.tx_key(), *SAMPLE_TX_KEY);

        let err = sub.next().await.expect("an item").unwrap_err();
        assert!(err
            .to_string()
            .contains("unknown subscription frame kind: heartbeat"));
        assert!(sub.next().await.is_none(), "done after error");
    }

    #[tokio::test]
    async fn subscription_surfaces_error_frame() {
        let mut payload = Vec::new();
        payload.extend(open_bytes());
        payload.extend(error_bytes());
        let stream =
            futures::stream::once(async move { Ok::<Bytes, io::Error>(Bytes::from(payload)) });

        let mut sub = Subscription::from_byte_stream(stream).await.unwrap();
        let err = sub.next().await.expect("an item").unwrap_err();
        assert!(err.to_string().contains("4000"));
        assert!(sub.next().await.is_none(), "done after error");
    }

    #[tokio::test]
    async fn subscription_preserves_queued_delta_before_error_frame() {
        let mut payload = Vec::new();
        payload.extend(open_bytes());
        payload.extend(delta_bytes("Alice"));
        payload.extend(error_bytes());
        let stream =
            futures::stream::once(async move { Ok::<Bytes, io::Error>(Bytes::from(payload)) });

        let mut sub = Subscription::from_byte_stream(stream).await.unwrap();
        let delta = sub.next().await.expect("first item").unwrap();
        assert_eq!(
            delta.rows,
            vec![(vec![DataType::String("Alice".to_string())], 1)]
        );

        let err = sub.next().await.expect("second item").unwrap_err();
        assert!(err.to_string().contains("4000"));
        assert!(sub.next().await.is_none(), "done after error");
    }

    struct CountingByteStream {
        chunks: VecDeque<Bytes>,
        yielded: Arc<AtomicUsize>,
    }

    impl Stream for CountingByteStream {
        type Item = io::Result<Bytes>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match self.chunks.pop_front() {
                Some(chunk) => {
                    self.yielded.fetch_add(1, Ordering::SeqCst);
                    Poll::Ready(Some(Ok(chunk)))
                }
                None => Poll::Ready(None),
            }
        }
    }

    #[tokio::test]
    async fn subscription_reads_ahead_until_delta_queue_is_full() {
        let yielded = Arc::new(AtomicUsize::new(0));
        let mut chunks = VecDeque::new();
        chunks.push_back(Bytes::from(open_bytes()));
        for idx in 0..SUBSCRIPTION_QUEUE_CAPACITY + 3 {
            chunks.push_back(Bytes::from(delta_bytes(&format!("Alice {idx}"))));
        }
        let stream = CountingByteStream {
            chunks,
            yielded: yielded.clone(),
        };

        let mut sub = Subscription::from_byte_stream(stream).await.unwrap();
        let blocked_at = SUBSCRIPTION_QUEUE_CAPACITY + 2;

        timeout(Duration::from_secs(1), async {
            while yielded.load(Ordering::SeqCst) < blocked_at {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reader should fill the bounded delta queue");

        sleep(Duration::from_millis(25)).await;
        assert_eq!(
            yielded.load(Ordering::SeqCst),
            blocked_at,
            "reader should stop pulling once the delta queue is full"
        );

        let delta = sub.next().await.expect("buffered delta").unwrap();
        assert_eq!(
            delta.rows,
            vec![(vec![DataType::String("Alice 0".to_string())], 1)]
        );
        timeout(Duration::from_secs(1), async {
            while yielded.load(Ordering::SeqCst) < blocked_at + 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("draining one delta should let the reader pull one more frame");
    }

    struct DropNotifyStream {
        chunks: VecDeque<Bytes>,
        polled_pending: Arc<AtomicBool>,
        dropped: Arc<AtomicBool>,
    }

    impl Stream for DropNotifyStream {
        type Item = io::Result<Bytes>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match self.chunks.pop_front() {
                Some(chunk) => Poll::Ready(Some(Ok(chunk))),
                None => {
                    self.polled_pending.store(true, Ordering::SeqCst);
                    Poll::Pending
                }
            }
        }
    }

    impl Drop for DropNotifyStream {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn dropping_subscription_aborts_reader_and_drops_stream() {
        let polled_pending = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let mut chunks = VecDeque::new();
        chunks.push_back(Bytes::from(open_bytes()));
        let stream = DropNotifyStream {
            chunks,
            polled_pending: polled_pending.clone(),
            dropped: dropped.clone(),
        };

        let sub = Subscription::from_byte_stream(stream).await.unwrap();
        timeout(Duration::from_secs(1), async {
            while !polled_pending.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reader should poll the upstream stream");

        drop(sub);

        timeout(Duration::from_secs(1), async {
            while !dropped.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropping subscription should abort the reader task");
    }
}
