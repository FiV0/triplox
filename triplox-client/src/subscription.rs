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
use tokio_util::codec::{Decoder, FramedRead};
use tokio_util::io::StreamReader;

use crate::msgpack_codec::{subscription_frame_from_value, ErrorResponseBody, SubscriptionFrame};
use crate::ops::DataType;
use crate::protocol::DEFAULT_MAX_MESSAGE_SIZE;
use crate::transaction::TxBasis;

type ByteStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>>;
type FrameStream = FramedRead<StreamReader<ByteStream, Bytes>, MsgpackFrameDecoder>;

/// A single transaction's z-set changes for a subscribed query.
#[derive(Debug, Clone, PartialEq)]
pub struct Delta {
    /// The transaction basis that produced this delta.
    pub basis: TxBasis,
    /// `(values, weight)` rows; `weight` is the raw signed multiplicity.
    pub rows: Vec<(Vec<DataType>, i64)>,
}

/// A live incremental query subscription.
///
/// Implements `Stream<Item = Result<Delta>>`; use `StreamExt::next().await` or
/// stream combinators. Dropping it cancels the HTTP/2 stream and unsubscribes.
pub struct Subscription {
    basis: TxBasis,
    frames: FrameStream,
    done: bool,
}

impl Subscription {
    /// The registration basis. Deltas describe transactions strictly after it.
    pub fn basis(&self) -> TxBasis {
        self.basis
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
        let basis = match frames.next().await {
            Some(Ok(SubscriptionFrame::Open { basis, .. })) => basis,
            Some(Ok(SubscriptionFrame::Error(err))) => return Err(error_frame_to_error(err)),
            Some(Ok(other)) => bail!("expected open frame, got {other:?}"),
            Some(Err(err)) => return Err(err),
            None => bail!("subscription stream closed before the open frame"),
        };
        Ok(Subscription {
            basis,
            frames,
            done: false,
        })
    }
}

impl Stream for Subscription {
    type Item = Result<Delta>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        match Pin::new(&mut this.frames).poll_next(cx) {
            Poll::Ready(Some(Ok(frame))) => match frame {
                SubscriptionFrame::Delta { basis, rows } => {
                    Poll::Ready(Some(Ok(Delta { basis, rows })))
                }
                SubscriptionFrame::Error(err) => {
                    this.done = true;
                    Poll::Ready(Some(Err(error_frame_to_error(err))))
                }
                SubscriptionFrame::Open { .. } => {
                    this.done = true;
                    Poll::Ready(Some(Err(anyhow!("unexpected open frame mid-stream"))))
                }
            },
            Poll::Ready(Some(Err(err))) => {
                this.done = true;
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn error_frame_to_error(err: ErrorResponseBody) -> Error {
    anyhow!("subscription error (code {}): {}", err.code, err.message)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msgpack_codec::encode_subscription_frame;
    use crate::protocol::ColumnDescription;
    use crate::transaction::TxKey;
    use chrono::{TimeZone, Utc};

    fn sample_basis() -> TxBasis {
        TxBasis {
            tx_key: TxKey {
                tx_id: 3,
                system_time: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            },
            tx_eid: 42,
        }
    }

    fn open_bytes() -> Vec<u8> {
        encode_subscription_frame(&SubscriptionFrame::Open {
            basis: sample_basis(),
            columns: vec![ColumnDescription {
                name: "n".to_string(),
                data_type: 255,
                members: None,
            }],
        })
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

    #[test]
    fn subscription_surfaces_unknown_frame_kind_error() {
        futures::executor::block_on(async {
            let mut payload = Vec::new();
            payload.extend(open_bytes());
            payload.extend(unknown_bytes());
            let stream =
                futures::stream::once(async move { Ok::<Bytes, io::Error>(Bytes::from(payload)) });

            let mut sub = Subscription::from_byte_stream(stream).await.unwrap();
            assert_eq!(sub.basis(), sample_basis());

            let err = sub.next().await.expect("an item").unwrap_err();
            assert!(err
                .to_string()
                .contains("unknown subscription frame kind: heartbeat"));
            assert!(sub.next().await.is_none(), "done after error");
        });
    }

    #[test]
    fn subscription_surfaces_error_frame() {
        futures::executor::block_on(async {
            let mut payload = Vec::new();
            payload.extend(open_bytes());
            payload.extend(
                encode_subscription_frame(&SubscriptionFrame::Error(ErrorResponseBody {
                    severity: b'F',
                    code: 4000,
                    message: "boom".to_string(),
                    detail: None,
                    hint: None,
                }))
                .unwrap(),
            );
            let stream =
                futures::stream::once(async move { Ok::<Bytes, io::Error>(Bytes::from(payload)) });

            let mut sub = Subscription::from_byte_stream(stream).await.unwrap();
            let err = sub.next().await.expect("an item").unwrap_err();
            assert!(err.to_string().contains("4000"));
            assert!(sub.next().await.is_none(), "done after error");
        });
    }
}
