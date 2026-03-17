use bytes::Bytes;

use anyhow::Error;
use tokio::runtime::Handle;

use crate::clock::Instant;
use crate::codec;
use crate::slate::DEFAULT_SCAN_OPTIONS;
use crate::util::next_prefix;

use super::slate_iterator::{Extractor, Index};

/// An iterator that wraps a SlateDB prefix scan and resolves temporal versions.
///
/// Keys are expected to have the layout: `[prefix...][data...][T:8][op:1]`
/// where T is an inverted big-endian timestamp (newest first in sort order).
///
/// For each distinct logical key (everything except T and op), emits only the
/// most recent version whose real timestamp <= `as_of`.
///
/// TODO(triplox-28q): Add a history mode option that iterates over all history
/// <= `as_of` including retractions, rather than resolving to current state.
pub(crate) struct TemporalFilterIterator {
    inner: slatedb::DbIterator,
    current_key: Option<Bytes>,
    prefix: Bytes,
    handle: Handle,
    extractor: Extractor,
    as_of_encoded: [u8; 8],
}

/// Extract the logical key from a full key (everything except timestamp + op suffix).
fn logical_key(key: &[u8]) -> &[u8] {
    &key[..key.len() - codec::TIMESTAMP_OP_SUFFIX]
}

/// Extract the encoded timestamp bytes from a full key.
fn timestamp_bytes(key: &[u8]) -> &[u8] {
    &key[key.len() - codec::TIMESTAMP_OP_SUFFIX..key.len() - codec::OP_LENGTH]
}

impl TemporalFilterIterator {
    pub fn new(
        prefix: &[u8],
        slate: &slatedb::DbSnapshot,
        handle: Handle,
        extractor: Extractor,
        as_of: Instant,
    ) -> Result<Self, Error> {
        let prefix_bytes = Bytes::from(prefix.to_vec());
        let as_of_encoded = codec::encode_timestamp(as_of);
        let mut iterator =
            handle.block_on(slate.scan_prefix_with_options(prefix, &DEFAULT_SCAN_OPTIONS))?;

        let mut iter = Self {
            inner: iterator,
            current_key: None,
            prefix: prefix_bytes,
            handle,
            extractor,
            as_of_encoded,
        };

        // Advance to the first valid entry
        iter.advance_to_next_valid()?;
        Ok(iter)
    }

    /// Advance the underlying iterator to the next key whose timestamp <= as_of,
    /// skipping entries newer than as_of and groups whose newest valid entry is a retraction.
    fn advance_to_next_valid(&mut self) -> Result<(), Error> {
        loop {
            let entry = self.handle.block_on(self.inner.next())?;
            match entry {
                None => {
                    self.current_key = None;
                    return Ok(());
                }
                Some(kv) => {
                    let key = kv.key;
                    assert!(
                        key.len() >= codec::TIMESTAMP_OP_SUFFIX,
                        "Key too short ({} bytes) to contain timestamp + op suffix",
                        key.len()
                    );

                    let ts = timestamp_bytes(&key);
                    // Inverted encoding: smaller encoded bytes = newer real time.
                    // We want real_T <= as_of, i.e. encoded_T >= as_of_encoded.
                    if ts >= &self.as_of_encoded[..] {
                        // This is the newest valid entry for this logical key group.
                        // Check the op byte: if retracted, skip the entire group.
                        let op = key[key.len() - 1];
                        if op == codec::RETRACT {
                            // Seek past this logical key group and find the next valid entry.
                            // advance_past_current_group calls advance_to_next_valid internally.
                            self.current_key = Some(key);
                            return self.advance_past_current_group();
                        }
                        self.current_key = Some(key);
                        return Ok(());
                    }
                    // ts < as_of_encoded means this entry is newer than as_of — skip it,
                    // but we're still in the same or a new group, keep scanning.
                }
            }
        }
    }

    /// Seek the underlying iterator past all entries in the current logical key group.
    fn advance_past_current_group(&mut self) -> Result<(), Error> {
        let current = match &self.current_key {
            Some(k) => k.clone(),
            None => return Ok(()),
        };

        match next_prefix(logical_key(&current)) {
            Some(target) => {
                self.handle
                    .block_on(self.inner.seek(Bytes::from(target)))?;
                self.advance_to_next_valid()
            }
            None => {
                // Logical key is all 0xFF — no successor, we're done
                self.current_key = None;
                Ok(())
            }
        }
    }
}

impl Index for TemporalFilterIterator {
    fn count(&self) -> Result<u64, Error> {
        // TODO: proper count estimation
        Ok(100)
    }

    fn seek(&mut self, extension: Bytes) -> Result<(), Error> {
        let mut full_key = self.prefix.to_vec();
        full_key.extend_from_slice(&extension);
        let full_key = Bytes::from(full_key);

        // Skip forward if already past the target
        match &self.current_key {
            Some(current) => {
                let current_logical = logical_key(current);
                if current_logical >= full_key.as_ref() {
                    return Ok(());
                }
            }
            _ => {}
        }

        self.handle.block_on(self.inner.seek(full_key))?;
        self.advance_to_next_valid()
    }

    fn next(&mut self) -> Result<Option<Bytes>, Error> {
        // Seek past current group, then find next valid entry
        self.advance_past_current_group()?;
        match &self.current_key {
            Some(key) => Ok(Some((self.extractor)(key.clone()))),
            None => Ok(None),
        }
    }

    fn get_value(&self) -> Result<Option<Bytes>, Error> {
        match &self.current_key {
            Some(key) => Ok(Some((self.extractor)(key.clone()))),
            None => Ok(None),
        }
    }

    fn has_next(&self) -> bool {
        self.current_key.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::st_from_unix_epoch;
    use crate::slate::in_memory_slate;

    const PFX: &[u8] = b"\x04"; // AV prefix

    fn make_key(prefix: &[u8], data: &[u8], time: Instant, op: u8) -> Vec<u8> {
        let mut key = prefix.to_vec();
        key.extend_from_slice(data);
        key.extend_from_slice(&codec::encode_timestamp(time));
        key.push(op);
        key
    }

    fn make_test_extractor(prefix_len: usize) -> Extractor {
        Box::new(move |key: Bytes| {
            key.slice(prefix_len..key.len() - codec::TIMESTAMP_OP_SUFFIX)
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_temporal_filter_single_version() {
        let slate = in_memory_slate().await;
        let t1 = st_from_unix_epoch(1_000_000);

        slate.put(&make_key(PFX, b"alice", t1, codec::ADD), b"").await;

        let snapshot = slate.snapshot().await.unwrap();
        let handle = tokio::runtime::Handle::current();

        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let iter = TemporalFilterIterator::new(
                PFX, &snapshot, handle, extractor, st_from_unix_epoch(2_000_000),
            ).unwrap();

            assert!(iter.has_next());
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("alice")));
        }).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_temporal_filter_skips_future() {
        let slate = in_memory_slate().await;
        let t1 = st_from_unix_epoch(1_000_000);
        let t2 = st_from_unix_epoch(2_000_000);

        // Two versions of "alice" at t1 and t2
        slate.put(&make_key(PFX, b"alice", t1, codec::ADD), b"").await;
        slate.put(&make_key(PFX, b"alice", t2, codec::ADD), b"").await;

        let snapshot = slate.snapshot().await.unwrap();
        let handle = tokio::runtime::Handle::current();

        tokio::task::spawn_blocking(move || {
            // Query as-of t1 — should see alice (t2 is in the future)
            let extractor = make_test_extractor(PFX.len());
            let iter = TemporalFilterIterator::new(
                PFX, &snapshot, handle, extractor, t1,
            ).unwrap();

            assert!(iter.has_next());
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("alice")));
        }).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_temporal_filter_before_all() {
        let slate = in_memory_slate().await;
        let t1 = st_from_unix_epoch(2_000_000);

        slate.put(&make_key(PFX, b"alice", t1, codec::ADD), b"").await;

        let snapshot = slate.snapshot().await.unwrap();
        let handle = tokio::runtime::Handle::current();

        tokio::task::spawn_blocking(move || {
            // Query as-of before t1 — should see nothing
            let extractor = make_test_extractor(PFX.len());
            let iter = TemporalFilterIterator::new(
                PFX, &snapshot, handle, extractor, st_from_unix_epoch(1_000_000),
            ).unwrap();

            assert!(!iter.has_next());
            assert_eq!(iter.get_value().unwrap(), None);
        }).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_temporal_filter_multiple_logical_keys() {
        let slate = in_memory_slate().await;
        let t1 = st_from_unix_epoch(1_000_000);
        let t2 = st_from_unix_epoch(2_000_000);

        slate.put(&make_key(PFX, b"alice", t1, codec::ADD), b"").await;
        slate.put(&make_key(PFX, b"bob", t1, codec::ADD), b"").await;
        slate.put(&make_key(PFX, b"bob", t2, codec::ADD), b"").await;

        let snapshot = slate.snapshot().await.unwrap();
        let handle = tokio::runtime::Handle::current();

        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let mut iter = TemporalFilterIterator::new(
                PFX, &snapshot, handle, extractor, st_from_unix_epoch(3_000_000),
            ).unwrap();

            // Should see alice and bob (deduplicated)
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("alice")));
            iter.next().unwrap();
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("bob")));
            iter.next().unwrap();
            assert!(!iter.has_next());
        }).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_temporal_filter_add_add_cycle() {
        // Same (E, A, V) added at T1 and T2.
        // Query as-of T1 sees it once, as-of T2 sees it once, as-of T0 sees nothing.
        let slate = in_memory_slate().await;
        let t0 = st_from_unix_epoch(500_000);
        let t1 = st_from_unix_epoch(1_000_000);
        let t2 = st_from_unix_epoch(2_000_000);

        slate.put(&make_key(PFX, b"alice", t1, codec::ADD), b"").await;
        slate.put(&make_key(PFX, b"alice", t2, codec::ADD), b"").await;

        let snapshot = slate.snapshot().await.unwrap();

        // as-of T0: nothing
        let handle = tokio::runtime::Handle::current();
        let snap = snapshot.clone();
        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let iter = TemporalFilterIterator::new(
                PFX, &snap, handle, extractor, t0,
            ).unwrap();
            assert!(!iter.has_next());
        }).await.unwrap();

        // as-of T1: alice once
        let handle = tokio::runtime::Handle::current();
        let snap = snapshot.clone();
        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let mut iter = TemporalFilterIterator::new(
                PFX, &snap, handle, extractor, t1,
            ).unwrap();
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("alice")));
            iter.next().unwrap();
            assert!(!iter.has_next());
        }).await.unwrap();

        // as-of T2: alice once (deduplicated)
        let handle = tokio::runtime::Handle::current();
        let snap = snapshot.clone();
        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let mut iter = TemporalFilterIterator::new(
                PFX, &snap, handle, extractor, t2,
            ).unwrap();
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("alice")));
            iter.next().unwrap();
            assert!(!iter.has_next());
        }).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_temporal_filter_retraction_skips_group() {
        // "alice" added at T1, retracted at T2. "bob" added at T1.
        // As-of T1: see alice and bob.
        // As-of T2: only bob (alice retracted).
        let slate = in_memory_slate().await;
        let t1 = st_from_unix_epoch(1_000_000);
        let t2 = st_from_unix_epoch(2_000_000);

        slate.put(&make_key(PFX, b"alice", t1, codec::ADD), b"").await;
        slate.put(&make_key(PFX, b"alice", t2, codec::RETRACT), b"").await;
        slate.put(&make_key(PFX, b"bob", t1, codec::ADD), b"").await;

        let snapshot = slate.snapshot().await.unwrap();

        // as-of T1: alice and bob
        let handle = tokio::runtime::Handle::current();
        let snap = snapshot.clone();
        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let mut iter = TemporalFilterIterator::new(
                PFX, &snap, handle, extractor, t1,
            ).unwrap();
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("alice")));
            iter.next().unwrap();
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("bob")));
            iter.next().unwrap();
            assert!(!iter.has_next());
        }).await.unwrap();

        // as-of T2: only bob (alice retracted)
        let handle = tokio::runtime::Handle::current();
        let snap = snapshot.clone();
        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let mut iter = TemporalFilterIterator::new(
                PFX, &snap, handle, extractor, t2,
            ).unwrap();
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("bob")));
            iter.next().unwrap();
            assert!(!iter.has_next());
        }).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_temporal_filter_retraction_then_re_add() {
        // "alice" added at T1, retracted at T2, re-added at T3.
        // As-of T2: not visible. As-of T3: visible again.
        let slate = in_memory_slate().await;
        let t1 = st_from_unix_epoch(1_000_000);
        let t2 = st_from_unix_epoch(2_000_000);
        let t3 = st_from_unix_epoch(3_000_000);

        slate.put(&make_key(PFX, b"alice", t1, codec::ADD), b"").await;
        slate.put(&make_key(PFX, b"alice", t2, codec::RETRACT), b"").await;
        slate.put(&make_key(PFX, b"alice", t3, codec::ADD), b"").await;

        let snapshot = slate.snapshot().await.unwrap();

        // as-of T1: alice visible
        let handle = tokio::runtime::Handle::current();
        let snap = snapshot.clone();
        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let iter = TemporalFilterIterator::new(
                PFX, &snap, handle, extractor, t1,
            ).unwrap();
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("alice")));
        }).await.unwrap();

        // as-of T2: alice retracted
        let handle = tokio::runtime::Handle::current();
        let snap = snapshot.clone();
        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let iter = TemporalFilterIterator::new(
                PFX, &snap, handle, extractor, t2,
            ).unwrap();
            assert!(!iter.has_next());
        }).await.unwrap();

        // as-of T3: alice visible again
        let handle = tokio::runtime::Handle::current();
        let snap = snapshot.clone();
        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let iter = TemporalFilterIterator::new(
                PFX, &snap, handle, extractor, t3,
            ).unwrap();
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("alice")));
        }).await.unwrap();
    }
}
