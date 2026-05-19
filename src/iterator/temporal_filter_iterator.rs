use std::sync::Arc;

use bytes::Bytes;

use anyhow::Error;
use slatedb::{DbMetadataOps, DbReadOps};
use tokio::runtime::Handle;

use crate::codec;
use crate::slate::DEFAULT_SCAN_OPTIONS;
use crate::util::next_prefix;

use super::slate_iterator::{Extractor, Index};

/// An iterator that wraps a SlateDB prefix scan and resolves temporal versions.
///
/// Keys are expected to have the layout: `[prefix...][data...][T:8][op:1]`
/// where T is a descending-encoded tx_eid (newest first in sort order).
///
/// For each distinct logical key (everything except T and op), emits only the
/// most recent version whose tx_eid <= `as_of`.
///
/// TODO(#102): Add a history mode option that iterates over all history
/// <= `as_of` including retractions, rather than resolving to current state.
pub(crate) struct TemporalFilterIterator<M>
where
    M: DbMetadataOps + Send + Sync,
{
    inner: slatedb::DbIterator,
    current_key: Option<Bytes>,
    prefix: Bytes,
    handle: Handle,
    extractor: Extractor,
    as_of_encoded: [u8; 8],
    range_stats: Arc<slatedb_estimates::RangeStats<M>>,
}

/// Extract the logical key from a full key (everything except timestamp + op suffix).
#[inline]
fn logical_key(key: &[u8]) -> &[u8] {
    &key[..key.len() - codec::TX_EID_OP_SUFFIX]
}

impl<M> TemporalFilterIterator<M>
where
    M: DbMetadataOps + Send + Sync,
{
    pub fn new<D>(
        prefix: &[u8],
        slate: &Arc<D>,
        handle: Handle,
        extractor: Extractor,
        as_of: i64,
        range_stats: Arc<slatedb_estimates::RangeStats<M>>,
    ) -> Result<Self, Error>
    where
        D: DbReadOps + Send + Sync,
    {
        let prefix_bytes = Bytes::from(prefix.to_vec());
        let as_of_encoded = codec::encode_i64_bytes(as_of);
        let iterator = handle.block_on(
            slate
                .as_ref()
                .scan_prefix_with_options(prefix, &DEFAULT_SCAN_OPTIONS),
        )?;

        let mut iter = Self {
            inner: iterator,
            current_key: None,
            prefix: prefix_bytes,
            handle,
            extractor,
            as_of_encoded,
            range_stats,
        };

        // Advance to the first valid entry
        iter.advance_to_next_valid()?;
        Ok(iter)
    }

    /// Seek the underlying iterator past a logical key group.
    /// Returns true if the seek succeeded, false if no successor exists (all 0xFF).
    fn seek_past_logical_key(&mut self, key: &[u8]) -> Result<bool, Error> {
        // Assumes prefix-free value encodings
        match next_prefix(logical_key(key)) {
            Some(target) => {
                self.handle.block_on(self.inner.seek(Bytes::from(target)))?;
                Ok(true)
            }
            None => Ok(false),
        }
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
                        key.len() >= codec::TX_EID_OP_SUFFIX,
                        "Key too short ({} bytes) to contain tx_eid + op suffix",
                        key.len()
                    );

                    // Descending encoding: ts >= as_of_encoded means real_T <= as_of
                    let ts =
                        &key[key.len() - codec::TX_EID_OP_SUFFIX..key.len() - codec::OP_LENGTH];
                    if ts >= self.as_of_encoded.as_slice() {
                        let op = key[key.len() - 1];
                        if op == codec::RETRACT {
                            if !self.seek_past_logical_key(&key)? {
                                self.current_key = None;
                                return Ok(());
                            }
                            continue;
                        }
                        self.current_key = Some(key);
                        return Ok(());
                    }
                    // Entry is newer than as_of — skip it, keep scanning.
                }
            }
        }
    }

    /// Seek the underlying iterator past all entries in the current logical key group,
    /// then advance to the next valid entry.
    fn advance_past_current_group(&mut self) -> Result<(), Error> {
        let current = match &self.current_key {
            Some(k) => k.clone(),
            None => return Ok(()),
        };
        if !self.seek_past_logical_key(&current)? {
            self.current_key = None;
            return Ok(());
        }
        self.advance_to_next_valid()
    }
}

impl<M> Index for TemporalFilterIterator<M>
where
    M: DbMetadataOps + Send + Sync,
{
    fn count(&self) -> Result<u64, Error> {
        Ok(self.handle.block_on(
            self.range_stats
                .estimate_key_count_with_prefix(&self.prefix),
        )?)
    }

    fn seek(&mut self, extension: Bytes) -> Result<(), Error> {
        let mut full_key = self.prefix.to_vec();
        full_key.extend_from_slice(&extension);
        let full_key = Bytes::from(full_key);

        // Skip forward if already past the target
        if let Some(current) = &self.current_key {
            let current_logical = logical_key(current);
            if current_logical >= full_key.as_ref() {
                return Ok(());
            }
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
    use crate::slate::in_memory_slate;

    const PFX: &[u8] = b"\x04"; // AV prefix

    fn make_key(prefix: &[u8], data: &[u8], tx_eid: i64, op: u8) -> Vec<u8> {
        let mut key = prefix.to_vec();
        key.extend_from_slice(data);
        key.extend_from_slice(&codec::encode_i64_bytes(tx_eid));
        key.push(op);
        key
    }

    fn make_test_extractor(prefix_len: usize) -> Extractor {
        Box::new(move |key: Bytes| key.slice(prefix_len..key.len() - codec::TX_EID_OP_SUFFIX))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_temporal_filter_single_version() {
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let range_stats = components.range_stats.clone();
        let t1 = 1000;

        slate
            .put(&make_key(PFX, b"alice", t1, codec::ADD), b"")
            .await;

        let sdb = slate;
        let handle = tokio::runtime::Handle::current();

        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let iter = TemporalFilterIterator::new(PFX, &sdb, handle, extractor, 2000, range_stats)
                .unwrap();

            assert!(iter.has_next());
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("alice")));
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_temporal_filter_skips_future() {
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let range_stats = components.range_stats.clone();
        let t1 = 1000;
        let t2 = 2000;

        // Two versions of "alice" at t1 and t2
        slate
            .put(&make_key(PFX, b"alice", t1, codec::ADD), b"")
            .await;
        slate
            .put(&make_key(PFX, b"alice", t2, codec::ADD), b"")
            .await;

        let sdb = slate;
        let handle = tokio::runtime::Handle::current();

        tokio::task::spawn_blocking(move || {
            // Query as-of t1 — should see alice (t2 is in the future)
            let extractor = make_test_extractor(PFX.len());
            let iter =
                TemporalFilterIterator::new(PFX, &sdb, handle, extractor, t1, range_stats).unwrap();

            assert!(iter.has_next());
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("alice")));
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_temporal_filter_before_all() {
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let range_stats = components.range_stats.clone();
        let t1 = 2000;

        slate
            .put(&make_key(PFX, b"alice", t1, codec::ADD), b"")
            .await;

        let sdb = slate;
        let handle = tokio::runtime::Handle::current();

        tokio::task::spawn_blocking(move || {
            // Query as-of before t1 — should see nothing
            let extractor = make_test_extractor(PFX.len());
            let iter = TemporalFilterIterator::new(PFX, &sdb, handle, extractor, 1000, range_stats)
                .unwrap();

            assert!(!iter.has_next());
            assert_eq!(iter.get_value().unwrap(), None);
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_temporal_filter_multiple_logical_keys() {
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let range_stats = components.range_stats.clone();
        let t1 = 1000;
        let t2 = 2000;

        slate
            .put(&make_key(PFX, b"alice", t1, codec::ADD), b"")
            .await;
        slate.put(&make_key(PFX, b"bob", t1, codec::ADD), b"").await;
        slate.put(&make_key(PFX, b"bob", t2, codec::ADD), b"").await;

        let sdb = slate;
        let handle = tokio::runtime::Handle::current();

        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let mut iter =
                TemporalFilterIterator::new(PFX, &sdb, handle, extractor, 3000, range_stats)
                    .unwrap();

            // Should see alice and bob (deduplicated)
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("alice")));
            iter.next().unwrap();
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("bob")));
            iter.next().unwrap();
            assert!(!iter.has_next());
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_temporal_filter_add_retract_add_cycle() {
        // Same (E, A, V) added at T1, retracted at T2, re-added at T3.
        // as-of T0: nothing
        // as-of T1: alice visible
        // as-of T2: alice hidden (retracted)
        // as-of T3: alice visible again (re-added)
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let range_stats = components.range_stats.clone();
        let t0 = 500;
        let t1 = 1000;
        let t2 = 2000;
        let t3 = 3000;

        slate
            .put(&make_key(PFX, b"alice", t1, codec::ADD), b"")
            .await;
        slate
            .put(&make_key(PFX, b"alice", t2, codec::RETRACT), b"")
            .await;
        slate
            .put(&make_key(PFX, b"alice", t3, codec::ADD), b"")
            .await;

        let sdb = slate;

        // as-of T0: nothing
        let handle = tokio::runtime::Handle::current();
        let sdb_for_query = sdb.clone();
        let rs = range_stats.clone();
        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let iter = TemporalFilterIterator::new(PFX, &sdb_for_query, handle, extractor, t0, rs)
                .unwrap();
            assert!(!iter.has_next());
        })
        .await
        .unwrap();

        // as-of T1: alice visible
        let handle = tokio::runtime::Handle::current();
        let sdb_for_query = sdb.clone();
        let rs = range_stats.clone();
        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let mut iter =
                TemporalFilterIterator::new(PFX, &sdb_for_query, handle, extractor, t1, rs)
                    .unwrap();
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("alice")));
            iter.next().unwrap();
            assert!(!iter.has_next());
        })
        .await
        .unwrap();

        // as-of T2: alice hidden (retracted)
        let handle = tokio::runtime::Handle::current();
        let sdb_for_query = sdb.clone();
        let rs = range_stats.clone();
        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let iter = TemporalFilterIterator::new(PFX, &sdb_for_query, handle, extractor, t2, rs)
                .unwrap();
            assert!(!iter.has_next());
        })
        .await
        .unwrap();

        // as-of T3: alice visible again (re-added)
        let handle = tokio::runtime::Handle::current();
        let sdb_for_query = sdb.clone();
        let rs = range_stats.clone();
        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let mut iter =
                TemporalFilterIterator::new(PFX, &sdb_for_query, handle, extractor, t3, rs)
                    .unwrap();
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("alice")));
            iter.next().unwrap();
            assert!(!iter.has_next());
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_temporal_filter_seek() {
        // Three logical keys: alice, bob, charlie at T1.
        // Seek to "bob" should land on bob, then next gives charlie.
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let range_stats = components.range_stats.clone();
        let t1 = 1000;

        slate
            .put(&make_key(PFX, b"alice", t1, codec::ADD), b"")
            .await;
        slate.put(&make_key(PFX, b"bob", t1, codec::ADD), b"").await;
        slate
            .put(&make_key(PFX, b"charlie", t1, codec::ADD), b"")
            .await;

        let sdb = slate;
        let handle = tokio::runtime::Handle::current();

        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let mut iter =
                TemporalFilterIterator::new(PFX, &sdb, handle, extractor, 2000, range_stats)
                    .unwrap();

            // Initially at alice
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("alice")));

            // Seek to bob
            iter.seek(Bytes::from("bob")).unwrap();
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("bob")));

            // Next gives charlie
            iter.next().unwrap();
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("charlie")));

            // No more
            iter.next().unwrap();
            assert!(!iter.has_next());
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_temporal_filter_retraction_skips_group() {
        // "alice" added at T1, retracted at T2. "bob" added at T1.
        // As-of T1: see alice and bob.
        // As-of T2: only bob (alice retracted).
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let range_stats = components.range_stats.clone();
        let t1 = 1000;
        let t2 = 2000;

        slate
            .put(&make_key(PFX, b"alice", t1, codec::ADD), b"")
            .await;
        slate
            .put(&make_key(PFX, b"alice", t2, codec::RETRACT), b"")
            .await;
        slate.put(&make_key(PFX, b"bob", t1, codec::ADD), b"").await;

        let sdb = slate;

        // as-of T1: alice and bob
        let handle = tokio::runtime::Handle::current();
        let sdb_for_query = sdb.clone();
        let rs = range_stats.clone();
        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let mut iter =
                TemporalFilterIterator::new(PFX, &sdb_for_query, handle, extractor, t1, rs)
                    .unwrap();
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("alice")));
            iter.next().unwrap();
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("bob")));
            iter.next().unwrap();
            assert!(!iter.has_next());
        })
        .await
        .unwrap();

        // as-of T2: only bob (alice retracted)
        let handle = tokio::runtime::Handle::current();
        let sdb_for_query = sdb.clone();
        let rs = range_stats.clone();
        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let mut iter =
                TemporalFilterIterator::new(PFX, &sdb_for_query, handle, extractor, t2, rs)
                    .unwrap();
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("bob")));
            iter.next().unwrap();
            assert!(!iter.has_next());
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_temporal_filter_retraction_then_re_add() {
        // "alice" added at T1, retracted at T2, re-added at T3.
        // As-of T2: not visible. As-of T3: visible again.
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let range_stats = components.range_stats.clone();
        let t1 = 1000;
        let t2 = 2000;
        let t3 = 3000;

        slate
            .put(&make_key(PFX, b"alice", t1, codec::ADD), b"")
            .await;
        slate
            .put(&make_key(PFX, b"alice", t2, codec::RETRACT), b"")
            .await;
        slate
            .put(&make_key(PFX, b"alice", t3, codec::ADD), b"")
            .await;

        let sdb = slate;

        // as-of T1: alice visible
        let handle = tokio::runtime::Handle::current();
        let sdb_for_query = sdb.clone();
        let rs = range_stats.clone();
        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let iter = TemporalFilterIterator::new(PFX, &sdb_for_query, handle, extractor, t1, rs)
                .unwrap();
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("alice")));
        })
        .await
        .unwrap();

        // as-of T2: alice retracted
        let handle = tokio::runtime::Handle::current();
        let sdb_for_query = sdb.clone();
        let rs = range_stats.clone();
        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let iter = TemporalFilterIterator::new(PFX, &sdb_for_query, handle, extractor, t2, rs)
                .unwrap();
            assert!(!iter.has_next());
        })
        .await
        .unwrap();

        // as-of T3: alice visible again
        let handle = tokio::runtime::Handle::current();
        let sdb_for_query = sdb.clone();
        let rs = range_stats.clone();
        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let iter = TemporalFilterIterator::new(PFX, &sdb_for_query, handle, extractor, t3, rs)
                .unwrap();
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("alice")));
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_temporal_filter_count() {
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let range_stats = components.range_stats.clone();
        let t1 = 1000;

        slate
            .put(&make_key(PFX, b"alice", t1, codec::ADD), b"")
            .await;
        slate.put(&make_key(PFX, b"bob", t1, codec::ADD), b"").await;
        slate
            .put(&make_key(PFX, b"charlie", t1, codec::ADD), b"")
            .await;

        // Flush memtable to SSTs so RangeStats can read the metadata
        slate
            .flush_with_options(slatedb::config::FlushOptions {
                flush_type: slatedb::config::FlushType::MemTable,
            })
            .await
            .unwrap();

        let sdb = slate;
        let handle = tokio::runtime::Handle::current();

        tokio::task::spawn_blocking(move || {
            let extractor = make_test_extractor(PFX.len());
            let iter = TemporalFilterIterator::new(PFX, &sdb, handle, extractor, 2000, range_stats)
                .unwrap();
            let count = iter.count().unwrap();
            assert_eq!(count, 3);
        })
        .await
        .unwrap();
    }
}
