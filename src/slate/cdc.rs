use std::collections::VecDeque;
use std::time::Duration;

use slatedb::{RowEntry, WalFile, WalFileIterator, WalReader};
use tokio_util::sync::CancellationToken;

/// Tracks position in the WAL stream for resumability.
#[derive(Debug, Default, Clone)]
pub struct CdcCursor {
    pub wal_id: u64,
    pub last_seq: u64,
}

/// A single CDC transaction: all RowEntries sharing one seq number.
pub struct CdcTransaction {
    pub seq: u64,
    pub entries: Vec<RowEntry>,
}

/// Streams CDC transactions from WAL, resumable via cursor.
/// Blocks (async) until a new transaction is available or the cancel token is triggered.
pub struct CdcStream {
    wal_reader: WalReader,
    wal_files: VecDeque<WalFile>,
    current_iter: Option<WalFileIterator>,
    buffered: Option<RowEntry>,
    cursor: CdcCursor,
    poll_interval: Duration,
    cancel: CancellationToken,
}

impl CdcStream {
    /// Create a new CDC stream starting from the given cursor position.
    ///
    /// - `cursor`: Use `CdcCursor::default()` to start from the beginning.
    /// - `poll_interval`: How often to poll for new WAL files when caught up.
    /// - `cancel`: Trigger to stop blocking and return `Ok(None)`.
    pub async fn new(
        wal_reader: WalReader,
        cursor: CdcCursor,
        poll_interval: Duration,
        cancel: CancellationToken,
    ) -> Result<Self, slatedb::Error> {
        let wal_files: VecDeque<WalFile> =
            wal_reader.list(cursor.wal_id..).await?.into();

        let mut stream = CdcStream {
            wal_reader,
            wal_files,
            current_iter: None,
            buffered: None,
            cursor,
            poll_interval,
            cancel,
        };

        // Open the first WAL file's iterator.
        stream.advance_to_next_file().await?;

        Ok(stream)
    }

    /// Return the next transaction. Blocks until data is available.
    /// Returns `Ok(None)` only when the cancel token is triggered.
    pub async fn next_transaction(&mut self) -> Result<Option<CdcTransaction>, slatedb::Error> {
        // Get the first entry for this transaction.
        let first = match self.next_entry().await? {
            Some(entry) => entry,
            None => return Ok(None), // cancelled
        };

        let seq = first.seq;
        let mut entries = vec![first];

        // Accumulate entries with the same seq.
        loop {
            match self.next_entry().await? {
                Some(entry) if entry.seq == seq => {
                    entries.push(entry);
                }
                Some(entry) => {
                    // Different seq — buffer for next call.
                    self.buffered = Some(entry);
                    break;
                }
                None => {
                    // Cancelled or no more data — yield what we have.
                    break;
                }
            }
        }

        self.cursor.last_seq = seq;
        Ok(Some(CdcTransaction { seq, entries }))
    }

    /// Return the current cursor position.
    pub fn cursor(&self) -> &CdcCursor {
        &self.cursor
    }

    /// Get the next entry, either from the buffer or by reading from WAL files.
    /// Skips entries with seq <= cursor.last_seq (already processed).
    /// Blocks (polls) when no data is available. Returns None if cancelled.
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, slatedb::Error> {
        // Return buffered entry first.
        if let Some(entry) = self.buffered.take() {
            return Ok(Some(entry));
        }

        let skip_seq = self.cursor.last_seq;

        loop {
            // Try reading from current iterator, skipping already-processed entries.
            if let Some(ref mut iter) = self.current_iter {
                while let Some(entry) = iter.next().await? {
                    if entry.seq > skip_seq {
                        return Ok(Some(entry));
                    }
                }
            }

            // Current file exhausted — try next file.
            if self.advance_to_next_file().await? {
                continue;
            }

            // No more files — poll for new WAL files (after the one we already processed).
            loop {
                let new_files: VecDeque<WalFile> = self
                    .wal_reader
                    .list((self.cursor.wal_id + 1)..)
                    .await?
                    .into();

                if !new_files.is_empty() {
                    self.wal_files = new_files;
                    self.advance_to_next_file().await?;
                    break;
                }

                // Wait for new data or cancellation.
                tokio::select! {
                    _ = tokio::time::sleep(self.poll_interval) => continue,
                    _ = self.cancel.cancelled() => return Ok(None),
                }
            }
        }
    }

    /// Pop the next WAL file from the queue and open its iterator.
    /// Returns true if a new file was opened, false if queue is empty.
    async fn advance_to_next_file(&mut self) -> Result<bool, slatedb::Error> {
        if let Some(file) = self.wal_files.pop_front() {
            self.cursor.wal_id = file.id;
            self.current_iter = Some(file.iterator().await?);
            Ok(true)
        } else {
            self.current_iter = None;
            Ok(false)
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::config::{FlushOptions, FlushType};
    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, ValueDeletable};
    use std::sync::Arc;

    async fn setup_db(path: &str) -> (Db, Arc<dyn slatedb::object_store::ObjectStore>) {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(InMemory::new());
        let db = Db::open(path, object_store.clone()).await.unwrap();
        (db, object_store)
    }

    fn flush_opts() -> FlushOptions {
        FlushOptions {
            flush_type: FlushType::Wal,
        }
    }

    #[tokio::test]
    async fn test_empty_wal() {
        let (db, object_store) = setup_db("/test_empty").await;
        let wal_reader = WalReader::new("/test_empty", object_store);
        let cancel = CancellationToken::new();
        cancel.cancel(); // cancel immediately so next_transaction returns None
        let mut stream = CdcStream::new(
            wal_reader,
            CdcCursor::default(),
            Duration::from_millis(10),
            cancel,
        )
        .await
        .unwrap();

        let result = stream.next_transaction().await.unwrap();
        assert!(result.is_none());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_single_transaction() {
        let (db, object_store) = setup_db("/test_single").await;

        db.put(b"k1", b"v1").await.unwrap();
        db.put(b"k2", b"v2").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();

        let wal_reader = WalReader::new("/test_single", object_store);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut stream = CdcStream::new(
            wal_reader,
            CdcCursor::default(),
            Duration::from_millis(10),
            cancel,
        )
        .await
        .unwrap();

        // Collect all transactions.
        let mut txs = Vec::new();
        while let Some(tx) = stream.next_transaction().await.unwrap() {
            txs.push(tx);
        }
        assert!(!txs.is_empty());
        // All entries across transactions should include our keys.
        let total_entries: usize = txs.iter().map(|t| t.entries.len()).sum();
        assert!(total_entries >= 2);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_multiple_transactions() {
        let (db, object_store) = setup_db("/test_multi").await;

        // First batch.
        db.put(b"k1", b"v1").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();

        // Second batch.
        db.put(b"k2", b"v2").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();

        // Third batch.
        db.put(b"k3", b"v3").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();

        let wal_reader = WalReader::new("/test_multi", object_store);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut stream = CdcStream::new(
            wal_reader,
            CdcCursor::default(),
            Duration::from_millis(10),
            cancel,
        )
        .await
        .unwrap();

        let mut txs = Vec::new();
        while let Some(tx) = stream.next_transaction().await.unwrap() {
            txs.push(tx);
        }

        assert!(txs.len() >= 3);
        // Seqs should be strictly increasing.
        for window in txs.windows(2) {
            assert!(window[0].seq < window[1].seq);
        }

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_filtering_by_cursor() {
        let (db, object_store) = setup_db("/test_filter").await;

        db.put(b"k1", b"v1").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();

        db.put(b"k2", b"v2").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();

        // First, read all to find the first transaction's seq.
        let wal_reader = WalReader::new("/test_filter", object_store.clone());
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut stream = CdcStream::new(
            wal_reader,
            CdcCursor::default(),
            Duration::from_millis(10),
            cancel,
        )
        .await
        .unwrap();

        let tx1 = stream.next_transaction().await.unwrap().unwrap();
        let cursor_after_tx1 = stream.cursor().clone();

        // Now create a new stream starting after tx1.
        let wal_reader2 = WalReader::new("/test_filter", object_store);
        let cancel2 = CancellationToken::new();
        cancel2.cancel();
        let mut stream2 = CdcStream::new(
            wal_reader2,
            cursor_after_tx1,
            Duration::from_millis(10),
            cancel2,
        )
        .await
        .unwrap();

        let tx2 = stream2.next_transaction().await.unwrap().unwrap();
        // tx2's seq should be greater than tx1's.
        assert!(tx2.seq > tx1.seq);

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_tombstones() {
        let (db, object_store) = setup_db("/test_tombstone").await;

        db.put(b"k1", b"v1").await.unwrap();
        db.delete(b"k1").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();

        let wal_reader = WalReader::new("/test_tombstone", object_store);
        let cancel = CancellationToken::new();
        // Cancel immediately so we don't block after consuming available data.
        cancel.cancel();
        let mut stream = CdcStream::new(
            wal_reader,
            CdcCursor::default(),
            Duration::from_millis(10),
            cancel,
        )
        .await
        .unwrap();

        // Collect all entries across transactions.
        let mut all_entries = Vec::new();
        while let Some(tx) = stream.next_transaction().await.unwrap() {
            all_entries.extend(tx.entries);
        }

        // Should have a tombstone entry.
        let has_tombstone = all_entries
            .iter()
            .any(|e| matches!(e.value, ValueDeletable::Tombstone));
        assert!(has_tombstone);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_start_seq_beyond_data() {
        let (db, object_store) = setup_db("/test_beyond").await;

        db.put(b"k1", b"v1").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();

        let wal_reader = WalReader::new("/test_beyond", object_store);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut stream = CdcStream::new(
            wal_reader,
            CdcCursor {
                wal_id: 0,
                last_seq: u64::MAX - 1,
            },
            Duration::from_millis(10),
            cancel,
        )
        .await
        .unwrap();

        let result = stream.next_transaction().await.unwrap();
        assert!(result.is_none());
        db.close().await.unwrap();
    }

    // --- Snapshot + CdcStream integration tests ---

    #[tokio::test]
    async fn test_cdc_after_snapshot_all_caught_up() {
        let (db, object_store) = setup_db("/test_snap_caught_up").await;

        db.put(b"k1", b"v1").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();
        db.put(b"k2", b"v2").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();
        db.put(b"k3", b"v3").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();

        let snapshot = db.snapshot().await.unwrap();
        let snap_seq = snapshot.seq();

        // CdcStream starting from snapshot seq — nothing new after snapshot.
        let wal_reader = WalReader::new("/test_snap_caught_up", object_store);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut stream = CdcStream::new(
            wal_reader,
            CdcCursor { wal_id: 0, last_seq: snap_seq },
            Duration::from_millis(10),
            cancel,
        )
        .await
        .unwrap();

        let result = stream.next_transaction().await.unwrap();
        assert!(result.is_none(), "expected no transactions after snapshot");
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_cdc_after_snapshot_some_new() {
        let (db, object_store) = setup_db("/test_snap_some_new").await;

        // Before snapshot.
        db.put(b"k1", b"v1").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();
        db.put(b"k2", b"v2").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();

        let snapshot = db.snapshot().await.unwrap();
        let snap_seq = snapshot.seq();

        // After snapshot.
        db.put(b"k3", b"v3").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();
        db.put(b"k4", b"v4").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();

        let wal_reader = WalReader::new("/test_snap_some_new", object_store);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut stream = CdcStream::new(
            wal_reader,
            CdcCursor { wal_id: 0, last_seq: snap_seq },
            Duration::from_millis(10),
            cancel,
        )
        .await
        .unwrap();

        let mut txs = Vec::new();
        while let Some(tx) = stream.next_transaction().await.unwrap() {
            assert!(tx.seq > snap_seq, "tx.seq {} should be > snapshot seq {}", tx.seq, snap_seq);
            txs.push(tx);
        }
        assert!(txs.len() >= 2, "expected at least 2 post-snapshot transactions, got {}", txs.len());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_cdc_after_snapshot_all_after() {
        let (db, object_store) = setup_db("/test_snap_all_after").await;

        // Snapshot on empty-ish DB.
        let snapshot = db.snapshot().await.unwrap();
        let snap_seq = snapshot.seq();

        // All transactions after snapshot.
        db.put(b"k1", b"v1").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();
        db.put(b"k2", b"v2").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();

        let wal_reader = WalReader::new("/test_snap_all_after", object_store);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut stream = CdcStream::new(
            wal_reader,
            CdcCursor { wal_id: 0, last_seq: snap_seq },
            Duration::from_millis(10),
            cancel,
        )
        .await
        .unwrap();

        let mut txs = Vec::new();
        while let Some(tx) = stream.next_transaction().await.unwrap() {
            assert!(tx.seq > snap_seq, "tx.seq {} should be > snapshot seq {}", tx.seq, snap_seq);
            txs.push(tx);
        }
        assert!(txs.len() >= 2, "expected at least 2 transactions, got {}", txs.len());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_cdc_after_snapshot_live_streaming() {
        let (db, object_store) = setup_db("/test_snap_live").await;

        db.put(b"k1", b"v1").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();

        let snapshot = db.snapshot().await.unwrap();
        let snap_seq = snapshot.seq();

        // Create stream BEFORE new transactions exist.
        let wal_reader = WalReader::new("/test_snap_live", object_store);
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let mut stream = CdcStream::new(
            wal_reader,
            CdcCursor { wal_id: 0, last_seq: snap_seq },
            Duration::from_millis(10),
            cancel,
        )
        .await
        .unwrap();

        // Spawn background task to write new transactions then cancel.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            db.put(b"k2", b"v2").await.unwrap();
            db.flush_with_options(flush_opts()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
            db.put(b"k3", b"v3").await.unwrap();
            db.flush_with_options(flush_opts()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
            cancel_clone.cancel();
        });

        let mut txs = Vec::new();
        while let Some(tx) = stream.next_transaction().await.unwrap() {
            assert!(tx.seq > snap_seq, "tx.seq {} should be > snapshot seq {}", tx.seq, snap_seq);
            txs.push(tx);
        }
        assert!(txs.len() >= 2, "expected at least 2 live transactions, got {}", txs.len());
    }

    #[tokio::test]
    async fn test_cdc_after_snapshot_mixed_timing() {
        let (db, object_store) = setup_db("/test_snap_mixed").await;

        // Before snapshot.
        db.put(b"k1", b"v1").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();

        let snapshot = db.snapshot().await.unwrap();
        let snap_seq = snapshot.seq();

        // After snapshot but before stream creation.
        db.put(b"k2", b"v2").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();

        // Create stream — k2 is already in WAL.
        let wal_reader = WalReader::new("/test_snap_mixed", object_store);
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let mut stream = CdcStream::new(
            wal_reader,
            CdcCursor { wal_id: 0, last_seq: snap_seq },
            Duration::from_millis(10),
            cancel,
        )
        .await
        .unwrap();

        // Spawn background task to write more then cancel.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            db.put(b"k3", b"v3").await.unwrap();
            db.flush_with_options(flush_opts()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
            cancel_clone.cancel();
        });

        let mut txs = Vec::new();
        while let Some(tx) = stream.next_transaction().await.unwrap() {
            assert!(tx.seq > snap_seq, "tx.seq {} should be > snapshot seq {}", tx.seq, snap_seq);
            txs.push(tx);
        }
        // Should see both k2 (already there) and k3 (arrived live).
        assert!(txs.len() >= 2, "expected at least 2 transactions (pre-stream + live), got {}", txs.len());
    }
}
