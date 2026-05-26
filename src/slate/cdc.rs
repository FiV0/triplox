use std::collections::VecDeque;
use std::time::Duration;

use slatedb::{RowEntry, ValueDeletable, WalFile, WalFileIterator, WalReader};
use tokio_util::sync::CancellationToken;

use crate::codec;
use crate::indexer::eav_key_to_parts;
use crate::ops::{DataType, Datom, DatomOp};
use crate::schema::Schema;

/// Tracks the CDC stream position.
///
/// `last_seq` is the exclusive lower bound for rows yielded by the stream.
/// `wal_id` is a WAL discovery hint and is updated to the last opened WAL file.
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
        let wal_files: VecDeque<WalFile> = wal_reader.list(cursor.wal_id..).await?.into();

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

        // Accumulate entries with the same seq inside the current WAL file.
        loop {
            match self.next_entry_in_current_file().await? {
                Some(entry) if entry.seq == seq => {
                    entries.push(entry);
                }
                Some(entry) => {
                    // Different seq — buffer for next call.
                    self.buffered = Some(entry);
                    break;
                }
                None => {
                    // WAL files are immutable and transactions do not cross file
                    // boundaries, so EOF completes the current transaction.
                    break;
                }
            }
        }

        self.cursor.last_seq = seq;
        Ok(Some(CdcTransaction { seq, entries }))
    }

    /// Get the next entry, either from the buffer or by reading from WAL files.
    /// Skips entries with seq <= cursor.last_seq (already processed).
    /// Blocks (polls) when no data is available. Returns None if cancelled.
    async fn next_entry(&mut self) -> Result<Option<RowEntry>, slatedb::Error> {
        // Return buffered entry first.
        if let Some(entry) = self.buffered.take() {
            return Ok(Some(entry));
        }

        loop {
            if let Some(entry) = self.next_entry_in_current_file().await? {
                return Ok(Some(entry));
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

                // Wait for a newly visible immutable WAL file or cancellation.
                tokio::select! {
                    _ = tokio::time::sleep(self.poll_interval) => continue,
                    _ = self.cancel.cancelled() => return Ok(None),
                }
            }
        }
    }

    /// Get the next entry from the already-open WAL file.
    /// Returns None when that immutable file is exhausted.
    async fn next_entry_in_current_file(&mut self) -> Result<Option<RowEntry>, slatedb::Error> {
        let Some(ref mut iter) = self.current_iter else {
            return Ok(None);
        };

        while let Some(entry) = iter.next().await? {
            if entry.seq > self.cursor.last_seq {
                return Ok(Some(entry));
            }
        }

        Ok(None)
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

/// Extract Datoms from a CDC transaction by decoding EAV-prefix index keys.
/// Skips non-EAV keys and tombstone entries.
pub fn datoms_from_cdc_transaction(
    tx: &CdcTransaction,
    schema: &Schema,
) -> Result<Vec<Datom>, anyhow::Error> {
    let mut datoms = Vec::new();
    for entry in &tx.entries {
        if entry.key.first() != Some(&codec::EAV) {
            continue;
        }
        if matches!(entry.value, ValueDeletable::Tombstone) {
            continue;
        }

        let (entity_dt, attribute_id, value, _tx_eid, op_byte) =
            eav_key_to_parts(entry.key.clone())?;

        let entity = match entity_dt {
            DataType::Long(id) => id,
            other => {
                return Err(anyhow::anyhow!(
                    "Expected Long entity in EAV key, got {:?}",
                    other
                ))
            }
        };

        let attribute = schema
            .get_ident(attribute_id)
            .ok_or_else(|| {
                anyhow::anyhow!("Unknown attribute entity_id {} in EAV key", attribute_id)
            })?
            .clone();

        let op = match op_byte {
            codec::ADD => DatomOp::Assert,
            codec::RETRACT => DatomOp::Retract,
            other => return Err(anyhow::anyhow!("Unknown op byte: {}", other)),
        };

        datoms.push(Datom {
            entity,
            attribute,
            value,
            op,
        });
    }
    Ok(datoms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::config::{FlushOptions, FlushType};
    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, ValueDeletable};
    use std::sync::Arc;

    async fn setup_db(path: &str) -> (Db, Arc<dyn slatedb::object_store::ObjectStore>) {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder(path, object_store.clone())
            .build()
            .await
            .unwrap();
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

        // Collect all entries across transactions.
        let mut all_entries = Vec::new();
        while let Some(tx) = stream.next_transaction().await.unwrap() {
            all_entries.extend(tx.entries);
        }

        // Verify the entries contain our keys and values.
        let kv_pairs: Vec<(&[u8], &[u8])> = all_entries
            .iter()
            .filter_map(|e| match &e.value {
                ValueDeletable::Value(v) => Some((e.key.as_ref(), v.as_ref())),
                _ => None,
            })
            .collect();
        assert!(kv_pairs.contains(&(b"k1".as_ref(), b"v1".as_ref())));
        assert!(kv_pairs.contains(&(b"k2".as_ref(), b"v2".as_ref())));
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

        // Now create a new stream starting after tx1's seq.
        let wal_reader2 = WalReader::new("/test_filter", object_store);
        let cancel2 = CancellationToken::new();
        cancel2.cancel();
        let mut stream2 = CdcStream::new(
            wal_reader2,
            CdcCursor {
                wal_id: 0,
                last_seq: tx1.seq,
            },
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

    // --- Cursor + CdcStream integration tests ---

    #[tokio::test]
    async fn test_cdc_cursor_at_end_returns_none() {
        let (db, object_store) = setup_db("/test_cursor_at_end").await;

        db.put(b"k1", b"v1").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();
        db.put(b"k2", b"v2").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();
        db.put(b"k3", b"v3").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();

        let current_seq = db.status().durable_seq;

        // CdcStream starting from the current durable seq has nothing new.
        let wal_reader = WalReader::new("/test_cursor_at_end", object_store);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut stream = CdcStream::new(
            wal_reader,
            CdcCursor {
                wal_id: 0,
                last_seq: current_seq,
            },
            Duration::from_millis(10),
            cancel,
        )
        .await
        .unwrap();

        let result = stream.next_transaction().await.unwrap();
        assert!(result.is_none(), "expected no transactions after cursor");
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_cdc_cursor_skips_existing_transactions() {
        let (db, object_store) = setup_db("/test_cursor_skips_existing").await;

        // Before cursor.
        db.put(b"k1", b"v1").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();
        db.put(b"k2", b"v2").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();

        let current_seq = db.status().durable_seq;

        // After cursor.
        db.put(b"k3", b"v3").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();
        db.put(b"k4", b"v4").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();

        let wal_reader = WalReader::new("/test_cursor_skips_existing", object_store);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut stream = CdcStream::new(
            wal_reader,
            CdcCursor {
                wal_id: 0,
                last_seq: current_seq,
            },
            Duration::from_millis(10),
            cancel,
        )
        .await
        .unwrap();

        let mut txs = Vec::new();
        while let Some(tx) = stream.next_transaction().await.unwrap() {
            assert!(
                tx.seq > current_seq,
                "tx.seq {} should be > cursor seq {}",
                tx.seq,
                current_seq
            );
            txs.push(tx);
        }
        assert!(
            txs.len() >= 2,
            "expected at least 2 post-cursor transactions, got {}",
            txs.len()
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_cdc_cursor_from_zero_reads_later_transactions() {
        let (db, object_store) = setup_db("/test_cursor_from_zero").await;

        // Cursor on empty-ish DB.
        let current_seq = db.status().durable_seq;

        // All transactions after cursor.
        db.put(b"k1", b"v1").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();
        db.put(b"k2", b"v2").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();

        let wal_reader = WalReader::new("/test_cursor_from_zero", object_store);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut stream = CdcStream::new(
            wal_reader,
            CdcCursor {
                wal_id: 0,
                last_seq: current_seq,
            },
            Duration::from_millis(10),
            cancel,
        )
        .await
        .unwrap();

        let mut txs = Vec::new();
        while let Some(tx) = stream.next_transaction().await.unwrap() {
            assert!(
                tx.seq > current_seq,
                "tx.seq {} should be > cursor seq {}",
                tx.seq,
                current_seq
            );
            txs.push(tx);
        }
        assert!(
            txs.len() >= 2,
            "expected at least 2 transactions, got {}",
            txs.len()
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_cdc_cursor_last_seq_skips_old_wals_then_polls_next_wal() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (db, object_store) = setup_db("/test_cursor_seq_start_then_live").await;

            db.put(b"k1", b"v1").await.unwrap();
            db.flush_with_options(flush_opts()).await.unwrap();
            db.put(b"k2", b"v2").await.unwrap();
            db.flush_with_options(flush_opts()).await.unwrap();

            let current_seq = db.status().durable_seq;
            let wal_reader = WalReader::new("/test_cursor_seq_start_then_live", object_store);
            let cancel = CancellationToken::new();
            let mut stream = CdcStream::new(
                wal_reader,
                CdcCursor {
                    wal_id: 0,
                    last_seq: current_seq,
                },
                Duration::from_millis(10),
                cancel,
            )
            .await
            .unwrap();

            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                db.put(b"k3", b"v3").await.unwrap();
                db.flush_with_options(flush_opts()).await.unwrap();
            });

            let tx = stream
                .next_transaction()
                .await
                .unwrap()
                .expect("stream should skip old WALs and yield the next live transaction");

            assert!(tx.seq > current_seq);
            assert!(tx.entries.iter().any(|entry| {
                entry.key.as_ref() == b"k3"
                    && matches!(&entry.value, ValueDeletable::Value(value) if value.as_ref() == b"v3")
            }));
        })
        .await
        .expect("test_cdc_cursor_last_seq_skips_old_wals_then_polls_next_wal timed out");
    }

    #[tokio::test]
    async fn test_cdc_cursor_waits_for_live_transactions() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (db, object_store) = setup_db("/test_cursor_live").await;

            db.put(b"k1", b"v1").await.unwrap();
            db.flush_with_options(flush_opts()).await.unwrap();

            let current_seq = db.status().durable_seq;

            // Create stream BEFORE new transactions exist.
            let wal_reader = WalReader::new("/test_cursor_live", object_store);
            let cancel = CancellationToken::new();
            let cancel_clone = cancel.clone();
            let mut stream = CdcStream::new(
                wal_reader,
                CdcCursor {
                    wal_id: 0,
                    last_seq: current_seq,
                },
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
                assert!(
                    tx.seq > current_seq,
                    "tx.seq {} should be > cursor seq {}",
                    tx.seq,
                    current_seq
                );
                txs.push(tx);
            }
            assert!(
                txs.len() >= 2,
                "expected at least 2 live transactions, got {}",
                txs.len()
            );
        })
        .await
        .expect("test_cdc_cursor_waits_for_live_transactions timed out");
    }

    #[tokio::test]
    async fn test_cdc_cursor_yields_transaction_at_wal_file_eof_without_cancellation() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (db, object_store) = setup_db("/test_cursor_wal_file_eof").await;

            db.put(b"k1", b"v1").await.unwrap();
            db.flush_with_options(flush_opts()).await.unwrap();

            let current_seq = db.status().durable_seq;
            let wal_reader = WalReader::new("/test_cursor_wal_file_eof", object_store);
            let cancel = CancellationToken::new();
            let mut stream = CdcStream::new(
                wal_reader,
                CdcCursor {
                    wal_id: 0,
                    last_seq: current_seq,
                },
                Duration::from_millis(10),
                cancel,
            )
            .await
            .unwrap();

            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                db.put(b"k2", b"v2").await.unwrap();
                db.flush_with_options(flush_opts()).await.unwrap();
            });

            let tx = stream
                .next_transaction()
                .await
                .unwrap()
                .expect("transaction at WAL file EOF should be yielded without cancellation");

            assert!(tx.seq > current_seq);
            assert_eq!(tx.entries.len(), 1);
        })
        .await
        .expect(
            "test_cdc_cursor_yields_transaction_at_wal_file_eof_without_cancellation timed out",
        );
    }

    #[tokio::test]
    async fn test_cdc_cursor_reads_existing_and_live_transactions() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (db, object_store) = setup_db("/test_cursor_mixed").await;

            // Before cursor.
            db.put(b"k1", b"v1").await.unwrap();
            db.flush_with_options(flush_opts()).await.unwrap();

            let current_seq = db.status().durable_seq;

            // After cursor but before stream creation.
            db.put(b"k2", b"v2").await.unwrap();
            db.flush_with_options(flush_opts()).await.unwrap();

            // Create stream — k2 is already in WAL.
            let wal_reader = WalReader::new("/test_cursor_mixed", object_store);
            let cancel = CancellationToken::new();
            let cancel_clone = cancel.clone();
            let mut stream = CdcStream::new(
                wal_reader,
                CdcCursor {
                    wal_id: 0,
                    last_seq: current_seq,
                },
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
                assert!(
                    tx.seq > current_seq,
                    "tx.seq {} should be > cursor seq {}",
                    tx.seq,
                    current_seq
                );
                txs.push(tx);
            }
            // Should see both k2 (already there) and k3 (arrived live).
            assert!(
                txs.len() >= 2,
                "expected at least 2 transactions (pre-stream + live), got {}",
                txs.len()
            );
        })
        .await
        .expect("test_cdc_cursor_reads_existing_and_live_transactions timed out");
    }

    // --- datoms_from_cdc_transaction tests (Node-based) ---

    use crate::memory_log::MemoryLog;
    use crate::node::{Node, SubmitNode};
    use crate::ops::TxOp;
    use crate::schema::{load_schema_from_indices, test_schema_tx};
    use crate::transaction::TransactionResult;
    use edn::kw;
    use std::collections::BTreeMap;

    async fn setup_node_with_schema() -> Node<MemoryLog> {
        let node = Node::memory_node().await;
        let result = node.execute_tx(test_schema_tx()).await.unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));
        node
    }

    async fn collect_cdc_datoms(node: &Node<MemoryLog>) -> Vec<Vec<Datom>> {
        // Flush WAL to object store so CdcStream can read it.
        node.slate
            .db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::Wal,
            })
            .await
            .unwrap();

        let wal_reader = WalReader::new(node.slate.path.as_str(), node.slate.object_store.clone());
        let cancel = CancellationToken::new();
        cancel.cancel();
        let schema = load_schema_from_indices(&node.slate).await;
        let mut stream = CdcStream::new(
            wal_reader,
            CdcCursor::default(),
            Duration::from_millis(10),
            cancel,
        )
        .await
        .unwrap();

        let mut all_tx_datoms = Vec::new();
        while let Some(tx) = stream.next_transaction().await.unwrap() {
            let datoms = datoms_from_cdc_transaction(&tx, &schema).unwrap();
            if !datoms.is_empty() {
                all_tx_datoms.push(datoms);
            }
        }
        all_tx_datoms
    }

    fn transactions_with_entity(datoms: &[Vec<Datom>], entity: i64) -> Vec<Vec<&Datom>> {
        datoms
            .iter()
            .map(|tx_datoms| {
                tx_datoms
                    .iter()
                    .filter(|datom| datom.entity == entity)
                    .collect::<Vec<_>>()
            })
            .filter(|tx_datoms| !tx_datoms.is_empty())
            .collect()
    }

    #[tokio::test]
    async fn test_datoms_from_cdc_happy_path() {
        let node = setup_node_with_schema().await;

        let mut doc = BTreeMap::new();
        doc.insert(kw!(:db/id), DataType::Long(100));
        doc.insert(kw!(:name), DataType::String("alice".to_string()));
        doc.insert(kw!(:age), DataType::Long(30));
        let result = node.execute_tx(vec![TxOp::Put(doc)]).await.unwrap();
        assert!(matches!(result, TransactionResult::TxCommited(_)));

        let all_tx_datoms = collect_cdc_datoms(&node).await;

        let entity_transactions = transactions_with_entity(&all_tx_datoms, 100);
        assert_eq!(entity_transactions.len(), 1);
        let entity_datoms = &entity_transactions[0];

        assert_eq!(entity_datoms.len(), 2);
        assert!(entity_datoms.iter().any(|d| d.attribute == kw!(:name)
            && d.value == DataType::String("alice".to_string())
            && d.op == DatomOp::Assert));
        assert!(entity_datoms.iter().any(|d| d.attribute == kw!(:age)
            && d.value == DataType::Long(30)
            && d.op == DatomOp::Assert));

        node.close().await;
    }

    #[tokio::test]
    async fn test_datoms_from_cdc_retract() {
        let node = setup_node_with_schema().await;

        // First put.
        let mut doc = BTreeMap::new();
        doc.insert(kw!(:db/id), DataType::Long(100));
        doc.insert(kw!(:name), DataType::String("alice".to_string()));
        node.execute_tx(vec![TxOp::Put(doc)]).await.unwrap();

        // Second put with different name triggers retract of old value.
        let mut doc2 = BTreeMap::new();
        doc2.insert(kw!(:db/id), DataType::Long(100));
        doc2.insert(kw!(:name), DataType::String("bob".to_string()));
        node.execute_tx(vec![TxOp::Put(doc2)]).await.unwrap();

        let all_tx_datoms = collect_cdc_datoms(&node).await;

        let entity_transactions = transactions_with_entity(&all_tx_datoms, 100);
        assert_eq!(entity_transactions.len(), 2);
        let name_transactions: Vec<Vec<&Datom>> = entity_transactions
            .into_iter()
            .map(|tx_datoms| {
                tx_datoms
                    .into_iter()
                    .filter(|datom| datom.attribute == kw!(:name))
                    .collect::<Vec<_>>()
            })
            .filter(|tx_datoms| !tx_datoms.is_empty())
            .collect();

        let initial_txs: Vec<&Vec<&Datom>> = name_transactions
            .iter()
            .filter(|tx_datoms| {
                tx_datoms.iter().any(|datom| {
                    datom.value == DataType::String("alice".to_string())
                        && datom.op == DatomOp::Assert
                })
            })
            .collect();
        assert_eq!(initial_txs.len(), 1);
        assert_eq!(initial_txs[0].len(), 1);

        let update_txs: Vec<&Vec<&Datom>> = name_transactions
            .iter()
            .filter(|tx_datoms| {
                tx_datoms.iter().any(|datom| {
                    datom.value == DataType::String("alice".to_string())
                        && datom.op == DatomOp::Retract
                }) || tx_datoms.iter().any(|datom| {
                    datom.value == DataType::String("bob".to_string())
                        && datom.op == DatomOp::Assert
                })
            })
            .collect();
        assert_eq!(update_txs.len(), 1);
        assert!(update_txs[0].iter().any(|datom| {
            datom.value == DataType::String("alice".to_string()) && datom.op == DatomOp::Retract
        }));
        assert!(update_txs[0].iter().any(|datom| {
            datom.value == DataType::String("bob".to_string()) && datom.op == DatomOp::Assert
        }));

        node.close().await;
    }

    #[tokio::test]
    async fn test_datoms_from_cdc_mixed_entities() {
        let node = setup_node_with_schema().await;

        let mut doc1 = BTreeMap::new();
        doc1.insert(kw!(:db/id), DataType::Long(100));
        doc1.insert(kw!(:name), DataType::String("alice".to_string()));
        let mut doc2 = BTreeMap::new();
        doc2.insert(kw!(:db/id), DataType::Long(200));
        doc2.insert(kw!(:name), DataType::String("bob".to_string()));
        doc2.insert(kw!(:age), DataType::Long(25));
        node.execute_tx(vec![TxOp::Put(doc1), TxOp::Put(doc2)])
            .await
            .unwrap();

        let all_tx_datoms = collect_cdc_datoms(&node).await;

        let target_transactions: Vec<&Vec<Datom>> = all_tx_datoms
            .iter()
            .filter(|tx_datoms| {
                tx_datoms.iter().any(|datom| datom.entity == 100)
                    || tx_datoms.iter().any(|datom| datom.entity == 200)
            })
            .collect();
        assert_eq!(target_transactions.len(), 1);
        let tx_datoms = target_transactions[0];
        let entity_100: Vec<&Datom> = tx_datoms.iter().filter(|d| d.entity == 100).collect();
        let entity_200: Vec<&Datom> = tx_datoms.iter().filter(|d| d.entity == 200).collect();

        assert_eq!(entity_100.len(), 1); // name only
        assert_eq!(entity_200.len(), 2); // name + age

        node.close().await;
    }
}
