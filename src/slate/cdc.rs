use std::collections::VecDeque;
use std::time::Duration;

use slatedb::{RowEntry, ValueDeletable, WalFile, WalReader};
use tokio_util::sync::CancellationToken;

use crate::codec;
use crate::indexer::eav_key_to_parts;
use crate::ops::{DataType, Datom, DatomOp};
use crate::schema::Schema;

/// Tracks the CDC stream position.
///
/// `wal_id` is the next WAL file to read.
/// `last_seq` is the exclusive lower bound for rows yielded by the stream.
#[derive(Debug, Clone)]
pub struct CdcCursor {
    pub wal_id: u64,
    pub last_seq: u64,
}

impl Default for CdcCursor {
    fn default() -> Self {
        Self {
            wal_id: 1,
            last_seq: 0,
        }
    }
}

/// All RowEntries of one WAL file after the cursor, in seq order.
pub struct CdcBatch {
    pub wal_id: u64,
    pub entries: Vec<RowEntry>,
}

/// Streams CDC batches from WAL, one per WAL file, resumable via cursor.
/// Blocks (async) until a new WAL file is available or the cancel token is triggered.
pub struct CdcStream {
    wal_reader: WalReader,
    wal_files: VecDeque<WalFile>,
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
    pub fn new(
        wal_reader: WalReader,
        cursor: CdcCursor,
        poll_interval: Duration,
        cancel: CancellationToken,
    ) -> Self {
        CdcStream {
            wal_reader,
            wal_files: VecDeque::new(),
            cursor,
            poll_interval,
            cancel,
        }
    }

    /// Read the entries of `file` that come after the cursor.
    async fn read_entries(&self, file: &WalFile) -> Result<Vec<RowEntry>, slatedb::Error> {
        let mut iter = file.iterator().await?;
        let mut entries = Vec::new();
        while let Some(entry) = iter.next().await? {
            if entry.seq > self.cursor.last_seq {
                entries.push(entry);
            }
        }
        Ok(entries)
    }

    /// Return the next non-empty WAL file as a batch. Blocks until one is available.
    /// Returns `Ok(None)` only when the cancel token is triggered.
    pub async fn next_batch(&mut self) -> Result<Option<CdcBatch>, slatedb::Error> {
        loop {
            let Some(file) = self.wal_files.pop_front() else {
                let files = self.wal_reader.list(self.cursor.wal_id..).await?;
                if files.is_empty() {
                    // Wait for a newly visible immutable WAL file or cancellation.
                    tokio::select! {
                        _ = tokio::time::sleep(self.poll_interval) => {}
                        _ = self.cancel.cancelled() => return Ok(None),
                    }
                }
                self.wal_files = files.into();
                continue;
            };

            let entries = self.read_entries(&file).await?;
            self.cursor.wal_id = file.id + 1;
            // Empty files are fencing markers or fully behind the cursor.
            if let Some(last) = entries.last() {
                self.cursor.last_seq = last.seq;
                return Ok(Some(CdcBatch {
                    wal_id: file.id,
                    entries,
                }));
            }
        }
    }
}

/// Extract Datoms from a CDC batch by decoding EAV-prefix index keys.
/// Each datom is paired with the tx entity id stored in its key.
/// Skips non-EAV keys and tombstone entries.
pub fn datoms_from_cdc_batch(
    entries: &[RowEntry],
    schema: &Schema,
) -> Result<Vec<(i64, Datom)>, anyhow::Error> {
    let mut datoms = Vec::new();
    for entry in entries {
        if entry.key.first() != Some(&codec::EAV) {
            continue;
        }
        if matches!(entry.value, ValueDeletable::Tombstone) {
            continue;
        }

        let (entity_dt, attribute_id, value, tx_eid, op_byte) =
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

        datoms.push((
            tx_eid,
            Datom {
                entity,
                attribute,
                value,
                op,
            },
        ));
    }
    Ok(datoms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::config::{FlushOptions, FlushType, Settings};
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
        );

        let result = stream.next_batch().await.unwrap();
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
        );

        // Collect all entries across batches.
        let mut all_entries = Vec::new();
        while let Some(batch) = stream.next_batch().await.unwrap() {
            all_entries.extend(batch.entries);
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
    async fn test_multiple_wal_files() {
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
        );

        let mut batches = Vec::new();
        while let Some(batch) = stream.next_batch().await.unwrap() {
            batches.push(batch);
        }

        assert!(batches.len() >= 3);
        // Batches follow WAL file order and seqs increase across them.
        for window in batches.windows(2) {
            assert!(window[0].wal_id < window[1].wal_id);
            assert!(window[0].entries.last().unwrap().seq < window[1].entries[0].seq);
        }

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wal_file_with_many_writes_is_one_batch() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        // Without periodic flushes only the explicit flush cuts a WAL file.
        let db = Db::builder("/test_one_batch", object_store.clone())
            .with_settings(Settings {
                flush_interval: None,
                ..Settings::default()
            })
            .build()
            .await
            .unwrap();

        db.put(b"k1", b"v1").await.unwrap();
        db.put(b"k2", b"v2").await.unwrap();
        db.put(b"k3", b"v3").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();

        let wal_reader = WalReader::new("/test_one_batch", object_store);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut stream = CdcStream::new(
            wal_reader,
            CdcCursor::default(),
            Duration::from_millis(10),
            cancel,
        );

        let batch = stream.next_batch().await.unwrap().unwrap();
        let keys: Vec<&[u8]> = batch.entries.iter().map(|e| e.key.as_ref()).collect();
        assert_eq!(keys, vec![b"k1".as_ref(), b"k2".as_ref(), b"k3".as_ref()]);
        assert!(batch.entries.windows(2).all(|w| w[0].seq < w[1].seq));
        assert!(stream.next_batch().await.unwrap().is_none());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_filtering_by_cursor() {
        let (db, object_store) = setup_db("/test_filter").await;

        db.put(b"k1", b"v1").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();

        db.put(b"k2", b"v2").await.unwrap();
        db.flush_with_options(flush_opts()).await.unwrap();

        // First, read the first batch to find its last seq.
        let wal_reader = WalReader::new("/test_filter", object_store.clone());
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut stream = CdcStream::new(
            wal_reader,
            CdcCursor::default(),
            Duration::from_millis(10),
            cancel,
        );

        let batch1 = stream.next_batch().await.unwrap().unwrap();
        let batch1_seq = batch1.entries.last().unwrap().seq;

        // Now create a new stream starting after batch1's seq.
        let wal_reader2 = WalReader::new("/test_filter", object_store);
        let cancel2 = CancellationToken::new();
        cancel2.cancel();
        let mut stream2 = CdcStream::new(
            wal_reader2,
            CdcCursor {
                wal_id: 0,
                last_seq: batch1_seq,
            },
            Duration::from_millis(10),
            cancel2,
        );

        let batch2 = stream2.next_batch().await.unwrap().unwrap();
        assert!(batch2.entries.iter().all(|entry| entry.seq > batch1_seq));

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
        );

        // Collect all entries across batches.
        let mut all_entries = Vec::new();
        while let Some(batch) = stream.next_batch().await.unwrap() {
            all_entries.extend(batch.entries);
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
        );

        let result = stream.next_batch().await.unwrap();
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
        );

        let result = stream.next_batch().await.unwrap();
        assert!(result.is_none(), "expected no batches after cursor");
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
        );

        let mut batches = Vec::new();
        while let Some(batch) = stream.next_batch().await.unwrap() {
            assert!(
                batch.entries[0].seq > current_seq,
                "batch seq {} should be > cursor seq {}",
                batch.entries[0].seq,
                current_seq
            );
            batches.push(batch);
        }
        assert!(
            batches.len() >= 2,
            "expected at least 2 post-cursor batches, got {}",
            batches.len()
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
        );

        let mut batches = Vec::new();
        while let Some(batch) = stream.next_batch().await.unwrap() {
            assert!(
                batch.entries[0].seq > current_seq,
                "batch seq {} should be > cursor seq {}",
                batch.entries[0].seq,
                current_seq
            );
            batches.push(batch);
        }
        assert!(
            batches.len() >= 2,
            "expected at least 2 batches, got {}",
            batches.len()
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_cdc_cursor_polls_cursor_wal_when_initial_list_is_empty() {
        tokio::time::timeout(Duration::from_secs(1), async {
            let (db, object_store) = setup_db("/test_cursor_empty_initial_list").await;

            let wal_reader = WalReader::new("/test_cursor_empty_initial_list", object_store);
            let next_wal_id = wal_reader
                .list(..)
                .await
                .unwrap()
                .last()
                .map(|file| file.id + 1)
                .unwrap_or(1);
            assert!(wal_reader.list(next_wal_id..).await.unwrap().is_empty());

            let cancel = CancellationToken::new();
            let mut stream = CdcStream::new(
                wal_reader,
                CdcCursor {
                    wal_id: next_wal_id,
                    last_seq: 0,
                },
                Duration::from_millis(10),
                cancel,
            );

            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                db.put(b"k1", b"v1").await.unwrap();
                db.flush_with_options(flush_opts()).await.unwrap();
            });

            let batch = stream
                .next_batch()
                .await
                .unwrap()
                .expect("stream should poll the cursor WAL id after an empty initial list");

            assert!(batch.entries.iter().any(|entry| {
                entry.key.as_ref() == b"k1"
                    && matches!(&entry.value, ValueDeletable::Value(value) if value.as_ref() == b"v1")
            }));
        })
        .await
        .expect("test_cdc_cursor_polls_cursor_wal_when_initial_list_is_empty timed out");
    }

    #[tokio::test]
    async fn test_cdc_cursor_skips_old_wals_then_reads_live_transactions() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (db, object_store) = setup_db("/test_cursor_skip_then_live").await;

            db.put(b"k1", b"v1").await.unwrap();
            db.flush_with_options(flush_opts()).await.unwrap();
            db.put(b"k2", b"v2").await.unwrap();
            db.flush_with_options(flush_opts()).await.unwrap();

            let current_seq = db.status().durable_seq;
            let wal_reader = WalReader::new("/test_cursor_skip_then_live", object_store);
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
            );

            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                db.put(b"k3", b"v3").await.unwrap();
                db.flush_with_options(flush_opts()).await.unwrap();
                tokio::time::sleep(Duration::from_millis(50)).await;
                db.put(b"k4", b"v4").await.unwrap();
                db.flush_with_options(flush_opts()).await.unwrap();
                tokio::time::sleep(Duration::from_millis(50)).await;
                cancel_clone.cancel();
            });

            let mut batches = Vec::new();
            while let Some(batch) = stream.next_batch().await.unwrap() {
                assert!(
                    batch.entries[0].seq > current_seq,
                    "batch seq {} should be > cursor seq {}",
                    batch.entries[0].seq,
                    current_seq
                );
                batches.push(batch);
            }
            assert!(
                batches.len() >= 2,
                "expected at least 2 live batches, got {}",
                batches.len()
            );
            assert!(batches.iter().any(|batch| {
                batch.entries.iter().any(|entry| {
                    entry.key.as_ref() == b"k3"
                        && matches!(&entry.value, ValueDeletable::Value(value) if value.as_ref() == b"v3")
                })
            }));
            assert!(batches.iter().any(|batch| {
                batch.entries.iter().any(|entry| {
                    entry.key.as_ref() == b"k4"
                        && matches!(&entry.value, ValueDeletable::Value(value) if value.as_ref() == b"v4")
                })
            }));
        })
        .await
        .expect("test_cdc_cursor_skips_old_wals_then_reads_live_transactions timed out");
    }

    #[tokio::test]
    async fn test_cdc_cursor_yields_live_batch_without_cancellation() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (db, object_store) = setup_db("/test_cursor_live_batch").await;

            db.put(b"k1", b"v1").await.unwrap();
            db.flush_with_options(flush_opts()).await.unwrap();

            let current_seq = db.status().durable_seq;
            let wal_reader = WalReader::new("/test_cursor_live_batch", object_store);
            let cancel = CancellationToken::new();
            let mut stream = CdcStream::new(
                wal_reader,
                CdcCursor {
                    wal_id: 0,
                    last_seq: current_seq,
                },
                Duration::from_millis(10),
                cancel,
            );

            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                db.put(b"k2", b"v2").await.unwrap();
                db.flush_with_options(flush_opts()).await.unwrap();
            });

            let batch = stream
                .next_batch()
                .await
                .unwrap()
                .expect("live batch should be yielded without cancellation");

            assert!(batch.entries[0].seq > current_seq);
            assert_eq!(batch.entries.len(), 1);
        })
        .await
        .expect("test_cdc_cursor_yields_live_batch_without_cancellation timed out");
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
            );

            // Spawn background task to write more then cancel.
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                db.put(b"k3", b"v3").await.unwrap();
                db.flush_with_options(flush_opts()).await.unwrap();
                tokio::time::sleep(Duration::from_millis(50)).await;
                cancel_clone.cancel();
            });

            let mut batches = Vec::new();
            while let Some(batch) = stream.next_batch().await.unwrap() {
                assert!(
                    batch.entries[0].seq > current_seq,
                    "batch seq {} should be > cursor seq {}",
                    batch.entries[0].seq,
                    current_seq
                );
                batches.push(batch);
            }
            // Should see both k2 (already there) and k3 (arrived live).
            assert!(
                batches.len() >= 2,
                "expected at least 2 batches (pre-stream + live), got {}",
                batches.len()
            );
        })
        .await
        .expect("test_cdc_cursor_reads_existing_and_live_transactions timed out");
    }

    // --- datoms_from_cdc_batch tests (Node-based) ---

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
        assert!(matches!(result, TransactionResult::TxCommitted(_)));
        node
    }

    /// Datoms of every transaction in the WAL, grouped by tx entity in tx order.
    async fn collect_cdc_datoms(node: &Node<MemoryLog>) -> Vec<Vec<Datom>> {
        // Flush WAL to object store so CdcStream can read it.
        node.slate
            .db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::Wal,
            })
            .await
            .unwrap();

        let wal_reader = WalReader::new(
            node.slate.object_path.as_str(),
            node.slate.object_store.clone(),
        );
        let cancel = CancellationToken::new();
        cancel.cancel();
        let schema = load_schema_from_indices(&node.slate).await;
        let mut stream = CdcStream::new(
            wal_reader,
            CdcCursor::default(),
            Duration::from_millis(10),
            cancel,
        );

        let mut by_tx: BTreeMap<i64, Vec<Datom>> = BTreeMap::new();
        while let Some(batch) = stream.next_batch().await.unwrap() {
            for (tx_eid, datom) in datoms_from_cdc_batch(&batch.entries, &schema).unwrap() {
                by_tx.entry(tx_eid).or_default().push(datom);
            }
        }
        by_tx.into_values().collect()
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
        assert!(matches!(result, TransactionResult::TxCommitted(_)));

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

        node.close().await.unwrap();
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

        node.close().await.unwrap();
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

        node.close().await.unwrap();
    }
}
