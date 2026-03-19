#![allow(dead_code, unused)]

use std::sync::Arc;
use slatedb::Db;
use slatedb::WriteBatch;
use slatedb::IsolationLevel;
use anyhow::{Error, Result};
use bincode;
use std::io::Cursor;
use bytes::Bytes;
use tokio::sync::broadcast;
use log::warn;

use serde::{Deserialize, Serialize};

use crate::log::{Record, Subscriber};
use crate::ops::{Datom, DatomOp, Document, TxOp, tx_ops_to_datoms};
use crate::codec;
use crate::schema::SchemaCache;
use crate::transaction::TxKey;
use crate::ops::DataType;
use crate::slate::{DEFAULT_READ_OPTIONS, DEFAULT_SCAN_OPTIONS, DEFAULT_WRITE_OPTIONS};
use crate::util::concat_bytes;
use crate::clock::Instant;

pub struct Indexer {
    slatedb: Arc<Db>,
    schema_cache: SchemaCache,
    latest_indexed_tx: Option<TxKey>,
    tx_completion_sender: broadcast::Sender<(TxKey, Result<(), Arc<anyhow::Error>>)>,
}

pub(crate) fn build_index_write_batch(
    datoms: &[Datom],
    schema_cache: &SchemaCache,
    system_time: Instant,
) -> Result<WriteBatch, Error> {
    let timestamp = codec::encode_timestamp(system_time);
    let mut write_batch = WriteBatch::new();

    for datom in datoms {
        // TODO: entity IDs encoded as DataType::Long to match value-position encoding.
        // Revisit with schema/custom encoding.
        let entity_id = bincode::serialize(&DataType::Long(datom.entity))?;
        let attribute_id = schema_cache
            .get(&datom.attribute)
            .map(|a| a.entity_id)
            .ok_or_else(|| anyhow::anyhow!("Unknown attribute: {}", datom.attribute))?;
        let attribute = bincode::serialize(&attribute_id)?;
        let value = bincode::serialize(&datom.value)?;
        let op_byte = match datom.op {
            DatomOp::Assert => codec::ADD,
            DatomOp::Retract => codec::RETRACT,
        };

        // Temporal indices include timestamp + op
        write_batch.put(&concat_bytes(&[&[codec::EAV], &entity_id, &attribute, &value, &timestamp, &[op_byte]]), &[]);
        write_batch.put(&concat_bytes(&[&[codec::AVE], &attribute, &value, &entity_id, &timestamp, &[op_byte]]), &[]);
        write_batch.put(&concat_bytes(&[&[codec::AEV], &attribute, &entity_id, &value, &timestamp, &[op_byte]]), &[]);

        // AE stores the current value for (attribute, entity) — overwritten on each Assert.
        // AV is atemporal and purely additive.
        // Retractions are not written to AE/AV.
        if datom.op == DatomOp::Assert {
            write_batch.put(&concat_bytes(&[&[codec::AE], &attribute, &entity_id]), &value);
            write_batch.put(&concat_bytes(&[&[codec::AV], &attribute, &value]), &[]);
        }
    }

    Ok(write_batch)
}

/// Metadata stored per transaction in SlateDB (keyed by tx_id under TX_TO_META prefix).
#[derive(Serialize, Deserialize)]
struct TxMeta {
    system_time: Instant,
}

fn tx_meta_key(tx_id: i64) -> Vec<u8> {
    concat_bytes(&[&[codec::TX_TO_META], &bincode::serialize(&tx_id).unwrap()])
}

/// Scan all TX_TO_META keys in a snapshot and return the TxKey for the highest tx_id.
/// If the snapshot contains no TX_TO_META entries, returns a sentinel TxKey
/// (tx_id=0, system_time=epoch).
///
/// TODO: return a real "empty database" TxKey once we have one.
///
/// TODO(triplox-1vr): Switch tx_meta_key() to big-endian encoding (tx_id.to_be_bytes())
/// so byte order matches numeric order, enabling a reverse iterator for O(1) lookup
/// instead of this full scan. Breaking change — requires migration or flag day.
pub async fn latest_tx_key_from_snapshot(snapshot: &Arc<slatedb::DbSnapshot>) -> Result<TxKey> {
    let mut iter = snapshot.scan_prefix_with_options(&[codec::TX_TO_META], &DEFAULT_SCAN_OPTIONS).await?;
    let mut latest: Option<(i64, TxMeta)> = None;
    while let Some(kv) = iter.next().await? {
        let tx_id: i64 = bincode::deserialize(&kv.key[1..])?;
        let meta: TxMeta = bincode::deserialize(&kv.value)?;
        if latest.as_ref().map_or(true, |(best_id, _)| tx_id > *best_id) {
            latest = Some((tx_id, meta));
        }
    }
    Ok(latest.map(|(tx_id, meta)| TxKey {
        tx_id,
        system_time: meta.system_time,
    }).unwrap_or_else(|| TxKey {
        tx_id: 0,
        system_time: crate::clock::st_from_unix_epoch(0),
    }))
}

impl Indexer {
    pub fn new(slatedb: Arc<Db>, schema_cache: SchemaCache) -> Self {
        let (tx_completion_sender, _) = broadcast::channel(1024);
        Indexer {
            slatedb,
            schema_cache,
            latest_indexed_tx: None,
            tx_completion_sender,
        }
    }

    pub fn schema_cache(&self) -> &SchemaCache {
        &self.schema_cache
    }

    /// Transact a set of operations, automatically retracting old values for
    /// cardinality:one attributes when a new value is asserted.
    ///
    /// Uses a SlateDB transaction for atomic read-then-write: the AE index lookup
    /// and all index writes happen in a single transaction.
    ///
    /// TODO: If AE/AV indices are dropped in favor of 3 covering indices (EAV/AVE/AEV),
    /// the current-value lookup must switch to an AEV prefix scan.
    pub async fn transact_tx(&mut self, tx_key: TxKey, tx_ops: Vec<TxOp>) -> Result<TxKey, Error> {
        let mut datoms = tx_ops_to_datoms(&tx_ops, tx_key.system_time)?;

        let new_schema_attrs = self.schema_cache.validate_tx(&datoms)?;

        let txn = self.slatedb.begin(IsolationLevel::Snapshot).await?;

        // For each Assert datom, look up the current value via AE index.
        // If an old value exists and differs, add a Retract datom.
        let mut retractions = Vec::new();
        for datom in &datoms {
            if datom.op != DatomOp::Assert {
                continue;
            }
            let attribute_id = self.schema_cache
                .get(&datom.attribute)
                .map(|a| a.entity_id)
                .ok_or_else(|| anyhow::anyhow!("Unknown attribute: {}", datom.attribute))?;
            let entity_id_bytes = bincode::serialize(&DataType::Long(datom.entity))?;
            let attr_id_bytes = bincode::serialize(&attribute_id)?;
            let ae_key = concat_bytes(&[&[codec::AE], &attr_id_bytes, &entity_id_bytes]);

            if let Some(old_value_bytes) = txn.get(&ae_key).await? {
                let old_value: DataType = bincode::deserialize(&old_value_bytes)?;
                if old_value != datom.value {
                    retractions.push(Datom {
                        entity: datom.entity,
                        attribute: datom.attribute.clone(),
                        value: old_value,
                        tx: datom.tx,
                        op: DatomOp::Retract,
                    });
                }
            }
        }
        datoms.extend(retractions);

        // Write all index entries within the transaction
        let timestamp = codec::encode_timestamp(tx_key.system_time);
        for datom in &datoms {
            let entity_id = bincode::serialize(&DataType::Long(datom.entity))?;
            let attribute_id = self.schema_cache
                .get(&datom.attribute)
                .map(|a| a.entity_id)
                .ok_or_else(|| anyhow::anyhow!("Unknown attribute: {}", datom.attribute))?;
            let attribute = bincode::serialize(&attribute_id)?;
            let value = bincode::serialize(&datom.value)?;
            let op_byte = match datom.op {
                DatomOp::Assert => codec::ADD,
                DatomOp::Retract => codec::RETRACT,
            };

            // Temporal indices include timestamp + op
            txn.put(&concat_bytes(&[&[codec::EAV], &entity_id, &attribute, &value, &timestamp, &[op_byte]]), &[])?;
            txn.put(&concat_bytes(&[&[codec::AVE], &attribute, &value, &entity_id, &timestamp, &[op_byte]]), &[])?;
            txn.put(&concat_bytes(&[&[codec::AEV], &attribute, &entity_id, &value, &timestamp, &[op_byte]]), &[])?;

            // AE stores the current value — overwritten on each Assert.
            // AV is atemporal and purely additive.
            if datom.op == DatomOp::Assert {
                txn.put(&concat_bytes(&[&[codec::AE], &attribute, &entity_id]), &value)?;
                txn.put(&concat_bytes(&[&[codec::AV], &attribute, &value]), &[])?;
            }
        }

        // Persist tx_id -> system_time mapping
        let meta = TxMeta { system_time: tx_key.system_time };
        txn.put(&tx_meta_key(tx_key.tx_id), &bincode::serialize(&meta)?)?;

        txn.commit().await?;

        // Update schema cache after commit so new attributes are only visible
        // in subsequent transactions.
        self.schema_cache.process_tx(new_schema_attrs);

        // Update latest indexed tx and broadcast completion
        self.latest_indexed_tx = Some(tx_key);

        // Send notification (warn if no receivers, matching memory_log.rs:51-52)
        // TODO: verify if warning on no receivers is idiomatic Rust broadcast channel pattern
        if let Err(e) = self.tx_completion_sender.send((tx_key, Ok(()))) {
            warn!("No receivers for indexed transaction {}: {}", tx_key.tx_id, e);
        }

        Ok(tx_key)
    }

    /// Wait until the specified transaction has been indexed.
    /// Returns immediately if the transaction is already indexed (based on TxKey ordering).
    ///
    /// This method returns a future that does NOT borrow `self`, so it's safe to use
    /// with RwLock - the lock can be dropped before awaiting the returned future.
    ///
    /// # Arguments
    /// * `tx_key` - The transaction to wait for
    ///
    /// # Returns
    /// * A future that resolves to `Ok(())` when the transaction is indexed
    ///
    /// # Example
    /// ```ignore
    /// // With RwLock - lock is dropped before waiting
    /// let future = indexer.read().await.await_tx(tx_key);
    /// future.await?;
    ///
    /// // With timeout
    /// let future = indexer.read().await.await_tx(tx_key);
    /// tokio::time::timeout(Duration::from_millis(500), future).await??;
    /// ```
    pub fn await_tx(&self, tx_key: TxKey) -> impl std::future::Future<Output = Result<(), Error>> + 'static {
        // Capture everything we need from self (clone/copy)
        let latest_tx = self.latest_indexed_tx;
        let mut rx = self.tx_completion_sender.subscribe();

        // Return a future that doesn't borrow self
        async move {
            // Fast path: Check if already indexed
            if let Some(latest_key) = latest_tx {
                if tx_key <= latest_key {
                    return Ok(());
                }
            }

            // TODO(triplox-j7t): The >= check can return a different tx's result if the
            // broadcast channel lags. Will be revisited when the log is removed and we
            // ingest directly into Slate.
            loop {
                match rx.recv().await {
                    Ok((completed_tx_key, result)) => {
                        if completed_tx_key >= tx_key {
                            return result.map_err(|e| anyhow::anyhow!("{:#}", e));
                        }
                        // Keep waiting for higher tx_id
                    },
                    Err(broadcast::error::RecvError::Lagged(_count)) => {
                        // Channel overflowed. Just continue waiting - we'll eventually
                        // get the notification or the channel will close.
                        continue;
                    },
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err(anyhow::anyhow!(
                            "Indexer shutdown while waiting for tx {}",
                            tx_key.tx_id
                        ));
                    }
                }
            }
        }
    }
}


impl Subscriber for Indexer {
    async fn accept(&mut self, record: Record) {
        let tx_ops: Vec<TxOp> = match bincode::deserialize(&record.record) {
            Ok(ops) => ops,
            Err(e) => {
                let err = anyhow::anyhow!("Failed to deserialize TxOps: {}", e);
                warn!("Transaction {} deserialization failed: {}", record.tx_key.tx_id, err);
                let _ = self.tx_completion_sender.send((record.tx_key, Err(Arc::new(err))));
                return;
            }
        };
        if let Err(e) = self.transact_tx(record.tx_key, tx_ops).await {
            warn!("Transaction {} failed: {}", record.tx_key.tx_id, e);
            let _ = self.tx_completion_sender.send((record.tx_key, Err(Arc::new(e))));
        }
    }
}

/// Strip a temporal index key into (data_bytes, timestamp, op).
fn strip_temporal_key<'a>(key: &'a [u8], expected_prefix: u8, name: &str) -> Result<(&'a [u8], Instant, u8), Error> {
    if key.first() != Some(&expected_prefix) {
        return Err(anyhow::anyhow!("Not a {} key", name));
    }
    if key.len() < 1 + codec::TIMESTAMP_OP_SUFFIX {
        return Err(anyhow::anyhow!("Key too short"));
    }
    let data = &key[1..key.len() - codec::TIMESTAMP_OP_SUFFIX];
    let timestamp = codec::decode_timestamp(&key[key.len() - codec::TIMESTAMP_OP_SUFFIX..key.len() - codec::OP_LENGTH])?;
    let op = key[key.len() - 1];
    Ok((data, timestamp, op))
}

/// Strip an atemporal index key, returning data bytes after the prefix.
fn strip_atemporal_key<'a>(key: &'a [u8], expected_prefix: u8, name: &str) -> Result<&'a [u8], Error> {
    if key.first() != Some(&expected_prefix) {
        return Err(anyhow::anyhow!("Not a {} key", name));
    }
    if key.len() < 2 {
        return Err(anyhow::anyhow!("Key too short"));
    }
    Ok(&key[1..])
}

pub fn eav_key_to_parts(key: Bytes) -> Result<(DataType, i64, DataType, Instant, u8), Error> {
    let (data, timestamp, op) = strip_temporal_key(key.as_ref(), codec::EAV, "EAV")?;
    let mut cursor = Cursor::new(data);
    let entity_id: DataType = bincode::deserialize_from(&mut cursor)?;
    let attribute: i64 = bincode::deserialize_from(&mut cursor)?;
    let value: DataType = bincode::deserialize_from(&mut cursor)?;
    Ok((entity_id, attribute, value, timestamp, op))
}

pub fn ave_key_to_parts(key: Bytes) -> Result<(i64, DataType, DataType, Instant, u8), Error> {
    let (data, timestamp, op) = strip_temporal_key(key.as_ref(), codec::AVE, "AVE")?;
    let mut cursor = Cursor::new(data);
    let attribute: i64 = bincode::deserialize_from(&mut cursor)?;
    let value: DataType = bincode::deserialize_from(&mut cursor)?;
    let entity_id: DataType = bincode::deserialize_from(&mut cursor)?;
    Ok((attribute, value, entity_id, timestamp, op))
}

pub fn aev_key_to_parts(key: Bytes) -> Result<(i64, DataType, DataType, Instant, u8), Error> {
    let (data, timestamp, op) = strip_temporal_key(key.as_ref(), codec::AEV, "AEV")?;
    let mut cursor = Cursor::new(data);
    let attribute: i64 = bincode::deserialize_from(&mut cursor)?;
    let entity_id: DataType = bincode::deserialize_from(&mut cursor)?;
    let value: DataType = bincode::deserialize_from(&mut cursor)?;
    Ok((attribute, entity_id, value, timestamp, op))
}

pub fn ae_key_to_parts(key: Bytes) -> Result<(i64, DataType), Error> {
    let data = strip_atemporal_key(key.as_ref(), codec::AE, "AE")?;
    let mut cursor = Cursor::new(data);
    let attribute: i64 = bincode::deserialize_from(&mut cursor)?;
    let entity_id: DataType = bincode::deserialize_from(&mut cursor)?;
    Ok((attribute, entity_id))
}

pub fn av_key_to_parts(key: Bytes) -> Result<(i64, DataType), Error> {
    let data = strip_atemporal_key(key.as_ref(), codec::AV, "AV")?;
    let mut cursor = Cursor::new(data);
    let attribute: i64 = bincode::deserialize_from(&mut cursor)?;
    let value: DataType = bincode::deserialize_from(&mut cursor)?;
    Ok((attribute, value))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use slatedb::{Db, config::ScanOptions};

    use crate::clock::st_from_unix_epoch;
    use crate::schema::test_schema_tx;
    use crate::slate::in_memory_slate;
    use super::*;

    /// Create an indexer with bootstrap schema and test attributes already transacted.
    /// Uses init_db for bootstrap, then transacts test schema via the indexer.
    /// Returns the indexer ready for test data at tx_id=1+.
    async fn bootstrapped_indexer(slate: Arc<Db>) -> Indexer {
        let cache = crate::bootstrap::init_db(slate.clone()).await;
        let mut indexer = Indexer::new(slate, cache);
        let tx_key_0 = TxKey { tx_id: 0, system_time: st_from_unix_epoch(1) };
        indexer.transact_tx(tx_key_0, test_schema_tx()).await.unwrap();
        indexer
    }

    #[tokio::test]
    async fn test_indexer() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;
        let tx_key = TxKey { tx_id: 1, system_time: st_from_unix_epoch(2) };
        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(100));
        map.insert("name".to_string(), DataType::String("alan".to_string()));
        let doc = Document(map);
        let tx_ops = vec![TxOp::Put(doc)];
        indexer.transact_tx(tx_key, tx_ops).await.unwrap();

        // name has entity_id 50 from test_schema_tx
        let name_id: i64 = 50;

        // Find the EAV entry for entity 100 (skip bootstrap entries)
        let mut iter = slate.scan_prefix_with_options(&[codec::EAV], &ScanOptions::default()).await.unwrap();
        let mut found = false;
        while let Some(kv) = iter.next().await? {
            let (entity_id, attribute, value, _timestamp, suffix) = eav_key_to_parts(kv.key).unwrap();
            if entity_id == DataType::Long(100) {
                assert_eq!(attribute, name_id);
                assert_eq!(value, DataType::String("alan".to_string()));
                assert_eq!(suffix, codec::ADD);
                found = true;
                break;
            }
        }
        assert!(found, "Expected EAV entry for entity 100");

        Ok(())
    }

    #[tokio::test]
    async fn test_indexer_write_persisted() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;
        let tx_key = TxKey { tx_id: 1, system_time: st_from_unix_epoch(2) };

        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(100));
        map.insert("name".to_string(), DataType::String("alan".to_string()));
        let doc = Document(map);
        let tx_ops = vec![TxOp::Put(doc)];
        indexer.transact_tx(tx_key, tx_ops).await.unwrap();

        // Verify EAV entry for entity 100 exists
        let mut iter = slate.scan_prefix_with_options(&[codec::EAV], &ScanOptions::default()).await.unwrap();
        let mut found = false;
        while let Some(kv) = iter.next().await? {
            let (entity_id, _, _, _, _) = eav_key_to_parts(kv.key).unwrap();
            if entity_id == DataType::Long(100) {
                found = true;
                break;
            }
        }
        assert!(found, "Expected EAV entry to be written to the database");

        Ok(())
    }

    #[tokio::test]
    async fn test_indexer_multi_attribute_document() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;
        let tx_key = TxKey { tx_id: 1, system_time: st_from_unix_epoch(2) };

        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(100));
        map.insert("name".to_string(), DataType::String("alan".to_string()));
        map.insert("age".to_string(), DataType::Long(30));
        let doc = Document(map);
        let tx_ops = vec![TxOp::Put(doc)];
        indexer.transact_tx(tx_key, tx_ops).await.unwrap();

        // Count EAV entries for entity 100 (should be 2: name + age)
        let mut iter = slate.scan_prefix_with_options(&[codec::EAV], &ScanOptions::default()).await.unwrap();
        let mut eav_count = 0;
        while let Some(kv) = iter.next().await? {
            let (entity_id, _, _, _, _) = eav_key_to_parts(kv.key).unwrap();
            if entity_id == DataType::Long(100) {
                eav_count += 1;
            }
        }
        assert_eq!(eav_count, 2, "Expected 2 EAV entries for entity 100");

        Ok(())
    }

    #[tokio::test]
    async fn test_tx_meta_written() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;
        let tx_key = TxKey { tx_id: 42, system_time: st_from_unix_epoch(1000) };

        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(100));
        map.insert("name".to_string(), DataType::String("alice".to_string()));
        let tx_ops = vec![TxOp::Put(Document(map))];
        indexer.transact_tx(tx_key, tx_ops).await?;

        let snapshot = Arc::new(slate.snapshot().await?);
        let latest = latest_tx_key_from_snapshot(&snapshot).await?;
        assert_eq!(latest.tx_id, 42);
        assert_eq!(latest.system_time, st_from_unix_epoch(1000));
        Ok(())
    }

    #[tokio::test]
    async fn test_tx_meta_empty_db() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await);
        let snapshot = Arc::new(slate.snapshot().await?);
        let latest = latest_tx_key_from_snapshot(&snapshot).await?;
        assert_eq!(latest.tx_id, 0, "Should return sentinel for empty DB");
        Ok(())
    }

    #[tokio::test]
    async fn test_tx_meta_latest_wins() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;

        // tx_id -1=bootstrap, 0=test schema, so data starts at 1
        for i in 0..3 {
            let tx_id = i + 1;
            let tx_key = TxKey { tx_id, system_time: st_from_unix_epoch(tx_id as u64 * 100) };
            let mut map = BTreeMap::new();
            map.insert("db/id".to_string(), DataType::Long(100 + i));
            map.insert("name".to_string(), DataType::String(format!("user{}", i)));
            let tx_ops = vec![TxOp::Put(Document(map))];
            indexer.transact_tx(tx_key, tx_ops).await?;
        }

        let snapshot = Arc::new(slate.snapshot().await?);
        let latest = latest_tx_key_from_snapshot(&snapshot).await?;
        assert_eq!(latest.tx_id, 3, "Should return highest tx_id");
        assert_eq!(latest.system_time, st_from_unix_epoch(300));
        Ok(())
    }

    #[tokio::test]
    async fn test_await_tx_already_indexed() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;

        // Index a data transaction
        let tx_key = TxKey { tx_id: 1, system_time: st_from_unix_epoch(2) };
        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(100));
        map.insert("name".to_string(), DataType::String("alice".to_string()));
        let tx_ops = vec![TxOp::Put(Document(map))];
        indexer.transact_tx(tx_key, tx_ops).await?;

        // await_tx should return immediately
        let start = std::time::Instant::now();
        indexer.await_tx(tx_key).await?;
        let elapsed = start.elapsed();

        assert!(elapsed < std::time::Duration::from_millis(10),
                "Should return immediately for already indexed tx");
        Ok(())
    }

    #[tokio::test]
    async fn test_await_tx_waits_for_future_tx() -> Result<(), Error> {
        use tokio::sync::RwLock;

        let slate = Arc::new(in_memory_slate().await);
        let indexer = Arc::new(RwLock::new(bootstrapped_indexer(slate.clone()).await));

        let tx_key_1 = TxKey { tx_id: 1, system_time: st_from_unix_epoch(200) };

        // Spawn task that calls await_tx - lock is dropped before awaiting
        let indexer_clone = indexer.clone();
        let wait_handle = tokio::spawn(async move {
            let future = indexer_clone.read().await.await_tx(tx_key_1);
            // Lock is dropped here, before we await
            future.await
        });

        // Give wait task time to subscribe
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Now index the transaction - can acquire write lock
        {
            let mut guard = indexer.write().await;
            let mut map = BTreeMap::new();
            map.insert("db/id".to_string(), DataType::Long(100));
            map.insert("name".to_string(), DataType::String("bob".to_string()));
            let tx_ops = vec![TxOp::Put(Document(map))];
            guard.transact_tx(tx_key_1, tx_ops).await?;
        }

        // Wait task should complete successfully
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            wait_handle
        ).await;

        assert!(result.is_ok(), "Should complete before timeout");
        assert!(result.unwrap()?.is_ok(), "Should return Ok");
        Ok(())
    }

    #[tokio::test]
    async fn test_await_tx_timeout() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await);
        let indexer = Indexer::new(slate.clone(), SchemaCache::new());

        let tx_key = TxKey { tx_id: 999, system_time: st_from_unix_epoch(999) };

        // Wait for transaction that never arrives
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            indexer.await_tx(tx_key)
        ).await;

        assert!(result.is_err(), "Should timeout waiting for non-existent tx");
        Ok(())
    }

    #[tokio::test]
    async fn test_await_tx_ordering() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;

        // Index tx 1 and tx 2 (-1=bootstrap, 0=test schema)
        for i in 0..2 {
            let tx_id = i + 1;
            let tx_key = TxKey { tx_id, system_time: st_from_unix_epoch(tx_id as u64 * 100) };
            let mut map = BTreeMap::new();
            map.insert("db/id".to_string(), DataType::Long(100 + i));
            map.insert("name".to_string(), DataType::String(format!("user{}", i)));
            let tx_ops = vec![TxOp::Put(Document(map))];
            indexer.transact_tx(tx_key, tx_ops).await?;
        }

        // Waiting for tx 1 should return immediately
        let tx_key_1 = TxKey { tx_id: 1, system_time: st_from_unix_epoch(100) };
        indexer.await_tx(tx_key_1).await?;

        // Waiting for tx 2 should also return immediately
        let tx_key_2 = TxKey { tx_id: 2, system_time: st_from_unix_epoch(200) };
        indexer.await_tx(tx_key_2).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_await_tx_multiple_waiters() -> Result<(), Error> {
        use tokio::sync::RwLock;

        let slate = Arc::new(in_memory_slate().await);
        let indexer = Arc::new(RwLock::new(bootstrapped_indexer(slate.clone()).await));

        let tx_key = TxKey { tx_id: 5, system_time: st_from_unix_epoch(500) };

        // Spawn multiple tasks that call await_tx
        let handles: Vec<_> = (0..5).map(|_| {
            let indexer_clone = indexer.clone();
            tokio::spawn(async move {
                let future = indexer_clone.read().await.await_tx(tx_key);
                // Lock dropped before awaiting
                future.await
            })
        }).collect();

        // Give waiters time to subscribe
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Index the transaction - can acquire write lock
        {
            let mut guard = indexer.write().await;
            let mut map = BTreeMap::new();
            map.insert("db/id".to_string(), DataType::Long(100));
            map.insert("name".to_string(), DataType::String("shared".to_string()));
            let tx_ops = vec![TxOp::Put(Document(map))];
            guard.transact_tx(tx_key, tx_ops).await?;
        }

        // All waiters should complete
        for handle in handles {
            let result = tokio::time::timeout(
                std::time::Duration::from_millis(200),
                handle
            ).await;
            assert!(result.is_ok(), "Task should complete before timeout");
            assert!(result.unwrap()?.is_ok(), "Task should succeed");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_retract_on_overwrite() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;

        // First tx: assert name="alice" for entity 100
        let tx1 = TxKey { tx_id: 1, system_time: st_from_unix_epoch(100) };
        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(100));
        map.insert("name".to_string(), DataType::String("alice".to_string()));
        indexer.transact_tx(tx1, vec![TxOp::Put(Document(map))]).await?;

        // Second tx: assert name="bob" for same entity — should auto-retract "alice"
        let tx2 = TxKey { tx_id: 2, system_time: st_from_unix_epoch(200) };
        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(100));
        map.insert("name".to_string(), DataType::String("bob".to_string()));
        indexer.transact_tx(tx2, vec![TxOp::Put(Document(map))]).await?;

        // Scan EAV for entity 100 — expect: alice ADD, alice RETRACT, bob ADD
        let name_id: i64 = 50;
        let mut iter = slate.scan_prefix_with_options(&[codec::EAV], &ScanOptions::default()).await?;
        let mut alice_add = false;
        let mut alice_retract = false;
        let mut bob_add = false;
        while let Some(kv) = iter.next().await? {
            let (entity_id, attribute, value, _ts, op) = eav_key_to_parts(kv.key)?;
            if entity_id != DataType::Long(100) || attribute != name_id {
                continue;
            }
            match (&value, op) {
                (DataType::String(s), codec::ADD) if s == "alice" => alice_add = true,
                (DataType::String(s), codec::RETRACT) if s == "alice" => alice_retract = true,
                (DataType::String(s), codec::ADD) if s == "bob" => bob_add = true,
                _ => {}
            }
        }
        assert!(alice_add, "Expected ADD for alice");
        assert!(alice_retract, "Expected RETRACT for alice");
        assert!(bob_add, "Expected ADD for bob");

        // Verify AE stores "bob" as current value
        let attr_bytes = bincode::serialize(&name_id)?;
        let entity_bytes = bincode::serialize(&DataType::Long(100i64))?;
        let ae_key = concat_bytes(&[&[codec::AE], &attr_bytes, &entity_bytes]);
        let ae_val = slate.get(&ae_key).await?.expect("AE entry should exist");
        let current: DataType = bincode::deserialize(&ae_val)?;
        assert_eq!(current, DataType::String("bob".to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn test_same_value_no_retract() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;

        // First tx: assert name="alice"
        let tx1 = TxKey { tx_id: 1, system_time: st_from_unix_epoch(100) };
        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(100));
        map.insert("name".to_string(), DataType::String("alice".to_string()));
        indexer.transact_tx(tx1, vec![TxOp::Put(Document(map))]).await?;

        // Second tx: assert same name="alice" — should NOT generate a retraction
        let tx2 = TxKey { tx_id: 2, system_time: st_from_unix_epoch(200) };
        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(100));
        map.insert("name".to_string(), DataType::String("alice".to_string()));
        indexer.transact_tx(tx2, vec![TxOp::Put(Document(map))]).await?;

        // Count EAV entries for entity 100, name attr — should be 2 ADDs, 0 RETRACTs
        let name_id: i64 = 50;
        let mut iter = slate.scan_prefix_with_options(&[codec::EAV], &ScanOptions::default()).await?;
        let mut add_count = 0;
        let mut retract_count = 0;
        while let Some(kv) = iter.next().await? {
            let (entity_id, attribute, _value, _ts, op) = eav_key_to_parts(kv.key)?;
            if entity_id != DataType::Long(100) || attribute != name_id {
                continue;
            }
            match op {
                codec::ADD => add_count += 1,
                codec::RETRACT => retract_count += 1,
                _ => {}
            }
        }
        assert_eq!(add_count, 2, "Expected 2 ADD entries (one per tx)");
        assert_eq!(retract_count, 0, "Expected no RETRACT entries");

        Ok(())
    }

    #[tokio::test]
    async fn test_ae_stores_value() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;

        let tx1 = TxKey { tx_id: 1, system_time: st_from_unix_epoch(100) };
        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(100));
        map.insert("name".to_string(), DataType::String("alice".to_string()));
        indexer.transact_tx(tx1, vec![TxOp::Put(Document(map))]).await?;

        // Verify AE stores the serialized value, not empty bytes
        let name_id: i64 = 50;
        let attr_bytes = bincode::serialize(&name_id)?;
        let entity_bytes = bincode::serialize(&DataType::Long(100i64))?;
        let ae_key = concat_bytes(&[&[codec::AE], &attr_bytes, &entity_bytes]);
        let ae_val = slate.get(&ae_key).await?.expect("AE entry should exist");
        let value: DataType = bincode::deserialize(&ae_val)?;
        assert_eq!(value, DataType::String("alice".to_string()));

        Ok(())
    }
}
