#![allow(dead_code, unused)]

use std::sync::Arc;
use slatedb::Db;
use slatedb::WriteBatch;
use anyhow::{Error, Result};
use bincode;
use std::io::Cursor;
use bytes::Bytes;
use tokio::sync::broadcast;
use log::warn;

use serde::{Deserialize, Serialize};

use crate::log::{Record, Subscriber};
use crate::ops::{Attribute, Document, Triple, TxOp};
use crate::codec;
use crate::schema::SchemaCache;
use crate::transaction::{Basis, TxKey};
use crate::ops::DataType;
use crate::slate::{DEFAULT_READ_OPTIONS, DEFAULT_WRITE_OPTIONS};
use crate::util::concat_bytes;
use crate::clock::Instant;

pub struct Indexer {
    slatedb: Arc<Db>,
    schema_cache: SchemaCache,
    latest_indexed_tx: Option<(TxKey, u64)>,
    tx_completion_sender: broadcast::Sender<(TxKey, u64)>,
}

struct TxIndexKeys {
    eav: Vec<Vec<u8>>,
    ave: Vec<Vec<u8>>,
    aev: Vec<Vec<u8>>,
    ae: Vec<Vec<u8>>,
    av: Vec<Vec<u8>>,
}

// db/ prefix restriction removed for schema bootstrap (triplox-6x7).
// Schema-defining attributes (db/ident, db/valueType, db/cardinality) are now
// allowed. Validation of user attributes is handled by the schema cache (triplox-jxz).

fn resolve_attribute_id(schema_cache: &SchemaCache, attr: &str) -> Result<i64, Error> {
    schema_cache
        .get(attr)
        .map(|a| a.entity_id)
        .ok_or_else(|| anyhow::anyhow!("Unknown attribute: {}", attr))
}

fn op_to_index_keys(tx_op: &TxOp, schema_cache: &SchemaCache) -> Result<TxIndexKeys, Error> {
    match tx_op {
        TxOp::Put(Document(doc)) => {
            let entity_id = match doc.get("db/id") {
                Some(DataType::Long(id)) => id,
                Some(_) => return Err(anyhow::anyhow!("Document db/id must be a long")),
                None => return Err(anyhow::anyhow!("Document must have a db/id")),
            };
            // TODO: entity IDs encoded as DataType::Long to match value-position encoding.
            // Revisit with schema/custom encoding.
            let entity_id = bincode::serialize(&DataType::Long(*entity_id))?;
            let mut attribute_and_values = Vec::new();
            for (k, v) in doc.iter().filter(|(k, _)| *k != "db/id") {
                let attribute_id = resolve_attribute_id(schema_cache, k)?;
                attribute_and_values.push((bincode::serialize(&attribute_id)?, bincode::serialize(v)?));
            }

            let mut eav: Vec<Vec<u8>> = Vec::new();
            let mut ave: Vec<Vec<u8>> = Vec::new();
            let mut aev: Vec<Vec<u8>> = Vec::new();
            let mut ae: Vec<Vec<u8>> = Vec::new();
            let mut av: Vec<Vec<u8>> = Vec::new();

            for (attribute, value) in attribute_and_values {
                eav.push(concat_bytes(&[&[codec::EAV], &entity_id, &attribute, &value, &[codec::ADD]]));
                ave.push(concat_bytes(&[&[codec::AVE], &attribute, &value, &entity_id, &[codec::ADD]]));
                aev.push(concat_bytes(&[&[codec::AEV], &attribute, &entity_id, &value, &[codec::ADD]]));
                ae.push(concat_bytes(&[&[codec::AE], &attribute, &entity_id, &[codec::ADD]]));
                av.push(concat_bytes(&[&[codec::AV], &attribute, &value, &[codec::ADD]]));
            }

            Ok(TxIndexKeys { eav, ave, aev, ae, av })
        },
        TxOp::Add(Triple { entity: entity_id, attribute, value }) => {
            let Attribute(attr) = attribute;
            // TODO: entity IDs encoded as DataType::Long to match value-position encoding.
            let entity_id = bincode::serialize(&DataType::Long(entity_id.0))?;
            let attribute_id = resolve_attribute_id(schema_cache, attr)?;
            let attribute = bincode::serialize(&attribute_id)?;
            let value = bincode::serialize(&value)?;

            let eav = concat_bytes(&[&[codec::EAV], &entity_id, &attribute, &value, &[codec::ADD]]);
            let ave = concat_bytes(&[&[codec::AVE], &attribute, &value, &entity_id, &[codec::ADD]]);
            let aev = concat_bytes(&[&[codec::AEV], &attribute, &entity_id, &value, &[codec::ADD]]);
            let ae = concat_bytes(&[&[codec::AE], &attribute, &entity_id, &[codec::ADD]]);
            let av = concat_bytes(&[&[codec::AV], &attribute, &value, &[codec::ADD]]);

            Ok(TxIndexKeys { eav: vec![eav], ave: vec![ave], aev: vec![aev], ae: vec![ae], av: vec![av] })
        },
        TxOp::Retract(Triple { entity: entity_id, attribute, value }) => {
            let Attribute(attr) = attribute;
            // TODO: entity IDs encoded as DataType::Long to match value-position encoding.
            let entity_id = bincode::serialize(&DataType::Long(entity_id.0))?;
            let attribute_id = resolve_attribute_id(schema_cache, attr)?;
            let attribute = bincode::serialize(&attribute_id)?;
            let value = bincode::serialize(&value)?;

            let eav = concat_bytes(&[&[codec::EAV], &entity_id, &attribute, &value, &[codec::RETRACT]]);
            let ave = concat_bytes(&[&[codec::AVE], &attribute, &value, &entity_id, &[codec::RETRACT]]);
            let aev = concat_bytes(&[&[codec::AEV], &attribute, &entity_id, &value, &[codec::RETRACT]]);
            let ae = concat_bytes(&[&[codec::AE], &attribute, &entity_id, &[codec::RETRACT]]);
            let av = concat_bytes(&[&[codec::AV], &attribute, &value, &[codec::RETRACT]]);

            Ok(TxIndexKeys { eav: vec![eav], ave: vec![ave], aev: vec![aev], ae: vec![ae], av: vec![av] })
        },
        TxOp::Delete(_entity) => todo!(),
        TxOp::Erase(_entity) => todo!(),
    }
}

pub(crate) fn build_index_write_batch(
    tx_ops: &[TxOp],
    schema_cache: &SchemaCache,
) -> Result<WriteBatch, Error> {
    let index_keys: Vec<TxIndexKeys> = tx_ops.iter()
        .map(|op| op_to_index_keys(op, schema_cache))
        .collect::<Result<Vec<_>, _>>()?;

    let mut write_batch = WriteBatch::new();

    for index_keys in index_keys.iter() {
        for key in &index_keys.eav { write_batch.put(key.as_slice(), &[]); }
        for key in &index_keys.ave { write_batch.put(key.as_slice(), &[]); }
        for key in &index_keys.aev { write_batch.put(key.as_slice(), &[]); }
        for key in &index_keys.ae { write_batch.put(key.as_slice(), &[]); }
        for key in &index_keys.av { write_batch.put(key.as_slice(), &[]); }
    }

    Ok(write_batch)
}

/// Interim mapping stored in SlateDB to look up seq_num (and system_time) by tx_id.
/// Will be replaced when SlateDB exposes WriteHandle.seqnum() (PR #1247).
#[derive(Serialize, Deserialize)]
struct TxMeta {
    seq_num: u64,
    system_time: Instant,
}

fn tx_to_seq_key(tx_id: i64) -> Vec<u8> {
    concat_bytes(&[&[codec::TX_TO_SEQ], &bincode::serialize(&tx_id).unwrap()])
}

/// Look up the Basis for a given tx_id from SlateDB.
/// Returns None if no mapping exists for that tx_id.
pub async fn get_basis_for_tx(slatedb: Arc<Db>, tx_id: i64) -> Option<Basis> {
    let key = tx_to_seq_key(tx_id);
    let bytes = slatedb.get_with_options(&key, &DEFAULT_READ_OPTIONS).await.ok()??;
    let meta: TxMeta = bincode::deserialize(&bytes).ok()?;
    Some(Basis {
        tx_key: TxKey { tx_id, system_time: meta.system_time },
        seq_num: meta.seq_num,
    })
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

    // TODO(triplox-5ox): Before writing, retract old values for :db.cardinality/one attributes
    // when a Put/Add overwrites an existing entity+attribute pair.
    pub async fn transact_tx(&mut self, tx_key: TxKey, tx_ops: Vec<TxOp>) -> Result<TxKey, Error> {
        self.schema_cache.validate_tx(&tx_ops)?;

        // Process schema definitions first so entity IDs are available for index key generation.
        // If the write below fails, the cache has phantom entries — acceptable tradeoff (see TODO).
        self.schema_cache.process_tx(&tx_ops)?;

        let write_batch = build_index_write_batch(&tx_ops, &self.schema_cache)?;

        self.slatedb.write_with_options(write_batch, &DEFAULT_WRITE_OPTIONS).await?;

        let seq_num = self.slatedb.last_committed_seq();

        // Persist tx_id -> seq_num mapping (interim, see SlateDB PR #1247)
        let meta = TxMeta { seq_num, system_time: tx_key.system_time };
        self.slatedb.put(&tx_to_seq_key(tx_key.tx_id), &bincode::serialize(&meta)?).await;

        // Update latest indexed tx and broadcast completion
        self.latest_indexed_tx = Some((tx_key, seq_num));

        // Send notification (warn if no receivers, matching memory_log.rs:51-52)
        // TODO: verify if warning on no receivers is idiomatic Rust broadcast channel pattern
        if let Err(e) = self.tx_completion_sender.send((tx_key, seq_num)) {
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
    pub fn await_tx(&self, tx_key: TxKey) -> impl std::future::Future<Output = Result<u64, Error>> + 'static {
        // Capture everything we need from self (clone/copy)
        let latest_tx = self.latest_indexed_tx;
        let mut rx = self.tx_completion_sender.subscribe();

        // Return a future that doesn't borrow self
        async move {
            // Fast path: Check if already indexed
            if let Some((latest_key, seq_num)) = latest_tx {
                if tx_key <= latest_key {
                    return Ok(seq_num);
                }
            }

            // Wait for matching or later transaction
            loop {
                match rx.recv().await {
                    Ok((completed_tx_key, seq_num)) => {
                        if completed_tx_key >= tx_key {
                            return Ok(seq_num);
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
        let tx_ops: Vec<TxOp> = bincode::deserialize(&record.record)
            .expect("Failed to deserialize TxOps from log record");
        self.transact_tx(record.tx_key, tx_ops).await
            .expect("Indexer failed to process transaction");
    }
}

// TODO: something to refactor

pub fn eav_key_to_parts(key: Bytes) -> Result<(DataType, i64, DataType, u8), Error> {
    let key = key.as_ref();

    if key.is_empty() || key[0] != codec::EAV {
        return Err(anyhow::anyhow!("Not an EAV key"));
    }

    if key.len() < 2 {
        return Err(anyhow::anyhow!("Key too short"));
    }
    let without_prefix = &key[1..key.len()-1];
    let suffix = key[key.len()-1];

    let mut cursor = Cursor::new(without_prefix);
    let entity_id: DataType = bincode::deserialize_from(&mut cursor)?;
    let attribute: i64 = bincode::deserialize_from(&mut cursor)?;
    let value: DataType = bincode::deserialize_from(&mut cursor)?;

    Ok((entity_id, attribute, value, suffix))
}

pub fn ave_key_to_parts(key: Bytes) -> Result<(i64, DataType, DataType, u8), Error> {
    let key = key.as_ref();

    if key.is_empty() || key[0] != codec::AVE {
        return Err(anyhow::anyhow!("Not an AVE key"));
    }

    if key.len() < 2 {
        return Err(anyhow::anyhow!("Key too short"));
    }
    let without_prefix = &key[1..key.len()-1];
    let suffix = key[key.len()-1];

    let mut cursor = Cursor::new(without_prefix);
    let attribute: i64 = bincode::deserialize_from(&mut cursor)?;
    let value: DataType = bincode::deserialize_from(&mut cursor)?;
    let entity_id: DataType = bincode::deserialize_from(&mut cursor)?;

    Ok((attribute, value, entity_id, suffix))
}

pub fn aev_key_to_parts(key: Bytes) -> Result<(i64, DataType, DataType, u8), Error> {
    let key = key.as_ref();

    if key.is_empty() || key[0] != codec::AEV {
        return Err(anyhow::anyhow!("Not an AEV key"));
    }

    if key.len() < 2 {
        return Err(anyhow::anyhow!("Key too short"));
    }
    let without_prefix = &key[1..key.len()-1];
    let suffix = key[key.len()-1];

    let mut cursor = std::io::Cursor::new(without_prefix);
    let attribute: i64 = bincode::deserialize_from(&mut cursor)?;
    let entity_id: DataType = bincode::deserialize_from(&mut cursor)?;
    let value: DataType = bincode::deserialize_from(&mut cursor)?;

    Ok((attribute, entity_id, value, suffix))
}

pub fn ae_key_to_parts(key: Bytes) -> Result<(i64, DataType, u8), Error> {
    let key = key.as_ref();

    if key.is_empty() || key[0] != codec::AE {
        return Err(anyhow::anyhow!("Not an AE key"));
    }

    if key.len() < 2 {
        return Err(anyhow::anyhow!("Key too short"));
    }
    let without_prefix = &key[1..key.len()-1];
    let suffix = key[key.len()-1];

    let mut cursor = std::io::Cursor::new(without_prefix);
    let attribute: i64 = bincode::deserialize_from(&mut cursor)?;
    let entity_id: DataType = bincode::deserialize_from(&mut cursor)?;

    Ok((attribute, entity_id, suffix))
}

pub fn av_key_to_parts(key: Bytes) -> Result<(i64, DataType, u8), Error> {
    let key = key.as_ref();

    if key.is_empty() || key[0] != codec::AV {
        return Err(anyhow::anyhow!("Not an AV key"));
    }

    if key.len() < 2 {
        return Err(anyhow::anyhow!("Key too short"));
    }
    let without_prefix = &key[1..key.len()-1];
    let suffix = key[key.len()-1];

    let mut cursor = std::io::Cursor::new(without_prefix);
    let attribute: i64 = bincode::deserialize_from(&mut cursor)?;
    let value: DataType = bincode::deserialize_from(&mut cursor)?;

    Ok((attribute, value, suffix))
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
            let (entity_id, attribute, value, suffix) = eav_key_to_parts(kv.key).unwrap();
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
            let (entity_id, _, _, _) = eav_key_to_parts(kv.key).unwrap();
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
            let (entity_id, _, _, _) = eav_key_to_parts(kv.key).unwrap();
            if entity_id == DataType::Long(100) {
                eav_count += 1;
            }
        }
        assert_eq!(eav_count, 2, "Expected 2 EAV entries for entity 100");

        Ok(())
    }

    #[tokio::test]
    async fn test_tx_to_seq_mapping_written() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;
        let tx_key = TxKey { tx_id: 42, system_time: st_from_unix_epoch(1000) };

        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(100));
        map.insert("name".to_string(), DataType::String("alice".to_string()));
        let tx_ops = vec![TxOp::Put(Document(map))];
        indexer.transact_tx(tx_key, tx_ops).await?;

        let basis = get_basis_for_tx(slate.clone(), 42).await;
        assert!(basis.is_some(), "Should find basis for tx_id 42");
        let basis = basis.unwrap();
        assert_eq!(basis.tx_key.tx_id, 42);
        assert_eq!(basis.tx_key.system_time, st_from_unix_epoch(1000));
        assert!(basis.seq_num > 0, "seq_num should be positive after a write");
        Ok(())
    }

    #[tokio::test]
    async fn test_tx_to_seq_mapping_not_found() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await);
        let basis = get_basis_for_tx(slate.clone(), 999).await;
        assert!(basis.is_none(), "Should return None for non-existent tx_id");
        Ok(())
    }

    #[tokio::test]
    async fn test_tx_to_seq_multiple_txs() -> Result<(), Error> {
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

        let basis0 = get_basis_for_tx(slate.clone(), 1).await.unwrap();
        let basis1 = get_basis_for_tx(slate.clone(), 2).await.unwrap();
        let basis2 = get_basis_for_tx(slate.clone(), 3).await.unwrap();

        assert!(basis0.seq_num < basis1.seq_num);
        assert!(basis1.seq_num < basis2.seq_num);
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
}
