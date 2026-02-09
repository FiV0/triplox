#![allow(dead_code, unused)]

use std::collections::HashMap;
use std::sync::Arc;
use slatedb::Db;
use slatedb::WriteBatch;
use anyhow::{Error, Result};
use bincode;
use futures::future::try_join_all;
use std::io::Cursor;
use bytes::Bytes;
use tokio::sync::broadcast;
use log::warn;

use crate::log::{Record, Subscriber};
use crate::ops::{Attribute, Document, Triple, TxOp};
use crate::codec;
use crate::transaction::TxKey;
use crate::ops::DataType;
use crate::slate::DEFAULT_WRITE_OPTIONS;
use crate::slate::{get_and_create_attribute_id, in_memory_slate, read_attribute_map};
use crate::util::concat_bytes;
use crate::clock::Instant;

pub struct Indexer {
    slatedb: Arc<Db>,
    attribute_to_id: HashMap<String, u64>,
    latest_indexed_tx: Option<TxKey>,
    tx_completion_sender: broadcast::Sender<TxKey>,
}

struct TxIndexKeys {
    eav: Vec<Vec<u8>>,
    ave: Vec<Vec<u8>>,
    aev: Vec<Vec<u8>>,
    ae: Vec<Vec<u8>>,
    av: Vec<Vec<u8>>,
}

fn assert_valid_attribute(attribute: &str) -> Result<(), Error> {
    if attribute.starts_with("db/") {
        return Err(anyhow::anyhow!("Attribute '{}' cannot start with db/", attribute));
    }
    Ok(())
}

impl Indexer {
    pub fn new(slatedb: Arc<Db>) -> Self {
        let attribute_to_id = HashMap::new();
        let (tx_completion_sender, _) = broadcast::channel(1024);
        Indexer {
            slatedb,
            attribute_to_id,
            latest_indexed_tx: None,
            tx_completion_sender,
        }
    }

    async fn op_to_index_keys(&self, _tx_key: TxKey, tx_op: &TxOp) -> Result<TxIndexKeys, Error> {
        // TODO: maybe move this to Bytes
        // TODO: this can likely be moved from the hotpath
        let mut attribute_map = read_attribute_map(self.slatedb.clone()).await;
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
                    assert_valid_attribute(k)?;
                    let attribute_id = get_and_create_attribute_id(self.slatedb.clone(), k, &mut attribute_map).await;
                    attribute_and_values.push((bincode::serialize(&attribute_id)?, bincode::serialize(v)?));
                }

                let mut eav : Vec<Vec<u8>> = Vec::new();
                let mut ave : Vec<Vec<u8>> = Vec::new();
                let mut aev : Vec<Vec<u8>> = Vec::new();
                let mut ae : Vec<Vec<u8>> = Vec::new();
                let mut av : Vec<Vec<u8>> = Vec::new();

                // TODO: would it be good to have length prefixed encoding here? 
                for (attribute, value) in attribute_and_values {
                    let value_len = bincode::serialize(&(value.len() as u64)).unwrap();
                    eav.push(concat_bytes(&[&[codec::EAV], &entity_id, &attribute, &value, &[codec::ADD]]));
                    ave.push(concat_bytes(&[&[codec::AVE], &attribute, &value, &entity_id, &[codec::ADD]]));
                    aev.push(concat_bytes(&[&[codec::AEV], &attribute, &entity_id, &value, &[codec::ADD]]));
                    ae.push(concat_bytes(&[&[codec::AE], &attribute, &entity_id, &[codec::ADD]]));
                    av.push(concat_bytes(&[&[codec::AV], &attribute, &value, &[codec::ADD]]));
                }

                Ok(TxIndexKeys { 
                    eav: eav, 
                    ave: ave, 
                    aev: aev,
                    ae: ae,
                    av: av
                })

            },
            TxOp::Add(Triple { entity: entity_id, attribute, value }) => {
                let Attribute(attr)= attribute;
                // TODO: entity IDs encoded as DataType::Long to match value-position encoding.
                let entity_id = bincode::serialize(&DataType::Long(entity_id.0))?;
                let attribute_id = get_and_create_attribute_id(self.slatedb.clone(), attr, &mut attribute_map).await;
                let attribute = bincode::serialize(&attribute_id)?;
                let value = bincode::serialize(&value)?;

                let eav = concat_bytes(&[&[codec::EAV], &entity_id, &attribute, &value, &[codec::ADD]]);
                let ave = concat_bytes(&[&[codec::AVE], &attribute, &value, &entity_id, &[codec::ADD]]);
                let aev = concat_bytes(&[&[codec::AEV], &attribute, &entity_id, &value, &[codec::ADD]]);
                let ae = concat_bytes(&[&[codec::AE], &attribute, &entity_id, &[codec::ADD]]);
                let av = concat_bytes(&[&[codec::AV], &attribute, &value, &[codec::ADD]]);

                Ok(TxIndexKeys { 
                    eav: vec![eav], 
                    ave: vec![ave], 
                    aev: vec![aev],
                    ae: vec![ae],
                    av: vec![av]
                })
            },
            TxOp::Retract(Triple { entity: entity_id, attribute, value }) => {
                let Attribute(attr)= attribute;
                // TODO: entity IDs encoded as DataType::Long to match value-position encoding.
                let entity_id = bincode::serialize(&DataType::Long(entity_id.0))?;
                let attribute_id = get_and_create_attribute_id(self.slatedb.clone(), attr, &mut attribute_map).await;
                let attribute = bincode::serialize(&attribute_id)?;
                let value = bincode::serialize(&value)?;

                let eav = concat_bytes(&[&[codec::EAV], &entity_id, &attribute, &value, &[codec::RETRACT]]);
                let ave = concat_bytes(&[&[codec::AVE], &attribute, &value, &entity_id, &[codec::RETRACT]]);
                let aev = concat_bytes(&[&[codec::AEV], &attribute, &entity_id, &value, &[codec::RETRACT]]);
                let ae = concat_bytes(&[&[codec::AE], &attribute, &entity_id, &[codec::RETRACT]]);
                let av = concat_bytes(&[&[codec::AV], &attribute, &value, &[codec::RETRACT]]);

                Ok(TxIndexKeys { 
                    eav: vec![eav], 
                    ave: vec![ave], 
                    aev: vec![aev],
                    ae: vec![ae],
                    av: vec![av]
                })  
            },
            TxOp::Delete(_entity) => todo!(),
            TxOp::Erase(_entity) => todo!(),
        }
    }


    pub async fn transact_tx(&mut self, tx_key: TxKey, tx_ops: Vec<TxOp>) -> Result<TxKey, Error> {
        let futures = tx_ops.iter()
            .map(|op| self.op_to_index_keys(tx_key, op));

        let index_keys = try_join_all(futures).await?;

        let mut write_batch = WriteBatch::new();

        for index_keys in index_keys.iter() {
            for key in &index_keys.eav { write_batch.put(key.as_slice(), &[]); }
            for key in &index_keys.ave { write_batch.put(key.as_slice(), &[]); }
            for key in &index_keys.aev { write_batch.put(key.as_slice(), &[]); }
            for key in &index_keys.ae { write_batch.put(key.as_slice(), &[]); }
            for key in &index_keys.av { write_batch.put(key.as_slice(), &[]); }
        }

        self.slatedb.write_with_options(write_batch, &DEFAULT_WRITE_OPTIONS).await?;

        // Update latest indexed tx and broadcast completion
        self.latest_indexed_tx = Some(tx_key);

        // Send notification (warn if no receivers, matching memory_log.rs:51-52)
        // TODO: verify if warning on no receivers is idiomatic Rust broadcast channel pattern
        if let Err(e) = self.tx_completion_sender.send(tx_key) {
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
            if let Some(latest) = latest_tx {
                if tx_key <= latest {
                    return Ok(());
                }
            }

            // Wait for matching or later transaction
            loop {
                match rx.recv().await {
                    Ok(completed_tx_key) => {
                        if completed_tx_key >= tx_key {
                            return Ok(());
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

pub fn eav_key_to_parts(key: Bytes) -> Result<(DataType, u64, DataType, u8), Error> {
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
    let attribute: u64 = bincode::deserialize_from(&mut cursor)?;
    let value: DataType = bincode::deserialize_from(&mut cursor)?;

    Ok((entity_id, attribute, value, suffix))
}

pub fn ave_key_to_parts(key: Bytes) -> Result<(u64, DataType, DataType, u8), Error> {
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
    let attribute: u64 = bincode::deserialize_from(&mut cursor)?;
    let value: DataType = bincode::deserialize_from(&mut cursor)?;
    let entity_id: DataType = bincode::deserialize_from(&mut cursor)?;

    Ok((attribute, value, entity_id, suffix))
}

pub fn aev_key_to_parts(key: Bytes) -> Result<(u64, DataType, DataType, u8), Error> {
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
    let attribute: u64 = bincode::deserialize_from(&mut cursor)?;
    let entity_id: DataType = bincode::deserialize_from(&mut cursor)?;
    let value: DataType = bincode::deserialize_from(&mut cursor)?;

    Ok((attribute, entity_id, value, suffix))
}

pub fn ae_key_to_parts(key: Bytes) -> Result<(u64, DataType, u8), Error> {
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
    let attribute: u64 = bincode::deserialize_from(&mut cursor)?;
    let entity_id: DataType = bincode::deserialize_from(&mut cursor)?;

    Ok((attribute, entity_id, suffix))
}

pub fn av_key_to_parts(key: Bytes) -> Result<(u64, DataType, u8), Error> {
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
    let attribute: u64 = bincode::deserialize_from(&mut cursor)?;
    let value: DataType = bincode::deserialize_from(&mut cursor)?;

    Ok((attribute, value, suffix))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use slatedb::{Db, Error as SlateDBError, config::ScanOptions};


    use crate::clock::st_from_unix_epoch;
    use super::*;

    #[tokio::test]
    async fn test_indexer() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await);
        let mut indexer = Indexer::new(slate.clone());
        let tx_key = TxKey { tx_id: 0, system_time: st_from_unix_epoch(0) };
        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(1));
        map.insert("name".to_string(), DataType::String("alan".to_string()));
        let doc = Document(map);
        let tx_ops = vec![TxOp::Put(doc)];
        indexer.transact_tx(tx_key, tx_ops).await.unwrap();

        let mut attribute_map = read_attribute_map(slate.clone()).await;
        let name_id = get_and_create_attribute_id(slate.clone(), "name", &mut attribute_map).await;

        let mut iter = slate.scan_prefix_with_options(&[codec::EAV], &ScanOptions::default()).await.unwrap();
        if let Some(kv2) = iter.next().await? {
            let (entity_id, attribute, value, suffix) = eav_key_to_parts(kv2.key).unwrap();
            assert_eq!(entity_id, DataType::Long(1));
            assert_eq!(attribute, name_id);
            assert_eq!(value, DataType::String("alan".to_string()));
            assert_eq!(suffix, codec::ADD);
            assert_eq!(kv2.value, Bytes::from(""));
        }
        assert_eq!(None , iter.next().await?);


        let mut iter = slate.scan_prefix_with_options(&[codec::AVE], &ScanOptions::default()).await.unwrap();
        if let Some(kv2) = iter.next().await? {
            let (attribute, value, entity_id, suffix) = ave_key_to_parts(kv2.key).unwrap();
            assert_eq!(entity_id, DataType::Long(1));
            assert_eq!(attribute, name_id);
            assert_eq!(value, DataType::String("alan".to_string()));
            assert_eq!(suffix, codec::ADD);
            assert_eq!(kv2.value, Bytes::from(""));
        }
        assert_eq!(None , iter.next().await?);


        let mut iter = slate.scan_prefix_with_options(&[codec::AEV], &ScanOptions::default()).await.unwrap();
        if let Some(kv2) = iter.next().await? {
            let (attribute, entity_id, value, suffix) = aev_key_to_parts(kv2.key).unwrap();
            assert_eq!(entity_id, DataType::Long(1));
            assert_eq!(attribute, name_id);
            assert_eq!(value, DataType::String("alan".to_string()));
            assert_eq!(suffix, codec::ADD);
            assert_eq!(kv2.value, Bytes::from(""));
        }
        assert_eq!(None , iter.next().await?);

        let mut iter = slate.scan_prefix_with_options(&[codec::AE], &ScanOptions::default()).await.unwrap();
        if let Some(kv2) = iter.next().await? {
            let (attribute, entity_id, suffix) = ae_key_to_parts(kv2.key).unwrap();
            assert_eq!(entity_id, DataType::Long(1));
            assert_eq!(attribute, name_id);
            assert_eq!(suffix, codec::ADD);
            assert_eq!(kv2.value, Bytes::from(""));
        }
        assert_eq!(None , iter.next().await?);

        let mut iter = slate.scan_prefix_with_options(&[codec::AV], &ScanOptions::default()).await.unwrap();
        if let Some(kv2) = iter.next().await? {
            let (attribute, value, suffix) = av_key_to_parts(kv2.key).unwrap();
            assert_eq!(attribute, name_id);
            assert_eq!(value, DataType::String("alan".to_string()));
            assert_eq!(suffix, codec::ADD);
            assert_eq!(kv2.value, Bytes::from(""));
        }
        assert_eq!(None , iter.next().await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_indexer_write_persisted() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await);
        let mut indexer = Indexer::new(slate.clone());
        let tx_key = TxKey { tx_id: 0, system_time: st_from_unix_epoch(0) };

        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(1));
        map.insert("name".to_string(), DataType::String("alan".to_string()));
        let doc = Document(map);
        let tx_ops = vec![TxOp::Put(doc)];
        indexer.transact_tx(tx_key, tx_ops).await.unwrap();

        // Verify EAV entry actually exists (not silently skipped like test_indexer's if-let)
        let mut iter = slate.scan_prefix_with_options(&[codec::EAV], &ScanOptions::default()).await.unwrap();
        let kv = iter.next().await?;
        assert!(kv.is_some(), "Expected EAV entry to be written to the database");

        Ok(())
    }

    #[tokio::test]
    async fn test_indexer_multi_attribute_document() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await);
        let mut indexer = Indexer::new(slate.clone());
        let tx_key = TxKey { tx_id: 0, system_time: st_from_unix_epoch(0) };

        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(1));
        map.insert("name".to_string(), DataType::String("alan".to_string()));
        map.insert("age".to_string(), DataType::Long(30));
        let doc = Document(map);
        let tx_ops = vec![TxOp::Put(doc)];
        indexer.transact_tx(tx_key, tx_ops).await.unwrap();

        // Verify all attributes are indexed in EAV
        let mut iter = slate.scan_prefix_with_options(&[codec::EAV], &ScanOptions::default()).await.unwrap();

        let mut eav_count = 0;
        while let Some(_kv) = iter.next().await? {
            eav_count += 1;
        }
        assert_eq!(eav_count, 2, "Expected 2 EAV entries (one per non-db/id attribute)");

        // Verify all attributes are indexed in AE
        let mut iter = slate.scan_prefix_with_options(&[codec::AE], &ScanOptions::default()).await.unwrap();

        let mut ae_count = 0;
        while let Some(_kv) = iter.next().await? {
            ae_count += 1;
        }
        assert_eq!(ae_count, 2, "Expected 2 AE entries (one per non-db/id attribute)");

        Ok(())
    }

    #[tokio::test]
    async fn test_await_tx_already_indexed() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await);
        let mut indexer = Indexer::new(slate.clone());

        // Index transaction
        let tx_key = TxKey { tx_id: 0, system_time: st_from_unix_epoch(0) };
        let mut map = BTreeMap::new();
        map.insert("db/id".to_string(), DataType::Long(1));
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
        let indexer = Arc::new(RwLock::new(Indexer::new(slate.clone())));

        let tx_key_1 = TxKey { tx_id: 1, system_time: st_from_unix_epoch(100) };

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
            map.insert("db/id".to_string(), DataType::Long(1));
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
        let indexer = Indexer::new(slate.clone());

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
        let mut indexer = Indexer::new(slate.clone());

        // Index tx 0 and tx 1
        for i in 0..2 {
            let tx_key = TxKey { tx_id: i, system_time: st_from_unix_epoch(i as u64 * 100) };
            let mut map = BTreeMap::new();
            map.insert("db/id".to_string(), DataType::Long(i + 1));
            map.insert("name".to_string(), DataType::String(format!("user{}", i)));
            let tx_ops = vec![TxOp::Put(Document(map))];
            indexer.transact_tx(tx_key, tx_ops).await?;
        }

        // Waiting for tx 0 should return immediately
        let tx_key_0 = TxKey { tx_id: 0, system_time: st_from_unix_epoch(0) };
        indexer.await_tx(tx_key_0).await?;

        // Waiting for tx 1 should also return immediately
        let tx_key_1 = TxKey { tx_id: 1, system_time: st_from_unix_epoch(100) };
        indexer.await_tx(tx_key_1).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_await_tx_multiple_waiters() -> Result<(), Error> {
        use tokio::sync::RwLock;

        let slate = Arc::new(in_memory_slate().await);
        let indexer = Arc::new(RwLock::new(Indexer::new(slate.clone())));

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
            map.insert("db/id".to_string(), DataType::Long(1));
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
