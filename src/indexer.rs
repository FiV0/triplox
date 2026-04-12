use anyhow::{bail, Error, Result};
use bytes::Bytes;
use log::{error, trace, warn};
use slatedb::Db;
use slatedb::IsolationLevel;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::broadcast;

use edn::kw;

use crate::codec::{
    self, decode_datatype, decode_i64, encode_datatype, encode_i64, encode_i64_bytes, Encode,
};
use crate::iterator::temporal_filter_iterator;
use crate::log::{Record, Subscriber};
use crate::metadata::Metadata;
use crate::ops::DataType;
use crate::ops::{Datom, DatomOp, TxOp};
use crate::partition::{partition_entity_prefix, TX_PARTITION};
use crate::schema::{Schema, DB_TX_ABORTED, DB_TX_COMMITTED};
use crate::slate::{DEFAULT_SCAN_OPTIONS, DEFAULT_WRITE_OPTIONS};
use crate::transaction::TxKey;
use crate::tx;
use crate::util::concat_bytes;
use edn::symbols::Keyword;

pub struct Indexer {
    slatedb: Arc<Db>,
    metadata: Metadata,
    latest_indexed_tx: Option<TxKey>,
    tx_completion_sender: broadcast::Sender<(TxKey, Result<(), Arc<anyhow::Error>>)>,
}

/// Write index entries for datoms into a SlateDB transaction.
pub(crate) fn write_index_entries(
    txn: &slatedb::DbTransaction,
    datoms: &[Datom],
    schema: &Schema,
    tx_eid: i64,
) -> Result<(), Error> {
    let tx_eid_bytes = encode_i64_bytes(tx_eid);

    for datom in datoms {
        // TODO: entity IDs encoded as DataType::Long to match value-position encoding.
        // Revisit with schema/custom encoding.
        let entity_id = DataType::Long(datom.entity).encode();
        let attribute_id = schema
            .get_attribute(&datom.attribute)
            .map(|(eid, _)| eid)
            .ok_or_else(|| anyhow::anyhow!("Unknown attribute: {}", datom.attribute))?;
        let attribute = encode_i64_bytes(attribute_id);
        let mut value = Vec::new();
        encode_datatype(&datom.value, &mut value);
        let op_byte = match datom.op {
            DatomOp::Assert => codec::ADD,
            DatomOp::Retract => codec::RETRACT,
        };

        // Temporal indices include tx_eid + op
        // TODO(triplox-7fl): concat_bytes allocates intermediate Vecs per component;
        // encoding directly into a single buffer would be faster.
        txn.put(
            concat_bytes(&[
                &[codec::EAV],
                &entity_id,
                &attribute,
                &value,
                &tx_eid_bytes,
                &[op_byte],
            ]),
            [],
        )?;
        txn.put(
            concat_bytes(&[
                &[codec::AVE],
                &attribute,
                &value,
                &entity_id,
                &tx_eid_bytes,
                &[op_byte],
            ]),
            [],
        )?;
        txn.put(
            concat_bytes(&[
                &[codec::AEV],
                &attribute,
                &entity_id,
                &value,
                &tx_eid_bytes,
                &[op_byte],
            ]),
            [],
        )?;

        // AE and AV are atemporal, purely additive indices.
        // Retractions are not written to AE/AV.
        if datom.op == DatomOp::Assert {
            txn.put(concat_bytes(&[&[codec::AE], &attribute, &entity_id]), [])?;
            txn.put(concat_bytes(&[&[codec::AV], &attribute, &value]), [])?;
        }
    }

    Ok(())
}

/// Scan EAV entries for TX_PARTITION entities and return (tx_eid, TxKey) for the latest tx.
/// If no transaction entities exist, returns a sentinel (0, TxKey{tx_id=0, system_time=epoch}).
///
/// Uses the high bits of TX_PARTITION entity IDs to build a targeted EAV prefix,
/// restricting the scan to TX_PARTITION entities. With descending entity encoding,
/// the first TX_PARTITION entity is the latest.
///
/// Collects tx_eid from the entity position, tx_id from `db/txId` value, and
/// system_time from `db/txInstant` value.
///
/// TODO(triplox-8mc): Return Option instead of sentinel. Needs coordination with
/// the bootstrap transaction (tx_id=0).
pub async fn latest_tx_key_from_snapshot(
    snapshot: &Arc<slatedb::DbSnapshot>,
) -> Result<(i64, TxKey)> {
    let eav_tx_prefix = concat_bytes(&[&[codec::EAV], &partition_entity_prefix(TX_PARTITION)]);
    let mut iter = snapshot
        .scan_prefix_with_options(&eav_tx_prefix, &DEFAULT_SCAN_OPTIONS)
        .await?;
    let mut first_eid: Option<i64> = None;
    let mut tx_id: Option<i64> = None;
    let mut system_time: Option<crate::clock::Instant> = None;
    while let Some(kv) = iter.next().await? {
        let (entity_dt, attribute, value, _tx_eid, _op) = eav_key_to_parts(kv.key)?;
        let eid = match entity_dt {
            DataType::Long(id) => id,
            other => bail!("Expected Long entity ID in EAV key, got {:?}", other),
        };
        match first_eid {
            None => first_eid = Some(eid),
            Some(first) if first != eid => break,
            _ => {}
        }
        if attribute == crate::schema::DB_TX_ID {
            if let DataType::Long(id) = value {
                tx_id = Some(id);
            }
        }
        if attribute == crate::schema::DB_TX_INSTANT {
            if let DataType::Instant(st) = value {
                system_time = Some(st);
            }
        }
    }
    match (first_eid, tx_id, system_time) {
        (Some(eid), Some(tid), Some(st)) => Ok((
            eid,
            TxKey {
                tx_id: tid,
                system_time: st,
            },
        )),
        _ => Ok((
            0,
            TxKey {
                tx_id: 0,
                system_time: crate::clock::st_from_unix_epoch(0),
            },
        )),
    }
}

/// Look up the tx_eid for a given TxKey using the AVE index.
/// AVE key layout: [AVE][attribute][value][entity_id][tx_eid][op]
/// We build a prefix for (db/txId, tx_key.tx_id) and extract the entity from the first match.
pub async fn tx_eid_for_tx_key(snapshot: &Arc<slatedb::DbSnapshot>, tx_key: &TxKey) -> Result<i64> {
    let attr_bytes = encode_i64_bytes(crate::schema::DB_TX_ID);
    let mut value_bytes = Vec::new();
    codec::encode_datatype(&DataType::Long(tx_key.tx_id), &mut value_bytes);
    let ave_prefix = concat_bytes(&[&[codec::AVE], &attr_bytes, &value_bytes]);
    let mut iter = snapshot
        .scan_prefix_with_options(&ave_prefix, &DEFAULT_SCAN_OPTIONS)
        .await?;
    match iter.next().await? {
        Some(kv) => {
            let (_attribute, _value, entity_dt, _tx_eid, _op) = ave_key_to_parts(kv.key)?;
            match entity_dt {
                DataType::Long(eid) => Ok(eid),
                other => bail!("Expected Long entity ID in AVE key, got {:?}", other),
            }
        }
        // TODO: the caller (db_as_of) should wait for the transaction to be
        // indexed before calling this function, so this case shouldn't happen
        // in normal operation.
        None => bail!("No tx entity found for tx_id={}", tx_key.tx_id),
    }
}

/// Build datoms for a first-class transaction entity in TX_PARTITION.
/// The `tx_eid` is the pre-allocated entity ID for this transaction.
fn build_tx_entity_datoms(
    tx_eid: i64,
    tx_key: TxKey,
    committed: bool,
    error: Option<String>,
) -> Vec<Datom> {
    let st = tx_key.system_time;
    let result_eid = if committed {
        DB_TX_COMMITTED
    } else {
        DB_TX_ABORTED
    };
    let mut datoms = vec![
        Datom {
            entity: tx_eid,
            attribute: kw!(:db/txInstant),
            value: DataType::Instant(st),
            op: DatomOp::Assert,
        },
        Datom {
            entity: tx_eid,
            attribute: kw!(:db/txId),
            value: DataType::Long(tx_key.tx_id),
            op: DatomOp::Assert,
        },
        Datom {
            entity: tx_eid,
            attribute: kw!(:db/txResult),
            value: DataType::Long(result_eid),
            op: DatomOp::Assert,
        },
    ];
    if let Some(err) = error {
        datoms.push(Datom {
            entity: tx_eid,
            attribute: kw!(:db.tx/error),
            value: DataType::String(err),
            op: DatomOp::Assert,
        });
    }
    datoms
}

impl Indexer {
    pub fn new(slatedb: Arc<Db>, metadata: Metadata, latest_indexed_tx: Option<TxKey>) -> Self {
        let (tx_completion_sender, _) = broadcast::channel(1024);
        Indexer {
            slatedb,
            metadata,
            latest_indexed_tx,
            tx_completion_sender,
        }
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Transact a set of operations, automatically retracting old values for
    /// cardinality:one attributes when a new value is asserted.
    ///
    /// Uses a SlateDB transaction for atomic read-then-write: the EAV index scan
    /// and all index writes happen in a single transaction.
    pub async fn transact_tx(&mut self, tx_key: TxKey, tx_ops: Vec<TxOp>) -> Result<TxKey, Error> {
        match self.transact_tx_inner(tx_key, tx_ops).await {
            Ok(tx_key) => Ok(tx_key),
            Err(e) => {
                if let Err(abort_err) = self.write_aborted_tx(tx_key, e.to_string()).await {
                    error!(
                        "Failed to write aborted tx entity for {}: {}",
                        tx_key.tx_id, abort_err
                    );
                }
                Err(e)
            }
        }
    }

    async fn transact_tx_inner(
        &mut self,
        tx_key: TxKey,
        tx_ops: Vec<TxOp>,
    ) -> Result<TxKey, Error> {
        // 1. Clone PartitionMap + allocate tx entity
        let mut pending_pm = self.metadata.partition_map.clone();
        let tx_eid = pending_pm.allocate_entid(TX_PARTITION);

        // 2. Expand TxOps to DatomWithTempids (resolves idents via schema)
        let expanded = tx::expand_tx_ops(&tx_ops, &self.metadata.schema)?;

        // 3. Resolve tempids + build tx entity datoms
        let mut datoms = tx::resolve_tempids(&expanded, &mut pending_pm)?;
        datoms.extend(build_tx_entity_datoms(tx_eid, tx_key, true, None));

        // 4. Finalize datoms (card-one rewrite) against current storage state
        let txn = self.slatedb.begin(IsolationLevel::Snapshot).await?;
        let datoms = self
            .finalize_datoms_for_commit(&txn, tx_eid, datoms)
            .await?;

        // 5. General validation
        let validation = self.metadata.schema.validate_datoms(&datoms)?;

        // 6. Write indices + commit
        write_index_entries(&txn, &datoms, &self.metadata.schema, tx_eid)?;
        txn.commit_with_options(&DEFAULT_WRITE_OPTIONS).await?;

        // 7. Apply on success only
        self.metadata.partition_map = pending_pm;
        if validation.schema_changes_detected {
            let schema_update = self.metadata.schema.prepare_schema_update(&datoms)?;
            if !schema_update.is_empty() {
                self.metadata.schema.apply_schema_update(schema_update);
                self.metadata.advance_generation();
            }
        }

        // Update latest indexed tx and broadcast completion
        self.latest_indexed_tx = Some(tx_key);

        if let Err(e) = self.tx_completion_sender.send((tx_key, Ok(()))) {
            trace!(
                "No receivers for indexed transaction {}: {}",
                tx_key.tx_id,
                e
            );
        }

        Ok(tx_key)
    }

    async fn finalize_datoms_for_commit(
        &self,
        txn: &slatedb::DbTransaction,
        tx_eid: i64,
        datoms: Vec<Datom>,
    ) -> Result<Vec<Datom>, Error> {
        // For each Assert datom, scan EAV for the current value of (entity, attribute).
        // If the old value equals the new value, drop the datom (no-op).
        // If the old value differs, add a Retract datom for the old value.
        // Collect into HashSet to deduplicate explicit + auto-generated retractions.
        let as_of_encoded = encode_i64_bytes(tx_eid);
        let mut resolved_datoms: HashSet<Datom> = HashSet::with_capacity(datoms.len());
        for datom in datoms {
            if datom.op != DatomOp::Assert {
                resolved_datoms.insert(datom);
                continue;
            }

            let (attribute_id, attr) = self
                .metadata
                .schema
                .get_attribute(&datom.attribute)
                .ok_or_else(|| anyhow::anyhow!("Unknown attribute: {}", datom.attribute))?;

            if attr.multival {
                resolved_datoms.insert(datom);
                continue;
            }

            let entity_id_bytes = DataType::Long(datom.entity).encode();
            let attr_id_bytes = encode_i64_bytes(attribute_id);
            let eav_prefix = concat_bytes(&[&[codec::EAV], &entity_id_bytes, &attr_id_bytes]);

            // Scan EAV prefix on the transaction to find the current value.
            // Uses async scan directly because TemporalFilterIterator is sync-only.
            // This duplicates the temporal resolution logic from advance_to_next_valid().
            // TODO(triplox-vbc): unify with TemporalFilterIterator once the iterator
            // layer becomes fully async, eliminating this duplication.
            let mut iter = txn
                .scan_prefix_with_options(&eav_prefix, &DEFAULT_SCAN_OPTIONS)
                .await?;
            let mut old_value: Option<DataType> = None;
            while let Some(kv) = iter.next().await? {
                let key = &kv.key;
                assert!(
                    key.len() >= codec::TX_EID_OP_SUFFIX,
                    "Key too short ({} bytes) to contain tx_eid + op suffix",
                    key.len()
                );
                match temporal_filter_iterator::resolve_temporal_key(key, &as_of_encoded) {
                    Some(op) if op == codec::RETRACT => {
                        old_value = None;
                        break;
                    }
                    Some(_) => {
                        let value_bytes =
                            &key[eav_prefix.len()..key.len() - codec::TX_EID_OP_SUFFIX];
                        let mut cursor = value_bytes;
                        let value: DataType = decode_datatype(&mut cursor)?;
                        old_value = Some(value);
                        break;
                    }
                    None => {}
                }
            }

            if let Some(old_value) = old_value {
                if old_value == datom.value {
                    continue;
                }
                resolved_datoms.insert(Datom {
                    entity: datom.entity,
                    attribute: datom.attribute.clone(),
                    value: old_value,
                    op: DatomOp::Retract,
                });
            }
            resolved_datoms.insert(datom);
        }

        Ok(resolved_datoms.into_iter().collect())
    }

    /// Subscribe to transaction completion notifications.
    ///
    /// Returns a `TxWaiter` that can later be used to wait for a specific transaction.
    /// Call this **before** appending to the log to avoid a race where the indexer
    /// broadcasts the result before the caller subscribes.
    ///
    /// # Example
    /// ```ignore
    /// let waiter = indexer.read().await.tx_waiter();
    /// // drop the lock, append to log, then wait:
    /// let tx_key = log.write().await.append_tx(data).await;
    /// waiter.await_tx(tx_key).await?;
    /// ```
    pub(crate) fn tx_waiter(&self) -> TxWaiter {
        TxWaiter {
            latest_tx: self.latest_indexed_tx,
            rx: self.tx_completion_sender.subscribe(),
        }
    }

    /// Write an aborted transaction entity (no user data) when transact_tx fails.
    async fn write_aborted_tx(&mut self, tx_key: TxKey, error: String) -> Result<(), Error> {
        let mut pending_pm = self.metadata.partition_map.clone();
        let tx_eid = pending_pm.allocate_entid(TX_PARTITION);
        let datoms = build_tx_entity_datoms(tx_eid, tx_key, false, Some(error));
        let txn = self.slatedb.begin(IsolationLevel::Snapshot).await?;
        write_index_entries(&txn, &datoms, &self.metadata.schema, tx_eid)?;
        txn.commit_with_options(&DEFAULT_WRITE_OPTIONS).await?;
        // No need to advance generation for aborted transactions
        self.metadata.partition_map = pending_pm;
        self.latest_indexed_tx = Some(tx_key);
        Ok(())
    }
}

/// A pre-subscribed handle for waiting on transaction completion.
///
/// Created by `Indexer::tx_waiter()`. Holds a broadcast receiver so that
/// no messages are missed between subscription and the actual wait.
pub(crate) struct TxWaiter {
    latest_tx: Option<TxKey>,
    rx: broadcast::Receiver<(TxKey, Result<(), Arc<anyhow::Error>>)>,
}

impl TxWaiter {
    /// Wait until `tx_key` has been indexed. Returns `Ok(())` on commit,
    /// `Err` on abort or if the indexer shuts down.
    pub async fn await_tx(mut self, tx_key: TxKey) -> Result<(), Error> {
        // Fast path: already indexed at subscription time
        if let Some(latest_key) = self.latest_tx {
            if tx_key <= latest_key {
                return Ok(());
            }
        }

        // TODO(triplox-j7t): The >= check can return a different tx's result if the
        // broadcast channel lags. Will be revisited when the log is removed and we
        // ingest directly into Slate.
        loop {
            match self.rx.recv().await {
                Ok((completed_tx_key, result)) => {
                    if completed_tx_key >= tx_key {
                        return result.map_err(|e| anyhow::anyhow!("{:#}", e));
                    }
                    // Keep waiting for higher tx_id
                }
                Err(broadcast::error::RecvError::Lagged(_count)) => {
                    // Channel overflowed. Just continue waiting - we'll eventually
                    // get the notification or the channel will close.
                    continue;
                }
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

impl Subscriber for Indexer {
    async fn accept(&mut self, record: Record) {
        let tx_ops: Vec<TxOp> = match bincode::deserialize(&record.record) {
            Ok(ops) => ops,
            Err(e) => {
                let err = anyhow::anyhow!("Failed to deserialize TxOps: {}", e);
                warn!(
                    "Transaction {} deserialization failed: {}",
                    record.tx_key.tx_id, err
                );
                let _ = self
                    .tx_completion_sender
                    .send((record.tx_key, Err(Arc::new(err))));
                return;
            }
        };
        // TODO: transact_tx now handles writing aborted tx entities internally;
        // the subscriber should propagate the error notification via tx_completion_sender.
        if let Err(e) = self.transact_tx(record.tx_key, tx_ops).await {
            warn!("Transaction {} failed: {}", record.tx_key.tx_id, e);
            let _ = self
                .tx_completion_sender
                .send((record.tx_key, Err(Arc::new(e))));
        }
    }
}

/// Strip a temporal index key into (data_bytes, tx_eid, op).
fn strip_temporal_key<'a>(
    key: &'a [u8],
    expected_prefix: u8,
    name: &str,
) -> Result<(&'a [u8], i64, u8), Error> {
    if key.first() != Some(&expected_prefix) {
        return Err(anyhow::anyhow!("Not a {} key", name));
    }
    if key.len() < 1 + codec::TX_EID_OP_SUFFIX {
        return Err(anyhow::anyhow!("Key too short"));
    }
    let data = &key[1..key.len() - codec::TX_EID_OP_SUFFIX];
    let mut cursor = &key[key.len() - codec::TX_EID_OP_SUFFIX..key.len() - codec::OP_LENGTH];
    let tx_eid = decode_i64(&mut cursor)?;
    let op = key[key.len() - 1];
    Ok((data, tx_eid, op))
}

/// Strip an atemporal index key, returning data bytes after the prefix.
fn strip_atemporal_key<'a>(
    key: &'a [u8],
    expected_prefix: u8,
    name: &str,
) -> Result<&'a [u8], Error> {
    if key.first() != Some(&expected_prefix) {
        return Err(anyhow::anyhow!("Not a {} key", name));
    }
    if key.len() < 2 {
        return Err(anyhow::anyhow!("Key too short"));
    }
    Ok(&key[1..])
}

pub fn eav_key_to_parts(key: Bytes) -> Result<(DataType, i64, DataType, i64, u8), Error> {
    let (data, tx_eid, op) = strip_temporal_key(key.as_ref(), codec::EAV, "EAV")?;
    let mut cursor = data;
    let entity_id = decode_datatype(&mut cursor)?;
    let attribute = decode_i64(&mut cursor)?;
    let value = decode_datatype(&mut cursor)?;
    Ok((entity_id, attribute, value, tx_eid, op))
}

pub fn ave_key_to_parts(key: Bytes) -> Result<(i64, DataType, DataType, i64, u8), Error> {
    let (data, tx_eid, op) = strip_temporal_key(key.as_ref(), codec::AVE, "AVE")?;
    let mut cursor = data;
    let attribute = decode_i64(&mut cursor)?;
    let value = decode_datatype(&mut cursor)?;
    let entity_id = decode_datatype(&mut cursor)?;
    Ok((attribute, value, entity_id, tx_eid, op))
}

pub fn aev_key_to_parts(key: Bytes) -> Result<(i64, DataType, DataType, i64, u8), Error> {
    let (data, tx_eid, op) = strip_temporal_key(key.as_ref(), codec::AEV, "AEV")?;
    let mut cursor = data;
    let attribute = decode_i64(&mut cursor)?;
    let entity_id = decode_datatype(&mut cursor)?;
    let value = decode_datatype(&mut cursor)?;
    Ok((attribute, entity_id, value, tx_eid, op))
}

pub fn ae_key_to_parts(key: Bytes) -> Result<(i64, DataType), Error> {
    let data = strip_atemporal_key(key.as_ref(), codec::AE, "AE")?;
    let mut cursor = data;
    let attribute = decode_i64(&mut cursor)?;
    let entity_id = decode_datatype(&mut cursor)?;
    Ok((attribute, entity_id))
}

pub fn av_key_to_parts(key: Bytes) -> Result<(i64, DataType), Error> {
    let data = strip_atemporal_key(key.as_ref(), codec::AV, "AV")?;
    let mut cursor = data;
    let attribute = decode_i64(&mut cursor)?;
    let value = decode_datatype(&mut cursor)?;
    Ok((attribute, value))
}

#[cfg(test)]
mod tests {
    use slatedb::{config::ScanOptions, Db};

    use super::*;
    use crate::clock::{st_from_unix_epoch, Instant};
    use crate::ops::{DataType, EntityRef};
    use crate::schema::{test_schema_tx, DB_CARDINALITY_ONE, DB_TYPE_LONG};
    use crate::slate::in_memory_slate;

    /// Create an indexer with bootstrap schema and test attributes already transacted.
    /// Uses init_db for bootstrap, then transacts test schema via the indexer.
    /// Returns the indexer ready for test data at tx_id=1+.
    async fn bootstrapped_indexer(slate: Arc<Db>) -> Indexer {
        let metadata = crate::bootstrap::init_db(slate.clone()).await;
        let mut indexer = Indexer::new(slate, metadata, None);
        let tx_key_0 = TxKey {
            tx_id: 0,
            system_time: st_from_unix_epoch(1),
        };
        indexer
            .transact_tx(tx_key_0, test_schema_tx())
            .await
            .unwrap();
        indexer
    }

    #[tokio::test]
    async fn test_indexer() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await.db);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;
        let name_id = indexer
            .metadata()
            .schema
            .get_attribute(&kw!(:name))
            .unwrap()
            .0;
        let tx_key = TxKey {
            tx_id: 1,
            system_time: st_from_unix_epoch(2),
        };
        let tx_ops = vec![TxOp::Add {
            entity: "alan".into(),
            attribute: kw!(:name),
            value: "alan".into(),
        }];
        indexer.transact_tx(tx_key, tx_ops).await.unwrap();

        // The entity was auto-assigned an ID in USER_PARTITION
        let user_base = crate::partition::USER_PARTITION as i64 * (1i64 << 42);

        // Find the EAV entry for the user entity (skip bootstrap/schema entries)
        let mut iter = slate
            .scan_prefix_with_options(&[codec::EAV], &ScanOptions::default())
            .await
            .unwrap();
        let mut found = false;
        while let Some(kv) = iter.next().await? {
            let (entity_id, attribute, value, _timestamp, suffix) =
                eav_key_to_parts(kv.key).unwrap();
            if let DataType::Long(eid) = &entity_id {
                if *eid >= user_base {
                    assert_eq!(attribute, name_id);
                    assert_eq!(value, DataType::String("alan".to_string()));
                    assert_eq!(suffix, codec::ADD);
                    found = true;
                    break;
                }
            }
        }
        assert!(found, "Expected EAV entry for user entity");

        Ok(())
    }

    #[tokio::test]
    async fn test_indexer_write_persisted() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await.db);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;
        let tx_key = TxKey {
            tx_id: 1,
            system_time: st_from_unix_epoch(2),
        };

        let tx_ops = vec![TxOp::Add {
            entity: "alan".into(),
            attribute: kw!(:name),
            value: "alan".into(),
        }];
        indexer.transact_tx(tx_key, tx_ops).await.unwrap();

        // Verify an EAV entry in USER_PARTITION exists
        let user_base = crate::partition::USER_PARTITION as i64 * (1i64 << 42);
        let mut iter = slate
            .scan_prefix_with_options(&[codec::EAV], &ScanOptions::default())
            .await
            .unwrap();
        let mut found = false;
        while let Some(kv) = iter.next().await? {
            let (entity_id, _, _, _, _) = eav_key_to_parts(kv.key).unwrap();
            if let DataType::Long(eid) = entity_id {
                if eid >= user_base {
                    found = true;
                    break;
                }
            }
        }
        assert!(found, "Expected EAV entry to be written to the database");

        Ok(())
    }

    #[tokio::test]
    async fn test_indexer_multi_attribute_document() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await.db);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;
        let tx_key = TxKey {
            tx_id: 1,
            system_time: st_from_unix_epoch(2),
        };

        let tx_ops = vec![TxOp::put(vec![
            (kw!(:name), "alan".into()),
            (kw!(:age), 30_i64.into()),
        ])];

        indexer.transact_tx(tx_key, tx_ops).await.unwrap();

        // Count EAV entries in USER_PARTITION (should be 2: name + age)
        let user_base = crate::partition::USER_PARTITION as i64 * (1i64 << 42);
        let mut iter = slate
            .scan_prefix_with_options(&[codec::EAV], &ScanOptions::default())
            .await
            .unwrap();
        let mut eav_count = 0;
        while let Some(kv) = iter.next().await? {
            let (entity_id, _, _, _, _) = eav_key_to_parts(kv.key).unwrap();
            if let DataType::Long(eid) = entity_id {
                if eid >= user_base {
                    eav_count += 1;
                }
            }
        }
        assert_eq!(eav_count, 2, "Expected 2 EAV entries for user entity");

        Ok(())
    }

    #[tokio::test]
    async fn test_latest_tx_key_after_transact() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await.db);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;
        let tx_key = TxKey {
            tx_id: 42,
            system_time: st_from_unix_epoch(1000),
        };

        let tx_ops = vec![TxOp::Add {
            entity: "alice".into(),
            attribute: kw!(:name),
            value: "alice".into(),
        }];
        indexer.transact_tx(tx_key, tx_ops).await?;

        let snapshot = Arc::new(slate.snapshot().await?);
        let (tx_eid, latest) = latest_tx_key_from_snapshot(&snapshot).await?;
        assert_eq!(latest.tx_id, 42);
        assert_eq!(latest.system_time, st_from_unix_epoch(1000));
        assert!(tx_eid > 0, "tx_eid should be a valid entity ID");
        Ok(())
    }

    #[tokio::test]
    async fn test_latest_tx_key_empty_db() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await.db);
        let snapshot = Arc::new(slate.snapshot().await?);
        let (tx_eid, latest) = latest_tx_key_from_snapshot(&snapshot).await?;
        assert_eq!(latest.tx_id, 0, "Should return sentinel for empty DB");
        assert_eq!(tx_eid, 0, "Should return sentinel tx_eid for empty DB");
        Ok(())
    }

    #[tokio::test]
    async fn test_latest_tx_key_highest_wins() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await.db);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;

        for i in 0..3 {
            let tx_id = i + 1;
            let tx_key = TxKey {
                tx_id,
                system_time: st_from_unix_epoch(tx_id as u64 * 100),
            };
            let tx_ops = vec![TxOp::Add {
                entity: format!("user{}", i).into(),
                attribute: kw!(:name),
                value: format!("user{}", i).into(),
            }];
            indexer.transact_tx(tx_key, tx_ops).await?;
        }

        let snapshot = Arc::new(slate.snapshot().await?);
        let (_tx_eid, latest) = latest_tx_key_from_snapshot(&snapshot).await?;
        assert_eq!(latest.tx_id, 3, "Should return highest tx_id");
        assert_eq!(latest.system_time, st_from_unix_epoch(300));
        Ok(())
    }

    #[tokio::test]
    async fn test_tx_eid_for_tx_key_found() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await.db);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;
        let tx_key = TxKey {
            tx_id: 42,
            system_time: st_from_unix_epoch(1000),
        };

        let tx_ops = vec![TxOp::Add {
            entity: "alice".into(),
            attribute: kw!(:name),
            value: "alice".into(),
        }];
        indexer.transact_tx(tx_key, tx_ops).await?;

        let snapshot = Arc::new(slate.snapshot().await?);
        let tx_eid = tx_eid_for_tx_key(&snapshot, &tx_key).await?;
        assert!(tx_eid > 0, "tx_eid should be a valid entity ID");

        // Should match what latest_tx_key_from_snapshot returns
        let (latest_tx_eid, _) = latest_tx_key_from_snapshot(&snapshot).await?;
        assert_eq!(tx_eid, latest_tx_eid);
        Ok(())
    }

    #[tokio::test]
    async fn test_tx_eid_for_tx_key_not_found() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await.db);
        let snapshot = Arc::new(slate.snapshot().await?);
        let tx_key = TxKey {
            tx_id: 999,
            system_time: st_from_unix_epoch(1000),
        };

        let result = tx_eid_for_tx_key(&snapshot, &tx_key).await;
        assert!(result.is_err(), "Should fail for non-existent tx_id");
        assert!(result.unwrap_err().to_string().contains("tx_id=999"));
        Ok(())
    }

    #[tokio::test]
    async fn test_await_tx_already_indexed() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await.db);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;

        let tx_key = TxKey {
            tx_id: 1,
            system_time: st_from_unix_epoch(2),
        };
        let tx_ops = vec![TxOp::Add {
            entity: "alice".into(),
            attribute: kw!(:name),
            value: "alice".into(),
        }];
        indexer.transact_tx(tx_key, tx_ops).await?;

        indexer.tx_waiter().await_tx(tx_key).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_await_tx_waits_for_future_tx() -> Result<(), Error> {
        use tokio::sync::RwLock;

        let slate = Arc::new(in_memory_slate().await.db);
        let indexer = Arc::new(RwLock::new(bootstrapped_indexer(slate.clone()).await));

        let tx_key_1 = TxKey {
            tx_id: 1,
            system_time: st_from_unix_epoch(200),
        };

        // Subscribe BEFORE transacting to avoid race
        let waiter = indexer.read().await.tx_waiter();

        {
            let mut guard = indexer.write().await;
            let tx_ops = vec![TxOp::Add {
                entity: "bob".into(),
                attribute: kw!(:name),
                value: "bob".into(),
            }];

            guard.transact_tx(tx_key_1, tx_ops).await?;
        }

        // Waiter should complete successfully
        waiter.await_tx(tx_key_1).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_await_tx_timeout() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await.db);
        let indexer = Indexer::new(
            slate.clone(),
            Metadata::new(Schema::default(), crate::metadata::PartitionMap::new()),
            None,
        );

        let tx_key = TxKey {
            tx_id: 999,
            system_time: st_from_unix_epoch(999),
        };

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            indexer.tx_waiter().await_tx(tx_key),
        )
        .await;

        assert!(
            result.is_err(),
            "Should timeout waiting for non-existent tx"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_await_tx_ordering() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await.db);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;

        for i in 0..2 {
            let tx_id = i + 1;
            let tx_key = TxKey {
                tx_id,
                system_time: st_from_unix_epoch(tx_id as u64 * 100),
            };
            let tx_ops = vec![TxOp::Add {
                entity: format!("user{}", i).into(),
                attribute: kw!(:name),
                value: format!("user{}", i).into(),
            }];
            indexer.transact_tx(tx_key, tx_ops).await?;
        }

        let tx_key_1 = TxKey {
            tx_id: 1,
            system_time: st_from_unix_epoch(100),
        };
        indexer.tx_waiter().await_tx(tx_key_1).await?;
        let tx_key_2 = TxKey {
            tx_id: 2,
            system_time: st_from_unix_epoch(200),
        };
        indexer.tx_waiter().await_tx(tx_key_2).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_await_tx_multiple_waiters() -> Result<(), Error> {
        use tokio::sync::RwLock;

        let slate = Arc::new(in_memory_slate().await.db);
        let indexer = Arc::new(RwLock::new(bootstrapped_indexer(slate.clone()).await));

        let tx_key = TxKey {
            tx_id: 5,
            system_time: st_from_unix_epoch(500),
        };

        // Subscribe multiple waiters BEFORE transacting
        let waiters: Vec<_> = {
            let guard = indexer.read().await;
            (0..5).map(|_| guard.tx_waiter()).collect()
        };

        // Index the transaction
        {
            let mut guard = indexer.write().await;
            let tx_ops = vec![TxOp::Add {
                entity: "shared".into(),
                attribute: kw!(:name),
                value: "shared".into(),
            }];

            guard.transact_tx(tx_key, tx_ops).await?;
        }

        // All waiters should complete
        for waiter in waiters {
            waiter.await_tx(tx_key).await?;
        }

        Ok(())
    }

    // TODO(triplox-qj0): replace explicit entity IDs with query-based verification
    #[tokio::test]
    async fn test_retract_on_overwrite() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await.db);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;
        let name_id = indexer
            .metadata()
            .schema
            .get_attribute(&kw!(:name))
            .unwrap()
            .0;

        // First tx: assert name="alice" for a new entity (auto-assigned)
        let tx1 = TxKey {
            tx_id: 1,
            system_time: st_from_unix_epoch(100),
        };
        let tx_ops = vec![TxOp::Add {
            entity: "alice".into(),
            attribute: kw!(:name),
            value: "alice".into(),
        }];
        indexer.transact_tx(tx1, tx_ops).await?;

        // Find the auto-assigned entity ID
        let user_base = crate::partition::USER_PARTITION as i64 * (1i64 << 42);
        let mut entity_id: i64 = 0;
        let mut iter = slate
            .scan_prefix_with_options(&[codec::EAV], &ScanOptions::default())
            .await?;
        while let Some(kv) = iter.next().await? {
            let (eid, _, _, _, _) = eav_key_to_parts(kv.key.clone())?;
            if let DataType::Long(id) = eid {
                if id >= user_base {
                    entity_id = id;
                    break;
                }
            }
        }
        assert!(entity_id >= user_base, "Should have found user entity");

        // Second tx: assert name="bob" for same entity — should auto-retract "alice"
        let tx2 = TxKey {
            tx_id: 2,
            system_time: st_from_unix_epoch(200),
        };
        indexer
            .transact_tx(
                tx2,
                vec![TxOp::Add {
                    entity: EntityRef::Id(entity_id),
                    attribute: kw!(:name),
                    value: "bob".into(),
                }],
            )
            .await?;

        // Scan EAV for entity — expect: alice ADD, alice RETRACT, bob ADD
        let mut iter = slate
            .scan_prefix_with_options(&[codec::EAV], &ScanOptions::default())
            .await?;
        let mut alice_add = false;
        let mut alice_retract = false;
        let mut bob_add = false;
        while let Some(kv) = iter.next().await? {
            let (eid, attribute, value, _ts, op) = eav_key_to_parts(kv.key)?;
            if eid != DataType::Long(entity_id) || attribute != name_id {
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

        // Verify AE entry exists
        let attr_bytes = encode_i64_bytes(name_id);
        let entity_bytes = DataType::Long(entity_id).encode();
        let ae_key = concat_bytes(&[&[codec::AE], &attr_bytes, &entity_bytes]);
        let ae_val = slate.get(&ae_key).await?.expect("AE entry should exist");
        assert!(ae_val.is_empty(), "AE should store empty bytes");

        Ok(())
    }

    #[tokio::test]
    async fn test_same_value_no_retract() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await.db);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;
        let name_id = indexer
            .metadata()
            .schema
            .get_attribute(&kw!(:name))
            .unwrap()
            .0;

        // First tx: assert name="alice" for a new entity
        let tx1 = TxKey {
            tx_id: 1,
            system_time: st_from_unix_epoch(100),
        };
        let tx_ops = vec![TxOp::Add {
            entity: "alice".into(),
            attribute: kw!(:name),
            value: "alice".into(),
        }];
        indexer.transact_tx(tx1, tx_ops).await?;

        // Find auto-assigned entity ID
        let user_base = crate::partition::USER_PARTITION as i64 * (1i64 << 42);
        let mut entity_id: i64 = 0;
        let mut iter = slate
            .scan_prefix_with_options(&[codec::EAV], &ScanOptions::default())
            .await?;
        while let Some(kv) = iter.next().await? {
            let (eid, _, _, _, _) = eav_key_to_parts(kv.key.clone())?;
            if let DataType::Long(id) = eid {
                if id >= user_base {
                    entity_id = id;
                    break;
                }
            }
        }

        // Second tx: assert same name="alice" — datom should be dropped entirely
        let tx2 = TxKey {
            tx_id: 2,
            system_time: st_from_unix_epoch(200),
        };
        indexer
            .transact_tx(
                tx2,
                vec![TxOp::Add {
                    entity: EntityRef::Id(entity_id),
                    attribute: kw!(:name),
                    value: "alice".into(),
                }],
            )
            .await?;

        // Count EAV entries for entity, name attr — should be 1 ADD, 0 RETRACTs
        let mut iter = slate
            .scan_prefix_with_options(&[codec::EAV], &ScanOptions::default())
            .await?;
        let mut add_count = 0;
        let mut retract_count = 0;
        while let Some(kv) = iter.next().await? {
            let (eid, attribute, _value, _ts, op) = eav_key_to_parts(kv.key)?;
            if eid != DataType::Long(entity_id) || attribute != name_id {
                continue;
            }
            match op {
                codec::ADD => add_count += 1,
                codec::RETRACT => retract_count += 1,
                _ => {}
            }
        }
        assert_eq!(add_count, 1, "Expected 1 ADD entry (second tx dropped)");
        assert_eq!(retract_count, 0, "Expected no RETRACT entries");

        Ok(())
    }

    #[tokio::test]
    async fn test_schema_immutability_rejected_in_pipeline() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await);
        let mut indexer = bootstrapped_indexer(slate).await;
        let (name_id, attr) = indexer
            .metadata()
            .schema
            .get_attribute(&kw!(:name))
            .unwrap();
        assert_eq!(attr.value_type, crate::schema::ValueType::String);

        let tx = TxKey {
            tx_id: 1,
            system_time: st_from_unix_epoch(100),
        };
        let err = indexer
            .transact_tx(
                tx,
                vec![TxOp::put(vec![
                    (kw!(:db/id), DataType::Long(name_id)),
                    (kw!(:db/ident), DataType::Keyword(kw!(:name))),
                    (kw!(:db/valueType), DataType::Long(DB_TYPE_LONG)),
                    (kw!(:db/cardinality), DataType::Long(DB_CARDINALITY_ONE)),
                ])],
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("Cannot modify schema entity"));
        let (_name_id, attr) = indexer
            .metadata()
            .schema
            .get_attribute(&kw!(:name))
            .unwrap();
        assert_eq!(attr.value_type, crate::schema::ValueType::String);

        Ok(())
    }

    /// Find the first user-partition entity ID by scanning the EAV index.
    async fn find_first_user_entity(slate: &Arc<slatedb::Db>) -> Result<i64, Error> {
        let user_base = crate::partition::USER_PARTITION as i64 * (1i64 << 42);
        let mut iter = slate
            .scan_prefix_with_options(&[codec::EAV], &ScanOptions::default())
            .await?;
        while let Some(kv) = iter.next().await? {
            let (eid, _, _, _, _) = eav_key_to_parts(kv.key.clone())?;
            if let DataType::Long(id) = eid {
                if id >= user_base {
                    return Ok(id);
                }
            }
        }
        panic!("No user-partition entity found in EAV index");
    }

    #[tokio::test]
    async fn test_cardinality_many_no_retract() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await.db);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;

        // First tx: assert tags="rust" for a new entity (auto-assigned ID)
        let tx1 = TxKey {
            tx_id: 1,
            system_time: st_from_unix_epoch(100),
        };
        let tx_ops1 = vec![TxOp::Add {
            entity: "tagged".into(),
            attribute: kw!(:tags),
            value: "rust".into(),
        }];
        indexer.transact_tx(tx1, tx_ops1).await?;

        // Discover auto-assigned entity ID
        let entity_id = find_first_user_entity(&slate).await?;
        let tags_id = indexer
            .metadata()
            .schema
            .get_attribute(&kw!(:tags))
            .unwrap()
            .0;

        // Second tx: assert tags="database" for same entity — should NOT retract "rust"
        let tx2 = TxKey {
            tx_id: 2,
            system_time: st_from_unix_epoch(200),
        };
        let tx_ops2 = vec![TxOp::Add {
            entity: EntityRef::Id(entity_id),
            attribute: kw!(:tags),
            value: "database".into(),
        }];
        indexer.transact_tx(tx2, tx_ops2).await?;

        // Scan EAV for entity, tags attr — expect 2 ADDs, 0 RETRACTs
        let mut iter = slate
            .scan_prefix_with_options(&[codec::EAV], &ScanOptions::default())
            .await?;
        let mut add_count = 0;
        let mut retract_count = 0;
        while let Some(kv) = iter.next().await? {
            let (eid, attribute, _value, _ts, op) = eav_key_to_parts(kv.key)?;
            if eid != DataType::Long(entity_id) || attribute != tags_id {
                continue;
            }
            match op {
                codec::ADD => add_count += 1,
                codec::RETRACT => retract_count += 1,
                _ => {}
            }
        }
        assert_eq!(add_count, 2, "Expected 2 ADD entries for cardinality-many");
        assert_eq!(
            retract_count, 0,
            "Expected no RETRACT entries for cardinality-many"
        );

        Ok(())
    }

    // NOTE: Currently cardinality-many has bag semantics (duplicate values are stored).
    // Datomic uses set semantics where asserting the same value twice is a no-op.
    // We may want to switch to set semantics in the future.
    #[tokio::test]
    async fn test_cardinality_many_same_value() -> Result<(), Error> {
        let slate = Arc::new(in_memory_slate().await.db);
        let mut indexer = bootstrapped_indexer(slate.clone()).await;

        // First tx: assert tags="rust" for a new entity (auto-assigned ID)
        let tx1 = TxKey {
            tx_id: 1,
            system_time: st_from_unix_epoch(100),
        };
        let tx_ops1 = vec![TxOp::Add {
            entity: "tagged".into(),
            attribute: kw!(:tags),
            value: "rust".into(),
        }];
        indexer.transact_tx(tx1, tx_ops1).await?;

        // Discover auto-assigned entity ID
        let entity_id = find_first_user_entity(&slate).await?;
        let tags_id = indexer
            .metadata()
            .schema
            .get_attribute(&kw!(:tags))
            .unwrap()
            .0;

        // Second tx: assert same tags="rust" again — should still be written (unlike card-one)
        let tx2 = TxKey {
            tx_id: 2,
            system_time: st_from_unix_epoch(200),
        };
        let tx_ops2 = vec![TxOp::Add {
            entity: EntityRef::Id(entity_id),
            attribute: kw!(:tags),
            value: "rust".into(),
        }];
        indexer.transact_tx(tx2, tx_ops2).await?;

        // Scan EAV for entity, tags attr — expect 2 ADDs (both written)
        let mut iter = slate
            .scan_prefix_with_options(&[codec::EAV], &ScanOptions::default())
            .await?;
        let mut add_count = 0;
        while let Some(kv) = iter.next().await? {
            let (eid, attribute, _value, _ts, op) = eav_key_to_parts(kv.key)?;
            if eid != DataType::Long(entity_id) || attribute != tags_id {
                continue;
            }
            if op == codec::ADD {
                add_count += 1;
            }
        }
        assert_eq!(
            add_count, 2,
            "Expected 2 ADD entries for same value on cardinality-many"
        );

        Ok(())
    }
}
