use anyhow::{bail, Error, Result};
use bytes::Bytes;
use log::{error, trace, warn};
use slatedb::Db;
use slatedb::DbReadOps;
use slatedb::WriteBatch;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::broadcast;

use edn::kw;

use crate::codec::{
    self, decode_datatype, decode_i64, encode_datatype, encode_i64, encode_i64_bytes, Encode,
};
use crate::log::{Record, Subscriber};
use crate::metadata::Metadata;
use crate::ops::DataType;
use crate::ops::{Datom, DatomOp, Entid, TxOp};
use crate::partition::{partition_entity_prefix, TX_PARTITION};
use crate::schema::{Schema, DB_TX_ABORTED, DB_TX_COMMITTED};
use crate::slate::{DEFAULT_SCAN_OPTIONS, DEFAULT_WRITE_OPTIONS};
use crate::tempids;
use crate::transaction::{TxBasis, TxKey};
use crate::tx;
use crate::util::concat_bytes;

type TxCompletionMessage = (TxKey, Option<TxBasis>, Result<(), Arc<Error>>);

pub struct Indexer {
    slatedb: Arc<Db>,
    metadata: Metadata,
    latest_indexed_tx: Option<TxBasis>,
    tx_completion_sender: broadcast::Sender<TxCompletionMessage>,
}

/// Write index entries for datoms into a SlateDB WriteBatch.
pub(crate) fn write_index_entries(
    batch: &mut WriteBatch,
    datoms: &[Datom],
    schema: &Schema,
    tx_eid: i64,
) -> Result<(), Error> {
    let tx_eid_bytes = encode_i64_bytes(tx_eid);
    let mut key_buf: Vec<u8> = Vec::with_capacity(64);
    let mut value_buf: Vec<u8> = Vec::with_capacity(32);
    // DataType::Long encodes as 1 type-tag byte + 8 big-endian bytes.
    let mut entity_buf: Vec<u8> = Vec::with_capacity(9);

    for datom in datoms {
        let (attribute_id, attribute) = schema
            .get_attribute(&datom.attribute)
            .ok_or_else(|| anyhow::anyhow!("Unknown attribute: {}", datom.attribute))?;
        let attr_bytes = encode_i64_bytes(attribute_id);
        // Entity IDs use value-position encoding (DataType::Long) so they can
        // share the value slot in EAV/AVE/AEV keys.
        entity_buf.clear();
        encode_datatype(&DataType::Long(datom.entity), &mut entity_buf);
        let entity_bytes = entity_buf.as_slice();

        value_buf.clear();
        encode_datatype(&datom.value, &mut value_buf);
        let value = value_buf.as_slice();

        let op_byte = match datom.op {
            DatomOp::Assert => codec::ADD,
            DatomOp::Retract => codec::RETRACT,
        };

        // EAV
        key_buf.clear();
        key_buf.push(codec::EAV);
        key_buf.extend_from_slice(entity_bytes);
        key_buf.extend_from_slice(&attr_bytes);
        key_buf.extend_from_slice(value);
        key_buf.extend_from_slice(&tx_eid_bytes);
        key_buf.push(op_byte);
        batch.put(&key_buf, b"");

        // AVE
        key_buf.clear();
        key_buf.push(codec::AVE);
        key_buf.extend_from_slice(&attr_bytes);
        key_buf.extend_from_slice(value);
        key_buf.extend_from_slice(entity_bytes);
        key_buf.extend_from_slice(&tx_eid_bytes);
        key_buf.push(op_byte);
        batch.put(&key_buf, b"");

        // AEV
        key_buf.clear();
        key_buf.push(codec::AEV);
        key_buf.extend_from_slice(&attr_bytes);
        key_buf.extend_from_slice(entity_bytes);
        key_buf.extend_from_slice(value);
        key_buf.extend_from_slice(&tx_eid_bytes);
        key_buf.push(op_byte);
        batch.put(&key_buf, b"");

        // VAE is a unique-only index used for uniqueness checks, lookup refs,
        // and :db.unique/identity upsert resolution.
        if attribute.unique.is_some() {
            key_buf.clear();
            key_buf.push(codec::VAE);
            key_buf.extend_from_slice(value);
            key_buf.extend_from_slice(&attr_bytes);
            key_buf.extend_from_slice(entity_bytes);
            key_buf.extend_from_slice(&tx_eid_bytes);
            key_buf.push(op_byte);
            batch.put(&key_buf, b"");
        }

        // AE and AV are atemporal, purely additive indices.
        // Retractions are not written to AE/AV.
        if datom.op == DatomOp::Assert {
            key_buf.clear();
            key_buf.push(codec::AE);
            key_buf.extend_from_slice(&attr_bytes);
            key_buf.extend_from_slice(entity_bytes);
            batch.put(&key_buf, b"");

            key_buf.clear();
            key_buf.push(codec::AV);
            key_buf.extend_from_slice(&attr_bytes);
            key_buf.extend_from_slice(value);
            batch.put(&key_buf, b"");
        }
    }

    Ok(())
}

/// Scan EAV entries for TX_PARTITION entities and return (tx_eid, TxKey) for the latest tx.
///
/// Uses the high bits of TX_PARTITION entity IDs to build a targeted EAV prefix,
/// restricting the scan to TX_PARTITION entities. With descending entity encoding,
/// the first TX_PARTITION entity is the latest.
///
/// Collects tx_eid from the entity position, tx_id from `db/txId` value, and
/// system_time from `db/txInstant` value.
///
/// Errors if no TX_PARTITION entity exists (an initialized DB always has the
/// bootstrap tx) or if the latest tx entity is missing `:db/txId` or
/// `:db/txInstant`.
pub async fn latest_tx_basis_from_sdb<D>(sdb: &D) -> Result<TxBasis>
where
    D: DbReadOps + Sync,
{
    let eav_tx_prefix = concat_bytes(&[&[codec::EAV], &partition_entity_prefix(TX_PARTITION)]);
    let mut iter = sdb
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
        (Some(eid), Some(tid), Some(st)) => Ok(TxBasis {
            tx_key: TxKey {
                tx_id: tid,
                system_time: st,
            },
            tx_eid: eid,
        }),
        (None, _, _) => bail!("TX_PARTITION is empty; database not initialized"),
        (Some(eid), tid, st) => {
            bail!("Tx entity {eid} missing required attributes (tx_id={tid:?}, system_time={st:?})")
        }
    }
}

/// Build datoms for a first-class transaction entity in TX_PARTITION.
/// The `tx_eid` is the pre-allocated entity ID for this transaction.
pub(crate) fn build_tx_entity_datoms(
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
            attribute: kw!(:db/txError),
            value: DataType::String(err),
            op: DatomOp::Assert,
        });
    }
    datoms
}

impl Indexer {
    pub fn new(slatedb: Arc<Db>, metadata: Metadata, latest_indexed_tx: Option<TxBasis>) -> Self {
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

    pub(crate) fn latest_tx_basis(&self) -> Option<TxBasis> {
        self.latest_indexed_tx
    }

    /// Transact a set of operations, automatically retracting old values for
    /// cardinality:one attributes when a new value is asserted.
    ///
    /// Pipeline reads go directly against the DB (the indexer is the only
    /// writer, so they see the latest committed state). Pipeline writes are
    /// buffered in a `WriteBatch` and committed atomically at the end.
    pub async fn transact_tx(
        &mut self,
        tx_key: TxKey,
        tx_ops: Vec<TxOp>,
    ) -> Result<TxBasis, Error> {
        match self.transact_tx_inner(tx_key, tx_ops).await {
            Ok(basis) => Ok(basis),
            Err(e) => match self.write_aborted_tx(tx_key, e.to_string()).await {
                // semantic error
                Ok(basis) => {
                    let err = Arc::new(e);
                    let _ = self
                        .tx_completion_sender
                        .send((tx_key, Some(basis), Err(err.clone())));
                    Ok(basis)
                }
                // technical error
                Err(abort_err) => {
                    let err = Arc::new(anyhow::anyhow!(
                        "Failed to write aborted tx entity for {}: {:#}; original transaction error: {:#}",
                        tx_key.tx_id,
                        abort_err,
                        e
                    ));
                    error!("{:#}", err);
                    let _ = self
                        .tx_completion_sender
                        .send((tx_key, None, Err(err.clone())));
                    Err(anyhow::anyhow!("{:#}", err))
                }
            },
        }
    }

    async fn transact_tx_inner(
        &mut self,
        tx_key: TxKey,
        tx_ops: Vec<TxOp>,
    ) -> Result<TxBasis, Error> {
        // 1. Clone PartitionMap + allocate tx entity
        let mut pending_pm = self.metadata.partition_map.clone();
        let tx_eid = pending_pm.allocate_entid(TX_PARTITION);

        // 2. Expand TxOps to DatomExpanded (resolves idents, keeps lookup refs + tempids)
        let expanded = tx::expand_tx_ops(&tx_ops, &self.metadata.schema)?;

        // 3. Resolve lookup refs via AVE index → DatomWithTempids
        let with_tempids =
            tx::resolve_lookup_refs(expanded, &self.metadata.schema, &self.slatedb).await?;

        // 4. Resolve tempids, including identity upserts, then build tx entity datoms
        let mut datoms = tempids::resolve_tempids(
            with_tempids,
            &self.metadata.schema,
            &self.slatedb,
            &mut pending_pm,
        )
        .await?;
        datoms.extend(build_tx_entity_datoms(tx_eid, tx_key, true, None));

        // 5. Finalize datoms (card-one rewrite) against current storage state
        let datoms = self.finalize_datoms_for_commit(datoms).await?;

        // 6. Unique + general validation
        self.validate_unique_constraints(&datoms).await?;
        let validation = self.metadata.schema.validate_datoms(&datoms)?;

        // 7. Write indices + commit
        let mut batch = WriteBatch::new();
        write_index_entries(&mut batch, &datoms, &self.metadata.schema, tx_eid)?;
        self.slatedb
            .write_with_options(batch, &DEFAULT_WRITE_OPTIONS)
            .await?;

        // 8. Apply on success only
        self.metadata.partition_map = pending_pm;
        if validation.schema_changes_detected {
            let schema_update = self.metadata.schema.prepare_schema_update(&datoms)?;
            if !schema_update.is_empty() {
                self.metadata.schema.apply_schema_update(schema_update);
                self.metadata.advance_generation();
            }
        }

        // Update latest indexed tx and broadcast completion
        let basis = TxBasis { tx_key, tx_eid };
        self.latest_indexed_tx = Some(basis);

        if let Err(e) = self
            .tx_completion_sender
            .send((tx_key, Some(basis), Ok(())))
        {
            trace!(
                "No receivers for indexed transaction {}: {}",
                tx_key.tx_id,
                e
            );
        }

        Ok(basis)
    }

    async fn finalize_datoms_for_commit(&self, datoms: Vec<Datom>) -> Result<Vec<Datom>, Error> {
        // Batch resolve all cardinality one assertion old values via an EAV scan
        // If the old value equals the new value, drop the datom (no-op).
        // If the old value differs, add a Retract datom for the old value.
        // Collect into HashSet to deduplicate explicit + auto-generated retractions.
        let mut eav_prefixes: BTreeMap<Vec<u8>, (Entid, Entid)> = BTreeMap::new();
        for datom in &datoms {
            if datom.op != DatomOp::Assert {
                continue;
            }

            // TODO: this attribute lookup is done twice. Here and in the final loop. Refactor
            let (attribute_id, attr) = self
                .metadata
                .schema
                .get_attribute(&datom.attribute)
                .ok_or_else(|| anyhow::anyhow!("Unknown attribute: {}", datom.attribute))?;

            if attr.multival || !self.metadata.partition_map.contains_entid(datom.entity) {
                continue;
            }

            let entity_id_bytes = DataType::Long(datom.entity).encode();
            let attr_id_bytes = encode_i64_bytes(attribute_id);
            let eav_prefix = concat_bytes(&[&[codec::EAV], &entity_id_bytes, &attr_id_bytes]);
            eav_prefixes.insert(eav_prefix, (datom.entity, attribute_id));
        }

        // uses entity entid and attribute entid
        let mut old_values: HashMap<(Entid, Entid), DataType> =
            HashMap::with_capacity(eav_prefixes.len());
        if let Some(first_prefix) = eav_prefixes.keys().next() {
            let mut iter = self
                .slatedb
                .scan_with_options(
                    first_prefix.clone()..vec![codec::EAV_END],
                    &DEFAULT_SCAN_OPTIONS,
                )
                .await?;

            let mut last_returned = Bytes::new();
            for (eav_prefix, (entity, attribute_id)) in &eav_prefixes {
                if last_returned.as_ref() < eav_prefix.as_slice() {
                    iter.seek(eav_prefix).await?;
                    let Some(kv) = iter.next().await? else {
                        // Prefixes are sorted, so no later prefix can match once the range is exhausted.
                        break;
                    };
                    last_returned = kv.key;
                }

                if last_returned.starts_with(eav_prefix) {
                    assert!(
                        last_returned.len() >= codec::TX_EID_OP_SUFFIX,
                        "Key too short ({} bytes) to contain tx_eid + op suffix",
                        last_returned.len()
                    );
                    if last_returned[last_returned.len() - 1] != codec::RETRACT {
                        let value_bytes = &last_returned
                            [eav_prefix.len()..last_returned.len() - codec::TX_EID_OP_SUFFIX];
                        let mut cursor = value_bytes;
                        old_values.insert((*entity, *attribute_id), decode_datatype(&mut cursor)?);
                    }
                }
            }
        }

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

            if let Some(old_value) = old_values.get(&(datom.entity, attribute_id)) {
                if old_value == &datom.value {
                    continue;
                }
                resolved_datoms.insert(Datom {
                    entity: datom.entity,
                    attribute: datom.attribute.clone(),
                    value: old_value.clone(),
                    op: DatomOp::Retract,
                });
            }
            resolved_datoms.insert(datom);
        }

        Ok(resolved_datoms.into_iter().collect())
    }

    async fn validate_unique_constraints(&self, datoms: &[Datom]) -> Result<(), Error> {
        let mut retractions: HashSet<(Entid, Entid, DataType)> = HashSet::new();
        for datom in datoms {
            if datom.op != DatomOp::Retract {
                continue;
            }
            let Some((attr_eid, attr)) = self.metadata.schema.get_attribute(&datom.attribute)
            else {
                continue;
            };
            if attr.unique.is_some() {
                retractions.insert((datom.entity, attr_eid, datom.value.clone()));
            }
        }

        let mut asserted_unique: HashMap<(Entid, DataType), Entid> = HashMap::new();
        let mut to_check: Vec<(&Datom, Entid)> = Vec::new();
        for datom in datoms {
            if datom.op != DatomOp::Assert {
                continue;
            }
            let (attr_eid, attr) = self
                .metadata
                .schema
                .get_attribute(&datom.attribute)
                .ok_or_else(|| anyhow::anyhow!("Unknown attribute: {}", datom.attribute))?;
            if attr.unique.is_none() {
                continue;
            }

            let key = (attr_eid, datom.value.clone());
            if let Some(existing_entity) = asserted_unique.insert(key, datom.entity) {
                if existing_entity != datom.entity {
                    return Err(anyhow::anyhow!(
                        "Unique constraint violation for attribute {} value {:?}: entities {} and {}",
                        datom.attribute,
                        datom.value,
                        existing_entity,
                        datom.entity
                    ));
                }
            }
            // We could already pass to some hashed version here for deduplication.
            to_check.push((datom, attr_eid));
        }

        let lookups: Vec<(Entid, DataType)> = to_check
            .iter()
            .map(|(d, a)| (*a, d.value.clone()))
            .collect();
        let resolved = tx::batch_lookup_unique_eids(&self.slatedb, &lookups).await?;

        for (datom, attr_eid) in to_check {
            if let Some(&owner) = resolved.get(&(attr_eid, datom.value.clone())) {
                if owner != datom.entity
                    && !retractions.contains(&(owner, attr_eid, datom.value.clone()))
                {
                    return Err(anyhow::anyhow!(
                        "Unique constraint violation for attribute {} value {:?}: entity {} already owns it",
                        datom.attribute,
                        datom.value,
                        owner
                    ));
                }
            }
        }

        Ok(())
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
    /// let tx_key = log.append_tx(data).await;
    /// waiter.await_tx(tx_key).await?;
    /// ```
    pub(crate) fn tx_waiter(&self) -> TxWaiter {
        TxWaiter {
            latest_tx: self.latest_indexed_tx,
            rx: self.tx_completion_sender.subscribe(),
        }
    }

    /// Write an aborted transaction entity (no user data) when transact_tx fails.
    async fn write_aborted_tx(&mut self, tx_key: TxKey, error: String) -> Result<TxBasis, Error> {
        let mut pending_pm = self.metadata.partition_map.clone();
        let tx_eid = pending_pm.allocate_entid(TX_PARTITION);
        let datoms = build_tx_entity_datoms(tx_eid, tx_key, false, Some(error));
        let mut batch = WriteBatch::new();
        write_index_entries(&mut batch, &datoms, &self.metadata.schema, tx_eid)?;
        self.slatedb
            .write_with_options(batch, &DEFAULT_WRITE_OPTIONS)
            .await?;
        // No need to advance generation for aborted transactions
        self.metadata.partition_map = pending_pm;
        let basis = TxBasis { tx_key, tx_eid };
        self.latest_indexed_tx = Some(basis);
        Ok(basis)
    }
}

/// A pre-subscribed handle for waiting on transaction completion.
///
/// Created by `Indexer::tx_waiter()`. Holds a broadcast receiver so that
/// no messages are missed between subscription and the actual wait.
pub(crate) struct TxWaiter {
    latest_tx: Option<TxBasis>,
    rx: broadcast::Receiver<TxCompletionMessage>,
}

pub(crate) struct TxCompletion {
    pub basis: Option<TxBasis>,
    pub result: Result<(), Arc<anyhow::Error>>,
}

impl TxWaiter {
    /// Wait until `tx_key` has been indexed. Returns the indexed basis and status,
    /// `Err` on abort or if the indexer shuts down.
    pub async fn await_tx(mut self, tx_key: TxKey) -> Result<TxCompletion, Error> {
        // Fast path: already indexed at subscription time
        if let Some(latest_basis) = self.latest_tx {
            if tx_key == latest_basis.tx_key {
                return Ok(TxCompletion {
                    basis: Some(latest_basis),
                    // TODO: We are not filling in the error here if there was a semantic error.
                    result: Ok(()),
                });
            }
            if tx_key < latest_basis.tx_key {
                return Ok(TxCompletion {
                    basis: None,
                    result: Ok(()),
                });
            }
        }

        // TODO: The >= check can return a different tx's result if the
        // broadcast channel lags. Will be revisited when the log is removed and we
        // ingest directly into Slate.
        loop {
            match self.rx.recv().await {
                Ok((completed_tx_key, completed_basis, result)) => {
                    if completed_tx_key >= tx_key {
                        return Ok(TxCompletion {
                            basis: completed_basis,
                            result,
                        });
                    }
                    // Keep waiting for higher tx_id
                }
                Err(broadcast::error::RecvError::Lagged(_count)) => {
                    // TODO(#282): recover exact tx outcome or fail instead of risking a later tx's result.
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

    /// Wait until indexing has reached `tx_key`, regardless of whether that
    /// transaction committed or aborted.
    pub async fn await_indexed(mut self, tx_key: TxKey) -> Result<(), Error> {
        if let Some(latest_basis) = self.latest_tx {
            if tx_key.tx_id <= latest_basis.tx_key.tx_id {
                return Ok(());
            }
        }

        loop {
            match self.rx.recv().await {
                Ok((completed_tx_key, _completed_basis, _result)) => {
                    if completed_tx_key.tx_id >= tx_key.tx_id {
                        return Ok(());
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_count)) => continue,
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
                    .send((record.tx_key, None, Err(Arc::new(err))));
                return;
            }
        };
        // TODO: Deal with proper error typing and escalation in case of non-recoverable errors. See #118.
        if let Err(e) = self.transact_tx(record.tx_key, tx_ops).await {
            error!("Transaction {} failed: {}", record.tx_key.tx_id, e);
            // TODO: an error in tx_transact_inner is currently always being treated as an aborted transaction.
            // There should likely be some seperation via types of semantic vs non-recoverable errors to
            // allow for proper escalation.
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

pub fn vae_key_to_parts(key: Bytes) -> Result<(DataType, i64, DataType, i64, u8), Error> {
    let (data, tx_eid, op) = strip_temporal_key(key.as_ref(), codec::VAE, "VAE")?;
    let mut cursor = data;
    let value = decode_datatype(&mut cursor)?;
    let attribute = decode_i64(&mut cursor)?;
    let entity_id = decode_datatype(&mut cursor)?;
    Ok((value, attribute, entity_id, tx_eid, op))
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
    use crate::query::execute_query;
    use crate::schema::{test_schema_tx, DB_CARDINALITY_ONE, DB_TYPE_LONG};
    use crate::slate::{in_memory_slate, SlateComponents};
    use edn::query::ParsedQuery;

    /// Create an indexer with bootstrap schema and test attributes already transacted.
    /// Uses init_db for bootstrap, then transacts test schema via the indexer.
    /// Returns the indexer ready for test data at tx_id=1+.
    async fn bootstrapped_indexer(slate: &SlateComponents) -> Indexer {
        let metadata = crate::bootstrap::init_db(slate).await.unwrap();
        let mut indexer = Indexer::new(slate.db.clone(), metadata, None);
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
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let mut indexer = bootstrapped_indexer(&components).await;
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
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let mut indexer = bootstrapped_indexer(&components).await;
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
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let mut indexer = bootstrapped_indexer(&components).await;
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
    async fn test_latest_tx_basis_after_transact() -> Result<(), Error> {
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let mut indexer = bootstrapped_indexer(&components).await;
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

        let latest = latest_tx_basis_from_sdb(slate.as_ref()).await?;
        assert_eq!(latest.tx_key.tx_id, 42);
        assert_eq!(latest.tx_key.system_time, st_from_unix_epoch(1000));
        assert!(latest.tx_eid > 0, "tx_eid should be a valid entity ID");
        Ok(())
    }

    #[tokio::test]
    async fn test_latest_tx_basis_highest_wins() -> Result<(), Error> {
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let mut indexer = bootstrapped_indexer(&components).await;

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

        let latest = latest_tx_basis_from_sdb(slate.as_ref()).await?;
        assert_eq!(latest.tx_key.tx_id, 3, "Should return highest tx_id");
        assert_eq!(latest.tx_key.system_time, st_from_unix_epoch(300));
        Ok(())
    }

    #[tokio::test]
    async fn test_await_tx_already_indexed() -> Result<(), Error> {
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let mut indexer = bootstrapped_indexer(&components).await;

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

        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let indexer = Arc::new(RwLock::new(bootstrapped_indexer(&components).await));

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
        let slate = in_memory_slate().await.db;
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
    async fn test_deserialization_failure_notifies_without_basis() -> Result<(), Error> {
        let components = in_memory_slate().await;
        let mut indexer = bootstrapped_indexer(&components).await;
        let tx_key = TxKey {
            tx_id: 1,
            system_time: st_from_unix_epoch(2),
        };
        let waiter = indexer.tx_waiter();

        indexer
            .accept(Record {
                tx_key,
                record: vec![0xff],
            })
            .await;

        let completion = waiter.await_tx(tx_key).await?;
        assert_eq!(completion.basis, None);
        assert!(completion
            .result
            .unwrap_err()
            .to_string()
            .contains("Failed to deserialize TxOps"));

        Ok(())
    }

    #[tokio::test]
    async fn test_transact_failure_writes_aborted_tx_and_notifies_with_basis() -> Result<(), Error>
    {
        let components = in_memory_slate().await;
        let mut indexer = bootstrapped_indexer(&components).await;
        let tx_key = TxKey {
            tx_id: 1,
            system_time: st_from_unix_epoch(2),
        };
        let waiter = indexer.tx_waiter();

        let basis = indexer
            .transact_tx(
                tx_key,
                vec![TxOp::Add {
                    entity: "e".into(),
                    attribute: kw!(:nonexistent),
                    value: "x".into(),
                }],
            )
            .await?;

        assert_eq!(basis.tx_key, tx_key);
        let completion = waiter.await_tx(tx_key).await?;
        assert_eq!(completion.basis.map(|basis| basis.tx_key), Some(tx_key));
        assert!(completion
            .result
            .unwrap_err()
            .to_string()
            .contains("Unknown attribute"));

        Ok(())
    }

    #[tokio::test]
    async fn test_transact_node_failure_notifies_without_basis() -> Result<(), Error> {
        let components = in_memory_slate().await;
        let mut indexer = Indexer::new(
            components.db.clone(),
            Metadata::new(Schema::default(), crate::metadata::PartitionMap::new()),
            None,
        );
        let tx_key = TxKey {
            tx_id: 1,
            system_time: st_from_unix_epoch(2),
        };
        let waiter = indexer.tx_waiter();

        let err = indexer
            .transact_tx(
                tx_key,
                vec![TxOp::Add {
                    entity: "e".into(),
                    attribute: kw!(:nonexistent),
                    value: "x".into(),
                }],
            )
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("Failed to write aborted tx entity"));
        let completion = waiter.await_tx(tx_key).await?;
        assert_eq!(completion.basis, None);
        assert!(completion
            .result
            .unwrap_err()
            .to_string()
            .contains("Failed to write aborted tx entity"));

        Ok(())
    }

    #[tokio::test]
    async fn test_await_tx_ordering() -> Result<(), Error> {
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let mut indexer = bootstrapped_indexer(&components).await;

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

        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let indexer = Arc::new(RwLock::new(bootstrapped_indexer(&components).await));

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

    // TODO(#96): replace explicit entity IDs with query-based verification
    #[tokio::test]
    async fn test_retract_on_overwrite() -> Result<(), Error> {
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let mut indexer = bootstrapped_indexer(&components).await;
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
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let mut indexer = bootstrapped_indexer(&components).await;
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
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let mut indexer = bootstrapped_indexer(&components).await;
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
        let waiter = indexer.tx_waiter();
        let basis = indexer
            .transact_tx(
                tx,
                vec![TxOp::put(vec![
                    (kw!(:db/id), DataType::Long(name_id)),
                    (kw!(:db/ident), DataType::Keyword(kw!(:name))),
                    (kw!(:db/valueType), DataType::Long(DB_TYPE_LONG)),
                    (kw!(:db/cardinality), DataType::Long(DB_CARDINALITY_ONE)),
                ])],
            )
            .await?;

        assert_eq!(basis.tx_key, tx);
        let completion = waiter.await_tx(tx).await?;
        assert_eq!(completion.basis.map(|basis| basis.tx_key), Some(tx));
        assert!(completion
            .result
            .unwrap_err()
            .to_string()
            .contains("Cannot modify schema entity"));
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

    // TODO move this test to a proper node level test once we support historic queries
    #[tokio::test]
    async fn test_retract_on_multiple_overwrites() -> Result<(), Error> {
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let mut indexer = bootstrapped_indexer(&components).await;
        let name_id = indexer
            .metadata()
            .schema
            .get_attribute(&kw!(:name))
            .unwrap()
            .0;

        let tx1 = TxKey {
            tx_id: 1,
            system_time: st_from_unix_epoch(100),
        };
        indexer
            .transact_tx(
                tx1,
                vec![
                    TxOp::Add {
                        entity: "alice".into(),
                        attribute: kw!(:name),
                        value: "alice".into(),
                    },
                    TxOp::Add {
                        entity: "bob".into(),
                        attribute: kw!(:name),
                        value: "bob".into(),
                    },
                ],
            )
            .await?;

        let query: ParsedQuery = r#"[:find ?e ?name :where [?e :name ?name]]"#.parse()?;
        let sdb = slate.clone();
        let handle = tokio::runtime::Handle::current();
        let ident_map = indexer.metadata().schema.ident_map.clone();
        let range_stats = components.range_stats.clone();
        let rows = tokio::task::spawn_blocking(move || {
            execute_query(&query, &[], sdb, handle, &ident_map, i64::MAX, range_stats)
        })
        .await??;

        let mut entities = std::collections::HashMap::new();
        for row in rows {
            if let [DataType::Long(eid), DataType::String(name)] = row.as_slice() {
                if name == "alice" || name == "bob" {
                    entities.insert(name.clone(), *eid);
                }
            }
        }
        let alice_eid = *entities.get("alice").expect("alice entity should exist");
        let bob_eid = *entities.get("bob").expect("bob entity should exist");

        let tx2 = TxKey {
            tx_id: 2,
            system_time: st_from_unix_epoch(200),
        };
        indexer
            .transact_tx(
                tx2,
                vec![
                    TxOp::Add {
                        entity: EntityRef::Id(alice_eid),
                        attribute: kw!(:name),
                        value: "alicia".into(),
                    },
                    TxOp::Add {
                        entity: EntityRef::Id(bob_eid),
                        attribute: kw!(:name),
                        value: "robert".into(),
                    },
                ],
            )
            .await?;

        let mut seen = std::collections::HashSet::new();
        let mut iter = slate
            .scan_prefix_with_options(&[codec::EAV], &ScanOptions::default())
            .await?;
        while let Some(kv) = iter.next().await? {
            let (eid, attribute, value, _ts, op) = eav_key_to_parts(kv.key)?;
            if attribute != name_id {
                continue;
            }
            let DataType::Long(entity_id) = eid else {
                continue;
            };
            let DataType::String(name) = value else {
                continue;
            };
            if entity_id == alice_eid || entity_id == bob_eid {
                seen.insert((entity_id, name, op));
            }
        }

        assert!(seen.contains(&(alice_eid, "alice".to_string(), codec::ADD)));
        assert!(seen.contains(&(alice_eid, "alice".to_string(), codec::RETRACT)));
        assert!(seen.contains(&(alice_eid, "alicia".to_string(), codec::ADD)));
        assert!(seen.contains(&(bob_eid, "bob".to_string(), codec::ADD)));
        assert!(seen.contains(&(bob_eid, "bob".to_string(), codec::RETRACT)));
        assert!(seen.contains(&(bob_eid, "robert".to_string(), codec::ADD)));

        Ok(())
    }

    // Regression: a batched-scan prefix with no entries must not swallow the
    // next prefix's first key. Sorting by attribute_id ensures the missing
    // prefix sorts first so the seek-past-missing path is exercised.
    #[tokio::test]
    async fn test_batch_old_value_scan_handles_missing_prefix() -> Result<(), Error> {
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let mut indexer = bootstrapped_indexer(&components).await;

        let mut attrs = [
            (
                kw!(:name),
                DataType::String("old-name".to_string()),
                DataType::String("new-name".to_string()),
                DataType::String("missing-name".to_string()),
            ),
            (
                kw!(:age),
                DataType::Long(1),
                DataType::Long(2),
                DataType::Long(3),
            ),
            (
                kw!(:email),
                DataType::String("old@example.com".to_string()),
                DataType::String("new@example.com".to_string()),
                DataType::String("missing@example.com".to_string()),
            ),
        ];
        attrs.sort_by_key(|(attr, _, _, _)| {
            indexer.metadata().schema.get_attribute(attr).unwrap().0
        });
        let (missing_attr, _, _, missing_value) = attrs.first().unwrap().clone();
        let (existing_attr, old_value, new_value, _) = attrs.last().unwrap().clone();

        indexer
            .transact_tx(
                TxKey {
                    tx_id: 1,
                    system_time: st_from_unix_epoch(100),
                },
                vec![TxOp::Add {
                    entity: "entity".into(),
                    attribute: existing_attr.clone(),
                    value: old_value.clone(),
                }],
            )
            .await?;

        let entity_id = find_first_user_entity(&slate).await?;

        indexer
            .transact_tx(
                TxKey {
                    tx_id: 2,
                    system_time: st_from_unix_epoch(200),
                },
                vec![
                    TxOp::Add {
                        entity: EntityRef::Id(entity_id),
                        attribute: missing_attr,
                        value: missing_value,
                    },
                    TxOp::Add {
                        entity: EntityRef::Id(entity_id),
                        attribute: existing_attr.clone(),
                        value: new_value.clone(),
                    },
                ],
            )
            .await?;

        let existing_attr_id = indexer
            .metadata()
            .schema
            .get_attribute(&existing_attr)
            .unwrap()
            .0;
        let mut seen = std::collections::HashSet::new();
        let mut iter = slate
            .scan_prefix_with_options(&[codec::EAV], &ScanOptions::default())
            .await?;
        while let Some(kv) = iter.next().await? {
            let (eid, attribute, value, _ts, op) = eav_key_to_parts(kv.key)?;
            if eid == DataType::Long(entity_id) && attribute == existing_attr_id {
                seen.insert((value, op));
            }
        }

        assert!(seen.contains(&(old_value, codec::RETRACT)));
        assert!(seen.contains(&(new_value, codec::ADD)));

        Ok(())
    }

    #[tokio::test]
    async fn test_cardinality_many_no_retract() -> Result<(), Error> {
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let mut indexer = bootstrapped_indexer(&components).await;

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
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let mut indexer = bootstrapped_indexer(&components).await;

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

    /// A lookup ref pointing at an entity being created in the SAME transaction
    /// must not resolve — the target isn't committed yet. Guards against
    /// accidentally reintroducing read-your-own-writes into the pipeline.
    #[tokio::test]
    async fn test_lookup_ref_to_same_tx_entity_is_rejected() -> Result<(), Error> {
        let components = in_memory_slate().await;
        let mut indexer = bootstrapped_indexer(&components).await;
        let tx_key = TxKey {
            tx_id: 1,
            system_time: st_from_unix_epoch(2),
        };
        let waiter = indexer.tx_waiter();
        let tx_ops = vec![
            TxOp::put(vec![
                (kw!(:name), "Alice".into()),
                (kw!(:email), "alice@example.com".into()),
            ]),
            TxOp::Add {
                entity: EntityRef::LookupRef(
                    kw!(:email),
                    DataType::String("alice@example.com".into()),
                ),
                attribute: kw!(:age),
                value: DataType::Long(30),
            },
        ];
        let basis = indexer.transact_tx(tx_key, tx_ops).await?;
        assert_eq!(basis.tx_key, tx_key);
        let completion = waiter.await_tx(tx_key).await?;
        assert_eq!(completion.basis.map(|basis| basis.tx_key), Some(tx_key));
        assert!(completion
            .result
            .unwrap_err()
            .to_string()
            .contains("No entity found for lookup ref"));
        Ok(())
    }

    #[tokio::test]
    async fn test_vae_written_only_for_unique_attributes() -> Result<(), Error> {
        let components = in_memory_slate().await;
        let slate = components.db.clone();
        let mut indexer = bootstrapped_indexer(&components).await;
        let name_id = indexer
            .metadata()
            .schema
            .get_attribute(&kw!(:name))
            .unwrap()
            .0;
        let email_id = indexer
            .metadata()
            .schema
            .get_attribute(&kw!(:email))
            .unwrap()
            .0;

        indexer
            .transact_tx(
                TxKey {
                    tx_id: 1,
                    system_time: st_from_unix_epoch(2),
                },
                vec![TxOp::put(vec![
                    (kw!(:name), "Alice".into()),
                    (kw!(:email), "alice@example.com".into()),
                ])],
            )
            .await?;

        let mut saw_email = false;
        let mut saw_name = false;
        let mut iter = slate
            .scan_prefix_with_options(&[codec::VAE], &ScanOptions::default())
            .await?;
        while let Some(kv) = iter.next().await? {
            let (_value, attribute, _entity, _tx, op) = vae_key_to_parts(kv.key)?;
            if op == codec::ADD && attribute == email_id {
                saw_email = true;
            }
            if op == codec::ADD && attribute == name_id {
                saw_name = true;
            }
        }

        assert!(saw_email, "unique :email should have a VAE entry");
        assert!(!saw_name, "non-unique :name should not have a VAE entry");
        Ok(())
    }
}
