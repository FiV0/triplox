use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{bail, Error, Result};
use edn::kw;
use edn::symbols::Keyword;
use slatedb::DbReadOps;

use crate::codec::{self, encode_datatype, encode_i64_bytes, Encode};
use crate::indexer::{
    ave_key_to_parts, eav_key_to_parts, vae_key_to_parts, TxCompletion, TxOutcome,
};
use crate::iterator::slate_key_iterator::SlateKeyIterator;
use crate::metadata::PartitionMap;
use crate::ops::{DataType, Datom, DatomOp, Entid, EntityRef, TxOp};
use crate::schema::{Schema, Unique, ValueType, DB_TX_ABORTED, DB_TX_COMMITTED};
use crate::slate::DEFAULT_SCAN_OPTIONS;
use crate::transaction::TxKey;
use crate::util::{concat_bytes, next_prefix};

// ---------------------------------------------------------------------------
// Stage 1 types: after ident resolution, before lookup ref resolution.
// May still contain LookupRef and TempId variants.
// ---------------------------------------------------------------------------

/// Entity reference after ident resolution, before lookup ref resolution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EntityExpanded {
    Id(i64),
    TempId(String),
    LookupRef(i64, DataType), // (attribute_entid, value)
}

/// Value after ident resolution, before lookup ref resolution.
#[derive(Debug, Clone, PartialEq)]
pub enum ValueExpanded {
    Data(DataType),
    TempRef(String),
    LookupRef(i64, DataType), // (attribute_entid, value)
}

/// A datom after TxOp expansion and ident resolution, before lookup ref resolution.
#[derive(Debug, Clone, PartialEq)]
pub struct DatomExpanded {
    pub entity: EntityExpanded,
    pub attribute: Keyword,
    pub value: ValueExpanded,
    pub op: DatomOp,
}

/// Expanded transaction operations before lookup-ref resolution.
#[derive(Debug, Clone, PartialEq)]
pub struct ExpandedTxOps {
    pub datoms: Vec<DatomExpanded>,
    pub retract_entities: Vec<EntityExpanded>,
}

// ---------------------------------------------------------------------------
// Stage 2 types: after lookup ref resolution, before tempid allocation.
// Only TempId variants remain.
// ---------------------------------------------------------------------------

/// Entity reference after lookup ref resolution: either a concrete ID or an unresolved tempid.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IdOrTempId {
    Id(i64),
    TempId(String),
}

/// Value after lookup ref resolution: either concrete data or a tempid reference.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ValueWithTempIds {
    Data(DataType),
    TempRef(String),
}

/// A datom after lookup ref resolution, before tempid allocation.
#[derive(Debug, Clone, PartialEq)]
pub struct DatomWithTempids {
    pub entity: IdOrTempId,
    pub attribute: Keyword,
    pub value: ValueWithTempIds,
    pub op: DatomOp,
}

/// Transaction operations after lookup-ref resolution.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedTxOps {
    pub datoms: Vec<DatomWithTempids>,
    pub retract_entities: Vec<Entid>,
}

/// Resolve an EntityRef to EntityExpanded, resolving idents via schema.
/// Lookup refs have their attribute keyword resolved to an entid but remain as LookupRef.
fn resolve_entity_ref(eref: &EntityRef, schema: &Schema) -> Result<EntityExpanded> {
    match eref {
        EntityRef::Id(id) => Ok(EntityExpanded::Id(*id)),
        EntityRef::TempId(s) => Ok(EntityExpanded::TempId(s.clone())),
        EntityRef::Ident(kw) => {
            let eid = schema
                .ident_map
                .get(kw)
                .ok_or_else(|| anyhow::anyhow!("Unknown ident: {}", kw))?;
            Ok(EntityExpanded::Id(*eid))
        }
        EntityRef::LookupRef(kw, dt) => {
            let attr_eid = schema
                .ident_map
                .get(kw)
                .ok_or_else(|| anyhow::anyhow!("Unknown attribute in lookup ref: {}", kw))?;
            Ok(EntityExpanded::LookupRef(*attr_eid, dt.clone()))
        }
    }
}

fn expand_retract_entity_ref(eref: &EntityRef, schema: &Schema) -> Result<EntityExpanded> {
    match eref {
        EntityRef::Id(id) => Ok(EntityExpanded::Id(*id)),
        EntityRef::LookupRef(kw, value) => {
            let attr_eid = schema
                .ident_map
                .get(kw)
                .ok_or_else(|| anyhow::anyhow!("Unknown attribute in lookup ref: {}", kw))?;
            Ok(EntityExpanded::LookupRef(*attr_eid, value.clone()))
        }
        EntityRef::Ident(_) => bail!("RetractEntity does not support ident entity references"),
        EntityRef::TempId(_) => bail!("RetractEntity does not support tempid entity references"),
    }
}

// TODO: The code duplication in the two functions below seems off. The main difference is that
// one deals with entity position and the other with value position. See also #198

/// Resolve a :db/id DataType value to EntityExpanded.
fn resolve_db_id(val: &DataType, schema: &Schema) -> Result<EntityExpanded> {
    match val {
        DataType::Long(id) => Ok(EntityExpanded::Id(*id)),
        DataType::String(s) => Ok(EntityExpanded::TempId(s.clone())),
        DataType::Keyword(kw) => {
            let eid = schema
                .ident_map
                .get(kw)
                .ok_or_else(|| anyhow::anyhow!("Unknown ident for :db/id: {}", kw))?;
            Ok(EntityExpanded::Id(*eid))
        }
        DataType::Vector(v) if v.len() == 2 => match (&v[0], &v[1]) {
            (DataType::Keyword(kw), val) => {
                let attr_eid = schema.ident_map.get(kw).ok_or_else(|| {
                    anyhow::anyhow!("Unknown attribute in :db/id lookup ref: {}", kw)
                })?;
                Ok(EntityExpanded::LookupRef(*attr_eid, val.clone()))
            }
            _ => Err(anyhow::anyhow!(
                ":db/id lookup ref must be [Keyword, Value], got {:?}",
                v
            )),
        },
        other => Err(anyhow::anyhow!(
            ":db/id must be Long, String, Keyword, or [Keyword, Value] lookup ref, got {:?}",
            other
        )),
    }
}

/// Resolve a value using the schema to determine if the attribute is ref-typed.
/// For ref-typed attributes, a 2-element vector [Keyword, Value] is treated as a lookup ref.
fn resolve_value(val: &DataType, attr: &Keyword, schema: &Schema) -> Result<ValueExpanded> {
    let is_ref = schema
        .get_attribute(attr)
        .map(|(_, a)| a.value_type == ValueType::Ref)
        .unwrap_or(false);

    if is_ref {
        match val {
            DataType::Long(id) => Ok(ValueExpanded::Data(DataType::Long(*id))),
            DataType::Keyword(kw) => {
                let eid = schema.ident_map.get(kw).ok_or_else(|| {
                    anyhow::anyhow!("Unknown ident in ref value position: {}", kw)
                })?;
                Ok(ValueExpanded::Data(DataType::Long(*eid)))
            }
            DataType::String(s) => Ok(ValueExpanded::TempRef(s.clone())),
            DataType::Vector(v) if v.len() == 2 => match (&v[0], &v[1]) {
                (DataType::Keyword(kw), val) => {
                    let attr_eid = schema.ident_map.get(kw).ok_or_else(|| {
                        anyhow::anyhow!("Unknown attribute in lookup ref: {}", kw)
                    })?;
                    Ok(ValueExpanded::LookupRef(*attr_eid, val.clone()))
                }
                _ => Err(anyhow::anyhow!(
                    "Lookup ref must be [Keyword, Value], got {:?}",
                    v
                )),
            },
            other => Err(anyhow::anyhow!(
                "Invalid value for ref-typed attribute {}: {:?}",
                attr,
                other
            )),
        }
    } else {
        Ok(ValueExpanded::Data(val.clone()))
    }
}

/// Expand TxOps, resolving idents and ref-typed values via schema.
/// Lookup refs and tempids pass through as-is for later resolution stages.
///
/// - `Put(map)` -> N DatomExpanded (one per non-`:db/id` attr). The `:db/id` key
///   identifies the entity (Long=ID, String=tempid, Keyword=ident). If absent, generates
///   an internal tempid.
/// - `Add/Retract` -> 1 DatomExpanded
/// - `RetractEntity` -> 1 entity reference for later EAV expansion
/// - `Erase` -> error (not yet implemented)
pub fn expand_tx_ops(ops: &[TxOp], schema: &Schema) -> Result<ExpandedTxOps> {
    let db_id_kw = Keyword::namespaced("db", "id");
    let mut datoms = Vec::new();
    let mut retract_entities = Vec::new();
    let mut auto_counter: u64 = 0;

    for op in ops {
        match op {
            TxOp::Put(map) => {
                let entity = match map.get(&db_id_kw) {
                    Some(val) => resolve_db_id(val, schema)?,
                    None => {
                        let tempid = format!("__auto_{}", auto_counter);
                        auto_counter += 1;
                        EntityExpanded::TempId(tempid)
                    }
                };
                for (attr, value) in map.iter().filter(|(k, _)| *k != &db_id_kw) {
                    datoms.push(DatomExpanded {
                        entity: entity.clone(),
                        attribute: attr.clone(),
                        value: resolve_value(value, attr, schema)?,
                        op: DatomOp::Assert,
                    });
                }
            }
            TxOp::Add {
                entity,
                attribute,
                value,
            } => {
                datoms.push(DatomExpanded {
                    entity: resolve_entity_ref(entity, schema)?,
                    attribute: attribute.clone(),
                    value: resolve_value(value, attribute, schema)?,
                    op: DatomOp::Assert,
                });
            }
            TxOp::Retract {
                entity,
                attribute,
                value,
            } => {
                datoms.push(DatomExpanded {
                    entity: resolve_entity_ref(entity, schema)?,
                    attribute: attribute.clone(),
                    value: resolve_value(value, attribute, schema)?,
                    op: DatomOp::Retract,
                });
            }
            TxOp::RetractEntity(entity) => {
                retract_entities.push(expand_retract_entity_ref(entity, schema)?)
            }
            TxOp::Erase(_) => bail!("Erase not yet implemented"),
        }
    }
    Ok(ExpandedTxOps {
        datoms,
        retract_entities,
    })
}

fn unique_vae_prefix(attr_eid: i64, value: &DataType) -> Vec<u8> {
    let mut value_bytes = Vec::new();
    encode_datatype(value, &mut value_bytes);
    let attr_bytes = encode_i64_bytes(attr_eid);
    concat_bytes(&[&[codec::VAE], &value_bytes, &attr_bytes])
}

fn lookup_ref_not_found(schema: &Schema, attr_eid: i64, value: &DataType) -> anyhow::Error {
    let attr_kw = schema
        .entid_map
        .get(&attr_eid)
        .map(|kw| kw.to_string())
        .unwrap_or_else(|| attr_eid.to_string());
    anyhow::anyhow!("No entity found for lookup ref [{} {:?}]", attr_kw, value)
}

fn validate_unique_identity_lookup(
    schema: &Schema,
    attr_eid: Entid,
    value: &DataType,
) -> Result<()> {
    let attr = schema
        .attribute_map
        .get(&attr_eid)
        .ok_or_else(|| anyhow::anyhow!("Unknown lookup ref attribute entid: {}", attr_eid))?;
    if attr.unique != Some(Unique::Identity) {
        let attr_kw = schema
            .entid_map
            .get(&attr_eid)
            .map(|kw| kw.to_string())
            .unwrap_or_else(|| attr_eid.to_string());
        return Err(anyhow::anyhow!(
            "Lookup ref attribute {} must be :db.unique/identity",
            attr_kw
        ));
    }
    if !attr.value_type.matches(value) {
        let attr_kw = schema
            .entid_map
            .get(&attr_eid)
            .map(|kw| kw.to_string())
            .unwrap_or_else(|| attr_eid.to_string());
        return Err(anyhow::anyhow!(
            "Lookup ref value {:?} does not match attribute {} type {}",
            value,
            attr_kw,
            attr.value_type
        ));
    }
    Ok(())
}

/// Advance `iter` to the first key under `prefix` whose logical group's latest
/// entry is an assert. A logical group is the key minus the `[tx_eid][op]`
/// suffix; tx_eids encode descending, so the first key per group is its latest
/// entry. Groups whose latest entry is a retraction are skipped via next_prefix.
/// Returns the live key, or None if the prefix has no live entry (callers
/// distinguish global iterator exhaustion via `iter.peek().is_none()`).
pub(crate) async fn find_live_key_under_prefix(
    iter: &mut SlateKeyIterator,
    prefix: &[u8],
) -> Result<Option<bytes::Bytes>> {
    iter.seek(prefix).await?;
    loop {
        let Some(key) = iter.peek() else {
            return Ok(None);
        };

        if !key.starts_with(prefix) {
            return Ok(None);
        }

        assert!(
            key.len() >= codec::TX_EID_OP_SUFFIX,
            "Key too short ({} bytes) to contain tx_eid + op suffix",
            key.len()
        );
        if key[key.len() - 1] != codec::RETRACT {
            return Ok(Some(key.clone()));
        }

        let logical_key_end = key.len() - codec::TX_EID_OP_SUFFIX;
        let Some(next_group) = next_prefix(&key[..logical_key_end]) else {
            return Ok(None);
        };
        iter.seek(&next_group).await?;
    }
}

/// Batch-resolve `[attribute, value]` pairs to entity IDs via the unique-only
/// VAE index. One forward scan covers all lookups, seeking to each prefix in
/// sorted order. Returns a map from `(attr_eid, value)` to the owning entity.
/// Pairs with no entry, or whose latest entry is a retraction, are absent.
pub async fn batch_lookup_unique_eids(
    db: &slatedb::Db,
    lookups: &[(Entid, DataType)],
) -> Result<HashMap<(Entid, DataType), Entid>> {
    let mut prefixes: BTreeMap<Vec<u8>, (Entid, DataType)> = BTreeMap::new();
    for (attr_eid, value) in lookups {
        prefixes.insert(
            unique_vae_prefix(*attr_eid, value),
            (*attr_eid, value.clone()),
        );
    }

    let mut resolved: HashMap<(Entid, DataType), Entid> = HashMap::new();

    if prefixes.is_empty() {
        return Ok(resolved);
    }

    let mut iter = SlateKeyIterator::scan_prefix(db, &[codec::VAE]).await?;

    for (vae_prefix, (attr_eid, value)) in &prefixes {
        match find_live_key_under_prefix(&mut iter, vae_prefix).await? {
            Some(key) => {
                let (_value, _attribute, entity, _tx_eid, _op) = vae_key_to_parts(key)?;
                match entity {
                    DataType::Long(eid) => {
                        resolved.insert((*attr_eid, value.clone()), eid);
                    }
                    other => {
                        return Err(anyhow::anyhow!(
                            "Expected Long entity ID in VAE key, got {:?}",
                            other
                        ));
                    }
                }
            }
            // Prefixes are sorted, so no later prefix can match once the range is exhausted.
            None if iter.peek().is_none() => break,
            None => {}
        }
    }

    Ok(resolved)
}

/// Batch-resolve all lookup refs via the unique-only VAE index.
/// Converts expanded datoms and RetractEntity targets, eliminating all LookupRef variants.
/// If no lookup refs are present, this is a cheap conversion with no I/O.
pub async fn resolve_lookup_refs(
    expanded: ExpandedTxOps,
    schema: &Schema,
    db: &slatedb::Db,
) -> Result<ResolvedTxOps> {
    let ExpandedTxOps {
        datoms,
        retract_entities,
    } = expanded;
    // Collect every (attr, value) pair referenced by a lookup ref.
    let mut lookups: Vec<(Entid, DataType)> = Vec::new();
    for d in &datoms {
        if let EntityExpanded::LookupRef(a, v) = &d.entity {
            validate_unique_identity_lookup(schema, *a, v)?;
            lookups.push((*a, v.clone()));
        }
        if let ValueExpanded::LookupRef(a, v) = &d.value {
            validate_unique_identity_lookup(schema, *a, v)?;
            lookups.push((*a, v.clone()));
        }
    }
    for retract_entity in &retract_entities {
        if let EntityExpanded::LookupRef(a, v) = retract_entity {
            validate_unique_identity_lookup(schema, *a, v)?;
            lookups.push((*a, v.clone()));
        }
    }

    let resolved_map = batch_lookup_unique_eids(db, &lookups).await?;

    // Convert DatomExpanded → DatomWithTempids, replacing lookup refs with resolved IDs.
    let mut result = Vec::with_capacity(datoms.len());
    for d in datoms {
        let entity = match d.entity {
            EntityExpanded::Id(id) => IdOrTempId::Id(id),
            EntityExpanded::TempId(s) => IdOrTempId::TempId(s),
            EntityExpanded::LookupRef(a, v) => match resolved_map.get(&(a, v.clone())) {
                Some(&eid) => IdOrTempId::Id(eid),
                None => return Err(lookup_ref_not_found(schema, a, &v)),
            },
        };
        let value = match d.value {
            ValueExpanded::Data(dt) => ValueWithTempIds::Data(dt),
            ValueExpanded::TempRef(s) => ValueWithTempIds::TempRef(s),
            ValueExpanded::LookupRef(a, v) => match resolved_map.get(&(a, v.clone())) {
                Some(&eid) => ValueWithTempIds::Data(DataType::Long(eid)),
                None => return Err(lookup_ref_not_found(schema, a, &v)),
            },
        };
        result.push(DatomWithTempids {
            entity,
            attribute: d.attribute,
            value,
            op: d.op,
        });
    }
    let mut resolved_retract_entities = Vec::with_capacity(retract_entities.len());
    for retract_entity in retract_entities {
        match retract_entity {
            EntityExpanded::Id(id) => resolved_retract_entities.push(id),
            EntityExpanded::LookupRef(a, v) => match resolved_map.get(&(a, v.clone())) {
                Some(&eid) => resolved_retract_entities.push(eid),
                None => return Err(lookup_ref_not_found(schema, a, &v)),
            },
            EntityExpanded::TempId(_) => {
                bail!("RetractEntity does not support tempid entity references")
            }
        }
    }

    Ok(ResolvedTxOps {
        datoms: result,
        retract_entities: resolved_retract_entities,
    })
}

fn unallocated_entity_id_error(id: Entid) -> anyhow::Error {
    anyhow::anyhow!("unallocated entity id {}", id)
}

pub(crate) fn validate_allocated_entity_ids(
    tx_ops: &ResolvedTxOps,
    schema: &Schema,
    partition_map: &PartitionMap,
) -> Result<()> {
    for datom in &tx_ops.datoms {
        let (_, attr) = schema
            .get_attribute(&datom.attribute)
            .ok_or_else(|| anyhow::anyhow!("Unknown attribute: {}", datom.attribute))?;

        if let IdOrTempId::Id(id) = datom.entity {
            if !partition_map.contains_entid(id) {
                return Err(unallocated_entity_id_error(id));
            }
        }

        if attr.value_type == ValueType::Ref {
            if let ValueWithTempIds::Data(DataType::Long(id)) = datom.value {
                if !partition_map.contains_entid(id) {
                    return Err(unallocated_entity_id_error(id));
                }
            }
        }
    }

    for id in &tx_ops.retract_entities {
        if !partition_map.contains_entid(*id) {
            return Err(unallocated_entity_id_error(*id));
        }
    }

    Ok(())
}

/// Batch-read all currently active datoms for the requested entities from EAV.
/// One forward-only iterator covers the sorted, deduplicated entity prefixes.
pub(crate) async fn batch_lookup_active_entity_datoms(
    db: &slatedb::Db,
    schema: &Schema,
    entity_ids: &[Entid],
) -> Result<Vec<Datom>> {
    let mut prefixes = BTreeMap::new();
    for entity_id in entity_ids {
        let entity = DataType::Long(*entity_id).encode();
        prefixes.insert(concat_bytes(&[&[codec::EAV], &entity]), *entity_id);
    }

    if prefixes.is_empty() {
        return Ok(Vec::new());
    }

    let mut iter = SlateKeyIterator::scan_prefix(db, &[codec::EAV]).await?;
    let mut datoms = Vec::new();

    for (entity_prefix, expected_entity) in prefixes {
        iter.seek(&entity_prefix).await?;
        while let Some(key) = iter.peek().cloned() {
            if !key.starts_with(&entity_prefix) {
                break;
            }
            if key.len() < codec::TX_EID_OP_SUFFIX {
                bail!("EAV key too short to contain tx_eid and op");
            }

            let logical_key_end = key.len() - codec::TX_EID_OP_SUFFIX;
            let next_group = next_prefix(&key[..logical_key_end]);
            match key[key.len() - 1] {
                codec::ADD => {
                    let (entity, attribute_id, value, _tx_eid, _op) = eav_key_to_parts(key)?;
                    if entity != DataType::Long(expected_entity) {
                        bail!(
                            "Expected entity {} in EAV entity-retraction scan, got {:?}",
                            expected_entity,
                            entity
                        );
                    }
                    let attribute = schema.get_ident(attribute_id).ok_or_else(|| {
                        anyhow::anyhow!("Unknown attribute entity id {}", attribute_id)
                    })?;
                    datoms.push(Datom {
                        entity: expected_entity,
                        attribute: attribute.clone(),
                        value,
                        op: DatomOp::Retract,
                    });
                }
                codec::RETRACT => {}
                op => bail!("Unknown EAV operation byte: {}", op),
            }

            let Some(next_group) = next_group else {
                return Ok(datoms);
            };
            iter.seek(&next_group).await?;
        }
    }

    Ok(datoms)
}

/// Look up a transaction's persisted outcome via the AVE index on `:db/txId`,
/// reading `:db/txResult`/`:db/txError` from the tx entity.
///
/// Returns `None` if no tx entity exists: the tx is either not yet indexed or
/// failed without persisting an outcome (technical abort, deserialize failure).
///
/// TODO: I think this function can be replaced by an entity API call once we have
/// simplified TxKey and an actual entity API.
pub(crate) async fn lookup_tx_completion<D>(
    sdb: &D,
    tx_key: TxKey,
) -> Result<Option<TxCompletion>, Error>
where
    D: DbReadOps + Sync,
{
    // Keyed on tx_id only; the log assigns system_time together with tx_id.
    let mut value_buf = Vec::new();
    encode_datatype(&DataType::Long(tx_key.tx_id), &mut value_buf);
    let ave_prefix = concat_bytes(&[
        &[codec::AVE],
        &encode_i64_bytes(crate::schema::DB_TX_ID),
        &value_buf,
    ]);
    let mut iter = sdb
        .scan_prefix_with_options(&ave_prefix, .., &DEFAULT_SCAN_OPTIONS)
        .await?;
    let mut tx_eid: Option<i64> = None;
    while let Some(kv) = iter.next().await? {
        let (_attribute, _value, entity, _tx_eid, op) = ave_key_to_parts(kv.key)?;
        if op == codec::RETRACT {
            continue;
        }
        match entity {
            DataType::Long(eid) => {
                tx_eid = Some(eid);
                break;
            }
            other => bail!("Expected Long entity ID in AVE key, got {:?}", other),
        }
    }
    let Some(tx_eid) = tx_eid else {
        return Ok(None);
    };

    let mut entity_buf = Vec::new();
    encode_datatype(&DataType::Long(tx_eid), &mut entity_buf);
    let eav_prefix = concat_bytes(&[&[codec::EAV], &entity_buf]);
    let mut iter = sdb
        .scan_prefix_with_options(&eav_prefix, .., &DEFAULT_SCAN_OPTIONS)
        .await?;
    let mut tx_result: Option<i64> = None;
    let mut tx_error: Option<String> = None;
    while let Some(kv) = iter.next().await? {
        let (_entity, attribute, value, _tx_eid, op) = eav_key_to_parts(kv.key)?;
        if op == codec::RETRACT {
            continue;
        }
        if attribute == crate::schema::DB_TX_RESULT {
            if let DataType::Long(result) = value {
                tx_result = Some(result);
            }
        }
        if attribute == crate::schema::DB_TX_ERROR {
            if let DataType::String(err) = value {
                tx_error = Some(err);
            }
        }
    }

    let outcome = match tx_result {
        Some(DB_TX_COMMITTED) => TxOutcome::Committed,
        // TODO: Assure identical errors on live and reconstruction path. See #393.
        Some(DB_TX_ABORTED) => TxOutcome::Aborted(Arc::new(anyhow::anyhow!(
            "{}",
            tx_error.unwrap_or_else(|| format!("Transaction {} aborted", tx_key.tx_id))
        ))),
        Some(other) => bail!("Tx entity {tx_eid} has unknown :db/txResult {other}"),
        None => bail!("Tx entity {tx_eid} missing :db/txResult"),
    };
    Ok(Some(TxCompletion { tx_key, outcome }))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use edn::kw;

    use crate::slate::in_memory_slate;

    fn empty_schema() -> Schema {
        Schema::default()
    }

    fn schema_with_ident(kw: Keyword, eid: i64) -> Schema {
        let mut schema = Schema::default();
        schema.ident_map.insert(kw.clone(), eid);
        schema.entid_map.insert(eid, kw);
        schema
    }

    fn schema_with_ref_attr(attr_kw: Keyword, attr_eid: i64) -> Schema {
        use crate::schema::Attribute;
        let mut schema = Schema::default();
        schema.ident_map.insert(attr_kw, attr_eid);
        schema.attribute_map.insert(
            attr_eid,
            Attribute {
                value_type: ValueType::Ref,
                multival: false,
                unique: None,
            },
        );
        schema
    }

    fn unique_vae_key(
        attr_eid: i64,
        value: &DataType,
        entity: i64,
        tx_eid: i64,
        op: u8,
    ) -> Vec<u8> {
        let mut key = unique_vae_prefix(attr_eid, value);
        key.extend_from_slice(&DataType::Long(entity).encode());
        key.extend_from_slice(&codec::encode_i64_bytes(tx_eid));
        key.push(op);
        key
    }

    fn eav_key(
        entity: Entid,
        attribute: Entid,
        value: &DataType,
        tx_eid: Entid,
        op: u8,
    ) -> Vec<u8> {
        concat_bytes(&[
            &[codec::EAV],
            &DataType::Long(entity).encode(),
            &encode_i64_bytes(attribute),
            &value.encode(),
            &encode_i64_bytes(tx_eid),
            &[op],
        ])
    }

    // --- expand_tx_ops tests ---

    #[test]
    fn test_expand_put_with_id() {
        let ops = vec![TxOp::put([
            (kw!(:db/id), DataType::Long(100)),
            (kw!(:name), "alice".into()),
            (kw!(:age), 30_i64.into()),
        ])];
        let datoms = expand_tx_ops(&ops, &empty_schema()).unwrap().datoms;
        assert_eq!(datoms.len(), 2);
        assert!(datoms.iter().all(|d| d.entity == EntityExpanded::Id(100)));
        assert!(datoms.iter().all(|d| d.op == DatomOp::Assert));
    }

    #[test]
    fn test_expand_put_without_id() {
        let ops = vec![
            TxOp::put([(kw!(:name), "alice".into())]),
            TxOp::put([(kw!(:name), "bob".into())]),
        ];
        let datoms = expand_tx_ops(&ops, &empty_schema()).unwrap().datoms;
        assert_eq!(datoms.len(), 2);
        assert_ne!(datoms[0].entity, datoms[1].entity);
        assert!(matches!(datoms[0].entity, EntityExpanded::TempId(_)));
        assert!(matches!(datoms[1].entity, EntityExpanded::TempId(_)));
    }

    #[test]
    fn test_expand_put_attrs_share_entity() {
        let ops = vec![TxOp::put([
            (kw!(:name), "alice".into()),
            (kw!(:age), 30_i64.into()),
        ])];
        let datoms = expand_tx_ops(&ops, &empty_schema()).unwrap().datoms;
        assert_eq!(datoms.len(), 2);
        assert_eq!(datoms[0].entity, datoms[1].entity);
    }

    #[test]
    fn test_expand_add() {
        let ops = vec![TxOp::Add {
            entity: EntityRef::Id(200),
            attribute: kw!(:name),
            value: DataType::String("bob".to_string()),
        }];
        let datoms = expand_tx_ops(&ops, &empty_schema()).unwrap().datoms;
        assert_eq!(datoms.len(), 1);
        assert_eq!(datoms[0].entity, EntityExpanded::Id(200));
        assert_eq!(datoms[0].attribute, kw!(:name));
        assert_eq!(
            datoms[0].value,
            ValueExpanded::Data(DataType::String("bob".to_string()))
        );
        assert_eq!(datoms[0].op, DatomOp::Assert);
    }

    #[test]
    fn test_expand_retract() {
        let ops = vec![TxOp::Retract {
            entity: EntityRef::Id(200),
            attribute: kw!(:name),
            value: DataType::String("bob".to_string()),
        }];
        let datoms = expand_tx_ops(&ops, &empty_schema()).unwrap().datoms;
        assert_eq!(datoms.len(), 1);
        assert_eq!(datoms[0].op, DatomOp::Retract);
    }

    #[test]
    fn test_expand_ident_resolution() {
        let schema = schema_with_ident(kw!(:person/name), 42);
        let ops = vec![TxOp::Add {
            entity: EntityRef::Ident(kw!(:person/name)),
            attribute: kw!(:some/attr),
            value: DataType::Long(1),
        }];
        let datoms = expand_tx_ops(&ops, &schema).unwrap().datoms;
        assert_eq!(datoms[0].entity, EntityExpanded::Id(42));
    }

    #[test]
    fn test_expand_unknown_ident_errors() {
        let ops = vec![TxOp::Add {
            entity: EntityRef::Ident(kw!(:unknown/ident)),
            attribute: kw!(:some/attr),
            value: DataType::Long(1),
        }];
        let result = expand_tx_ops(&ops, &empty_schema());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unknown ident"));
    }

    #[test]
    fn test_expand_entity_lookup_ref() {
        let schema = schema_with_ident(kw!(:email), 42);
        let ops = vec![TxOp::Add {
            entity: EntityRef::LookupRef(kw!(:email), DataType::String("a@b.com".into())),
            attribute: kw!(:name),
            value: DataType::Long(1),
        }];
        let datoms = expand_tx_ops(&ops, &schema).unwrap().datoms;
        assert_eq!(
            datoms[0].entity,
            EntityExpanded::LookupRef(42, DataType::String("a@b.com".into()))
        );
    }

    #[test]
    fn test_expand_entity_lookup_ref_unknown_attr_errors() {
        let ops = vec![TxOp::Add {
            entity: EntityRef::LookupRef(kw!(:unknown/attr), DataType::String("a@b.com".into())),
            attribute: kw!(:name),
            value: DataType::Long(1),
        }];
        let result = expand_tx_ops(&ops, &empty_schema());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unknown attribute in lookup ref"));
    }

    #[test]
    fn test_expand_value_ref_tempid() {
        let schema = schema_with_ref_attr(kw!(:follows), 999);
        let ops = vec![TxOp::Add {
            entity: EntityRef::Id(100),
            attribute: kw!(:follows),
            value: DataType::String("friend".to_string()),
        }];
        let datoms = expand_tx_ops(&ops, &schema).unwrap().datoms;
        assert_eq!(
            datoms[0].value,
            ValueExpanded::TempRef("friend".to_string())
        );
    }

    #[test]
    fn test_expand_value_ref_id() {
        let schema = schema_with_ref_attr(kw!(:follows), 999);
        let ops = vec![TxOp::Add {
            entity: EntityRef::Id(100),
            attribute: kw!(:follows),
            value: DataType::Long(200),
        }];
        let datoms = expand_tx_ops(&ops, &schema).unwrap().datoms;
        assert_eq!(datoms[0].value, ValueExpanded::Data(DataType::Long(200)));
    }

    #[test]
    fn test_expand_value_ref_ident() {
        let mut schema = schema_with_ref_attr(kw!(:follows), 999);
        schema.ident_map.insert(kw!(:person/bob), 99);
        schema.entid_map.insert(99, kw!(:person/bob));
        let ops = vec![TxOp::Add {
            entity: EntityRef::Id(100),
            attribute: kw!(:follows),
            value: DataType::Keyword(kw!(:person/bob)),
        }];
        let datoms = expand_tx_ops(&ops, &schema).unwrap().datoms;
        assert_eq!(datoms[0].value, ValueExpanded::Data(DataType::Long(99)));
    }

    #[test]
    fn test_expand_value_lookup_ref() {
        let mut schema = schema_with_ref_attr(kw!(:follows), 999);
        schema.ident_map.insert(kw!(:email), 42);
        schema.entid_map.insert(42, kw!(:email));
        let ops = vec![TxOp::Add {
            entity: EntityRef::Id(100),
            attribute: kw!(:follows),
            value: DataType::Vector(vec![
                DataType::Keyword(kw!(:email)),
                DataType::String("a@b.com".into()),
            ]),
        }];
        let datoms = expand_tx_ops(&ops, &schema).unwrap().datoms;
        assert_eq!(
            datoms[0].value,
            ValueExpanded::LookupRef(42, DataType::String("a@b.com".into()))
        );
    }

    #[test]
    fn test_expand_value_lookup_ref_bad_shape_errors() {
        let schema = schema_with_ref_attr(kw!(:follows), 999);
        // Wrong length (3 elements)
        let ops = vec![TxOp::Add {
            entity: EntityRef::Id(100),
            attribute: kw!(:follows),
            value: DataType::Vector(vec![
                DataType::Keyword(kw!(:email)),
                DataType::String("a".into()),
                DataType::String("b".into()),
            ]),
        }];
        let result = expand_tx_ops(&ops, &schema);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid value for ref-typed attribute"));

        // Right length but first element is not keyword
        let ops = vec![TxOp::Add {
            entity: EntityRef::Id(100),
            attribute: kw!(:follows),
            value: DataType::Vector(vec![DataType::Long(1), DataType::String("a".into())]),
        }];
        let result = expand_tx_ops(&ops, &schema);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Lookup ref must be [Keyword, Value]"));
    }

    #[test]
    fn test_expand_value_ref_rejects_invalid_type() {
        let schema = schema_with_ref_attr(kw!(:follows), 999);
        let ops = vec![TxOp::Add {
            entity: EntityRef::Id(100),
            attribute: kw!(:follows),
            value: DataType::Boolean(true),
        }];
        let result = expand_tx_ops(&ops, &schema);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("Invalid value for ref-typed attribute"));
    }

    #[test]
    fn test_expand_retract_entity() {
        let expanded =
            expand_tx_ops(&[TxOp::RetractEntity(EntityRef::Id(100))], &empty_schema()).unwrap();
        assert!(expanded.datoms.is_empty());
        assert_eq!(expanded.retract_entities, vec![EntityExpanded::Id(100)]);
    }

    #[test]
    fn test_expand_retract_entity_lookup_ref() {
        let schema = schema_with_ident(kw!(:email), 42);
        let expanded = expand_tx_ops(
            &[TxOp::RetractEntity(EntityRef::LookupRef(
                kw!(:email),
                DataType::String("alice@example.com".into()),
            ))],
            &schema,
        )
        .unwrap();

        assert_eq!(
            expanded.retract_entities,
            vec![EntityExpanded::LookupRef(
                42,
                DataType::String("alice@example.com".into())
            )]
        );
    }

    #[test]
    fn test_expand_retract_entity_rejects_ident_and_tempid() {
        let ident_err = expand_tx_ops(
            &[TxOp::RetractEntity(EntityRef::Ident(kw!(:person/alice)))],
            &empty_schema(),
        )
        .unwrap_err();
        assert!(ident_err.to_string().contains("does not support ident"));

        let tempid_err = expand_tx_ops(
            &[TxOp::RetractEntity(EntityRef::TempId("alice".into()))],
            &empty_schema(),
        )
        .unwrap_err();
        assert!(tempid_err.to_string().contains("does not support tempid"));
    }

    #[test]
    fn test_expand_erase_errors() {
        let err = expand_tx_ops(&[TxOp::Erase(EntityRef::Id(200))], &empty_schema()).unwrap_err();
        assert_eq!(err.to_string(), "Erase not yet implemented");
    }

    #[tokio::test]
    async fn test_batch_lookup_unique_eids_resolves_lookup_after_overshoot() -> Result<()> {
        let slate = in_memory_slate().await.db;
        let attr_eid = 42;
        let missing_value = DataType::String("a@example.com".into());
        let present_value = DataType::String("b@example.com".into());
        let present_eid = 100;

        slate
            .put(
                &unique_vae_key(attr_eid, &present_value, present_eid, 1, codec::ADD),
                b"",
            )
            .await?;

        let resolved = batch_lookup_unique_eids(
            slate.as_ref(),
            &[
                (attr_eid, missing_value.clone()),
                (attr_eid, present_value.clone()),
            ],
        )
        .await?;

        assert!(!resolved.contains_key(&(attr_eid, missing_value)));
        assert_eq!(resolved.get(&(attr_eid, present_value)), Some(&present_eid));
        Ok(())
    }

    #[tokio::test]
    async fn test_batch_lookup_unique_eids_skips_retracted_entity_for_live_entity() -> Result<()> {
        let slate = in_memory_slate().await.db;
        let attr_eid = 42;
        let value = DataType::String("email@example.com".into());
        let retracted_eid = 200;
        let live_eid = 100;

        slate
            .put(
                &unique_vae_key(attr_eid, &value, retracted_eid, 1, codec::ADD),
                b"",
            )
            .await?;
        slate
            .put(
                &unique_vae_key(attr_eid, &value, retracted_eid, 2, codec::RETRACT),
                b"",
            )
            .await?;
        slate
            .put(
                &unique_vae_key(attr_eid, &value, live_eid, 3, codec::ADD),
                b"",
            )
            .await?;

        let resolved =
            batch_lookup_unique_eids(slate.as_ref(), &[(attr_eid, value.clone())]).await?;

        assert_eq!(resolved.get(&(attr_eid, value)), Some(&live_eid));
        Ok(())
    }

    #[tokio::test]
    async fn test_batch_lookup_unique_eids_ignores_only_retracted_entity() -> Result<()> {
        let slate = in_memory_slate().await.db;
        let attr_eid = 42;
        let value = DataType::String("email@example.com".into());
        let retracted_eid = 100;

        slate
            .put(
                &unique_vae_key(attr_eid, &value, retracted_eid, 1, codec::ADD),
                b"",
            )
            .await?;
        slate
            .put(
                &unique_vae_key(attr_eid, &value, retracted_eid, 2, codec::RETRACT),
                b"",
            )
            .await?;

        let resolved =
            batch_lookup_unique_eids(slate.as_ref(), &[(attr_eid, value.clone())]).await?;

        assert!(!resolved.contains_key(&(attr_eid, value)));
        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_lookup_refs_resolves_retract_entity_target() -> Result<()> {
        use crate::schema::Attribute;

        let slate = in_memory_slate().await.db;
        let mut schema = empty_schema();
        schema.ident_map.insert(kw!(:email), 42);
        schema.entid_map.insert(42, kw!(:email));
        schema.attribute_map.insert(
            42,
            Attribute {
                value_type: ValueType::String,
                multival: false,
                unique: Some(Unique::Identity),
            },
        );
        let value = DataType::String("alice@example.com".into());
        slate
            .put(&unique_vae_key(42, &value, 100, 1, codec::ADD), b"")
            .await?;

        let expanded = expand_tx_ops(
            &[TxOp::RetractEntity(EntityRef::LookupRef(
                kw!(:email),
                value,
            ))],
            &schema,
        )?;
        let resolved = resolve_lookup_refs(expanded, &schema, &slate).await?;

        assert!(resolved.datoms.is_empty());
        assert_eq!(resolved.retract_entities, vec![100]);
        Ok(())
    }

    #[tokio::test]
    async fn test_batch_lookup_active_entity_datoms() -> Result<()> {
        let slate = in_memory_slate().await.db;
        let mut schema = empty_schema();
        schema.entid_map.insert(10, kw!(:name));
        schema.entid_map.insert(11, kw!(:age));

        let alice = DataType::String("alice".into());
        let bob = DataType::String("bob".into());
        let carol = DataType::String("carol".into());
        let age = DataType::Long(30);
        for key in [
            eav_key(100, 10, &alice, 1, codec::ADD),
            eav_key(100, 10, &alice, 2, codec::RETRACT),
            eav_key(100, 10, &bob, 1, codec::ADD),
            eav_key(100, 11, &age, 1, codec::ADD),
            eav_key(200, 10, &carol, 1, codec::ADD),
        ] {
            slate.put(&key, b"").await?;
        }

        let actual: HashSet<_> =
            batch_lookup_active_entity_datoms(&slate, &schema, &[200, 100, 100])
                .await?
                .into_iter()
                .collect();
        let expected = HashSet::from([
            Datom {
                entity: 100,
                attribute: kw!(:name),
                value: bob,
                op: DatomOp::Retract,
            },
            Datom {
                entity: 100,
                attribute: kw!(:age),
                value: age,
                op: DatomOp::Retract,
            },
            Datom {
                entity: 200,
                attribute: kw!(:name),
                value: carol,
                op: DatomOp::Retract,
            },
        ]);

        assert_eq!(actual, expected);
        Ok(())
    }
}
