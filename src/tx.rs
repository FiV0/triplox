use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::Result;
use edn::kw;
use edn::symbols::Keyword;
use indexmap::IndexSet;

use crate::codec::{self, encode_datatype, encode_i64_bytes, Encode};
use crate::indexer::vae_key_to_parts;
use crate::metadata::PartitionMap;
use crate::ops::{DataType, Datom, DatomOp, Entid, EntityRef, TxOp};
use crate::partition::{DB_PARTITION, USER_PARTITION};
use crate::schema::{Schema, Unique, ValueType};
use crate::slate::DEFAULT_SCAN_OPTIONS;
use crate::union_find::UnionFind;
use crate::util::concat_bytes;

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

/// Expand TxOps into DatomExpanded, resolving idents and ref-typed values via schema.
/// Lookup refs and tempids pass through as-is for later resolution stages.
///
/// - `Put(map)` -> N DatomExpanded (one per non-`:db/id` attr). The `:db/id` key
///   identifies the entity (Long=ID, String=tempid, Keyword=ident). If absent, generates
///   an internal tempid.
/// - `Add/Retract` -> 1 DatomExpanded
/// - `Delete/Erase` -> panics (not yet implemented)
pub fn expand_tx_ops(ops: &[TxOp], schema: &Schema) -> Result<Vec<DatomExpanded>> {
    let db_id_kw = Keyword::namespaced("db", "id");
    let mut datoms = Vec::new();
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
            TxOp::Delete(_) | TxOp::Erase(_) => {
                panic!("Delete/Erase not yet implemented");
            }
        }
    }
    Ok(datoms)
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
        prefixes.insert(unique_vae_prefix(*attr_eid, value), (*attr_eid, value.clone()));
    }

    let mut resolved: HashMap<(Entid, DataType), Entid> = HashMap::new();

    let Some(first_prefix) = prefixes.keys().next() else {
        return Ok(resolved);
    };

    let mut iter = db
        .scan_with_options(
            first_prefix.clone()..vec![codec::VAE_END],
            &DEFAULT_SCAN_OPTIONS,
        )
        .await?;

    // Slatedb forbids seeking strictly backward past the iterator's last
    // returned key. On a miss, `next()` returns the next DB key past our
    // prefix, which may exceed a subsequent prefix; we skip those — anything
    // the iterator already overshot has no entry in the DB.
    let mut last_returned: Option<Vec<u8>> = None;

    for (vae_prefix, (attr_eid, value)) in &prefixes {
        if let Some(last) = &last_returned {
            if vae_prefix < last {
                continue;
            }
        }

        iter.seek(vae_prefix).await?;
        let Some(kv) = iter.next().await? else {
            break;
        };
        last_returned = Some(kv.key.to_vec());

        if !kv.key.starts_with(vae_prefix) {
            continue;
        }

        assert!(
            kv.key.len() >= codec::TX_EID_OP_SUFFIX,
            "Key too short ({} bytes) to contain tx_eid + op suffix",
            kv.key.len()
        );
        if kv.key[kv.key.len() - 1] == codec::RETRACT {
            continue;
        }

        let (_value, _attribute, entity, _tx_eid, _op) = vae_key_to_parts(kv.key)?;
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

    Ok(resolved)
}

/// Batch-resolve all lookup refs via the unique-only VAE index.
/// Converts DatomExpanded → DatomWithTempids, eliminating all LookupRef variants.
/// If no lookup refs are present, this is a cheap conversion with no I/O.
pub async fn resolve_lookup_refs(
    datoms: Vec<DatomExpanded>,
    schema: &Schema,
    db: &slatedb::Db,
) -> Result<Vec<DatomWithTempids>> {
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
    Ok(result)
}

type TempIdMap = HashMap<String, Entid>;
type UniqueLookup = (Entid, DataType);

/// A "Simple upsert" that looks like `[:db/add TEMPID a v]`, where `a` is `:db.unique/identity`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct UpsertE(String, Entid, DataType);

/// A "Complex upsert" that looks like `[:db/add TEMPID a OTHERTEMPID]`, where `a` is
/// `:db.unique/identity`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct UpsertEV(String, Entid, String);

/// A generation collects entities into populations at a single evolutionary step in the upsert
/// resolution evolution process.
///
/// The upsert resolution process is only concerned with `[:db/add ...]` entities until the final
/// entid allocations. That's why we separate into special simple and complex upsert types
/// immediately, and then collect the more general datom types for final resolution.
#[derive(Debug, Default)]
struct Generation {
    /// "Simple upserts" that look like `[:db/add TEMPID a v]`, where `a` is `:db.unique/identity`.
    upserts_e: Vec<UpsertE>,

    /// "Complex upserts" that look like `[:db/add TEMPID a OTHERTEMPID]`, where `a` is
    /// `:db.unique/identity`.
    upserts_ev: Vec<UpsertEV>,

    /// Entities that look like:
    /// - `[:db/add TEMPID b OTHERTEMPID]`. `b` may be `:db.unique/identity` if it has failed to upsert.
    /// - `[:db/add TEMPID b v]`. `b` may be `:db.unique/identity` if it has failed to upsert.
    /// - `[:db/add e b OTHERTEMPID]`.
    allocations: Vec<DatomWithTempids>,

    /// Entities that upserted and no longer reference tempids. These assertions are guaranteed to
    /// be in the store.
    upserted: Vec<Datom>,

    /// Entities that resolved due to other upserts and no longer reference tempids. These
    /// assertions may or may not be in the store.
    resolved: Vec<Datom>,
}

#[derive(Debug, Default)]
struct FinalPopulations {
    /// Upserts that upserted.
    upserted: Vec<Datom>,

    /// Allocations that resolved due to other upserts.
    resolved: Vec<Datom>,

    /// Allocations that required new entid allocations.
    allocated: Vec<Datom>,
}

fn is_identity_attr(schema: &Schema, attr_eid: Entid) -> Result<bool> {
    Ok(schema
        .attribute_map
        .get(&attr_eid)
        .ok_or_else(|| anyhow::anyhow!("Unknown attribute entid: {}", attr_eid))?
        .unique
        == Some(Unique::Identity))
}

fn datom_with_entid_attr(d: &DatomWithTempids, schema: &Schema) -> Result<(Entid, bool)> {
    let (attr_eid, attr) = schema
        .get_attribute(&d.attribute)
        .ok_or_else(|| anyhow::anyhow!("Unknown attribute: {}", d.attribute))?;
    Ok((attr_eid, attr.unique == Some(Unique::Identity)))
}

fn substitute_datom(d: DatomWithTempids, temp_id_map: &TempIdMap) -> Result<EitherDatom> {
    let entity = match d.entity {
        IdOrTempId::Id(id) => IdOrTempId::Id(id),
        IdOrTempId::TempId(s) => match temp_id_map.get(&s) {
            Some(eid) => IdOrTempId::Id(*eid),
            None => IdOrTempId::TempId(s),
        },
    };
    let value = match d.value {
        ValueWithTempIds::Data(dt) => ValueWithTempIds::Data(dt),
        ValueWithTempIds::TempRef(s) => match temp_id_map.get(&s) {
            Some(eid) => ValueWithTempIds::Data(DataType::Long(*eid)),
            None => ValueWithTempIds::TempRef(s),
        },
    };

    match (&entity, &value) {
        (IdOrTempId::Id(entity), ValueWithTempIds::Data(value)) => {
            Ok(EitherDatom::Concrete(Datom {
                entity: *entity,
                attribute: d.attribute,
                value: value.clone(),
                op: d.op,
            }))
        }
        _ => Ok(EitherDatom::WithTempids(DatomWithTempids {
            entity,
            attribute: d.attribute,
            value,
            op: d.op,
        })),
    }
}

enum EitherDatom {
    Concrete(Datom),
    WithTempids(DatomWithTempids),
}

impl Generation {
    /// Split datoms into a generation of populations that need to evolve to have their tempids
    /// resolved or allocated, and a population of inert datoms that do not reference tempids.
    fn from(datoms: Vec<DatomWithTempids>, schema: &Schema) -> Result<(Self, Vec<Datom>)> {
        let mut generation = Generation::default();
        let mut inert = Vec::new();

        for d in datoms {
            let (attr_eid, identity) = datom_with_entid_attr(&d, schema)?;
            match (&d.entity, &d.value) {
                (IdOrTempId::TempId(t), ValueWithTempIds::Data(v))
                    if d.op == DatomOp::Assert && identity =>
                {
                    generation
                        .upserts_e
                        .push(UpsertE(t.clone(), attr_eid, v.clone()));
                }
                (IdOrTempId::TempId(t1), ValueWithTempIds::TempRef(t2))
                    if d.op == DatomOp::Assert && identity =>
                {
                    generation
                        .upserts_ev
                        .push(UpsertEV(t1.clone(), attr_eid, t2.clone()));
                }
                (IdOrTempId::Id(entity), ValueWithTempIds::Data(value)) => {
                    inert.push(Datom {
                        entity: *entity,
                        attribute: d.attribute,
                        value: value.clone(),
                        op: d.op,
                    });
                }
                _ => generation.allocations.push(d),
            }
        }

        Ok((generation, inert))
    }

    /// Return true if it's possible to evolve this generation further.
    ///
    /// Note that there can be complex upserts but no simple upserts to help resolve them, and in
    /// this case, we cannot evolve further.
    fn can_evolve(&self) -> bool {
        !self.upserts_e.is_empty()
    }

    /// Collect tempid -> [a v] pairs that might upsert at this evolutionary step.
    fn temp_id_avs(&self) -> Vec<(String, UniqueLookup)> {
        self.upserts_e
            .iter()
            .map(|UpsertE(t, a, v)| (t.clone(), (*a, v.clone())))
            .collect()
    }

    /// Evolve potential upserts that haven't resolved into allocations.
    fn allocate_unresolved_upserts(&mut self, schema: &Schema) -> Result<()> {
        for UpsertEV(t1, attr_eid, t2) in self.upserts_ev.drain(..) {
            let attr = schema
                .entid_map
                .get(&attr_eid)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Unknown attribute entid: {}", attr_eid))?;
            self.allocations.push(DatomWithTempids {
                entity: IdOrTempId::TempId(t1),
                attribute: attr,
                value: ValueWithTempIds::TempRef(t2),
                op: DatomOp::Assert,
            });
        }
        Ok(())
    }

    /// Evolve this generation one step further by rewriting the existing `:db/add` datoms using
    /// the given temporary IDs.
    ///
    /// Tempids resolved in earlier generations (tracked in `resolved_tempids`) are also honored,
    /// so that a tempid which surfaces a second time (e.g. via an `UpsertEV` promotion) does not
    /// get re-allocated a fresh entid.
    fn evolve_one_step(
        self,
        temp_id_map: &TempIdMap,
        resolved_tempids: &BTreeMap<String, Entid>,
        schema: &Schema,
    ) -> Result<Generation> {
        let mut next = Generation {
            resolved: self.resolved,
            ..Generation::default()
        };

        for UpsertE(t, a, v) in self.upserts_e {
            let attr = schema
                .entid_map
                .get(&a)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Unknown attribute entid: {}", a))?;
            match temp_id_map.get(&t) {
                Some(eid) => next.upserted.push(Datom {
                    entity: *eid,
                    attribute: attr,
                    value: v,
                    op: DatomOp::Assert,
                }),
                None if resolved_tempids.contains_key(&t) => next.resolved.push(Datom {
                    entity: resolved_tempids[&t],
                    attribute: attr,
                    value: v,
                    op: DatomOp::Assert,
                }),
                None => next.allocations.push(DatomWithTempids {
                    entity: IdOrTempId::TempId(t),
                    attribute: attr,
                    value: ValueWithTempIds::Data(v),
                    op: DatomOp::Assert,
                }),
            }
        }

        for UpsertEV(t1, a, t2) in self.upserts_ev {
            let attr = schema
                .entid_map
                .get(&a)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Unknown attribute entid: {}", a))?;
            let t1_eid = temp_id_map.get(&t1).or_else(|| resolved_tempids.get(&t1));
            let t2_eid = temp_id_map.get(&t2).or_else(|| resolved_tempids.get(&t2));
            match (t1_eid, t2_eid) {
                // Even though we can resolve entirely when both tempids are known, it's possible
                // that the remaining upsert could conflict. Moving straight to resolved doesn't
                // give us a chance to search the store for the conflict.
                (Some(_), Some(t2_eid)) | (None, Some(t2_eid)) => {
                    next.upserts_e.push(UpsertE(t1, a, DataType::Long(*t2_eid)));
                }
                (Some(t1_eid), None) => next.allocations.push(DatomWithTempids {
                    entity: IdOrTempId::Id(*t1_eid),
                    attribute: attr,
                    value: ValueWithTempIds::TempRef(t2),
                    op: DatomOp::Assert,
                }),
                (None, None) => next.upserts_ev.push(UpsertEV(t1, a, t2)),
            }
        }

        let combined: TempIdMap = resolved_tempids
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .chain(temp_id_map.iter().map(|(k, v)| (k.clone(), *v)))
            .collect();
        for d in self.allocations {
            match substitute_datom(d, &combined)? {
                EitherDatom::Concrete(d) => next.resolved.push(d),
                EitherDatom::WithTempids(d) => next.allocations.push(d),
            }
        }

        Ok(next)
    }

    /// After evolution is complete, yield the set of tempids that require entid allocation.
    ///
    /// Some of the tempids may be identified, so we also provide a map from tempid to a dense set
    /// of contiguous integer labels.
    fn temp_ids_in_allocations(&self, schema: &Schema) -> Result<BTreeMap<String, usize>> {
        let mut tempids = BTreeSet::new();
        let mut identity_groups: HashMap<(Entid, ValueWithTempIds), Vec<String>> = HashMap::new();

        for d in &self.allocations {
            if d.op == DatomOp::Retract {
                if matches!(d.entity, IdOrTempId::TempId(_))
                    || matches!(d.value, ValueWithTempIds::TempRef(_))
                {
                    return Err(anyhow::anyhow!(
                        "[:db/retract ...] referenced tempid that did not upsert"
                    ));
                }
            }

            if d.op != DatomOp::Assert {
                continue;
            }

            if let IdOrTempId::TempId(t) = &d.entity {
                tempids.insert(t.clone());
            }
            if let ValueWithTempIds::TempRef(t) = &d.value {
                tempids.insert(t.clone());
            }

            let (attr_eid, identity) = datom_with_entid_attr(d, schema)?;
            if identity {
                if let IdOrTempId::TempId(t) = &d.entity {
                    identity_groups
                        .entry((attr_eid, d.value.clone()))
                        .or_default()
                        .push(t.clone());
                }
            }
        }

        // Now we union-find all the known tempids. Two tempids are unioned if they both appear as
        // the entity of an `[a v]` upsert, including when the value column `v` is itself a tempid.
        // Our `UnionFind` operates on contiguous indices, so we maintain the map from tempids to
        // indices ourselves (sorted via `BTreeSet` for deterministic results).
        let tempid_indices: BTreeMap<String, usize> = tempids
            .into_iter()
            .enumerate()
            .map(|(i, t)| (t, i))
            .collect();
        let mut uf = UnionFind::new(tempid_indices.len());

        for group in identity_groups.values() {
            if let Some(first) = group.first().and_then(|t| tempid_indices.get(t)) {
                for t in group {
                    if let Some(i) = tempid_indices.get(t) {
                        uf.union(*first, *i);
                    }
                }
            }
        }

        // Now that we have aggregated tempids, label them using the smallest number of contiguous
        // labels possible. We allocate labels for tempids in sorted order (driven by the
        // `BTreeMap` iteration above) so that "a" gets a smaller label than "b", which is pleasant
        // for testing.
        let mut dense_labels: IndexSet<usize> = IndexSet::new();
        let mut tempid_labels = BTreeMap::new();
        for (tempid, index) in tempid_indices {
            let rep = uf.find(index);
            let (label, _) = dense_labels.insert_full(rep);
            tempid_labels.insert(tempid, label);
        }

        Ok(tempid_labels)
    }

    /// After evolution is complete, use the provided allocated entids to segment `self` into
    /// populations, each with no references to tempids.
    fn into_final_populations(self, temp_id_map: &TempIdMap) -> Result<FinalPopulations> {
        let mut populations = FinalPopulations {
            upserted: self.upserted,
            resolved: self.resolved,
            allocated: Vec::new(),
        };

        for d in self.allocations {
            match substitute_datom(d, temp_id_map)? {
                EitherDatom::Concrete(d) => populations.allocated.push(d),
                EitherDatom::WithTempids(_) => {
                    return Err(anyhow::anyhow!("Unresolved tempid after allocation"))
                }
            }
        }

        Ok(populations)
    }
}

async fn resolve_temp_id_avs(
    tempid_avs: &[(String, UniqueLookup)],
    db: &slatedb::Db,
) -> Result<TempIdMap> {
    let mut unique_lookups: HashMap<UniqueLookup, Vec<String>> = HashMap::new();
    for (tempid, lookup) in tempid_avs {
        unique_lookups
            .entry(lookup.clone())
            .or_default()
            .push(tempid.clone());
    }

    let lookups: Vec<UniqueLookup> = unique_lookups.keys().cloned().collect();
    let resolved = batch_lookup_unique_eids(db, &lookups).await?;

    let mut temp_id_map = HashMap::new();
    for (lookup, tempids) in unique_lookups {
        if let Some(&eid) = resolved.get(&lookup) {
            for tempid in tempids {
                temp_id_map.insert(tempid, eid);
            }
        }
    }
    Ok(temp_id_map)
}

fn record_resolutions(
    resolved_tempids: &mut BTreeMap<String, Entid>,
    temp_id_map: TempIdMap,
) -> Result<()> {
    let mut conflicts: BTreeMap<String, BTreeSet<Entid>> = BTreeMap::new();
    for (tempid, entid) in temp_id_map {
        if let Some(previous) = resolved_tempids.insert(tempid.clone(), entid) {
            if previous != entid {
                conflicts
                    .entry(tempid)
                    .or_default()
                    .extend([previous, entid]);
            }
        }
    }

    if !conflicts.is_empty() {
        return Err(anyhow::anyhow!("Conflicting upserts: {:?}", conflicts));
    }
    Ok(())
}

fn allocation_partitions(
    allocations: &[DatomWithTempids],
    tempid_labels: &BTreeMap<String, usize>,
) -> Vec<u32> {
    let label_count = tempid_labels.values().copied().max().map_or(0, |n| n + 1);
    let mut partitions = vec![USER_PARTITION; label_count];
    for d in allocations {
        if d.op == DatomOp::Assert && d.attribute == kw!(:db/ident) {
            if let IdOrTempId::TempId(t) = &d.entity {
                if let Some(label) = tempid_labels.get(t) {
                    partitions[*label] = DB_PARTITION;
                }
            }
        }
    }
    partitions
}

/// Resolve tempids using Mentat-style :db.unique/identity upsert generations.
pub async fn resolve_tempids_with_upserts(
    datoms: Vec<DatomWithTempids>,
    schema: &Schema,
    db: &slatedb::Db,
    partition_map: &mut PartitionMap,
) -> Result<Vec<Datom>> {
    let (mut generation, inert_terms) = Generation::from(datoms, schema)?;
    let mut resolved_tempids = BTreeMap::new();

    while generation.can_evolve() {
        let tempid_avs = generation.temp_id_avs();
        let temp_id_map = resolve_temp_id_avs(&tempid_avs, db).await?;
        record_resolutions(&mut resolved_tempids, temp_id_map.clone())?;
        generation = generation.evolve_one_step(&temp_id_map, &resolved_tempids, schema)?;
    }

    generation.allocate_unresolved_upserts(schema)?;
    let tempid_labels = generation.temp_ids_in_allocations(schema)?;
    let partitions = allocation_partitions(&generation.allocations, &tempid_labels);

    let mut allocated_tempids = HashMap::new();
    let mut label_entids = Vec::with_capacity(partitions.len());
    for partition in partitions {
        label_entids.push(partition_map.allocate_entid(partition));
    }
    for (tempid, label) in tempid_labels {
        allocated_tempids.insert(tempid, label_entids[label]);
    }

    let final_populations = generation.into_final_populations(&allocated_tempids)?;
    let mut datoms = Vec::new();
    datoms.extend(final_populations.upserted);
    datoms.extend(final_populations.resolved);
    datoms.extend(final_populations.allocated);
    datoms.extend(inert_terms);
    Ok(datoms)
}

/// Resolve tempids in DatomWithTempids to produce final Datoms.
///
/// Tempids with a `:db/ident` datom are allocated from DB_PARTITION;
/// all others from USER_PARTITION.
pub fn resolve_tempids(
    datoms: &[DatomWithTempids],
    partition_map: &mut PartitionMap,
) -> Result<Vec<Datom>> {
    // Pre-scan: determine partition for each tempid
    let mut tempid_partitions: HashMap<&str, u32> = HashMap::new();
    for d in datoms {
        if let IdOrTempId::TempId(ref s) = d.entity {
            if d.attribute == kw!(:db/ident) {
                // insert() overrides so :db/ident always wins regardless of ordering
                tempid_partitions.insert(s, DB_PARTITION);
            } else {
                // or_insert() preserves existing, so a prior :db/ident won't be overwritten
                tempid_partitions.entry(s).or_insert(USER_PARTITION);
            }
        }
    }
    // Also check tempids that only appear in value position
    for d in datoms {
        if let ValueWithTempIds::TempRef(ref s) = d.value {
            tempid_partitions.entry(s).or_insert(USER_PARTITION);
        }
    }

    // Allocate entids
    let mut tempid_map: HashMap<&str, i64> = HashMap::new();
    for (tempid, partition) in &tempid_partitions {
        let eid = partition_map.allocate_entid(*partition);
        tempid_map.insert(tempid, eid);
    }

    // Resolve
    let mut resolved = Vec::with_capacity(datoms.len());
    for d in datoms {
        let entity = match &d.entity {
            IdOrTempId::Id(id) => *id,
            IdOrTempId::TempId(s) => *tempid_map.get(s.as_str()).unwrap(),
        };
        let value = match &d.value {
            // TODO: get rid of the clone()
            ValueWithTempIds::Data(data) => data.clone(),
            ValueWithTempIds::TempRef(s) => DataType::Long(*tempid_map.get(s.as_str()).unwrap()),
        };
        resolved.push(Datom {
            entity,
            attribute: d.attribute.clone(),
            value,
            op: d.op,
        });
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use edn::kw;

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

    // --- expand_tx_ops tests ---

    #[test]
    fn test_expand_put_with_id() {
        let ops = vec![TxOp::put(vec![
            (kw!(:db/id), DataType::Long(100)),
            (kw!(:name), "alice".into()),
            (kw!(:age), 30_i64.into()),
        ])];
        let datoms = expand_tx_ops(&ops, &empty_schema()).unwrap();
        assert_eq!(datoms.len(), 2);
        assert!(datoms.iter().all(|d| d.entity == EntityExpanded::Id(100)));
        assert!(datoms.iter().all(|d| d.op == DatomOp::Assert));
    }

    #[test]
    fn test_expand_put_without_id() {
        let ops = vec![
            TxOp::put(vec![(kw!(:name), "alice".into())]),
            TxOp::put(vec![(kw!(:name), "bob".into())]),
        ];
        let datoms = expand_tx_ops(&ops, &empty_schema()).unwrap();
        assert_eq!(datoms.len(), 2);
        assert_ne!(datoms[0].entity, datoms[1].entity);
        assert!(matches!(datoms[0].entity, EntityExpanded::TempId(_)));
        assert!(matches!(datoms[1].entity, EntityExpanded::TempId(_)));
    }

    #[test]
    fn test_expand_put_attrs_share_entity() {
        let ops = vec![TxOp::put(vec![
            (kw!(:name), "alice".into()),
            (kw!(:age), 30_i64.into()),
        ])];
        let datoms = expand_tx_ops(&ops, &empty_schema()).unwrap();
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
        let datoms = expand_tx_ops(&ops, &empty_schema()).unwrap();
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
        let datoms = expand_tx_ops(&ops, &empty_schema()).unwrap();
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
        let datoms = expand_tx_ops(&ops, &schema).unwrap();
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
        let datoms = expand_tx_ops(&ops, &schema).unwrap();
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
        let datoms = expand_tx_ops(&ops, &schema).unwrap();
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
        let datoms = expand_tx_ops(&ops, &schema).unwrap();
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
        let datoms = expand_tx_ops(&ops, &schema).unwrap();
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
        let datoms = expand_tx_ops(&ops, &schema).unwrap();
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
    #[should_panic(expected = "Delete/Erase not yet implemented")]
    fn test_expand_delete_panics() {
        expand_tx_ops(&[TxOp::Delete(EntityRef::Id(100))], &empty_schema()).unwrap();
    }

    #[test]
    #[should_panic(expected = "Delete/Erase not yet implemented")]
    fn test_expand_erase_panics() {
        expand_tx_ops(&[TxOp::Erase(EntityRef::Id(200))], &empty_schema()).unwrap();
    }

    // --- resolve_tempids tests ---

    use crate::partition::{extract_counter, extract_partition};

    #[test]
    fn test_resolve_same_tempid_same_entid() {
        let mut pm = PartitionMap::new();
        let datoms = vec![
            DatomWithTempids {
                entity: IdOrTempId::TempId("t1".to_string()),
                attribute: kw!(:name),
                value: ValueWithTempIds::Data(DataType::String("alice".to_string())),
                op: DatomOp::Assert,
            },
            DatomWithTempids {
                entity: IdOrTempId::TempId("t1".to_string()),
                attribute: kw!(:age),
                value: ValueWithTempIds::Data(DataType::Long(30)),
                op: DatomOp::Assert,
            },
        ];
        let resolved = resolve_tempids(&datoms, &mut pm).unwrap();
        assert_eq!(resolved[0].entity, resolved[1].entity);
    }

    #[test]
    fn test_resolve_different_tempids_different_entids() {
        let mut pm = PartitionMap::new();
        let datoms = vec![
            DatomWithTempids {
                entity: IdOrTempId::TempId("t1".to_string()),
                attribute: kw!(:name),
                value: ValueWithTempIds::Data(DataType::String("alice".to_string())),
                op: DatomOp::Assert,
            },
            DatomWithTempids {
                entity: IdOrTempId::TempId("t2".to_string()),
                attribute: kw!(:name),
                value: ValueWithTempIds::Data(DataType::String("bob".to_string())),
                op: DatomOp::Assert,
            },
        ];
        let resolved = resolve_tempids(&datoms, &mut pm).unwrap();
        assert_ne!(resolved[0].entity, resolved[1].entity);
    }

    #[test]
    fn test_resolve_tempref_in_value() {
        let mut pm = PartitionMap::new();
        let datoms = vec![
            DatomWithTempids {
                entity: IdOrTempId::TempId("alice".to_string()),
                attribute: kw!(:name),
                value: ValueWithTempIds::Data(DataType::String("alice".to_string())),
                op: DatomOp::Assert,
            },
            DatomWithTempids {
                entity: IdOrTempId::Id(999),
                attribute: kw!(:follows),
                value: ValueWithTempIds::TempRef("alice".to_string()),
                op: DatomOp::Assert,
            },
        ];
        let resolved = resolve_tempids(&datoms, &mut pm).unwrap();
        let alice_eid = resolved[0].entity;
        assert_eq!(resolved[1].value, DataType::Long(alice_eid));
    }

    #[test]
    fn test_resolve_db_ident_goes_to_db_partition() {
        let mut pm = PartitionMap::new();
        let datoms = vec![DatomWithTempids {
            entity: IdOrTempId::TempId("schema-attr".to_string()),
            attribute: kw!(:db/ident),
            value: ValueWithTempIds::Data(DataType::Keyword(kw!(:my/attr))),
            op: DatomOp::Assert,
        }];
        let resolved = resolve_tempids(&datoms, &mut pm).unwrap();
        assert_eq!(extract_partition(resolved[0].entity), DB_PARTITION);
    }

    #[test]
    fn test_resolve_regular_tempid_goes_to_user_partition() {
        let mut pm = PartitionMap::new();
        let datoms = vec![DatomWithTempids {
            entity: IdOrTempId::TempId("user-entity".to_string()),
            attribute: kw!(:name),
            value: ValueWithTempIds::Data(DataType::String("alice".to_string())),
            op: DatomOp::Assert,
        }];
        let resolved = resolve_tempids(&datoms, &mut pm).unwrap();
        assert_eq!(extract_partition(resolved[0].entity), USER_PARTITION);
    }

    #[test]
    fn test_resolve_id_passthrough() {
        let mut pm = PartitionMap::new();
        let datoms = vec![DatomWithTempids {
            entity: IdOrTempId::Id(42),
            attribute: kw!(:name),
            value: ValueWithTempIds::Data(DataType::String("alice".to_string())),
            op: DatomOp::Assert,
        }];
        let resolved = resolve_tempids(&datoms, &mut pm).unwrap();
        assert_eq!(resolved[0].entity, 42);
        assert!(pm.is_empty(), "no allocation for explicit IDs");
    }

    #[test]
    fn test_resolve_counter_advances() {
        let mut pm = PartitionMap::from([(USER_PARTITION, 5_i64)]);
        let datoms = vec![
            DatomWithTempids {
                entity: IdOrTempId::TempId("a".to_string()),
                attribute: kw!(:name),
                value: ValueWithTempIds::Data(DataType::String("a".to_string())),
                op: DatomOp::Assert,
            },
            DatomWithTempids {
                entity: IdOrTempId::TempId("b".to_string()),
                attribute: kw!(:name),
                value: ValueWithTempIds::Data(DataType::String("b".to_string())),
                op: DatomOp::Assert,
            },
        ];
        let resolved = resolve_tempids(&datoms, &mut pm).unwrap();
        let mut counters: Vec<i64> = resolved.iter().map(|d| extract_counter(d.entity)).collect();
        counters.sort();
        assert_eq!(counters, vec![5, 6]);
        assert_eq!(pm[&USER_PARTITION], 7);
    }
}
