use std::collections::{HashMap, HashSet};

use anyhow::Result;
use edn::kw;
use edn::symbols::Keyword;

use crate::codec::{self, encode_datatype, encode_i64_bytes, Encode};
use crate::indexer::ave_key_to_parts;
use crate::metadata::PartitionMap;
use crate::ops::{DataType, Datom, DatomOp, EntityRef, TxOp};
use crate::partition::{DB_PARTITION, USER_PARTITION};
use crate::schema::{Schema, ValueType};
use crate::slate::DEFAULT_SCAN_OPTIONS;
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

/// Narrowing conversion from DatomExpanded to DatomWithTempids.
/// Panics if any LookupRef variants remain — only use when lookup refs are known to be absent
/// (e.g., in tests without a DB).
impl From<DatomExpanded> for DatomWithTempids {
    fn from(d: DatomExpanded) -> Self {
        DatomWithTempids {
            entity: match d.entity {
                EntityExpanded::Id(id) => IdOrTempId::Id(id),
                EntityExpanded::TempId(s) => IdOrTempId::TempId(s),
                EntityExpanded::LookupRef(_, _) => {
                    panic!("From<DatomExpanded>: unresolved lookup ref in entity position")
                }
            },
            attribute: d.attribute,
            value: match d.value {
                ValueExpanded::Data(dt) => ValueWithTempIds::Data(dt),
                ValueExpanded::TempRef(s) => ValueWithTempIds::TempRef(s),
                ValueExpanded::LookupRef(_, _) => {
                    panic!("From<DatomExpanded>: unresolved lookup ref in value position")
                }
            },
            op: d.op,
        }
    }
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
#[derive(Debug, Clone, PartialEq)]
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
        other => Err(anyhow::anyhow!(
            ":db/id must be Long, String, or Keyword, got {:?}",
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

/// Batch-resolve all lookup refs via the AVE index.
/// Converts DatomExpanded → DatomWithTempids, eliminating all LookupRef variants.
/// If no lookup refs are present, this is a cheap conversion with no I/O.
pub async fn resolve_lookup_refs(
    datoms: Vec<DatomExpanded>,
    schema: &Schema,
    txn: &slatedb::DbTransaction,
) -> Result<Vec<DatomWithTempids>> {
    // Collect all unique (attr_entid, value) lookup ref pairs.
    let mut lookup_refs: HashSet<(i64, DataType)> = HashSet::new();
    for d in &datoms {
        if let EntityExpanded::LookupRef(a, v) = &d.entity {
            lookup_refs.insert((*a, v.clone()));
        }
        if let ValueExpanded::LookupRef(a, v) = &d.value {
            lookup_refs.insert((*a, v.clone()));
        }
    }

    // Batch resolve via AVE index.
    let mut resolved_map: HashMap<(i64, DataType), i64> = HashMap::new();
    for (attr_eid, value) in &lookup_refs {
        let attr_bytes = encode_i64_bytes(*attr_eid);
        let mut value_bytes = Vec::new();
        encode_datatype(value, &mut value_bytes);
        let ave_prefix = concat_bytes(&[&[codec::AVE], &attr_bytes, &value_bytes]);

        let mut iter = txn
            .scan_prefix_with_options(&ave_prefix, &DEFAULT_SCAN_OPTIONS)
            .await?;

        match iter.next().await? {
            Some(kv) => {
                let (_attribute, _value, entity_dt, _tx_eid, _op) = ave_key_to_parts(kv.key)?;
                match entity_dt {
                    DataType::Long(eid) => {
                        resolved_map.insert((*attr_eid, value.clone()), eid);
                    }
                    other => {
                        return Err(anyhow::anyhow!(
                            "Expected Long entity ID in AVE key, got {:?}",
                            other
                        ));
                    }
                }
            }
            None => {
                let attr_kw = schema
                    .entid_map
                    .get(attr_eid)
                    .map(|kw| kw.to_string())
                    .unwrap_or_else(|| attr_eid.to_string());
                return Err(anyhow::anyhow!(
                    "No entity found for lookup ref [{} {:?}]",
                    attr_kw,
                    value
                ));
            }
        }
    }

    // Convert DatomExpanded → DatomWithTempids, replacing lookup refs with resolved IDs.
    let mut result = Vec::with_capacity(datoms.len());
    for d in datoms {
        let entity = match d.entity {
            EntityExpanded::Id(id) => IdOrTempId::Id(id),
            EntityExpanded::TempId(s) => IdOrTempId::TempId(s),
            EntityExpanded::LookupRef(a, v) => {
                let eid = resolved_map[&(a, v)];
                IdOrTempId::Id(eid)
            }
        };
        let value = match d.value {
            ValueExpanded::Data(dt) => ValueWithTempIds::Data(dt),
            ValueExpanded::TempRef(s) => ValueWithTempIds::TempRef(s),
            ValueExpanded::LookupRef(a, v) => {
                let eid = resolved_map[&(a, v)];
                ValueWithTempIds::Data(DataType::Long(eid))
            }
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
