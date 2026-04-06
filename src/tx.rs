use std::collections::HashMap;

use anyhow::Result;
use edn::kw;
use edn::symbols::Keyword;

use crate::metadata::PartitionMap;
use crate::ops::{DataType, Datom, DatomOp, EntityRef, TxOp, TxValue};
use crate::partition::{DB_PARTITION, USER_PARTITION};
use crate::schema::Schema;

/// Entity reference after ident resolution: either a concrete ID or an unresolved tempid.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IdOrTempId {
    Id(i64),
    TempId(String),
}

/// Value after ident resolution: either concrete data or a tempid reference.
#[derive(Debug, Clone, PartialEq)]
pub enum ValueWithTempIds {
    Data(DataType),
    TempRef(String),
}

/// A datom-shaped tuple after TxOp expansion and ident resolution, but before tempid allocation.
#[derive(Debug, Clone, PartialEq)]
pub struct DatomWithTempids {
    pub entity: IdOrTempId,
    pub attribute: Keyword,
    pub value: ValueWithTempIds,
    pub op: DatomOp,
}

/// Resolve an EntityRef to IdOrTempId, resolving idents via schema.
fn resolve_entity_ref(eref: &EntityRef, schema: &Schema) -> Result<IdOrTempId> {
    match eref {
        EntityRef::Id(id) => Ok(IdOrTempId::Id(*id)),
        EntityRef::TempId(s) => Ok(IdOrTempId::TempId(s.clone())),
        EntityRef::Ident(kw) => {
            let eid = schema.ident_map.get(kw)
                .ok_or_else(|| anyhow::anyhow!("Unknown ident: {}", kw))?;
            Ok(IdOrTempId::Id(*eid))
        }
        EntityRef::LookupRef(_, _) => Err(anyhow::anyhow!("Lookup refs not yet supported")),
    }
}

/// Resolve a TxValue to ValueWithTempIds, resolving idents via schema.
fn resolve_tx_value(val: &TxValue, schema: &Schema) -> Result<ValueWithTempIds> {
    match val {
        TxValue::Data(d) => Ok(ValueWithTempIds::Data(d.clone())),
        TxValue::Ref(EntityRef::Id(id)) => Ok(ValueWithTempIds::Data(DataType::Long(*id))),
        TxValue::Ref(EntityRef::TempId(s)) => Ok(ValueWithTempIds::TempRef(s.clone())),
        TxValue::Ref(EntityRef::Ident(kw)) => {
            let eid = schema.ident_map.get(kw)
                .ok_or_else(|| anyhow::anyhow!("Unknown ident in value position: {}", kw))?;
            Ok(ValueWithTempIds::Data(DataType::Long(*eid)))
        }
        TxValue::Ref(EntityRef::LookupRef(_, _)) => {
            Err(anyhow::anyhow!("Lookup refs not yet supported"))
        }
    }
}

/// Expand TxOps into DatomWithTempids, resolving idents via schema.
///
/// - `Put(map)` → N DatomWithTempids (one per non-`:db/id` attr). The `:db/id` key
///   must map to `TxValue::Ref(EntityRef)`. If absent, generates an internal tempid.
/// - `Add/Retract` → 1 DatomWithTempids
/// - `Delete/Erase` → panics (not yet implemented)
pub fn expand_tx_ops(ops: &[TxOp], schema: &Schema) -> Result<Vec<DatomWithTempids>> {
    let db_id_kw = Keyword::namespaced("db", "id");
    let mut datoms = Vec::new();
    let mut auto_counter: u64 = 0;

    for op in ops {
        match op {
            TxOp::Put(map) => {
                let entity = match map.get(&db_id_kw) {
                    Some(TxValue::Ref(eref)) => resolve_entity_ref(eref, schema)?,
                    Some(other) => return Err(anyhow::anyhow!(
                        "Put :db/id must be TxValue::Ref(EntityRef), got {:?}", other
                    )),
                    None => {
                        let tempid = format!("__auto_{}", auto_counter);
                        auto_counter += 1;
                        IdOrTempId::TempId(tempid)
                    }
                };
                for (attr, value) in map.iter().filter(|(k, _)| *k != &db_id_kw) {
                    datoms.push(DatomWithTempids {
                        entity: entity.clone(),
                        attribute: attr.clone(),
                        value: resolve_tx_value(value, schema)?,
                        op: DatomOp::Assert,
                    });
                }
            }
            TxOp::Add { entity, attribute, value } => {
                datoms.push(DatomWithTempids {
                    entity: resolve_entity_ref(entity, schema)?,
                    attribute: attribute.clone(),
                    value: resolve_tx_value(value, schema)?,
                    op: DatomOp::Assert,
                });
            }
            TxOp::Retract { entity, attribute, value } => {
                datoms.push(DatomWithTempids {
                    entity: resolve_entity_ref(entity, schema)?,
                    attribute: attribute.clone(),
                    value: resolve_tx_value(value, schema)?,
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

/// Resolve tempids in DatomWithTempids to produce final Datoms.
///
/// Tempids with a `:db/ident` datom are allocated from DB_PARTITION;
/// all others from USER_PARTITION.
pub fn resolve_tempids(
    datoms: &[DatomWithTempids],
    partition_map: &mut PartitionMap,
    _schema: &Schema,
) -> Result<Vec<Datom>> {
    // Pre-scan: determine partition for each tempid
    let mut tempid_partitions: HashMap<&str, u32> = HashMap::new();
    for d in datoms {
        if let IdOrTempId::TempId(ref s) = d.entity {
            if d.attribute == kw!(:db/ident) {
                tempid_partitions.insert(s, DB_PARTITION);
            } else {
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

    // --- expand_tx_ops tests ---

    #[test]
    fn test_expand_put_with_id() {
        let ops = vec![TxOp::put_with_id(100_i64, vec![
            (kw!(:name), "alice".into()),
            (kw!(:age), 30_i64.into()),
        ])];
        let datoms = expand_tx_ops(&ops, &empty_schema()).unwrap();
        assert_eq!(datoms.len(), 2);
        assert!(datoms.iter().all(|d| d.entity == IdOrTempId::Id(100)));
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
        // Different auto-tempids
        assert_ne!(datoms[0].entity, datoms[1].entity);
        assert!(matches!(datoms[0].entity, IdOrTempId::TempId(_)));
        assert!(matches!(datoms[1].entity, IdOrTempId::TempId(_)));
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
            value: TxValue::Data(DataType::String("bob".to_string())),
        }];
        let datoms = expand_tx_ops(&ops, &empty_schema()).unwrap();
        assert_eq!(datoms.len(), 1);
        assert_eq!(datoms[0].entity, IdOrTempId::Id(200));
        assert_eq!(datoms[0].attribute, kw!(:name));
        assert_eq!(datoms[0].value, ValueWithTempIds::Data(DataType::String("bob".to_string())));
        assert_eq!(datoms[0].op, DatomOp::Assert);
    }

    #[test]
    fn test_expand_retract() {
        let ops = vec![TxOp::Retract {
            entity: EntityRef::Id(200),
            attribute: kw!(:name),
            value: TxValue::Data(DataType::String("bob".to_string())),
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
            value: TxValue::Data(DataType::Long(1)),
        }];
        let datoms = expand_tx_ops(&ops, &schema).unwrap();
        assert_eq!(datoms[0].entity, IdOrTempId::Id(42));
    }

    #[test]
    fn test_expand_unknown_ident_errors() {
        let ops = vec![TxOp::Add {
            entity: EntityRef::Ident(kw!(:unknown/ident)),
            attribute: kw!(:some/attr),
            value: TxValue::Data(DataType::Long(1)),
        }];
        let result = expand_tx_ops(&ops, &empty_schema());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unknown ident"));
    }

    #[test]
    fn test_expand_lookup_ref_errors() {
        let ops = vec![TxOp::Add {
            entity: EntityRef::LookupRef(kw!(:email), DataType::String("a@b.com".into())),
            attribute: kw!(:name),
            value: TxValue::Data(DataType::Long(1)),
        }];
        let result = expand_tx_ops(&ops, &empty_schema());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Lookup refs"));
    }

    #[test]
    fn test_expand_value_ref_tempid() {
        let ops = vec![TxOp::Add {
            entity: EntityRef::Id(100),
            attribute: kw!(:follows),
            value: TxValue::Ref(EntityRef::TempId("friend".to_string())),
        }];
        let datoms = expand_tx_ops(&ops, &empty_schema()).unwrap();
        assert_eq!(datoms[0].value, ValueWithTempIds::TempRef("friend".to_string()));
    }

    #[test]
    fn test_expand_value_ref_id() {
        let ops = vec![TxOp::Add {
            entity: EntityRef::Id(100),
            attribute: kw!(:follows),
            value: TxValue::Ref(EntityRef::Id(200)),
        }];
        let datoms = expand_tx_ops(&ops, &empty_schema()).unwrap();
        assert_eq!(datoms[0].value, ValueWithTempIds::Data(DataType::Long(200)));
    }

    #[test]
    fn test_expand_value_ref_ident() {
        let schema = schema_with_ident(kw!(:person/bob), 99);
        let ops = vec![TxOp::Add {
            entity: EntityRef::Id(100),
            attribute: kw!(:follows),
            value: TxValue::Ref(EntityRef::Ident(kw!(:person/bob))),
        }];
        let datoms = expand_tx_ops(&ops, &schema).unwrap();
        assert_eq!(datoms[0].value, ValueWithTempIds::Data(DataType::Long(99)));
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

    use crate::partition::{extract_partition, extract_counter};

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
        let resolved = resolve_tempids(&datoms, &mut pm, &empty_schema()).unwrap();
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
        let resolved = resolve_tempids(&datoms, &mut pm, &empty_schema()).unwrap();
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
        let resolved = resolve_tempids(&datoms, &mut pm, &empty_schema()).unwrap();
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
        let resolved = resolve_tempids(&datoms, &mut pm, &empty_schema()).unwrap();
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
        let resolved = resolve_tempids(&datoms, &mut pm, &empty_schema()).unwrap();
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
        let resolved = resolve_tempids(&datoms, &mut pm, &empty_schema()).unwrap();
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
        let resolved = resolve_tempids(&datoms, &mut pm, &empty_schema()).unwrap();
        let mut counters: Vec<i64> = resolved.iter().map(|d| extract_counter(d.entity)).collect();
        counters.sort();
        assert_eq!(counters, vec![5, 6]);
        assert_eq!(pm[&USER_PARTITION], 7);
    }
}
