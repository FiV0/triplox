//! End-to-end tempid resolution.
//!
//! Tempid resolution owns the outer problem of eliminating placeholders before
//! commit. Identity upsert resolution is one phase within that process; any
//! tempids that do not adopt existing entities are allocated afterward.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::Result;
use edn::kw;

use crate::metadata::PartitionMap;
use crate::ops::{DataType, Datom, DatomOp, Entid};
use crate::partition::{dominant_partition, DB_PARTITION, DEFAULT_PARTITION, USER_PARTITION};
use crate::schema::Schema;
use crate::tx::{self, DatomWithTempids, IdOrTempId, ValueWithTempIds};
use crate::upsert_resolution::{Generation, TempIdMap, UniqueLookup};

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
    let resolved = tx::batch_lookup_unique_eids(db, &lookups).await?;

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

/// Per-tempid partition decision, computed once from the input datoms.
///
/// Encodes the only inference rule we use: a tempid asserting `:db/ident` is
/// a schema entity and routes to `DB_PARTITION`; everything else routes to
/// `USER_PARTITION`. Allocators downstream consult this map and never look at
/// attribute content themselves.
type TempIdPartitions = HashMap<String, u32>;

fn infer_tempid_partitions(datoms: &[DatomWithTempids]) -> TempIdPartitions {
    let mut out: TempIdPartitions = HashMap::new();
    for d in datoms {
        if let IdOrTempId::TempId(t) = &d.entity {
            if d.op == DatomOp::Assert && d.attribute == kw!(:db/ident) {
                // `:db/ident` always wins, regardless of ordering.
                out.insert(t.clone(), DB_PARTITION);
            } else {
                out.entry(t.clone()).or_insert(USER_PARTITION);
            }
        }
        if let ValueWithTempIds::TempRef(t) = &d.value {
            out.entry(t.clone()).or_insert(USER_PARTITION);
        }
    }
    out
}

fn allocation_partitions(
    inferred: &TempIdPartitions,
    tempid_labels: &BTreeMap<String, usize>,
) -> Vec<u32> {
    let label_count = tempid_labels.values().copied().max().map_or(0, |n| n + 1);
    let mut partitions = vec![DEFAULT_PARTITION; label_count];
    for (tempid, &label) in tempid_labels {
        if let Some(&p) = inferred.get(tempid) {
            partitions[label] = dominant_partition(partitions[label], p);
        }
    }
    partitions
}

/// Resolve tempids by first attempting `:db.unique/identity` upserts, then
/// allocating any remaining tempid groups.
pub async fn resolve_tempids(
    datoms: Vec<DatomWithTempids>,
    schema: &Schema,
    db: &slatedb::Db,
    partition_map: &mut PartitionMap,
) -> Result<Vec<Datom>> {
    let inferred = infer_tempid_partitions(&datoms);
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
    let partitions = allocation_partitions(&inferred, &tempid_labels);

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

#[cfg(test)]
mod tests {
    use super::*;
    use edn::kw;

    use crate::partition::{extract_counter, extract_partition};
    use crate::schema::{Attribute, Unique, ValueType, DB_IDENT};
    use crate::slate::in_memory_slate;

    fn tempid_test_schema() -> Schema {
        let mut schema = Schema::default();
        for (kw, eid, value_type, unique) in [
            (
                kw!(:db/ident),
                DB_IDENT,
                ValueType::Keyword,
                Some(Unique::Identity),
            ),
            (kw!(:name), 100, ValueType::String, None),
            (kw!(:age), 101, ValueType::Long, None),
            (kw!(:follows), 102, ValueType::Ref, None),
        ] {
            schema.ident_map.insert(kw.clone(), eid);
            schema.entid_map.insert(eid, kw);
            schema.attribute_map.insert(
                eid,
                Attribute {
                    value_type,
                    multival: false,
                    unique,
                },
            );
        }
        schema
    }

    async fn resolve(datoms: Vec<DatomWithTempids>) -> (Vec<Datom>, PartitionMap) {
        let slate = in_memory_slate().await;
        let schema = tempid_test_schema();
        let mut pm = PartitionMap::new();
        let resolved = resolve_tempids(datoms, &schema, &slate.db, &mut pm)
            .await
            .unwrap();
        (resolved, pm)
    }

    #[tokio::test]
    async fn test_resolve_same_tempid_same_entid() {
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
        let (resolved, _) = resolve(datoms).await;
        assert_eq!(resolved[0].entity, resolved[1].entity);
    }

    #[tokio::test]
    async fn test_resolve_different_tempids_different_entids() {
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
        let (resolved, _) = resolve(datoms).await;
        assert_ne!(resolved[0].entity, resolved[1].entity);
    }

    #[tokio::test]
    async fn test_resolve_tempref_in_value() {
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
        let (resolved, _) = resolve(datoms).await;
        let alice_eid = resolved
            .iter()
            .find(|d| d.attribute == kw!(:name))
            .unwrap()
            .entity;
        assert!(resolved
            .iter()
            .any(|d| d.attribute == kw!(:follows) && d.value == DataType::Long(alice_eid)));
    }

    #[tokio::test]
    async fn test_resolve_db_ident_goes_to_db_partition() {
        let datoms = vec![DatomWithTempids {
            entity: IdOrTempId::TempId("schema-attr".to_string()),
            attribute: kw!(:db/ident),
            value: ValueWithTempIds::Data(DataType::Keyword(kw!(:my/attr))),
            op: DatomOp::Assert,
        }];
        let (resolved, _) = resolve(datoms).await;
        assert_eq!(extract_partition(resolved[0].entity), DB_PARTITION);
    }

    #[tokio::test]
    async fn test_resolve_regular_tempid_goes_to_user_partition() {
        let datoms = vec![DatomWithTempids {
            entity: IdOrTempId::TempId("user-entity".to_string()),
            attribute: kw!(:name),
            value: ValueWithTempIds::Data(DataType::String("alice".to_string())),
            op: DatomOp::Assert,
        }];
        let (resolved, _) = resolve(datoms).await;
        assert_eq!(extract_partition(resolved[0].entity), USER_PARTITION);
    }

    #[tokio::test]
    async fn test_resolve_id_passthrough() {
        let datoms = vec![DatomWithTempids {
            entity: IdOrTempId::Id(42),
            attribute: kw!(:name),
            value: ValueWithTempIds::Data(DataType::String("alice".to_string())),
            op: DatomOp::Assert,
        }];
        let (resolved, pm) = resolve(datoms).await;
        assert_eq!(resolved[0].entity, 42);
        assert!(pm.is_empty(), "no allocation for explicit IDs");
    }

    #[tokio::test]
    async fn test_resolve_counter_advances() {
        let slate = in_memory_slate().await;
        let schema = tempid_test_schema();
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
        let resolved = resolve_tempids(datoms, &schema, &slate.db, &mut pm)
            .await
            .unwrap();
        let mut counters: Vec<i64> = resolved.iter().map(|d| extract_counter(d.entity)).collect();
        counters.sort();
        assert_eq!(counters, vec![5, 6]);
        assert_eq!(pm[&USER_PARTITION], 7);
    }
}
