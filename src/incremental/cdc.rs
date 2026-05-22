use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use dbsp::{utils::Tup2, OrdZSet, ZWeight};

use crate::codec::Encode;
use crate::inc_query::IncrementalQueryPlan;
use crate::incremental::EncodedTriple;
use crate::indexer::eav_key_to_parts;
use crate::ops::{DataType, Datom, DatomOp};
use crate::schema::Schema;
use crate::slate::DEFAULT_SCAN_OPTIONS;
use crate::{codec, util::concat_bytes};

pub(crate) fn datoms_to_zset(datoms: &[Datom], schema: &Schema) -> Result<OrdZSet<EncodedTriple>> {
    let tuples = datoms
        .iter()
        .map(|datom| {
            let (attribute, _) = schema
                .get_attribute(&datom.attribute)
                .ok_or_else(|| anyhow!("Unknown attribute: {}", datom.attribute))?;
            let weight: ZWeight = match datom.op {
                DatomOp::Assert => 1,
                DatomOp::Retract => -1,
            };
            Ok(Tup2(
                EncodedTriple {
                    entity: DataType::Long(datom.entity).encode(),
                    attribute,
                    value: datom.value.encode(),
                },
                weight,
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(OrdZSet::from_keys((), tuples))
}

pub(crate) async fn scan_current_triples<D>(
    db: &D,
    plan: &IncrementalQueryPlan,
    as_of_tx_eid: i64,
) -> Result<Vec<Tup2<EncodedTriple, ZWeight>>>
where
    D: slatedb::DbReadOps + Sync,
{
    let attributes = plan
        .patterns
        .iter()
        .map(|pattern| pattern.attribute)
        .collect::<HashSet<_>>();
    let mut latest_by_triple: HashMap<EncodedTriple, (i64, u8)> = HashMap::new();
    let mut iter = db
        .scan_with_options(
            concat_bytes(&[&[codec::EAV]])..vec![codec::EAV_END],
            &DEFAULT_SCAN_OPTIONS,
        )
        .await?;

    while let Some(kv) = iter.next().await? {
        let (entity, attribute, value, tx_eid, op) = eav_key_to_parts(Bytes::from(kv.key))?;
        if tx_eid > as_of_tx_eid || !attributes.contains(&attribute) {
            continue;
        }

        let entity = match entity {
            DataType::Long(entity) => DataType::Long(entity).encode(),
            other => return Err(anyhow!("Expected Long entity in EAV key, got {:?}", other)),
        };
        match op {
            codec::ADD | codec::RETRACT => {}
            other => return Err(anyhow!("Unknown op byte: {}", other)),
        }

        let triple = EncodedTriple {
            entity,
            attribute,
            value: value.encode(),
        };
        let should_replace = latest_by_triple
            .get(&triple)
            .is_none_or(|(latest_tx_eid, _)| tx_eid >= *latest_tx_eid);
        if should_replace {
            latest_by_triple.insert(triple, (tx_eid, op));
        }
    }

    let mut triples = latest_by_triple
        .into_iter()
        .filter_map(|(triple, (_tx_eid, op))| (op == codec::ADD).then_some(Tup2(triple, 1)))
        .collect::<Vec<_>>();
    triples.sort();
    Ok(triples)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use dbsp::{typed_batch::IndexedZSetReader, ZSet};
    use edn::kw;

    use super::*;
    use crate::schema::{Attribute, Schema, ValueType};

    fn test_schema() -> Schema {
        let name = kw!(:name);
        let age = kw!(:age);
        let mut ident_map = HashMap::new();
        ident_map.insert(name.clone(), 10);
        ident_map.insert(age.clone(), 11);

        let mut entid_map = HashMap::new();
        entid_map.insert(10, name);
        entid_map.insert(11, age);

        let mut attribute_map = HashMap::new();
        attribute_map.insert(
            10,
            Attribute {
                value_type: ValueType::String,
                multival: true,
                unique: None,
            },
        );
        attribute_map.insert(
            11,
            Attribute {
                value_type: ValueType::Long,
                multival: true,
                unique: None,
            },
        );

        Schema {
            entid_map,
            ident_map,
            attribute_map,
        }
    }

    fn zset_tuples(zset: &OrdZSet<EncodedTriple>) -> Vec<(EncodedTriple, ZWeight)> {
        zset.iter()
            .map(|(triple, (), weight)| (triple, weight))
            .collect()
    }

    #[test]
    fn assert_datom_becomes_positive_encoded_triple() {
        let schema = test_schema();
        let datoms = [Datom {
            entity: 42,
            attribute: kw!(:name),
            value: DataType::String("Alice".to_string()),
            op: DatomOp::Assert,
        }];

        let zset = datoms_to_zset(&datoms, &schema).unwrap();
        let tuples = zset_tuples(&zset);

        assert_eq!(zset.weighted_count(), 1);
        assert_eq!(
            tuples,
            vec![(
                EncodedTriple {
                    entity: DataType::Long(42).encode(),
                    attribute: 10,
                    value: DataType::String("Alice".to_string()).encode(),
                },
                1,
            )]
        );
    }

    #[test]
    fn retract_datom_becomes_negative_encoded_triple() {
        let schema = test_schema();
        let datoms = [Datom {
            entity: 42,
            attribute: kw!(:age),
            value: DataType::Long(30),
            op: DatomOp::Retract,
        }];

        let zset = datoms_to_zset(&datoms, &schema).unwrap();
        let tuples = zset_tuples(&zset);

        assert_eq!(zset.weighted_count(), -1);
        assert_eq!(
            tuples,
            vec![(
                EncodedTriple {
                    entity: DataType::Long(42).encode(),
                    attribute: 11,
                    value: DataType::Long(30).encode(),
                },
                -1,
            )]
        );
    }

    #[test]
    fn unknown_attribute_errors() {
        let schema = test_schema();
        let datoms = [Datom {
            entity: 42,
            attribute: kw!(:unknown),
            value: DataType::Long(30),
            op: DatomOp::Assert,
        }];

        let err = datoms_to_zset(&datoms, &schema).unwrap_err();
        assert!(err.to_string().contains("Unknown attribute: :unknown"));
    }

    #[test]
    fn duplicate_triples_are_consolidated() {
        let schema = test_schema();
        let datoms = [
            Datom {
                entity: 42,
                attribute: kw!(:age),
                value: DataType::Long(30),
                op: DatomOp::Assert,
            },
            Datom {
                entity: 42,
                attribute: kw!(:age),
                value: DataType::Long(30),
                op: DatomOp::Assert,
            },
            Datom {
                entity: 42,
                attribute: kw!(:age),
                value: DataType::Long(30),
                op: DatomOp::Retract,
            },
        ];

        let zset = datoms_to_zset(&datoms, &schema).unwrap();
        let tuples = zset_tuples(&zset);

        assert_eq!(zset.weighted_count(), 1);
        assert_eq!(
            tuples,
            vec![(
                EncodedTriple {
                    entity: DataType::Long(42).encode(),
                    attribute: 11,
                    value: DataType::Long(30).encode(),
                },
                1,
            )]
        );
    }
}
