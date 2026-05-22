use anyhow::{anyhow, Result};
use dbsp::{utils::Tup2, OrdZSet, ZWeight};

use crate::codec::Encode;
use crate::incremental::EncodedTriple;
use crate::ops::{DataType, Datom, DatomOp};
use crate::schema::Schema;

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
