use std::collections::HashMap;

use crate::codec;
use crate::protocol;

/// Entity ID bit layout (from PARTITIONS.md):
///
/// ```text
///  63  62   61                    42  41                              0
/// +----+----+---------------------+---+-------------------------------+
/// | S  | 0  | partition (20 bits) |          counter (42 bits)        |
/// +----+----+---------------------+---+-------------------------------+
/// ```

pub const COUNTER_BITS: u32 = 42;
pub const COUNTER_MASK: i64 = (1i64 << 42) - 1;
pub const PARTITION_MASK: i64 = ((1i64 << 20) - 1) << 42;

pub const DB_PARTITION: u32 = 0;
pub const TX_PARTITION: u32 = 1;
pub const USER_PARTITION: u32 = 2;

/// Return the encoded byte prefix shared by all entity IDs in the given partition.
///
/// The prefix is `TAG_LONG` followed by the high bytes of the order-preserving
/// encoding of the partition's base entity ID (`make_entity_id(partition, 0)`),
/// truncated to the 3 bytes that carry the partition bits.
///
/// For TX_PARTITION this produces `[7, 0x7F, 0xFF, 0xFB]`.
///
/// **Note**: the partition/counter boundary (bit 42) is not byte-aligned, so
/// byte 2 contains both partition bits (47–42) and counter bits (41–40). The
/// 3-byte prefix fixes those counter bits to zero, limiting coverage to
/// counter < 2^40 (~1 trillion) — sufficient for all practical use.
pub fn partition_entity_prefix(partition: u32) -> Vec<u8> {
    let base = make_entity_id(partition, 0);
    let encoded = codec::encode_i64_bytes(base);
    let mut prefix = vec![protocol::TAG_LONG];
    prefix.extend_from_slice(&encoded[..3]);
    prefix
}

/// Construct an entity ID from a partition number and counter value.
///
/// For partition 0, the result equals the counter (small, readable IDs).
pub fn make_entity_id(partition: u32, counter: i64) -> i64 {
    assert!(partition < (1 << 20), "partition must fit in 20 bits");
    assert!(counter >= 0 && counter <= COUNTER_MASK, "counter must fit in 42 bits");
    ((partition as i64) << COUNTER_BITS) | counter
}

/// Extract the partition number (bits 61–42) from an entity ID.
pub fn extract_partition(eid: i64) -> u32 {
    ((eid & PARTITION_MASK) >> COUNTER_BITS) as u32
}

/// Extract the counter value (bits 41–0) from an entity ID.
pub fn extract_counter(eid: i64) -> i64 {
    eid & COUNTER_MASK
}

/// Per-partition counter map: partition number → next counter value to allocate.
pub type PartitionCounters = HashMap<u32, i64>;

/// Allocate the next counter value for a partition, returning the current value
/// and incrementing it. Initializes to 0 if the partition has no entry yet.
pub fn allocate_counter(counters: &mut PartitionCounters, partition: u32) -> i64 {
    let counter = counters.entry(partition).or_insert(0);
    let value = *counter;
    *counter = value + 1;
    value
}

/// Returns true if the entity ID is a tempid (sign bit set, i.e. negative).
pub fn is_tempid(eid: i64) -> bool {
    eid < 0
}

/// Resolve entity IDs for Put documents that lack a `db/id` field.
///
/// - If `db/id` is `DataType::Long` → keep as-is (explicit ID).
/// - If `db/id` is missing → allocate from per-partition counters:
///   - Documents with `db/ident` → `DB_PARTITION` (schema/enum entities).
///   - All other documents → `USER_PARTITION`.
/// - Add/Retract/Delete/Erase → pass through unchanged (require explicit IDs).
pub fn resolve_entity_ids(ops: &[crate::ops::TxOp], counters: &mut PartitionCounters) -> anyhow::Result<Vec<crate::ops::TxOp>> {
    use crate::ops::{TxOp, DataType};

    let mut resolved = Vec::with_capacity(ops.len());
    for op in ops {
        match op {
            TxOp::Put(doc) => {
                match doc.get("db/id") {
                    Some(DataType::Long(_)) => {
                        resolved.push(op.clone());
                    }
                    None => {
                        let partition = if doc.contains_key("db/ident") {
                            DB_PARTITION
                        } else {
                            USER_PARTITION
                        };
                        let eid = make_entity_id(partition, allocate_counter(counters, partition));
                        let mut new_doc = doc.clone();
                        new_doc.insert("db/id".to_string(), DataType::Long(eid));
                        resolved.push(TxOp::Put(new_doc));
                    }
                    Some(_) => {
                        return Err(anyhow::anyhow!("Document db/id must be a Long"));
                    }
                }
            }
            _ => resolved.push(op.clone()),
        }
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_zero_preserves_counter() {
        assert_eq!(make_entity_id(DB_PARTITION, 0), 0);
        assert_eq!(make_entity_id(DB_PARTITION, 1), 1);
        assert_eq!(make_entity_id(DB_PARTITION, 31), 31);
        assert_eq!(make_entity_id(DB_PARTITION, 100), 100);
    }

    #[test]
    fn test_bootstrap_ids_in_db_partition() {
        // DB_PARTITION is 0, so make_entity_id(0, n) == n
        assert_eq!(make_entity_id(DB_PARTITION, crate::schema::DB_IDENT), crate::schema::DB_IDENT);
        assert_eq!(make_entity_id(DB_PARTITION, crate::schema::DB_VALUE_TYPE), crate::schema::DB_VALUE_TYPE);
        assert_eq!(make_entity_id(DB_PARTITION, crate::schema::DB_CARDINALITY), crate::schema::DB_CARDINALITY);
        assert_eq!(make_entity_id(DB_PARTITION, crate::schema::DB_CARDINALITY_ONE), crate::schema::DB_CARDINALITY_ONE);
        assert_eq!(make_entity_id(DB_PARTITION, crate::schema::DB_CARDINALITY_MANY), crate::schema::DB_CARDINALITY_MANY);
    }

    #[test]
    fn test_round_trip_db_partition() {
        let eid = make_entity_id(DB_PARTITION, 42);
        assert_eq!(extract_partition(eid), DB_PARTITION);
        assert_eq!(extract_counter(eid), 42);
    }

    #[test]
    fn test_round_trip_tx_partition() {
        let eid = make_entity_id(TX_PARTITION, 100);
        assert_eq!(extract_partition(eid), TX_PARTITION);
        assert_eq!(extract_counter(eid), 100);
        assert_eq!(eid, (1i64 << 42) | 100);
    }

    #[test]
    fn test_round_trip_user_partition() {
        let eid = make_entity_id(USER_PARTITION, 500);
        assert_eq!(extract_partition(eid), USER_PARTITION);
        assert_eq!(extract_counter(eid), 500);
        assert_eq!(eid, (2i64 << 42) | 500);
    }

    #[test]
    fn test_is_tempid() {
        assert!(!is_tempid(0));
        assert!(!is_tempid(1));
        assert!(!is_tempid(make_entity_id(USER_PARTITION, 100)));
        assert!(is_tempid(-1));
        assert!(is_tempid(-100));
        assert!(is_tempid(i64::MIN));
    }

    #[test]
    #[should_panic(expected = "partition must fit in 20 bits")]
    fn test_partition_overflow() {
        make_entity_id(1 << 20, 0);
    }

    #[test]
    #[should_panic(expected = "counter must fit in 42 bits")]
    fn test_counter_overflow() {
        make_entity_id(0, COUNTER_MASK + 1);
    }

    #[test]
    #[should_panic(expected = "counter must fit in 42 bits")]
    fn test_counter_negative() {
        make_entity_id(0, -1);
    }

    #[test]
    fn test_partitions_non_overlapping() {
        let db_max = make_entity_id(DB_PARTITION, COUNTER_MASK);
        let tx_min = make_entity_id(TX_PARTITION, 0);
        let tx_max = make_entity_id(TX_PARTITION, COUNTER_MASK);
        let user_min = make_entity_id(USER_PARTITION, 0);
        assert!(db_max < tx_min);
        assert!(tx_max < user_min);
    }

    // --- resolve_entity_ids tests ---

    use std::collections::BTreeMap;
    use crate::ops::{TxOp, DataType, Attribute};

    #[test]
    fn test_resolve_entity_ids_explicit_id_passthrough() {
        let mut counters = PartitionCounters::new();
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(42));
        doc.insert("name".to_string(), DataType::String("alice".into()));
        let ops = vec![TxOp::Put(doc)];

        let resolved = resolve_entity_ids(&ops, &mut counters).unwrap();
        assert!(counters.is_empty(), "counters should not advance for explicit IDs");
        match &resolved[0] {
            TxOp::Put(doc) => assert_eq!(doc.get("db/id"), Some(&DataType::Long(42))),
            _ => panic!("Expected Put"),
        }
    }

    #[test]
    fn test_resolve_entity_ids_user_partition() {
        let mut counters = PartitionCounters::new();
        let mut doc = BTreeMap::new();
        doc.insert("name".to_string(), DataType::String("alice".into()));
        let ops = vec![TxOp::Put(doc)];

        let resolved = resolve_entity_ids(&ops, &mut counters).unwrap();
        assert_eq!(counters[&USER_PARTITION], 1);
        match &resolved[0] {
            TxOp::Put(doc) => {
                let eid = match doc.get("db/id") {
                    Some(DataType::Long(id)) => *id,
                    _ => panic!("Expected db/id Long"),
                };
                assert_eq!(extract_partition(eid), USER_PARTITION);
            }
            _ => panic!("Expected Put"),
        }
    }

    #[test]
    fn test_resolve_entity_ids_db_partition_for_schema() {
        let mut counters = PartitionCounters::new();
        let mut doc = BTreeMap::new();
        doc.insert("db/ident".to_string(), DataType::Keyword(
            edn::kw!(:my-attr),
        ));
        doc.insert("db/valueType".to_string(), DataType::Keyword(
            edn::kw!(:db.type/string),
        ));
        let ops = vec![TxOp::Put(doc)];

        let resolved = resolve_entity_ids(&ops, &mut counters).unwrap();
        assert_eq!(counters[&DB_PARTITION], 1);
        match &resolved[0] {
            TxOp::Put(doc) => {
                let eid = match doc.get("db/id") {
                    Some(DataType::Long(id)) => *id,
                    _ => panic!("Expected db/id Long"),
                };
                assert_eq!(extract_partition(eid), DB_PARTITION);
            }
            _ => panic!("Expected Put"),
        }
    }

    #[test]
    fn test_resolve_entity_ids_db_partition_for_enum() {
        let mut counters = PartitionCounters::new();
        let mut doc = BTreeMap::new();
        doc.insert("db/ident".to_string(), DataType::Keyword(
            edn::kw!(:db.type/string),
        ));
        let ops = vec![TxOp::Put(doc)];

        let resolved = resolve_entity_ids(&ops, &mut counters).unwrap();
        match &resolved[0] {
            TxOp::Put(doc) => {
                let eid = match doc.get("db/id") {
                    Some(DataType::Long(id)) => *id,
                    _ => panic!("Expected db/id Long"),
                };
                assert_eq!(extract_partition(eid), DB_PARTITION);
            }
            _ => panic!("Expected Put"),
        }
    }

    #[test]
    fn test_resolve_entity_ids_add_retract_passthrough() {
        let mut counters = PartitionCounters::new();
        let ops = vec![
            TxOp::Add {
                entity_id: 100,
                attribute: Attribute("name".into()),
                value: DataType::String("alice".into()),
            },
            TxOp::Retract {
                entity_id: 100,
                attribute: Attribute("name".into()),
                value: DataType::String("alice".into()),
            },
        ];

        let resolved = resolve_entity_ids(&ops, &mut counters).unwrap();
        assert!(counters.is_empty(), "Add/Retract should not allocate IDs");
        assert_eq!(resolved.len(), 2);
    }

    #[test]
    fn test_resolve_entity_ids_multiple_docs() {
        let mut counters = PartitionCounters::from([(USER_PARTITION, 5i64)]);
        let ops = vec![
            TxOp::Put({
                let mut m = BTreeMap::new();
                m.insert("name".to_string(), DataType::String("alice".into()));
                m
            }),
            TxOp::Put({
                let mut m = BTreeMap::new();
                m.insert("name".to_string(), DataType::String("bob".into()));
                m
            }),
        ];

        let resolved = resolve_entity_ids(&ops, &mut counters).unwrap();
        assert_eq!(counters[&USER_PARTITION], 7);

        for (i, op) in resolved.iter().enumerate() {
            match op {
                TxOp::Put(doc) => {
                    let eid = match doc.get("db/id") {
                        Some(DataType::Long(id)) => *id,
                        _ => panic!("Expected db/id Long"),
                    };
                    assert_eq!(extract_partition(eid), USER_PARTITION);
                    assert_eq!(extract_counter(eid), 5 + i as i64);
                }
                _ => panic!("Expected Put"),
            }
        }
    }
}
