#[allow(unused_imports)]
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
#[allow(unused_imports)]
use edn::symbols::{Keyword, NamespacedSymbol};
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;
use anyhow::Result;
use crate::clock::Instant;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct EntityId(pub i64);

impl EntityId {
    pub fn new(id: i64) -> Self {
        EntityId(id)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct Attribute(pub String);

// TODO: Ref commented out for now — entity refs are stored as DataType::Long.
// Revisit when schema is added (ref-typed attributes should use entity encoding).
// pub type Ref = i64;

// TODO maybe use also clock::Instant here
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum DataType {
    //BigDecimal(BigDecimal),          // Arbitrary precision decimal numbers
    BigInt(i128),  // Arbitrary large integers
    Boolean(bool), // Booleans (true or false)
    // TODO: use Bytes instead of Vec<u8> ?
    Bytes(Vec<u8>),         // Binary data (as bytes)
    Double(f64),            // Double precision floating point
    Float(f32),             // Single precision floating point
    Instant(DateTime<Utc>), // Timestamps or instants
    Keyword(Keyword),       // Keywords
    Long(i64),              // Long integers
    // TODO: Ref commented out — entity refs stored as Long for now. Revisit with schema.
    // Ref(Ref),                     // Reference (for shared ownership, like pointers)
    String(String), // Strings
    // Symbol(NamespacedSymbol),                  // Symbols (can be represented as strings)
    Tuple(Vec<DataType>), // Tuples (can be represented as a vector of DataTypes)
    Uuid(Uuid),           // Universally unique identifier
    // TODO
    //Uri(Uri),                        // URIs (could also be represented as strings)

    // Composite types
    Vector(Vec<DataType>), // List (vector of DataTypes)
    // TODO think about tradeoffs of using a BTreeSet vs a HashSet
    //Set(BTreeSet<DataType>),         // Set (BTreeSet of DataTypes)
    Map(BTreeMap<String, DataType>), // Map (BTreeMap of string keys and DataType values)
}


impl Eq for DataType {}

impl std::hash::Hash for DataType {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            DataType::BigInt(v) => v.hash(state),
            DataType::Boolean(v) => v.hash(state),
            DataType::Bytes(v) => v.hash(state),
            DataType::Double(v) => v.to_bits().hash(state),
            DataType::Float(v) => v.to_bits().hash(state),
            DataType::Instant(v) => v.hash(state),
            DataType::Keyword(v) => v.hash(state),
            DataType::Long(v) => v.hash(state),
            DataType::String(v) => v.hash(state),
            DataType::Tuple(v) => v.hash(state),
            DataType::Uuid(v) => v.hash(state),
            DataType::Vector(v) => v.hash(state),
            DataType::Map(v) => v.hash(state),
        }
    }
}

impl DataType {
    /// Return the ValueType corresponding to this DataType's variant.
    pub fn value_type(&self) -> crate::schema::ValueType {
        use crate::schema::ValueType;
        match self {
            DataType::BigInt(_) => ValueType::BigInt,
            DataType::Boolean(_) => ValueType::Boolean,
            DataType::Bytes(_) => ValueType::Bytes,
            DataType::Double(_) => ValueType::Double,
            DataType::Float(_) => ValueType::Float,
            DataType::Instant(_) => ValueType::Instant,
            DataType::Keyword(_) => ValueType::Keyword,
            DataType::Long(_) => ValueType::Long,
            DataType::String(_) => ValueType::String,
            DataType::Tuple(_) => ValueType::Tuple,
            DataType::Uuid(_) => ValueType::Uuid,
            DataType::Vector(_) => ValueType::Vector,
            DataType::Map(_) => ValueType::Map,
        }
    }

    /// Compare two DataType values. Returns None if the types are incompatible
    /// or if floats are NaN.
    pub fn partial_compare(&self, other: &DataType) -> Option<std::cmp::Ordering> {
        use DataType::*;
        match (self, other) {
            (Long(a), Long(b)) => Some(a.cmp(b)),
            (BigInt(a), BigInt(b)) => Some(a.cmp(b)),
            (Double(a), Double(b)) => a.partial_cmp(b),
            (Float(a), Float(b)) => a.partial_cmp(b),
            (String(a), String(b)) => Some(a.cmp(b)),
            (Boolean(a), Boolean(b)) => Some(a.cmp(b)),
            (Instant(a), Instant(b)) => Some(a.cmp(b)),
            // Cross-numeric promotion
            (Long(a), BigInt(b)) => Some((*a as i128).cmp(b)),
            (BigInt(a), Long(b)) => Some(a.cmp(&(*b as i128))),
            (Long(a), Double(b)) => (*a as f64).partial_cmp(b),
            (Double(a), Long(b)) => a.partial_cmp(&(*b as f64)),
            _ => None,
        }
    }
}

macro_rules! impl_from_for_enum {
    ($enum_name:ident, $(($variant:ident, $type:ty)),*) => {
        $(
            impl From<$type> for $enum_name {
                fn from(value: $type) -> Self {
                    $enum_name::$variant(value)
                }
            }
        )*
    };
}

impl_from_for_enum!(
    DataType,
    (BigInt, i128),
    (Boolean, bool),
    (Bytes, Vec<u8>),
    (Double, f64),
    (Float, f32),
    (Instant, DateTime<Utc>),
    (Long, i64),
    // as Ref is a type alias for i64, we can't have From for both of them
    // (Ref, Ref),
    (String, String),
    (Uuid, Uuid),
    (Vector, Vec<DataType>),
    //(Set, BTreeSet<DataType>),
    (Map, BTreeMap<String, DataType>)
);

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct Document(pub BTreeMap<String, DataType>);

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum TxOp {
    Put(Document),
    Add { entity_id: EntityId, attribute: Attribute, value: DataType },
    Retract { entity_id: EntityId, attribute: Attribute, value: DataType },
    Delete(EntityId),
    Erase(EntityId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DatomOp {
    Assert,
    Retract,
}

/// A normalized fact: (entity, attribute, value, tx, op).
/// The attribute is an unresolved string name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Datom {
    pub entity: i64,
    pub attribute: String, // TODO(triplox-gaz): avoid cloning, consider Cow or interning
    pub value: DataType,
    pub tx: Instant,
    pub op: DatomOp,
}

/// Assign entity IDs to Put documents that lack a `db/id` field.
///
/// - If `db/id` is `DataType::Long` → keep as-is (explicit ID).
/// - If `db/id` is missing → allocate from the global counter:
///   - Documents with `db/ident` → `DB_PARTITION` (schema/enum entities).
///   - All other documents → `USER_PARTITION`.
/// - Add/Retract/Delete/Erase → pass through unchanged (require explicit IDs).
pub fn assign_entity_ids(ops: &[TxOp], next_t: &mut i64) -> Result<Vec<TxOp>> {
    use crate::partition::{make_entity_id, DB_PARTITION, USER_PARTITION};

    let mut resolved = Vec::with_capacity(ops.len());
    for op in ops {
        match op {
            TxOp::Put(Document(doc)) => {
                match doc.get("db/id") {
                    Some(DataType::Long(_)) => {
                        // Explicit ID — keep as-is
                        resolved.push(op.clone());
                    }
                    None => {
                        // Allocate a new entity ID
                        let partition = if doc.contains_key("db/ident") {
                            DB_PARTITION
                        } else {
                            USER_PARTITION
                        };
                        let eid = make_entity_id(partition, *next_t);
                        *next_t += 1;
                        let mut new_doc = doc.clone();
                        new_doc.insert("db/id".to_string(), DataType::Long(eid));
                        resolved.push(TxOp::Put(Document(new_doc)));
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

/// Expand TxOps into a flat vec of Datoms.
/// - Put(doc) → N Assert datoms (one per non-db/id field)
/// - Add(triple) → 1 Assert datom
/// - Retract(triple) → 1 Retract datom
/// - Delete/Erase → panics (not yet implemented)
pub fn tx_ops_to_datoms(ops: &[TxOp], tx: Instant) -> Result<Vec<Datom>> {
    let mut datoms = Vec::new();
    for op in ops {
        match op {
            TxOp::Put(Document(doc)) => {
                let entity = match doc.get("db/id") {
                    Some(DataType::Long(id)) => *id,
                    Some(_) => return Err(anyhow::anyhow!("Document db/id must be a Long")),
                    None => return Err(anyhow::anyhow!("Document must have a db/id")),
                };
                for (attr, value) in doc.iter().filter(|(k, _)| *k != "db/id") {
                    datoms.push(Datom {
                        entity,
                        attribute: attr.clone(),
                        value: value.clone(),
                        tx,
                        op: DatomOp::Assert,
                    });
                }
            }
            TxOp::Add { entity_id, attribute, value } => {
                datoms.push(Datom {
                    entity: entity_id.0,
                    attribute: attribute.0.clone(),
                    value: value.clone(),
                    tx,
                    op: DatomOp::Assert,
                });
            }
            TxOp::Retract { entity_id, attribute, value } => {
                datoms.push(Datom {
                    entity: entity_id.0,
                    attribute: attribute.0.clone(),
                    value: value.clone(),
                    tx,
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

#[cfg(test)]
mod tests {
    use super::*;
    use bincode;

    #[test]
    fn test_partial_compare_same_type() {
        use std::cmp::Ordering;
        assert_eq!(DataType::Long(1).partial_compare(&DataType::Long(2)), Some(Ordering::Less));
        assert_eq!(DataType::Long(2).partial_compare(&DataType::Long(2)), Some(Ordering::Equal));
        assert_eq!(DataType::Long(3).partial_compare(&DataType::Long(2)), Some(Ordering::Greater));

        assert_eq!(
            DataType::String("a".into()).partial_compare(&DataType::String("b".into())),
            Some(Ordering::Less),
        );
        assert_eq!(
            DataType::Boolean(false).partial_compare(&DataType::Boolean(true)),
            Some(Ordering::Less),
        );
        assert_eq!(
            DataType::Double(1.5).partial_compare(&DataType::Double(2.5)),
            Some(Ordering::Less),
        );
    }

    #[test]
    fn test_partial_compare_cross_numeric() {
        use std::cmp::Ordering;
        assert_eq!(DataType::Long(10).partial_compare(&DataType::BigInt(20)), Some(Ordering::Less));
        assert_eq!(DataType::BigInt(20).partial_compare(&DataType::Long(10)), Some(Ordering::Greater));
        assert_eq!(DataType::Long(5).partial_compare(&DataType::Double(5.0)), Some(Ordering::Equal));
        assert_eq!(DataType::Double(3.0).partial_compare(&DataType::Long(4)), Some(Ordering::Less));
    }

    #[test]
    fn test_partial_compare_incompatible() {
        assert_eq!(DataType::Long(1).partial_compare(&DataType::String("a".into())), None);
        assert_eq!(DataType::Boolean(true).partial_compare(&DataType::Long(1)), None);
    }

    #[test]
    fn test_partial_compare_nan() {
        assert_eq!(DataType::Double(f64::NAN).partial_compare(&DataType::Double(1.0)), None);
    }

    #[test]
    fn test_op_put() {
        let mut document: BTreeMap<String, DataType> = BTreeMap::new();
        document.insert("string".to_string(), "string_value".to_string().into());
        document.insert("int".to_string(), 1i64.into());
        let op = TxOp::Put(Document(document));
        let serialized = bincode::serialize(&op).unwrap();
        let deserialized: TxOp = bincode::deserialize(&serialized).unwrap();
        assert_eq!(op, deserialized);
    }

    #[test]
    fn test_op_add() {
        let op = TxOp::Add {
            entity_id: EntityId(1),
            attribute: Attribute("string".to_string()),
            value: DataType::String("string_value".to_string()),
        };
        let serialized = bincode::serialize(&op).unwrap();
        let deserialized: TxOp = bincode::deserialize(&serialized).unwrap();
        assert_eq!(op, deserialized);
    }

    #[test]
    fn test_op_retract() {
        let op = TxOp::Retract {
            entity_id: EntityId(1),
            attribute: Attribute("string".to_string()),
            value: DataType::String("string_value".to_string()),
        };
        let serialized = bincode::serialize(&op).unwrap();
        let deserialized: TxOp = bincode::deserialize(&serialized).unwrap();
        assert_eq!(op, deserialized);
    }

    #[test]
    fn test_op_delete() {
        let op = TxOp::Delete(EntityId(1));
        let serialized = bincode::serialize(&op).unwrap();
        let deserialized: TxOp = bincode::deserialize(&serialized).unwrap();
        assert_eq!(op, deserialized);
    }

    #[test]
    fn test_op_erase() {
        let op = TxOp::Erase(EntityId(1));
        let serialized = bincode::serialize(&op).unwrap();
        let deserialized: TxOp = bincode::deserialize(&serialized).unwrap();
        assert_eq!(op, deserialized);
    }

    #[test]
    fn test_tx_ops_to_datoms_put() {
        let tx = crate::clock::st_from_unix_epoch(1000);
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(100));
        doc.insert("name".to_string(), DataType::String("alice".to_string()));
        doc.insert("age".to_string(), DataType::Long(30));
        let ops = vec![TxOp::Put(Document(doc))];

        let datoms = tx_ops_to_datoms(&ops, tx).unwrap();
        assert_eq!(datoms.len(), 2);
        assert!(datoms.iter().all(|d| d.entity == 100));
        assert!(datoms.iter().all(|d| d.op == DatomOp::Assert));
        assert!(datoms.iter().all(|d| d.tx == tx));
    }

    #[test]
    fn test_tx_ops_to_datoms_add() {
        let tx = crate::clock::st_from_unix_epoch(1000);
        let ops = vec![TxOp::Add {
            entity_id: EntityId(200),
            attribute: Attribute("name".to_string()),
            value: DataType::String("bob".to_string()),
        }];

        let datoms = tx_ops_to_datoms(&ops, tx).unwrap();
        assert_eq!(datoms.len(), 1);
        assert_eq!(datoms[0].entity, 200);
        assert_eq!(datoms[0].attribute, "name");
        assert_eq!(datoms[0].value, DataType::String("bob".to_string()));
        assert_eq!(datoms[0].op, DatomOp::Assert);
    }

    #[test]
    fn test_tx_ops_to_datoms_retract() {
        let tx = crate::clock::st_from_unix_epoch(1000);
        let ops = vec![TxOp::Retract {
            entity_id: EntityId(200),
            attribute: Attribute("name".to_string()),
            value: DataType::String("bob".to_string()),
        }];

        let datoms = tx_ops_to_datoms(&ops, tx).unwrap();
        assert_eq!(datoms.len(), 1);
        assert_eq!(datoms[0].op, DatomOp::Retract);
    }

    #[test]
    #[should_panic(expected = "Delete/Erase not yet implemented")]
    fn test_tx_ops_to_datoms_delete_panics() {
        let tx = crate::clock::st_from_unix_epoch(1000);
        tx_ops_to_datoms(&[TxOp::Delete(EntityId(100))], tx).unwrap();
    }

    #[test]
    #[should_panic(expected = "Delete/Erase not yet implemented")]
    fn test_tx_ops_to_datoms_erase_panics() {
        let tx = crate::clock::st_from_unix_epoch(1000);
        tx_ops_to_datoms(&[TxOp::Erase(EntityId(200))], tx).unwrap();
    }

    #[test]
    fn test_tx_ops_to_datoms_put_missing_id() {
        let tx = crate::clock::st_from_unix_epoch(1000);
        let mut doc = BTreeMap::new();
        doc.insert("name".to_string(), DataType::String("alice".to_string()));
        let ops = vec![TxOp::Put(Document(doc))];

        let result = tx_ops_to_datoms(&ops, tx);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("db/id"));
    }

    #[test]
    fn test_tx_ops_to_datoms_put_wrong_id_type() {
        let tx = crate::clock::st_from_unix_epoch(1000);
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::String("not-a-long".to_string()));
        doc.insert("name".to_string(), DataType::String("alice".to_string()));
        let ops = vec![TxOp::Put(Document(doc))];

        let result = tx_ops_to_datoms(&ops, tx);
        assert!(result.is_err());
    }

    // --- assign_entity_ids tests ---

    #[test]
    fn test_assign_entity_ids_explicit_id_passthrough() {
        let mut next_t = 0;
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(42));
        doc.insert("name".to_string(), DataType::String("alice".into()));
        let ops = vec![TxOp::Put(Document(doc))];

        let resolved = assign_entity_ids(&ops, &mut next_t).unwrap();
        assert_eq!(next_t, 0, "next_t should not advance for explicit IDs");
        match &resolved[0] {
            TxOp::Put(Document(doc)) => assert_eq!(doc.get("db/id"), Some(&DataType::Long(42))),
            _ => panic!("Expected Put"),
        }
    }

    #[test]
    fn test_assign_entity_ids_user_partition() {
        use crate::partition::{extract_partition, USER_PARTITION};
        let mut next_t = 0;
        let mut doc = BTreeMap::new();
        doc.insert("name".to_string(), DataType::String("alice".into()));
        let ops = vec![TxOp::Put(Document(doc))];

        let resolved = assign_entity_ids(&ops, &mut next_t).unwrap();
        assert_eq!(next_t, 1);
        match &resolved[0] {
            TxOp::Put(Document(doc)) => {
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
    fn test_assign_entity_ids_db_partition_for_schema() {
        use crate::partition::{extract_partition, DB_PARTITION};
        let mut next_t = 0;
        let mut doc = BTreeMap::new();
        doc.insert("db/ident".to_string(), DataType::Keyword(
            edn::symbols::Keyword::plain("my-attr"),
        ));
        doc.insert("db/valueType".to_string(), DataType::Keyword(
            edn::symbols::Keyword::namespaced("db.type", "string"),
        ));
        let ops = vec![TxOp::Put(Document(doc))];

        let resolved = assign_entity_ids(&ops, &mut next_t).unwrap();
        assert_eq!(next_t, 1);
        match &resolved[0] {
            TxOp::Put(Document(doc)) => {
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
    fn test_assign_entity_ids_db_partition_for_enum() {
        use crate::partition::{extract_partition, DB_PARTITION};
        let mut next_t = 0;
        // Enum entity: has db/ident but no db/valueType
        let mut doc = BTreeMap::new();
        doc.insert("db/ident".to_string(), DataType::Keyword(
            edn::symbols::Keyword::namespaced("db.type", "string"),
        ));
        let ops = vec![TxOp::Put(Document(doc))];

        let resolved = assign_entity_ids(&ops, &mut next_t).unwrap();
        match &resolved[0] {
            TxOp::Put(Document(doc)) => {
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
    fn test_assign_entity_ids_add_retract_passthrough() {
        let mut next_t = 0;
        let ops = vec![
            TxOp::Add {
                entity_id: EntityId(100),
                attribute: Attribute("name".into()),
                value: DataType::String("alice".into()),
            },
            TxOp::Retract {
                entity_id: EntityId(100),
                attribute: Attribute("name".into()),
                value: DataType::String("alice".into()),
            },
        ];

        let resolved = assign_entity_ids(&ops, &mut next_t).unwrap();
        assert_eq!(next_t, 0, "Add/Retract should not allocate IDs");
        assert_eq!(resolved.len(), 2);
    }

    #[test]
    fn test_assign_entity_ids_multiple_docs() {
        use crate::partition::{extract_counter, USER_PARTITION, extract_partition};
        let mut next_t = 5;
        let ops = vec![
            TxOp::Put(Document({
                let mut m = BTreeMap::new();
                m.insert("name".to_string(), DataType::String("alice".into()));
                m
            })),
            TxOp::Put(Document({
                let mut m = BTreeMap::new();
                m.insert("name".to_string(), DataType::String("bob".into()));
                m
            })),
        ];

        let resolved = assign_entity_ids(&ops, &mut next_t).unwrap();
        assert_eq!(next_t, 7);

        // Both should be in USER_PARTITION with sequential counters
        for (i, op) in resolved.iter().enumerate() {
            match op {
                TxOp::Put(Document(doc)) => {
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
