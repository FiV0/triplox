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

pub type Entid = i64;

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

    /// Compare two DataType values. Returns an error if the types are incompatible
    /// or if floats are NaN.
    pub fn partial_compare(&self, other: &DataType) -> Result<std::cmp::Ordering> {
        use DataType::*;
        let nan_err = || anyhow::anyhow!("cannot compare NaN values");
        match (self, other) {
            (Long(a), Long(b)) => Ok(a.cmp(b)),
            (BigInt(a), BigInt(b)) => Ok(a.cmp(b)),
            (Double(a), Double(b)) => a.partial_cmp(b).ok_or_else(nan_err),
            (Float(a), Float(b)) => a.partial_cmp(b).ok_or_else(nan_err),
            (String(a), String(b)) => Ok(a.cmp(b)),
            (Boolean(a), Boolean(b)) => Ok(a.cmp(b)),
            (Instant(a), Instant(b)) => Ok(a.cmp(b)),
            // Cross-numeric promotion
            // NOTE: casting BigInt(i128) to f32/f64 may lose precision for large values.
            (Long(a), BigInt(b)) => Ok((*a as i128).cmp(b)),
            (BigInt(a), Long(b)) => Ok(a.cmp(&(*b as i128))),
            (Long(a), Double(b)) => (*a as f64).partial_cmp(b).ok_or_else(nan_err),
            (Double(a), Long(b)) => a.partial_cmp(&(*b as f64)).ok_or_else(nan_err),
            (Long(a), Float(b)) => (*a as f32).partial_cmp(b).ok_or_else(nan_err),
            (Float(a), Long(b)) => a.partial_cmp(&(*b as f32)).ok_or_else(nan_err),
            (BigInt(a), Float(b)) => (*a as f32).partial_cmp(b).ok_or_else(nan_err),
            (Float(a), BigInt(b)) => a.partial_cmp(&(*b as f32)).ok_or_else(nan_err),
            (BigInt(a), Double(b)) => (*a as f64).partial_cmp(b).ok_or_else(nan_err),
            (Double(a), BigInt(b)) => a.partial_cmp(&(*b as f64)).ok_or_else(nan_err),
            (Float(a), Double(b)) => (*a as f64).partial_cmp(b).ok_or_else(nan_err),
            (Double(a), Float(b)) => a.partial_cmp(&(*b as f64)).ok_or_else(nan_err),
            _ => Err(anyhow::anyhow!(
                "cannot compare {:?} with {:?}", self.value_type(), other.value_type()
            )),
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
pub enum TxOp {
    Put(BTreeMap<String, DataType>),
    Add { entity_id: Entid, attribute: Attribute, value: DataType },
    Retract { entity_id: Entid, attribute: Attribute, value: DataType },
    Delete(Entid),
    Erase(Entid),
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
    pub tx: i64,
    pub op: DatomOp,
}

/// Expand TxOps into a flat vec of Datoms.
/// - Put(doc) → N Assert datoms (one per non-db/id field)
/// - Add(triple) → 1 Assert datom
/// - Retract(triple) → 1 Retract datom
/// - Delete/Erase → panics (not yet implemented)
pub fn tx_ops_to_datoms(ops: &[TxOp], tx: i64) -> Result<Vec<Datom>> {
    let mut datoms = Vec::new();
    for op in ops {
        match op {
            TxOp::Put(doc) => {
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
                    entity: *entity_id,
                    attribute: attribute.0.clone(),
                    value: value.clone(),
                    tx,
                    op: DatomOp::Assert,
                });
            }
            TxOp::Retract { entity_id, attribute, value } => {
                datoms.push(Datom {
                    entity: *entity_id,
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
        assert_eq!(DataType::Long(1).partial_compare(&DataType::Long(2)).unwrap(), Ordering::Less);
        assert_eq!(DataType::Long(2).partial_compare(&DataType::Long(2)).unwrap(), Ordering::Equal);
        assert_eq!(DataType::Long(3).partial_compare(&DataType::Long(2)).unwrap(), Ordering::Greater);

        assert_eq!(
            DataType::String("a".into()).partial_compare(&DataType::String("b".into())).unwrap(),
            Ordering::Less,
        );
        assert_eq!(
            DataType::Boolean(false).partial_compare(&DataType::Boolean(true)).unwrap(),
            Ordering::Less,
        );
        assert_eq!(
            DataType::Double(1.5).partial_compare(&DataType::Double(2.5)).unwrap(),
            Ordering::Less,
        );
    }

    #[test]
    fn test_partial_compare_cross_numeric() {
        use std::cmp::Ordering;
        assert_eq!(DataType::Long(10).partial_compare(&DataType::BigInt(20)).unwrap(), Ordering::Less);
        assert_eq!(DataType::BigInt(20).partial_compare(&DataType::Long(10)).unwrap(), Ordering::Greater);
        assert_eq!(DataType::Long(5).partial_compare(&DataType::Double(5.0)).unwrap(), Ordering::Equal);
        assert_eq!(DataType::Double(3.0).partial_compare(&DataType::Long(4)).unwrap(), Ordering::Less);

        // Float cross-numeric
        assert_eq!(DataType::Float(1.0).partial_compare(&DataType::Long(2)).unwrap(), Ordering::Less);
        assert_eq!(DataType::Long(2).partial_compare(&DataType::Float(1.0)).unwrap(), Ordering::Greater);
        assert_eq!(DataType::Float(1.0).partial_compare(&DataType::Double(1.0)).unwrap(), Ordering::Equal);
        assert_eq!(DataType::Double(2.0).partial_compare(&DataType::Float(1.0)).unwrap(), Ordering::Greater);
        assert_eq!(DataType::Float(1.0).partial_compare(&DataType::BigInt(2)).unwrap(), Ordering::Less);
        assert_eq!(DataType::BigInt(2).partial_compare(&DataType::Float(1.0)).unwrap(), Ordering::Greater);
        assert_eq!(DataType::BigInt(1).partial_compare(&DataType::Double(2.0)).unwrap(), Ordering::Less);
        assert_eq!(DataType::Double(2.0).partial_compare(&DataType::BigInt(1)).unwrap(), Ordering::Greater);
    }

    #[test]
    fn test_partial_compare_incompatible() {
        assert!(DataType::Long(1).partial_compare(&DataType::String("a".into())).is_err());
        assert!(DataType::Boolean(true).partial_compare(&DataType::Long(1)).is_err());
    }

    #[test]
    fn test_partial_compare_nan() {
        // Same-type NaN
        assert!(DataType::Double(f64::NAN).partial_compare(&DataType::Double(1.0)).is_err());
        assert!(DataType::Float(f32::NAN).partial_compare(&DataType::Float(1.0)).is_err());
        // Cross-type NaN
        assert!(DataType::Float(f32::NAN).partial_compare(&DataType::Long(1)).is_err());
        assert!(DataType::Float(f32::NAN).partial_compare(&DataType::Double(1.0)).is_err());
        assert!(DataType::Float(f32::NAN).partial_compare(&DataType::BigInt(1)).is_err());
        assert!(DataType::Double(f64::NAN).partial_compare(&DataType::Long(1)).is_err());
        assert!(DataType::Double(f64::NAN).partial_compare(&DataType::BigInt(1)).is_err());
    }

    #[test]
    fn test_op_put() {
        let mut document: BTreeMap<String, DataType> = BTreeMap::new();
        document.insert("string".to_string(), "string_value".to_string().into());
        document.insert("int".to_string(), 1i64.into());
        let op = TxOp::Put(document);
        let serialized = bincode::serialize(&op).unwrap();
        let deserialized: TxOp = bincode::deserialize(&serialized).unwrap();
        assert_eq!(op, deserialized);
    }

    #[test]
    fn test_op_add() {
        let op = TxOp::Add {
            entity_id: 1,
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
            entity_id: 1,
            attribute: Attribute("string".to_string()),
            value: DataType::String("string_value".to_string()),
        };
        let serialized = bincode::serialize(&op).unwrap();
        let deserialized: TxOp = bincode::deserialize(&serialized).unwrap();
        assert_eq!(op, deserialized);
    }

    #[test]
    fn test_op_delete() {
        let op = TxOp::Delete(1);
        let serialized = bincode::serialize(&op).unwrap();
        let deserialized: TxOp = bincode::deserialize(&serialized).unwrap();
        assert_eq!(op, deserialized);
    }

    #[test]
    fn test_op_erase() {
        let op = TxOp::Erase(1);
        let serialized = bincode::serialize(&op).unwrap();
        let deserialized: TxOp = bincode::deserialize(&serialized).unwrap();
        assert_eq!(op, deserialized);
    }

    #[test]
    fn test_tx_ops_to_datoms_put() {
        let tx = 1000_i64;
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(100));
        doc.insert("name".to_string(), DataType::String("alice".to_string()));
        doc.insert("age".to_string(), DataType::Long(30));
        let ops = vec![TxOp::Put(doc)];

        let datoms = tx_ops_to_datoms(&ops, tx).unwrap();
        assert_eq!(datoms.len(), 2);
        assert!(datoms.iter().all(|d| d.entity == 100));
        assert!(datoms.iter().all(|d| d.op == DatomOp::Assert));
        assert!(datoms.iter().all(|d| d.tx == tx));
    }

    #[test]
    fn test_tx_ops_to_datoms_add() {
        let tx = 1000_i64;
        let ops = vec![TxOp::Add {
            entity_id: 200,
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
        let tx = 1000_i64;
        let ops = vec![TxOp::Retract {
            entity_id: 200,
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
        let tx = 1000_i64;
        tx_ops_to_datoms(&[TxOp::Delete(100)], tx).unwrap();
    }

    #[test]
    #[should_panic(expected = "Delete/Erase not yet implemented")]
    fn test_tx_ops_to_datoms_erase_panics() {
        let tx = 1000_i64;
        tx_ops_to_datoms(&[TxOp::Erase(200)], tx).unwrap();
    }

    #[test]
    fn test_tx_ops_to_datoms_put_missing_id() {
        let tx = 1000_i64;
        let mut doc = BTreeMap::new();
        doc.insert("name".to_string(), DataType::String("alice".to_string()));
        let ops = vec![TxOp::Put(doc)];

        let result = tx_ops_to_datoms(&ops, tx);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("db/id"));
    }

    #[test]
    fn test_tx_ops_to_datoms_put_wrong_id_type() {
        let tx = 1000_i64;
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::String("not-a-long".to_string()));
        doc.insert("name".to_string(), DataType::String("alice".to_string()));
        let ops = vec![TxOp::Put(doc)];

        let result = tx_ops_to_datoms(&ops, tx);
        assert!(result.is_err());
    }
}
