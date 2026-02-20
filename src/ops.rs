#[allow(unused_imports)]
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
#[allow(unused_imports)]
use edn::symbols::{Keyword, NamespacedSymbol};
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct EntityId(pub i64);

impl EntityId {
    pub fn new(id: i64) -> Self {
        EntityId(id)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Attribute(pub String);

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Value(DataType);

impl Value {
    pub fn new(data: DataType) -> Self {
        Value(data)
    }
}

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


impl DataType {
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

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Document(pub BTreeMap<String, DataType>);

// either extend this with t and op as options or create another type for running through indices
// make value optional ?
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Triple {
    pub entity: EntityId,
    pub attribute: Attribute,
    pub value: Value,
}
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum TxOp {
    Put(Document),
    Add(Triple),
    Retract(Triple),
    Delete(EntityId),
    Erase(EntityId),
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
        let op = TxOp::Add(Triple {
            entity: EntityId(1),
            attribute: Attribute("string".to_string()),
            value: Value(DataType::String("string_value".to_string())),
        });
        let serialized = bincode::serialize(&op).unwrap();
        let deserialized: TxOp = bincode::deserialize(&serialized).unwrap();
        assert_eq!(op, deserialized);
    }

    #[test]
    fn test_op_retract() {
        let op = TxOp::Retract(Triple {
            entity: EntityId(1),
            attribute: Attribute("string".to_string()),
            value: Value(DataType::String("string_value".to_string())),
        });
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
}
