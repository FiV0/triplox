use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use bigdecimal::BigDecimal;
#[allow(unused_imports)]
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;
#[allow(unused_imports)]
use edn::symbols::{Keyword, NamespacedSymbol};
use chrono::{DateTime, Utc};

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
    Nil,
    //BigDecimal(BigDecimal),          // Arbitrary precision decimal numbers
    BigInt(i128),                    // Arbitrary large integers
    Boolean(bool),                   // Booleans (true or false)
    // TODO: use Bytes instead of Vec<u8> ?
    Bytes(Vec<u8>),                  // Binary data (as bytes)
    Double(f64),                     // Double precision floating point
    Float(f32),                      // Single precision floating point
    Instant(DateTime<Utc>),          // Timestamps or instants
    // Keyword(Keyword),                 // Keywords (can be represented as strings)
    Long(i64),                       // Long integers
    // TODO: Ref commented out — entity refs stored as Long for now. Revisit with schema.
    // Ref(Ref),                     // Reference (for shared ownership, like pointers)
    String(String),                  // Strings
    // Symbol(NamespacedSymbol),                  // Symbols (can be represented as strings)
    Tuple(Vec<DataType>),            // Tuples (can be represented as a vector of DataTypes)
    Uuid(Uuid),                      // Universally unique identifier
    // TODO
    //Uri(Uri),                        // URIs (could also be represented as strings)

    // Composite types
    Vector(Vec<DataType>),             // List (vector of DataTypes)
    // TODO think about tradeoffs of using a BTreeSet vs a HashSet
    //Set(BTreeSet<DataType>),         // Set (BTreeSet of DataTypes)
    Map(BTreeMap<String, DataType>),  // Map (BTreeMap of string keys and DataType values)
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

impl_from_for_enum!(DataType,
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
    fn test_op_put() {
        let mut document : BTreeMap<String, DataType> = BTreeMap::new();
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
