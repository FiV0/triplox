use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use bigdecimal::BigDecimal;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;
use crate::transaction::Instant;
use edn::symbols::{Keyword, NamespacedSymbol};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct EntityId(i64);

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Attribute(String);

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Value(DataType);

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Ref(i64);

#[derive(Serialize, Deserialize, Debug, PartialEq)]
enum DataType {
    Nil,
    //BigDecimal(BigDecimal),          // Arbitrary precision decimal numbers
    BigInt(i128),                    // Arbitrary large integers
    Boolean(bool),                   // Booleans (true or false)
    Bytes(Vec<u8>),                  // Binary data (as bytes)
    Double(f64),                     // Double precision floating point
    Float(f32),                      // Single precision floating point
    Instant(Instant),                // Timestamps or instants
    // Keyword(Keyword),                 // Keywords (can be represented as strings)
    Long(i64),                       // Long integers
    Ref(Ref),                        // Reference (for shared ownership, like pointers)
    String(String),                  // Strings
    // Symbol(NamespacedSymbol),                  // Symbols (can be represented as strings)
    Tuple(Vec<DataType>),            // Tuples (can be represented as a vector of DataTypes)
    Uuid(Uuid),                      // Universally unique identifier
    // TODO
    //Uri(Uri),                        // URIs (could also be represented as strings)

    // Composite types
    Vector(Vec<DataType>),             // List (vector of DataTypes)
    // TODO there is an interesting note in the edn create about using a BTreeMap vs a HashMap
    // Set(HashSet<DataType>),         // Set (BTreeSet of DataTypes)
    Map(HashMap<String, DataType>),  // Map (BTreeMap of string keys and DataType values)
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
    (Instant, Instant),
    (Long, i64),
    (Ref, Ref),
    (String, String),
    (Uuid, Uuid),
    (Vector, Vec<DataType>),
    (Map, HashMap<String, DataType>)
);

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Document(HashMap<String, DataType>);

// either extend this with t and op as options or create another type for running through indices
// make value optional ?
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Triple {
    entity: EntityId,
    attribute: Attribute,
    value: Value,
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
        let mut document : HashMap<String, DataType> = HashMap::new();
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
