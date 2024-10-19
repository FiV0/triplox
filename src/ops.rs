use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use bigdecimal::BigDecimal;
use std::collections::HashMap;
use uuid::Uuid;
use crate::transaction::Instant;

type EntityId = i64;
type Attribute = String;
type Value = DataType;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
enum DataType {
    //BigDecimal(BigDecimal),          // Arbitrary precision decimal numbers
    BigInt(i128),                    // Arbitrary large integers
    Boolean(bool),                   // Booleans (true or false)
    Bytes(Vec<u8>),                  // Binary data (as bytes)
    Double(f64),                     // Double precision floating point
    Float(f32),                      // Single precision floating point
    Instant(Instant),                // Timestamps or instants
    Keyword(String),                 // Keywords (can be represented as strings)
    Long(i64),                       // Long integers
    Ref(i64),                        // Reference (for shared ownership, like pointers)
    String(String),                  // Strings
    Symbol(String),                  // Symbols (can be represented as strings)
    Tuple(Vec<DataType>),            // Tuples (can be represented as a vector of DataTypes)
    Uuid(Uuid),                      // Universally unique identifier
    // TODO
    //Uri(Uri),                        // URIs (could also be represented as strings)

    // Composite types
    List(Vec<DataType>),             // List (vector of DataTypes)
    Map(HashMap<String, DataType>),  // Map (HashMap of string keys and DataType values)
}

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
pub enum Op {
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
    fn test_op_serde() {
        let op = Op::Put(Document(HashMap::new()));
        let serialized = bincode::serialize(&op).unwrap();
        let deserialized: Op = bincode::deserialize(&serialized).unwrap();
        assert_eq!(op, deserialized);
    }
}
