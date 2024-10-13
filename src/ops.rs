use bigdecimal::BigDecimal;
use std::collections::HashMap;
use std::time::Instant;
use uuid::Uuid;

type EntityId = i64;
type Attribute = String;
type Value = DataType;

#[derive(Debug)]
enum DataType {
    BigDecimal(BigDecimal),          // Arbitrary precision decimal numbers
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

#[derive(Debug)]
pub struct Document(HashMap<String, DataType>);

// either extend this with t and op as options or create another type for running through indices
// make value optional ?
#[derive(Debug)]
pub struct Triple {
    entity: EntityId,
    attribute: Attribute,
    value: Value,
}
#[derive(Debug)]
pub enum Op {
    Put(Document),
    Add(Triple),
    Retract(Triple),
    Delete(EntityId),
    Erase(EntityId),
}