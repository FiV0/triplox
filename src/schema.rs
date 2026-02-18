use std::collections::{BTreeMap, HashMap};

use anyhow::{Error, Result};
use edn::symbols::Keyword;

use crate::ops::{DataType, Document, Triple, TxOp};

// --- Reserved entity IDs ---

// Schema attribute entities
pub const DB_IDENT: i64 = 1;
pub const DB_VALUE_TYPE: i64 = 2;
pub const DB_CARDINALITY: i64 = 3;

// Value type enum entities
pub const DB_TYPE_KEYWORD: i64 = 10;
pub const DB_TYPE_STRING: i64 = 11;
pub const DB_TYPE_LONG: i64 = 12;
pub const DB_TYPE_REF: i64 = 13;
pub const DB_TYPE_BOOLEAN: i64 = 14;
pub const DB_TYPE_DOUBLE: i64 = 15;
pub const DB_TYPE_FLOAT: i64 = 16;
pub const DB_TYPE_INSTANT: i64 = 17;
pub const DB_TYPE_UUID: i64 = 18;
pub const DB_TYPE_BYTES: i64 = 19;
pub const DB_TYPE_BIGINT: i64 = 20;
pub const DB_TYPE_TUPLE: i64 = 21;
pub const DB_TYPE_VECTOR: i64 = 22;
pub const DB_TYPE_MAP: i64 = 23;

// Cardinality enum entities
pub const DB_CARDINALITY_ONE: i64 = 30;
pub const DB_CARDINALITY_MANY: i64 = 31;

// --- Schema types ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    Keyword,
    String,
    Long,
    Ref,
    Boolean,
    Double,
    Float,
    Instant,
    Uuid,
    Bytes,
    BigInt,
    Tuple,
    Vector,
    Map,
}

impl ValueType {
    /// Map a value type enum entity ID to a ValueType.
    pub fn from_entity_id(id: i64) -> Result<Self> {
        match id {
            DB_TYPE_KEYWORD => Ok(ValueType::Keyword),
            DB_TYPE_STRING => Ok(ValueType::String),
            DB_TYPE_LONG => Ok(ValueType::Long),
            DB_TYPE_REF => Ok(ValueType::Ref),
            DB_TYPE_BOOLEAN => Ok(ValueType::Boolean),
            DB_TYPE_DOUBLE => Ok(ValueType::Double),
            DB_TYPE_FLOAT => Ok(ValueType::Float),
            DB_TYPE_INSTANT => Ok(ValueType::Instant),
            DB_TYPE_UUID => Ok(ValueType::Uuid),
            DB_TYPE_BYTES => Ok(ValueType::Bytes),
            DB_TYPE_BIGINT => Ok(ValueType::BigInt),
            DB_TYPE_TUPLE => Ok(ValueType::Tuple),
            DB_TYPE_VECTOR => Ok(ValueType::Vector),
            DB_TYPE_MAP => Ok(ValueType::Map),
            _ => Err(anyhow::anyhow!("Unknown value type entity ID: {}", id)),
        }
    }

    /// Check if a DataType matches this ValueType.
    pub fn matches(&self, data: &DataType) -> bool {
        match (self, data) {
            (ValueType::Keyword, DataType::Keyword(_)) => true,
            (ValueType::String, DataType::String(_)) => true,
            (ValueType::Long, DataType::Long(_)) => true,
            (ValueType::Ref, DataType::Long(_)) => true, // refs stored as Long for now
            (ValueType::Boolean, DataType::Boolean(_)) => true,
            (ValueType::Double, DataType::Double(_)) => true,
            (ValueType::Float, DataType::Float(_)) => true,
            (ValueType::Instant, DataType::Instant(_)) => true,
            (ValueType::Uuid, DataType::Uuid(_)) => true,
            (ValueType::Bytes, DataType::Bytes(_)) => true,
            (ValueType::BigInt, DataType::BigInt(_)) => true,
            (ValueType::Tuple, DataType::Tuple(_)) => true,
            (ValueType::Vector, DataType::Vector(_)) => true,
            (ValueType::Map, DataType::Map(_)) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cardinality {
    One,
    Many,
}

impl Cardinality {
    /// Map a cardinality enum entity ID to a Cardinality.
    pub fn from_entity_id(id: i64) -> Result<Self> {
        match id {
            DB_CARDINALITY_ONE => Ok(Cardinality::One),
            DB_CARDINALITY_MANY => Ok(Cardinality::Many),
            _ => Err(anyhow::anyhow!("Unknown cardinality entity ID: {}", id)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SchemaAttribute {
    pub ident: String,
    pub value_type: ValueType,
    pub cardinality: Cardinality,
    pub entity_id: i64,
}

#[derive(Debug)]
pub struct SchemaCache {
    by_ident: HashMap<String, SchemaAttribute>,
}

impl SchemaCache {
    pub fn new() -> Self {
        SchemaCache {
            by_ident: HashMap::new(),
        }
    }

    pub fn get(&self, ident: &str) -> Option<&SchemaAttribute> {
        self.by_ident.get(ident)
    }

    /// Try to extract a schema attribute definition from a Put document.
    /// A document defines a schema attribute if it has db/ident AND db/valueType.
    /// Returns Ok(Some(attr)) if it's a schema definition, Ok(None) if not,
    /// Err if the definition is malformed.
    pub fn extract_schema_attribute(doc: &BTreeMap<String, DataType>) -> Result<Option<SchemaAttribute>> {
        let ident = match doc.get("db/ident") {
            Some(DataType::Keyword(kw)) => {
                let s = kw.to_string();
                // Strip EDN colon prefix (:ns/name -> ns/name) to match document key format
                s.strip_prefix(':').unwrap_or(&s).to_string()
            },
            Some(_) => return Err(anyhow::anyhow!("db/ident must be a Keyword")),
            None => return Ok(None),
        };

        let value_type = match doc.get("db/valueType") {
            Some(DataType::Long(id)) => ValueType::from_entity_id(*id)?,
            Some(_) => return Err(anyhow::anyhow!("db/valueType must be a Long (entity ref)")),
            None => return Ok(None), // has db/ident but no db/valueType — enum entity, not a schema attribute
        };

        let cardinality = match doc.get("db/cardinality") {
            Some(DataType::Long(id)) => Cardinality::from_entity_id(*id)?,
            Some(_) => return Err(anyhow::anyhow!("db/cardinality must be a Long (entity ref)")),
            None => Cardinality::One, // default to cardinality one
        };

        let entity_id = match doc.get("db/id") {
            Some(DataType::Long(id)) => *id,
            _ => return Err(anyhow::anyhow!("Schema attribute must have db/id as Long")),
        };

        Ok(Some(SchemaAttribute {
            ident,
            value_type,
            cardinality,
            entity_id,
        }))
    }

    /// Process a transaction's ops and update the cache with any new schema attributes.
    pub fn process_tx(&mut self, tx_ops: &[TxOp]) -> Result<()> {
        for op in tx_ops {
            if let TxOp::Put(Document(doc)) = op {
                if let Some(attr) = Self::extract_schema_attribute(doc)? {
                    self.by_ident.insert(attr.ident.clone(), attr);
                }
            }
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.by_ident.len()
    }

    /// Validate a transaction's ops against the schema.
    /// Every attribute must exist in the schema and values must match declared valueType.
    pub fn validate_tx(&self, tx_ops: &[TxOp]) -> Result<()> {
        for op in tx_ops {
            match op {
                TxOp::Put(Document(doc)) => {
                    for (attr_name, value) in doc.iter() {
                        if attr_name == "db/id" {
                            continue;
                        }
                        self.validate_attribute_value(attr_name, value)?;
                    }
                },
                TxOp::Add(Triple { attribute, value, .. }) => {
                    self.validate_attribute_value(&attribute.0, value.data_type())?;
                },
                TxOp::Retract(Triple { attribute, value, .. }) => {
                    self.validate_attribute_value(&attribute.0, value.data_type())?;
                },
                TxOp::Delete(_) | TxOp::Erase(_) => {},
            }
        }
        Ok(())
    }

    fn validate_attribute_value(&self, attr_name: &str, value: &DataType) -> Result<()> {
        let schema_attr = self.by_ident.get(attr_name)
            .ok_or_else(|| anyhow::anyhow!("Unknown attribute: {}", attr_name))?;

        if !schema_attr.value_type.matches(value) {
            return Err(anyhow::anyhow!(
                "Type mismatch for attribute {}: expected {:?}, got {:?}",
                attr_name, schema_attr.value_type, value
            ));
        }

        Ok(())
    }
}

// --- Bootstrap transaction builders ---

/// Build a Put operation for a schema attribute entity.
/// Schema attributes have db/ident, db/valueType, and db/cardinality.
fn schema_attribute(id: i64, ns: &str, name: &str, value_type: i64, cardinality: i64) -> TxOp {
    let mut doc = BTreeMap::new();
    doc.insert("db/id".to_string(), DataType::Long(id));
    doc.insert("db/ident".to_string(), DataType::Keyword(Keyword::namespaced(ns, name)));
    doc.insert("db/valueType".to_string(), DataType::Long(value_type));
    doc.insert("db/cardinality".to_string(), DataType::Long(cardinality));
    TxOp::Put(Document(doc))
}

/// Build a Put operation for an enum entity (value type or cardinality).
/// Enum entities only have db/ident.
fn enum_entity(id: i64, ns: &str, name: &str) -> TxOp {
    let mut doc = BTreeMap::new();
    doc.insert("db/id".to_string(), DataType::Long(id));
    doc.insert("db/ident".to_string(), DataType::Keyword(Keyword::namespaced(ns, name)));
    TxOp::Put(Document(doc))
}

/// Build the bootstrap schema transaction.
/// This is the first transaction on a fresh database (tx_id=0).
pub fn bootstrap_schema_tx() -> Vec<TxOp> {
    vec![
        // Schema attribute entities (IDs 1-3)
        schema_attribute(DB_IDENT, "db", "ident", DB_TYPE_KEYWORD, DB_CARDINALITY_ONE),
        schema_attribute(DB_VALUE_TYPE, "db", "valueType", DB_TYPE_REF, DB_CARDINALITY_ONE),
        schema_attribute(DB_CARDINALITY, "db", "cardinality", DB_TYPE_REF, DB_CARDINALITY_ONE),

        // Value type enum entities (IDs 10-23)
        enum_entity(DB_TYPE_KEYWORD, "db.type", "keyword"),
        enum_entity(DB_TYPE_STRING, "db.type", "string"),
        enum_entity(DB_TYPE_LONG, "db.type", "long"),
        enum_entity(DB_TYPE_REF, "db.type", "ref"),
        enum_entity(DB_TYPE_BOOLEAN, "db.type", "boolean"),
        enum_entity(DB_TYPE_DOUBLE, "db.type", "double"),
        enum_entity(DB_TYPE_FLOAT, "db.type", "float"),
        enum_entity(DB_TYPE_INSTANT, "db.type", "instant"),
        enum_entity(DB_TYPE_UUID, "db.type", "uuid"),
        enum_entity(DB_TYPE_BYTES, "db.type", "bytes"),
        enum_entity(DB_TYPE_BIGINT, "db.type", "bigint"),
        enum_entity(DB_TYPE_TUPLE, "db.type", "tuple"),
        enum_entity(DB_TYPE_VECTOR, "db.type", "vector"),
        enum_entity(DB_TYPE_MAP, "db.type", "map"),

        // Cardinality enum entities (IDs 30-31)
        enum_entity(DB_CARDINALITY_ONE, "db.cardinality", "one"),
        enum_entity(DB_CARDINALITY_MANY, "db.cardinality", "many"),
    ]
}

/// Build a Put operation for a plain (non-namespaced) schema attribute.
/// Used by tests that define attributes like "name", "age", etc.
#[cfg(test)]
fn plain_schema_attribute(id: i64, name: &str, value_type: i64, cardinality: i64) -> TxOp {
    let mut doc = BTreeMap::new();
    doc.insert("db/id".to_string(), DataType::Long(id));
    doc.insert("db/ident".to_string(), DataType::Keyword(Keyword::plain(name)));
    doc.insert("db/valueType".to_string(), DataType::Long(value_type));
    doc.insert("db/cardinality".to_string(), DataType::Long(cardinality));
    TxOp::Put(Document(doc))
}

/// Build a transaction that defines common test attributes.
/// Use entity IDs 50-59 (between bootstrap schema 1-31 and test data 100+).
#[cfg(test)]
pub(crate) fn test_schema_tx() -> Vec<TxOp> {
    vec![
        plain_schema_attribute(50, "name", DB_TYPE_STRING, DB_CARDINALITY_ONE),
        plain_schema_attribute(51, "age", DB_TYPE_LONG, DB_CARDINALITY_ONE),
        plain_schema_attribute(52, "email", DB_TYPE_STRING, DB_CARDINALITY_ONE),
        plain_schema_attribute(53, "follows", DB_TYPE_LONG, DB_CARDINALITY_ONE),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bootstrap_schema_tx_count() {
        let tx = bootstrap_schema_tx();
        // 3 schema attributes + 14 value type enums + 2 cardinality enums = 19
        assert_eq!(tx.len(), 19);
    }

    #[test]
    fn test_bootstrap_schema_tx_serializable() {
        let tx = bootstrap_schema_tx();
        let serialized = bincode::serialize(&tx).unwrap();
        let deserialized: Vec<TxOp> = bincode::deserialize(&serialized).unwrap();
        assert_eq!(tx.len(), deserialized.len());
    }

    #[test]
    fn test_bootstrap_entity_ids_unique() {
        let tx = bootstrap_schema_tx();
        let mut ids: Vec<i64> = tx.iter().map(|op| match op {
            TxOp::Put(Document(doc)) => match doc.get("db/id") {
                Some(DataType::Long(id)) => *id,
                _ => panic!("Expected db/id Long"),
            },
            _ => panic!("Expected Put"),
        }).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), tx.len(), "All bootstrap entity IDs must be unique");
    }

    #[test]
    fn test_schema_cache_from_bootstrap() {
        let mut cache = SchemaCache::new();
        let tx = bootstrap_schema_tx();
        cache.process_tx(&tx).unwrap();

        // Bootstrap defines 3 schema attributes (db/ident, db/valueType, db/cardinality)
        // The 14 type enums and 2 cardinality enums only have db/ident (no db/valueType),
        // so they are NOT schema attributes.
        assert_eq!(cache.len(), 3);

        let db_ident = cache.get("db/ident").unwrap();
        assert_eq!(db_ident.entity_id, DB_IDENT);
        assert_eq!(db_ident.value_type, ValueType::Keyword);
        assert_eq!(db_ident.cardinality, Cardinality::One);

        let db_value_type = cache.get("db/valueType").unwrap();
        assert_eq!(db_value_type.entity_id, DB_VALUE_TYPE);
        assert_eq!(db_value_type.value_type, ValueType::Ref);
        assert_eq!(db_value_type.cardinality, Cardinality::One);

        let db_cardinality = cache.get("db/cardinality").unwrap();
        assert_eq!(db_cardinality.entity_id, DB_CARDINALITY);
        assert_eq!(db_cardinality.value_type, ValueType::Ref);
        assert_eq!(db_cardinality.cardinality, Cardinality::One);
    }

    #[test]
    fn test_schema_cache_user_attribute() {
        let mut cache = SchemaCache::new();

        // Simulate bootstrap
        cache.process_tx(&bootstrap_schema_tx()).unwrap();

        // User defines a new attribute: person/name of type string, cardinality one
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(100));
        doc.insert("db/ident".to_string(), DataType::Keyword(Keyword::namespaced("person", "name")));
        doc.insert("db/valueType".to_string(), DataType::Long(DB_TYPE_STRING));
        doc.insert("db/cardinality".to_string(), DataType::Long(DB_CARDINALITY_ONE));

        cache.process_tx(&[TxOp::Put(Document(doc))]).unwrap();
        assert_eq!(cache.len(), 4);

        let person_name = cache.get("person/name").unwrap();
        assert_eq!(person_name.entity_id, 100);
        assert_eq!(person_name.value_type, ValueType::String);
        assert_eq!(person_name.cardinality, Cardinality::One);
    }

    #[test]
    fn test_value_type_matches() {
        assert!(ValueType::String.matches(&DataType::String("hello".to_string())));
        assert!(ValueType::Long.matches(&DataType::Long(42)));
        assert!(ValueType::Ref.matches(&DataType::Long(1))); // refs are Long for now
        assert!(ValueType::Boolean.matches(&DataType::Boolean(true)));

        assert!(!ValueType::String.matches(&DataType::Long(42)));
        assert!(!ValueType::Long.matches(&DataType::String("hello".to_string())));
    }

    #[test]
    fn test_enum_entity_not_added_to_cache() {
        let mut cache = SchemaCache::new();

        // An enum entity only has db/ident (no db/valueType), should NOT be a schema attribute
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(50));
        doc.insert("db/ident".to_string(), DataType::Keyword(Keyword::namespaced("my.enum", "value")));

        cache.process_tx(&[TxOp::Put(Document(doc))]).unwrap();
        assert_eq!(cache.len(), 0);
    }

    fn bootstrapped_cache_with_person_name() -> SchemaCache {
        let mut cache = SchemaCache::new();
        cache.process_tx(&bootstrap_schema_tx()).unwrap();

        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(100));
        doc.insert("db/ident".to_string(), DataType::Keyword(Keyword::namespaced("person", "name")));
        doc.insert("db/valueType".to_string(), DataType::Long(DB_TYPE_STRING));
        doc.insert("db/cardinality".to_string(), DataType::Long(DB_CARDINALITY_ONE));
        cache.process_tx(&[TxOp::Put(Document(doc))]).unwrap();

        cache
    }

    #[test]
    fn test_validate_tx_valid_put() {
        let cache = bootstrapped_cache_with_person_name();

        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(200));
        doc.insert("person/name".to_string(), DataType::String("Alice".to_string()));
        let result = cache.validate_tx(&[TxOp::Put(Document(doc))]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_tx_unknown_attribute() {
        let cache = bootstrapped_cache_with_person_name();

        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(200));
        doc.insert("person/age".to_string(), DataType::Long(30));
        let result = cache.validate_tx(&[TxOp::Put(Document(doc))]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unknown attribute: person/age"));
    }

    #[test]
    fn test_validate_tx_type_mismatch() {
        let cache = bootstrapped_cache_with_person_name();

        // person/name is String, but we're passing a Long
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(200));
        doc.insert("person/name".to_string(), DataType::Long(42));
        let result = cache.validate_tx(&[TxOp::Put(Document(doc))]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Type mismatch"));
    }

    #[test]
    fn test_validate_tx_schema_defining_tx() {
        let mut cache = SchemaCache::new();
        cache.process_tx(&bootstrap_schema_tx()).unwrap();

        // Defining a new attribute should validate against the bootstrap schema
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(100));
        doc.insert("db/ident".to_string(), DataType::Keyword(Keyword::namespaced("person", "name")));
        doc.insert("db/valueType".to_string(), DataType::Long(DB_TYPE_STRING));
        doc.insert("db/cardinality".to_string(), DataType::Long(DB_CARDINALITY_ONE));
        let result = cache.validate_tx(&[TxOp::Put(Document(doc))]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_tx_add_triple() {
        use crate::ops::{Attribute, EntityId, Value};

        let cache = bootstrapped_cache_with_person_name();

        // Valid add
        let op = TxOp::Add(Triple {
            entity: EntityId(200),
            attribute: Attribute("person/name".to_string()),
            value: Value::new(DataType::String("Bob".to_string())),
        });
        assert!(cache.validate_tx(&[op]).is_ok());

        // Type mismatch in add
        let op = TxOp::Add(Triple {
            entity: EntityId(200),
            attribute: Attribute("person/name".to_string()),
            value: Value::new(DataType::Long(42)),
        });
        assert!(cache.validate_tx(&[op]).is_err());
    }

    #[test]
    fn test_validate_tx_retract_triple() {
        use crate::ops::{Attribute, EntityId, Value};

        let cache = bootstrapped_cache_with_person_name();

        // Valid retract
        let op = TxOp::Retract(Triple {
            entity: EntityId(200),
            attribute: Attribute("person/name".to_string()),
            value: Value::new(DataType::String("Bob".to_string())),
        });
        assert!(cache.validate_tx(&[op]).is_ok());

        // Unknown attribute in retract
        let op = TxOp::Retract(Triple {
            entity: EntityId(200),
            attribute: Attribute("person/age".to_string()),
            value: Value::new(DataType::Long(30)),
        });
        assert!(cache.validate_tx(&[op]).is_err());
    }
}
