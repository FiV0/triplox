use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{Error, Result};
use edn::symbols::Keyword;
use tokio::runtime::Handle;

use crate::datalog::{FindElement, FindSpec, PatternElement, Query, TriplePattern, WhereClause};
use crate::ops::{DataType, Datom, DatomOp, Document, TxOp};
use crate::query::{execute_query, validate_query};

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
    // TODO: Ref commented out — entity refs stored as Long for now. Revisit with DataType::Ref.
    // Ref,
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

impl std::fmt::Display for ValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueType::Keyword => write!(f, "keyword"),
            ValueType::String => write!(f, "string"),
            ValueType::Long => write!(f, "long"),
            ValueType::Boolean => write!(f, "boolean"),
            ValueType::Double => write!(f, "double"),
            ValueType::Float => write!(f, "float"),
            ValueType::Instant => write!(f, "instant"),
            ValueType::Uuid => write!(f, "uuid"),
            ValueType::Bytes => write!(f, "bytes"),
            ValueType::BigInt => write!(f, "bigint"),
            ValueType::Tuple => write!(f, "tuple"),
            ValueType::Vector => write!(f, "vector"),
            ValueType::Map => write!(f, "map"),
        }
    }
}

impl ValueType {
    // TODO(triplox-r5a): In Datomic, db/valueType values are entity refs (Longs pointing to
    // type enum entities like :db.type/string). We use keywords directly for now, which is
    // simpler but diverges from the "everything is entities" model. Revisit when we add
    // DataType::Ref and want schema-defining transactions to look like regular data.
    /// Map a value type keyword (e.g. :db.type/string) to a ValueType.
    pub fn from_keyword(kw: &Keyword) -> Result<Self> {
        let s = kw.to_string();
        // TODO: this destructing is brittle. Let's address it when we move to only use the EDN crate.
        let s = s.strip_prefix(':').unwrap_or(&s);
        match s {
            "db.type/keyword" => Ok(ValueType::Keyword),
            "db.type/string" => Ok(ValueType::String),
            "db.type/long" => Ok(ValueType::Long),
            "db.type/ref" => Ok(ValueType::Long), // refs stored as Long for now
            "db.type/boolean" => Ok(ValueType::Boolean),
            "db.type/double" => Ok(ValueType::Double),
            "db.type/float" => Ok(ValueType::Float),
            "db.type/instant" => Ok(ValueType::Instant),
            "db.type/uuid" => Ok(ValueType::Uuid),
            "db.type/bytes" => Ok(ValueType::Bytes),
            "db.type/bigint" => Ok(ValueType::BigInt),
            "db.type/tuple" => Ok(ValueType::Tuple),
            "db.type/vector" => Ok(ValueType::Vector),
            "db.type/map" => Ok(ValueType::Map),
            _ => Err(anyhow::anyhow!("Unknown value type keyword: {}", kw)),
        }
    }

    /// Check if a DataType matches this ValueType.
    pub fn matches(&self, data: &DataType) -> bool {
        *self == data.value_type()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cardinality {
    One,
    Many,
}

impl std::fmt::Display for Cardinality {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Cardinality::One => write!(f, "one"),
            Cardinality::Many => write!(f, "many"),
        }
    }
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
    pub entity_id: i64,
    pub ident: String,
    pub value_type: ValueType,
    pub cardinality: Cardinality,
}

#[derive(Debug, Default)]
pub struct SchemaCache {
    by_ident: HashMap<String, SchemaAttribute>,
    by_entity_id: HashMap<i64, String>,
}

impl SchemaCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, ident: &str) -> Option<&SchemaAttribute> {
        self.by_ident.get(ident)
    }

    pub fn is_schema_entity(&self, entity_id: i64) -> bool {
        self.by_entity_id.contains_key(&entity_id)
    }

    fn insert(&mut self, attr: SchemaAttribute) {
        self.by_entity_id.insert(attr.entity_id, attr.ident.clone());
        self.by_ident.insert(attr.ident.clone(), attr);
    }

    pub fn attribute_map(&self) -> HashMap<String, i64> {
        self.by_entity_id.iter().map(|(id, ident)| (ident.clone(), *id)).collect()
    }

    /// Extract schema attributes defined by assert datoms.
    /// Entities with both db/ident and db/valueType become SchemaAttributes.
    /// Entities with only db/ident (enum entities) are skipped.
    fn extract_schema_attrs(datoms: &[Datom]) -> Result<Vec<SchemaAttribute>> {
        let mut facts: HashMap<i64, HashMap<&str, &DataType>> = HashMap::new();
        for d in datoms.iter().filter(|d| d.op == DatomOp::Assert) {
            if matches!(d.attribute.as_str(), "db/ident" | "db/valueType") {
                facts.entry(d.entity).or_default().insert(&d.attribute, &d.value);
            }
        }

        let mut attrs = Vec::new();
        for (entity_id, f) in &facts {
            let ident = match f.get("db/ident") {
                Some(DataType::Keyword(kw)) => {
                    let s = kw.to_string();
                    s.strip_prefix(':').unwrap_or(&s).to_string()
                }
                Some(_) => return Err(anyhow::anyhow!("db/ident must be a Keyword")),
                None => continue,
            };
            let value_type = match f.get("db/valueType") {
                Some(DataType::Keyword(kw)) => ValueType::from_keyword(kw)?,
                Some(_) => return Err(anyhow::anyhow!("db/valueType must be a Keyword")),
                None => continue, // enum entity
            };
            attrs.push(SchemaAttribute {
                ident, value_type, cardinality: Cardinality::One, entity_id: *entity_id,
            });
        }
        Ok(attrs)
    }

    pub fn process_tx(&mut self, datoms: &[Datom]) -> Result<()> {
        for attr in Self::extract_schema_attrs(datoms)? {
            self.insert(attr);
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.by_ident.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_ident.is_empty()
    }

    pub fn validate_tx(&self, datoms: &[Datom]) -> Result<()> {
        for datom in datoms {
            // Schema immutability
            if self.is_schema_entity(datom.entity) {
                return Err(anyhow::anyhow!(
                    "Cannot modify schema entity {}", datom.entity
                ));
            }

            // Attribute existence and type check
            let schema_attr = self.by_ident.get(&datom.attribute)
                .ok_or_else(|| anyhow::anyhow!("Unknown attribute: {}", datom.attribute))?;
            if !schema_attr.value_type.matches(&datom.value) {
                return Err(anyhow::anyhow!(
                    "Type mismatch for attribute {}: expected {}, got {:?}",
                    datom.attribute, schema_attr.value_type, datom.value
                ));
            }
        }

        // Validate that new schema definitions parse correctly
        Self::extract_schema_attrs(datoms)?;
        Ok(())
    }
}

// --- Bootstrap transaction builders ---

/// Build a Put operation for a schema attribute entity.
/// Schema attributes have db/ident, db/valueType, and db/cardinality.
fn schema_attribute(id: i64, ns: &str, name: &str, value_type: &str, cardinality: i64) -> TxOp {
    let mut doc = BTreeMap::new();
    doc.insert("db/id".to_string(), DataType::Long(id));
    doc.insert(
        "db/ident".to_string(),
        DataType::Keyword(Keyword::namespaced(ns, name)),
    );
    doc.insert(
        "db/valueType".to_string(),
        DataType::Keyword(Keyword::namespaced("db.type", value_type)),
    );
    doc.insert("db/cardinality".to_string(), DataType::Long(cardinality));
    TxOp::Put(Document(doc))
}

/// Build a Put operation for an enum entity (value type or cardinality).
/// Enum entities only have db/ident.
fn enum_entity(id: i64, ns: &str, name: &str) -> TxOp {
    let mut doc = BTreeMap::new();
    doc.insert("db/id".to_string(), DataType::Long(id));
    doc.insert(
        "db/ident".to_string(),
        DataType::Keyword(Keyword::namespaced(ns, name)),
    );
    TxOp::Put(Document(doc))
}

/// Build the bootstrap schema transaction.
/// This is the first transaction on a fresh database (tx_id=0).
pub fn bootstrap_schema_tx() -> Vec<TxOp> {
    vec![
        // Schema attribute entities (IDs 1-3)
        schema_attribute(DB_IDENT, "db", "ident", "keyword", DB_CARDINALITY_ONE),
        schema_attribute(
            DB_VALUE_TYPE,
            "db",
            "valueType",
            "keyword",
            DB_CARDINALITY_ONE,
        ),
        // TODO: db/cardinality still uses Long (entity ref) values. Consider switching to
        // keywords (like db/valueType) for consistency.
        schema_attribute(
            DB_CARDINALITY,
            "db",
            "cardinality",
            "long",
            DB_CARDINALITY_ONE,
        ),
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

/// Load the SchemaCache from indices by querying with the Datalog engine.
/// Finds all entities with db/ident + db/valueType + db/cardinality (inner join).
/// Uses bootstrap attribute constants (DB_IDENT, DB_VALUE_TYPE, DB_CARDINALITY) to
/// bootstrap the query — these are the only attributes needed to query schema entities.
pub async fn load_schema_from_indices(slatedb: Arc<slatedb::Db>) -> SchemaCache {
    // Build attribute map from bootstrap constants — sufficient to query schema entities
    let mut attribute_map = HashMap::new();
    attribute_map.insert("db/ident".to_string(), DB_IDENT);
    attribute_map.insert("db/valueType".to_string(), DB_VALUE_TYPE);
    attribute_map.insert("db/cardinality".to_string(), DB_CARDINALITY);
    let snapshot = slatedb.snapshot().await.expect("Failed to create snapshot");
    let handle = Handle::current();

    let query = Query {
        find: FindSpec::FindRel(vec![
            FindElement::Variable("?e".to_string()),
            FindElement::Variable("?ident".to_string()),
            FindElement::Variable("?vt".to_string()),
            FindElement::Variable("?card".to_string()),
        ]),
        where_clauses: vec![
            WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?e".to_string()),
                attribute: PatternElement::Constant(DataType::Keyword(Keyword::namespaced(
                    "db", "ident",
                ))),
                value: PatternElement::Variable("?ident".to_string()),
            }),
            WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?e".to_string()),
                attribute: PatternElement::Constant(DataType::Keyword(Keyword::namespaced(
                    "db",
                    "valueType",
                ))),
                value: PatternElement::Variable("?vt".to_string()),
            }),
            WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?e".to_string()),
                attribute: PatternElement::Constant(DataType::Keyword(Keyword::namespaced(
                    "db",
                    "cardinality",
                ))),
                value: PatternElement::Variable("?card".to_string()),
            }),
        ],
    };

    validate_query(&query).expect("Schema query validation failed");

    let results = tokio::task::spawn_blocking(move || {
        // TODO: use the latest submitted system_time instead of Utc::now() for consistency
        execute_query(&query, snapshot, handle, &attribute_map, chrono::Utc::now())
    })
    .await
    .expect("Schema query task failed")
    .expect("Schema query execution failed");

    let mut cache = SchemaCache::new();

    for row in results {
        let entity_id = match &row[0] {
            DataType::Long(id) => *id,
            other => panic!("Expected Long for entity_id, got {:?}", other),
        };
        let ident = match &row[1] {
            DataType::Keyword(kw) => {
                let s = kw.to_string();
                // TODO: this destructing is brittle. Let's address it when we move to only use the EDN crate.
                s.strip_prefix(':').unwrap_or(&s).to_string()
            }
            other => panic!("Expected Keyword for ident, got {:?}", other),
        };
        let value_type = match &row[2] {
            DataType::Keyword(kw) => {
                ValueType::from_keyword(kw).unwrap_or_else(|e| panic!("Invalid value type: {}", e))
            }
            other => panic!("Expected Keyword for valueType, got {:?}", other),
        };
        let cardinality = match &row[3] {
            DataType::Long(id) => Cardinality::from_entity_id(*id)
                .unwrap_or_else(|e| panic!("Invalid cardinality: {}", e)),
            other => panic!("Expected Long for cardinality, got {:?}", other),
        };

        cache.insert(SchemaAttribute {
            ident,
            value_type,
            cardinality,
            entity_id,
        });
    }

    cache
}

/// Build a Put operation for a plain (non-namespaced) schema attribute.
/// Used by tests that define attributes like "name", "age", etc.
#[cfg(any(test, feature = "test-helpers"))]
fn plain_schema_attribute(id: i64, name: &str, value_type: &str) -> TxOp {
    let mut doc = BTreeMap::new();
    doc.insert("db/id".to_string(), DataType::Long(id));
    doc.insert(
        "db/ident".to_string(),
        DataType::Keyword(Keyword::plain(name)),
    );
    doc.insert(
        "db/valueType".to_string(),
        DataType::Keyword(Keyword::namespaced("db.type", value_type)),
    );
    TxOp::Put(Document(doc))
}

/// Build a transaction that defines common test attributes.
/// Use entity IDs 50-59 (between bootstrap schema 1-31 and test data 100+).
#[cfg(any(test, feature = "test-helpers"))]
pub fn test_schema_tx() -> Vec<TxOp> {
    vec![
        plain_schema_attribute(50, "name", "string"),
        plain_schema_attribute(51, "age", "long"),
        plain_schema_attribute(52, "email", "string"),
        // TODO: update this to ref once we support DataType::Ref
        plain_schema_attribute(53, "follows", "long"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::st_from_unix_epoch;
    use crate::ops::tx_ops_to_datoms;

    fn kw_ns(ns: &str, name: &str) -> DataType {
        DataType::Keyword(Keyword::namespaced(ns, name))
    }

    fn to_datoms(ops: &[TxOp]) -> Vec<Datom> {
        tx_ops_to_datoms(ops, st_from_unix_epoch(0)).unwrap()
    }

    fn bootstrapped_cache() -> SchemaCache {
        let mut cache = SchemaCache::new();
        cache.process_tx(&to_datoms(&bootstrap_schema_tx())).unwrap();
        cache
    }

    fn bootstrapped_cache_with_person_name() -> SchemaCache {
        let mut cache = bootstrapped_cache();
        cache.process_tx(&to_datoms(&[plain_schema_attribute(100, "name", "string")])).unwrap();
        cache
    }

    #[test]
    fn test_bootstrap_schema_tx() {
        let tx = bootstrap_schema_tx();
        assert_eq!(tx.len(), 19); // 3 attrs + 14 type enums + 2 cardinality enums

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

        let serialized = bincode::serialize(&tx).unwrap();
        let deserialized: Vec<TxOp> = bincode::deserialize(&serialized).unwrap();
        assert_eq!(tx.len(), deserialized.len());
    }

    #[test]
    fn test_schema_cache_from_bootstrap() {
        let cache = bootstrapped_cache();
        // 3 schema attrs; enum entities (db/ident only, no db/valueType) are skipped
        assert_eq!(cache.len(), 3);

        let db_ident = cache.get("db/ident").unwrap();
        assert_eq!(db_ident.entity_id, DB_IDENT);
        assert_eq!(db_ident.value_type, ValueType::Keyword);

        let db_vt = cache.get("db/valueType").unwrap();
        assert_eq!(db_vt.entity_id, DB_VALUE_TYPE);
        assert_eq!(db_vt.value_type, ValueType::Keyword);

        let db_card = cache.get("db/cardinality").unwrap();
        assert_eq!(db_card.entity_id, DB_CARDINALITY);
        assert_eq!(db_card.value_type, ValueType::Long);
    }

    #[test]
    fn test_process_tx_user_attribute() {
        let mut cache = bootstrapped_cache();
        cache.process_tx(&to_datoms(&[plain_schema_attribute(100, "name", "string")])).unwrap();
        assert_eq!(cache.len(), 4);

        let attr = cache.get("name").unwrap();
        assert_eq!(attr.entity_id, 100);
        assert_eq!(attr.value_type, ValueType::String);
    }

    #[test]
    fn test_enum_entity_not_added_to_cache() {
        let cache = bootstrapped_cache();
        // enum entities have db/ident but no db/valueType → not schema attributes
        assert!(cache.get("db.type/string").is_none());
    }

    #[test]
    fn test_validate_tx_valid() {
        let cache = bootstrapped_cache_with_person_name();
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(200));
        doc.insert("name".to_string(), DataType::String("Alice".to_string()));
        assert!(cache.validate_tx(&to_datoms(&[TxOp::Put(Document(doc))])).is_ok());
    }

    #[test]
    fn test_validate_tx_unknown_attribute() {
        let cache = bootstrapped_cache_with_person_name();
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(200));
        doc.insert("person/age".to_string(), DataType::Long(30));
        let err = cache.validate_tx(&to_datoms(&[TxOp::Put(Document(doc))])).unwrap_err();
        assert!(err.to_string().contains("Unknown attribute: person/age"));
    }

    #[test]
    fn test_validate_tx_type_mismatch() {
        let cache = bootstrapped_cache_with_person_name();
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(200));
        doc.insert("name".to_string(), DataType::Long(42));
        let err = cache.validate_tx(&to_datoms(&[TxOp::Put(Document(doc))])).unwrap_err();
        assert!(err.to_string().contains("Type mismatch"));
    }

    #[test]
    fn test_validate_tx_schema_defining_tx() {
        let cache = bootstrapped_cache();
        let ops = [plain_schema_attribute(100, "name", "string")];
        assert!(cache.validate_tx(&to_datoms(&ops)).is_ok());
    }

    #[test]
    fn test_schema_immutability() {
        let cache = bootstrapped_cache_with_person_name();

        // Cannot redefine existing schema entity
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(100));
        doc.insert("db/ident".to_string(), DataType::Keyword(Keyword::plain("name")));
        doc.insert("db/valueType".to_string(), kw_ns("db.type", "long"));
        let err = cache.validate_tx(&to_datoms(&[TxOp::Put(Document(doc))])).unwrap_err();
        assert!(err.to_string().contains("Cannot modify schema entity"));
    }

    #[test]
    fn test_value_type_matches() {
        assert!(ValueType::String.matches(&DataType::String("hello".to_string())));
        assert!(ValueType::Long.matches(&DataType::Long(42)));
        assert!(!ValueType::String.matches(&DataType::Long(42)));
    }
}
