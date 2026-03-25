use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{Error, Result};
use edn::symbols::Keyword;
use tokio::runtime::Handle;

use crate::ops::{Attribute, DataType, Datom, DatomOp, Entid, TxOp};
use crate::parse::parse_query;
use crate::query::{execute_query, validate_query};

// --- Reserved entity IDs ---
// Used as explicit db/id values in bootstrap_schema_tx().

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
#[allow(dead_code)]
pub const DB_CARDINALITY_ONE: i64 = 30;
#[allow(dead_code)]
pub const DB_CARDINALITY_MANY: i64 = 31;

// Transaction schema attribute entities
pub const DB_TX_INSTANT: i64 = 40;
pub const DB_TX_ID: i64 = 41;
pub const DB_TX_RESULT: i64 = 42;
pub const DB_TX_ERROR: i64 = 43;

// Transaction result enum entities
pub const DB_TX_COMMITTED: i64 = 50;
pub const DB_TX_ABORTED: i64 = 51;

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
            Cardinality::One => write!(f, "db.cardinality/one"),
            Cardinality::Many => write!(f, "db.cardinality/many"),
        }
    }
}

impl Cardinality {
    /// Map a cardinality keyword (e.g. :db.cardinality/one) to a Cardinality.
    pub fn from_keyword(kw: &Keyword) -> Result<Self> {
        let s = kw.to_string();
        let s = s.strip_prefix(':').unwrap_or(&s);
        match s {
            "db.cardinality/one" => Ok(Cardinality::One),
            "db.cardinality/many" => Ok(Cardinality::Many),
            _ => Err(anyhow::anyhow!("Unknown cardinality keyword: {}", kw)),
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
    pub fn validate_schema_attrs(datoms: &[Datom]) -> Result<Vec<SchemaAttribute>> {
        if !datoms.iter().any(|d| d.op == DatomOp::Assert
            && matches!(d.attribute.as_str(), "db/ident" | "db/valueType" | "db/cardinality"))
        {
            return Ok(Vec::new());
        }

        let mut facts: HashMap<i64, HashMap<&str, &DataType>> = HashMap::new();
        for d in datoms.iter().filter(|d| d.op == DatomOp::Assert) {
            if matches!(d.attribute.as_str(), "db/ident" | "db/valueType" | "db/cardinality") {
                facts.entry(d.entity).or_default().insert(&d.attribute, &d.value);
            }
        }

        let mut attrs = Vec::new();
        for (entity_id, f) in &facts {
            // TODO: should match against a namespaced Keyword and use its
            // namespace/name directly instead of string-stripping the colon prefix.
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
            let cardinality = match f.get("db/cardinality") {
                Some(DataType::Keyword(kw)) => Cardinality::from_keyword(kw)?,
                Some(_) => return Err(anyhow::anyhow!("db/cardinality must be a Keyword")),
                None => return Err(anyhow::anyhow!(
                    "db/cardinality is required for schema attribute '{}'", ident
                )),
            };
            attrs.push(SchemaAttribute {
                ident, value_type, cardinality, entity_id: *entity_id,
            });
        }
        Ok(attrs)
    }

    pub fn process_tx(&mut self, new_attrs: Vec<SchemaAttribute>) {
        for attr in new_attrs {
            self.insert(attr);
        }
    }

    pub fn len(&self) -> usize {
        self.by_ident.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_ident.is_empty()
    }

    /// Validate datoms against the schema and return any new schema attributes
    /// defined by this transaction.
    pub fn validate_tx(&self, datoms: &[Datom]) -> Result<Vec<SchemaAttribute>> {
        for datom in datoms {
            if self.is_schema_entity(datom.entity) {
                return Err(anyhow::anyhow!(
                    "Cannot modify schema entity {}", datom.entity
                ));
            }

            let schema_attr = self.by_ident.get(&datom.attribute)
                .ok_or_else(|| anyhow::anyhow!("Unknown attribute: {}", datom.attribute))?;
            if !schema_attr.value_type.matches(&datom.value) {
                return Err(anyhow::anyhow!(
                    "Type mismatch for attribute {}: expected {}, got {:?}",
                    datom.attribute, schema_attr.value_type, datom.value
                ));
            }
        }

        // Also validates schema definitions
        // (e.g. db/ident must be a Keyword, db/valueType must map to a valid ValueType).
        Self::validate_schema_attrs(datoms)
    }
}

// --- Bootstrap transaction builders ---

/// Build a Put for a schema attribute without explicit db/id.
fn schema_attribute_with_cardinality(ident: Keyword, value_type: &str, cardinality: &str) -> TxOp {
    let mut doc = BTreeMap::new();
    doc.insert("db/ident".to_string(), DataType::Keyword(ident));
    doc.insert(
        "db/valueType".to_string(),
        DataType::Keyword(Keyword::namespaced("db.type", value_type)),
    );
    doc.insert(
        "db/cardinality".to_string(),
        DataType::Keyword(Keyword::namespaced("db.cardinality", cardinality)),
    );
    TxOp::Put(doc)
}

fn schema_attribute(ident: Keyword, value_type: &str) -> TxOp {
    schema_attribute_with_cardinality(ident, value_type, "one")
}

/// Build a bootstrap Put with an explicit entity ID.
fn bootstrap_put(id: i64, ident: Keyword) -> TxOp {
    let mut doc = BTreeMap::new();
    doc.insert("db/id".to_string(), DataType::Long(id));
    doc.insert("db/ident".to_string(), DataType::Keyword(ident));
    TxOp::Put(doc)
}

/// Build a bootstrap Put for a schema attribute with an explicit entity ID.
fn bootstrap_schema_attribute(id: i64, ident: Keyword, value_type: &str) -> TxOp {
    let mut doc = BTreeMap::new();
    doc.insert("db/id".to_string(), DataType::Long(id));
    doc.insert("db/ident".to_string(), DataType::Keyword(ident));
    doc.insert(
        "db/valueType".to_string(),
        DataType::Keyword(Keyword::namespaced("db.type", value_type)),
    );
    doc.insert(
        "db/cardinality".to_string(),
        DataType::Keyword(Keyword::namespaced("db.cardinality", "one")),
    );
    TxOp::Put(doc)
}

/// Build the bootstrap schema transaction.
/// This is the first transaction on a fresh database.
/// Each entity has an explicit db/id matching the constants above.
pub fn bootstrap_schema_tx() -> Vec<TxOp> {
    vec![
        // Schema attribute entities
        bootstrap_schema_attribute(DB_IDENT, Keyword::namespaced("db", "ident"), "keyword"),
        bootstrap_schema_attribute(DB_VALUE_TYPE, Keyword::namespaced("db", "valueType"), "keyword"),
        bootstrap_schema_attribute(DB_CARDINALITY, Keyword::namespaced("db", "cardinality"), "keyword"),
        // Value type enum entities
        bootstrap_put(DB_TYPE_KEYWORD, Keyword::namespaced("db.type", "keyword")),
        bootstrap_put(DB_TYPE_STRING, Keyword::namespaced("db.type", "string")),
        bootstrap_put(DB_TYPE_LONG, Keyword::namespaced("db.type", "long")),
        bootstrap_put(DB_TYPE_REF, Keyword::namespaced("db.type", "ref")),
        bootstrap_put(DB_TYPE_BOOLEAN, Keyword::namespaced("db.type", "boolean")),
        bootstrap_put(DB_TYPE_DOUBLE, Keyword::namespaced("db.type", "double")),
        bootstrap_put(DB_TYPE_FLOAT, Keyword::namespaced("db.type", "float")),
        bootstrap_put(DB_TYPE_INSTANT, Keyword::namespaced("db.type", "instant")),
        bootstrap_put(DB_TYPE_UUID, Keyword::namespaced("db.type", "uuid")),
        bootstrap_put(DB_TYPE_BYTES, Keyword::namespaced("db.type", "bytes")),
        bootstrap_put(DB_TYPE_BIGINT, Keyword::namespaced("db.type", "bigint")),
        bootstrap_put(DB_TYPE_TUPLE, Keyword::namespaced("db.type", "tuple")),
        bootstrap_put(DB_TYPE_VECTOR, Keyword::namespaced("db.type", "vector")),
        bootstrap_put(DB_TYPE_MAP, Keyword::namespaced("db.type", "map")),
        // Cardinality enum entities
        bootstrap_put(DB_CARDINALITY_ONE, Keyword::namespaced("db.cardinality", "one")),
        bootstrap_put(DB_CARDINALITY_MANY, Keyword::namespaced("db.cardinality", "many")),
        // Transaction schema attributes
        bootstrap_schema_attribute(DB_TX_INSTANT, Keyword::namespaced("db", "txInstant"), "instant"),
        bootstrap_schema_attribute(DB_TX_ID, Keyword::namespaced("db", "txId"), "long"),
        bootstrap_schema_attribute(DB_TX_RESULT, Keyword::namespaced("db", "txResult"), "keyword"),
        bootstrap_schema_attribute(DB_TX_ERROR, Keyword::namespaced("db.tx", "error"), "string"),
        // Transaction result enum entities
        bootstrap_put(DB_TX_COMMITTED, Keyword::namespaced("db.tx", "committed")),
        bootstrap_put(DB_TX_ABORTED, Keyword::namespaced("db.tx", "aborted")),
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

    let query = parse_query(
        "[:find ?e ?ident ?vt ?card :where [?e :db/ident ?ident] [?e :db/valueType ?vt] [?e :db/cardinality ?card]]"
    ).expect("Schema query parse failed");

    validate_query(&query).expect("Schema query validation failed");

    let results = tokio::task::spawn_blocking(move || {
        // Use i64::MAX to see all facts (no temporal filtering)
        execute_query(&query, snapshot, handle, &attribute_map, i64::MAX)
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
            DataType::Keyword(kw) => Cardinality::from_keyword(kw)
                .unwrap_or_else(|e| panic!("Invalid cardinality: {}", e)),
            other => panic!("Expected Keyword for cardinality, got {:?}", other),
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

/// Build a transaction that defines common test attributes.
/// Entity IDs are auto-assigned by resolve_entity_ids at transaction time.
#[cfg(any(test, feature = "test-helpers"))]
pub fn test_schema_tx() -> Vec<TxOp> {
    vec![
        schema_attribute(Keyword::plain("name"), "string"),
        schema_attribute(Keyword::plain("age"), "long"),
        schema_attribute(Keyword::plain("email"), "string"),
        // TODO: update this to ref once we support DataType::Ref
        schema_attribute(Keyword::plain("follows"), "long"),
        schema_attribute_with_cardinality(Keyword::plain("tags"), "string", "many"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::tx_ops_to_datoms;

    fn kw_ns(ns: &str, name: &str) -> DataType {
        DataType::Keyword(Keyword::namespaced(ns, name))
    }

    fn to_datoms(ops: &[TxOp]) -> Vec<Datom> {
        let mut counters = crate::partition::PartitionCounters::new();
        let resolved = crate::partition::resolve_entity_ids(ops, &mut counters).unwrap();
        tx_ops_to_datoms(&resolved, 0_i64).unwrap()
    }

    fn extract_and_process(cache: &mut SchemaCache, datoms: &[Datom]) {
        cache.process_tx(SchemaCache::validate_schema_attrs(datoms).unwrap());
    }

    fn bootstrapped_cache() -> SchemaCache {
        let mut cache = SchemaCache::new();
        let tx_ops = bootstrap_schema_tx();
        let datoms = tx_ops_to_datoms(&tx_ops, 0_i64).unwrap();
        extract_and_process(&mut cache, &datoms);
        cache
    }

    fn bootstrapped_cache_with_person_name() -> SchemaCache {
        let mut cache = bootstrapped_cache();
        let ops = [schema_attribute(Keyword::plain("name"), "string")];
        extract_and_process(&mut cache, &to_datoms(&ops));
        cache
    }

    #[test]
    fn test_schema_cache_from_bootstrap() {
        let cache = bootstrapped_cache();
        // 7 schema attrs (3 core + 4 tx); enum entities (db/ident only, no db/valueType) are skipped
        assert_eq!(cache.len(), 7);

        let db_ident = cache.get("db/ident").unwrap();
        assert_eq!(db_ident.entity_id, DB_IDENT);
        assert_eq!(db_ident.value_type, ValueType::Keyword);

        let db_vt = cache.get("db/valueType").unwrap();
        assert_eq!(db_vt.entity_id, DB_VALUE_TYPE);
        assert_eq!(db_vt.value_type, ValueType::Keyword);

        let db_card = cache.get("db/cardinality").unwrap();
        assert_eq!(db_card.entity_id, DB_CARDINALITY);
        assert_eq!(db_card.value_type, ValueType::Keyword);
    }

    #[test]
    fn test_process_tx_user_attribute() {
        let cache = bootstrapped_cache_with_person_name();
        assert_eq!(cache.len(), 8);

        let attr = cache.get("name").unwrap();
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
        assert!(cache.validate_tx(&to_datoms(&[TxOp::Put(doc)])).is_ok());
    }

    #[test]
    fn test_validate_tx_unknown_attribute() {
        let cache = bootstrapped_cache_with_person_name();
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(200));
        doc.insert("person/age".to_string(), DataType::Long(30));
        let err = cache.validate_tx(&to_datoms(&[TxOp::Put(doc)])).unwrap_err();
        assert!(err.to_string().contains("Unknown attribute: person/age"));
    }

    #[test]
    fn test_validate_tx_type_mismatch() {
        let cache = bootstrapped_cache_with_person_name();
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(200));
        doc.insert("name".to_string(), DataType::Long(42));
        let err = cache.validate_tx(&to_datoms(&[TxOp::Put(doc)])).unwrap_err();
        assert!(err.to_string().contains("Type mismatch"));
    }

    #[test]
    fn test_validate_tx_schema_defining_tx() {
        let cache = bootstrapped_cache();
        let ops = [schema_attribute(Keyword::plain("name"), "string")];
        assert!(cache.validate_tx(&to_datoms(&ops)).is_ok());
    }

    #[test]
    fn test_schema_immutability() {
        let cache = bootstrapped_cache_with_person_name();
        let name_id = cache.get("name").unwrap().entity_id;

        // Cannot redefine existing schema entity
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(name_id));
        doc.insert("db/ident".to_string(), DataType::Keyword(Keyword::plain("name")));
        doc.insert("db/valueType".to_string(), kw_ns("db.type", "long"));
        doc.insert("db/cardinality".to_string(), DataType::Keyword(Keyword::namespaced("db.cardinality", "one")));
        let err = cache.validate_tx(&to_datoms(&[TxOp::Put(doc)])).unwrap_err();
        assert!(err.to_string().contains("Cannot modify schema entity"));
    }

    #[test]
    fn test_value_type_matches() {
        assert!(ValueType::String.matches(&DataType::String("hello".to_string())));
        assert!(ValueType::Long.matches(&DataType::Long(42)));
        assert!(!ValueType::String.matches(&DataType::Long(42)));
    }

    #[test]
    fn test_validate_schema_attrs_parses_cardinality_many() {
        let ops = [schema_attribute_with_cardinality(
            Keyword::plain("tags"), "string", "many",
        )];
        let attrs = SchemaCache::validate_schema_attrs(&to_datoms(&ops)).unwrap();
        assert_eq!(attrs.len(), 1);
        assert_eq!(attrs[0].ident, "tags");
        assert_eq!(attrs[0].cardinality, Cardinality::Many);
    }

    #[test]
    fn test_validate_schema_attrs_parses_cardinality_one() {
        let ops = [schema_attribute(Keyword::plain("name"), "string")];
        let attrs = SchemaCache::validate_schema_attrs(&to_datoms(&ops)).unwrap();
        assert_eq!(attrs.len(), 1);
        assert_eq!(attrs[0].cardinality, Cardinality::One);
    }

    #[test]
    fn test_validate_schema_attrs_missing_cardinality_errors() {
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(100));
        doc.insert("db/ident".to_string(), DataType::Keyword(Keyword::plain("name")));
        doc.insert("db/valueType".to_string(), kw_ns("db.type", "string"));
        // No db/cardinality
        let err = SchemaCache::validate_schema_attrs(&to_datoms(&[TxOp::Put(doc)])).unwrap_err();
        assert!(err.to_string().contains("db/cardinality is required"));
    }
}
