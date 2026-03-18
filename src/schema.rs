use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{Error, Result};
use edn::symbols::Keyword;
use tokio::runtime::Handle;

use crate::datalog::{FindElement, FindSpec, PatternElement, Query, TriplePattern, WhereClause};
use crate::ops::{Datom, DatomOp, DataType, Document, Triple, TxOp};
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
    pub ident: String,
    pub value_type: ValueType,
    pub cardinality: Cardinality,
    pub entity_id: i64,
}

/// Intermediate state for collecting schema-defining datoms within a transaction.
#[derive(Debug, Default)]
struct PendingSchemaAttr {
    ident: Option<DataType>,
    value_type: Option<DataType>,
    cardinality: Option<DataType>,
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

    /// Build an attribute name → entity_id map for the query engine.
    pub fn attribute_map(&self) -> HashMap<String, i64> {
        self.by_ident
            .iter()
            .map(|(ident, attr)| (ident.clone(), attr.entity_id))
            .collect()
    }

    /// Collect schema-relevant datoms grouped by entity.
    /// For each entity that has any of db/ident, db/valueType, db/cardinality,
    /// collect the values into a PendingSchemaAttr.
    fn collect_schema_facts(datoms: &[Datom]) -> HashMap<i64, PendingSchemaAttr> {
        let mut pending: HashMap<i64, PendingSchemaAttr> = HashMap::new();
        for datom in datoms {
            if datom.op != DatomOp::Assert {
                continue;
            }
            match datom.attribute.as_str() {
                "db/ident" => {
                    pending.entry(datom.entity).or_default().ident = Some(datom.value.clone());
                }
                "db/valueType" => {
                    pending.entry(datom.entity).or_default().value_type = Some(datom.value.clone());
                }
                "db/cardinality" => {
                    pending.entry(datom.entity).or_default().cardinality = Some(datom.value.clone());
                }
                _ => {}
            }
        }
        pending
    }

    /// Try to build a SchemaAttribute from collected schema facts for an entity.
    /// Returns Ok(Some(attr)) if all three required properties are present,
    /// Ok(None) if this is just an enum entity (has db/ident but no db/valueType),
    /// Err if the definition is malformed or incomplete.
    fn build_schema_attribute(entity_id: i64, pending: &PendingSchemaAttr) -> Result<Option<SchemaAttribute>> {
        let ident = match &pending.ident {
            Some(DataType::Keyword(kw)) => {
                let s = kw.to_string();
                // TODO: this destructing is brittle. Let's address it when we move to only use the EDN crate.
                s.strip_prefix(':').unwrap_or(&s).to_string()
            }
            Some(_) => return Err(anyhow::anyhow!("db/ident must be a Keyword")),
            None => return Ok(None),
        };

        let value_type = match &pending.value_type {
            Some(DataType::Keyword(kw)) => ValueType::from_keyword(kw)?,
            Some(_) => return Err(anyhow::anyhow!("db/valueType must be a Keyword")),
            None => return Ok(None), // enum entity — has db/ident but no db/valueType
        };

        let cardinality = match &pending.cardinality {
            Some(DataType::Long(id)) => Cardinality::from_entity_id(*id)?,
            Some(_) => return Err(anyhow::anyhow!("db/cardinality must be a Long (entity ref)")),
            None => Cardinality::One, // default to cardinality one
        };

        Ok(Some(SchemaAttribute {
            ident,
            value_type,
            cardinality,
            entity_id,
        }))
    }

    /// Process a transaction's datoms and update the cache with any new schema attributes.
    pub fn process_tx(&mut self, datoms: &[Datom]) -> Result<()> {
        let pending = Self::collect_schema_facts(datoms);
        for (entity_id, facts) in &pending {
            if let Some(attr) = Self::build_schema_attribute(*entity_id, facts)? {
                self.insert(attr);
            }
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.by_ident.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_ident.is_empty()
    }

    /// Validate a transaction's datoms against the schema.
    /// Checks:
    /// 1. Schema immutability — cannot redefine/retract/delete/erase schema entities
    /// 2. Attribute existence and type matching
    /// 3. Schema completeness — new schema attributes must have all 3 required properties
    pub fn validate_tx(&self, datoms: &[Datom], tx_ops: &[TxOp]) -> Result<()> {
        // Check Delete/Erase against schema entities (these aren't in datoms)
        for op in tx_ops {
            match op {
                TxOp::Delete(entity_id) => {
                    if self.is_schema_entity(entity_id.0) {
                        return Err(anyhow::anyhow!("Cannot delete schema entity {}", entity_id.0));
                    }
                }
                TxOp::Erase(entity_id) => {
                    if self.is_schema_entity(entity_id.0) {
                        return Err(anyhow::anyhow!("Cannot erase schema entity {}", entity_id.0));
                    }
                }
                _ => {}
            }
        }

        // Check immutability: no assertions or retractions on existing schema entities
        for datom in datoms {
            if self.is_schema_entity(datom.entity) {
                return Err(anyhow::anyhow!(
                    "Cannot modify schema attribute entity {}",
                    datom.entity
                ));
            }

            // Check retraction of schema-defining attributes on any entity
            if datom.op == DatomOp::Retract {
                match datom.attribute.as_str() {
                    "db/ident" | "db/valueType" | "db/cardinality" => {
                        return Err(anyhow::anyhow!(
                            "Cannot retract schema attribute '{}'",
                            datom.attribute
                        ));
                    }
                    _ => {}
                }
            }

            // Attribute existence and type check
            self.validate_attribute_value(&datom.attribute, &datom.value)?;
        }

        // Schema completeness: if a tx defines db/ident + db/valueType, db/cardinality must also be present
        let pending = Self::collect_schema_facts(datoms);
        for (entity_id, facts) in &pending {
            // build_schema_attribute returns Err if db/ident + db/valueType present but db/cardinality missing
            Self::build_schema_attribute(*entity_id, facts)?;
        }

        Ok(())
    }

    fn validate_attribute_value(&self, attr_name: &str, value: &DataType) -> Result<()> {
        let schema_attr = self
            .by_ident
            .get(attr_name)
            .ok_or_else(|| anyhow::anyhow!("Unknown attribute: {}", attr_name))?;

        if !schema_attr.value_type.matches(value) {
            return Err(anyhow::anyhow!(
                "Type mismatch for attribute {}: expected {}, got {:?}",
                attr_name,
                schema_attr.value_type,
                value
            ));
        }

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
    doc.insert("db/valueType".to_string(), DataType::Keyword(Keyword::namespaced("db.type", value_type)));
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
        schema_attribute(DB_VALUE_TYPE, "db", "valueType", "keyword", DB_CARDINALITY_ONE),
        // TODO: db/cardinality still uses Long (entity ref) values. Consider switching to
        // keywords (like db/valueType) for consistency.
        schema_attribute(DB_CARDINALITY, "db", "cardinality", "long", DB_CARDINALITY_ONE),
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
                attribute: PatternElement::Constant(DataType::Keyword(
                    Keyword::namespaced("db", "ident"),
                )),
                value: PatternElement::Variable("?ident".to_string()),
            }),
            WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?e".to_string()),
                attribute: PatternElement::Constant(DataType::Keyword(
                    Keyword::namespaced("db", "valueType"),
                )),
                value: PatternElement::Variable("?vt".to_string()),
            }),
            WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?e".to_string()),
                attribute: PatternElement::Constant(DataType::Keyword(
                    Keyword::namespaced("db", "cardinality"),
                )),
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
    doc.insert("db/valueType".to_string(), DataType::Keyword(Keyword::namespaced("db.type", value_type)));
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
    use crate::ops::{tx_ops_to_datoms, EntityId};

    fn kw_ns(ns: &str, name: &str) -> DataType {
        DataType::Keyword(Keyword::namespaced(ns, name))
    }

    fn test_tx() -> crate::clock::Instant {
        st_from_unix_epoch(0)
    }

    /// Convert TxOps to datoms using a zero timestamp (convenience for tests).
    fn to_datoms(ops: &[TxOp]) -> Vec<Datom> {
        tx_ops_to_datoms(ops, test_tx()).unwrap()
    }

    /// Bootstrap a SchemaCache from the bootstrap transaction.
    fn bootstrapped_cache() -> SchemaCache {
        let mut cache = SchemaCache::new();
        cache.process_tx(&to_datoms(&bootstrap_schema_tx())).unwrap();
        cache
    }

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
        let mut ids: Vec<i64> = tx
            .iter()
            .map(|op| match op {
                TxOp::Put(Document(doc)) => match doc.get("db/id") {
                    Some(DataType::Long(id)) => *id,
                    _ => panic!("Expected db/id Long"),
                },
                _ => panic!("Expected Put"),
            })
            .collect();
        ids.sort();
        ids.dedup();
        assert_eq!(
            ids.len(),
            tx.len(),
            "All bootstrap entity IDs must be unique"
        );
    }

    #[test]
    fn test_schema_cache_from_bootstrap() {
        let cache = bootstrapped_cache();

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
        assert_eq!(db_value_type.value_type, ValueType::Keyword);
        assert_eq!(db_value_type.cardinality, Cardinality::One);

        let db_cardinality = cache.get("db/cardinality").unwrap();
        assert_eq!(db_cardinality.entity_id, DB_CARDINALITY);
        assert_eq!(db_cardinality.value_type, ValueType::Long);
        assert_eq!(db_cardinality.cardinality, Cardinality::One);
    }

    #[test]
    fn test_schema_cache_user_attribute_via_put() {
        let mut cache = bootstrapped_cache();

        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(100));
        doc.insert("db/ident".to_string(), kw_ns("person", "name"));
        doc.insert("db/valueType".to_string(), kw_ns("db.type", "string"));
        doc.insert("db/cardinality".to_string(), DataType::Long(DB_CARDINALITY_ONE));

        cache.process_tx(&to_datoms(&[TxOp::Put(Document(doc))])).unwrap();
        assert_eq!(cache.len(), 4);

        let person_name = cache.get("person/name").unwrap();
        assert_eq!(person_name.entity_id, 100);
        assert_eq!(person_name.value_type, ValueType::String);
        assert_eq!(person_name.cardinality, Cardinality::One);
    }

    #[test]
    fn test_schema_cache_user_attribute_via_add_triples() {
        let mut cache = bootstrapped_cache();

        let datoms = vec![
            Datom {
                entity: 100,
                attribute: "db/ident".to_string(),
                value: kw_ns("person", "age"),
                tx: test_tx(),
                op: DatomOp::Assert,
            },
            Datom {
                entity: 100,
                attribute: "db/valueType".to_string(),
                value: kw_ns("db.type", "long"),
                tx: test_tx(),
                op: DatomOp::Assert,
            },
            Datom {
                entity: 100,
                attribute: "db/cardinality".to_string(),
                value: DataType::Long(DB_CARDINALITY_ONE),
                tx: test_tx(),
                op: DatomOp::Assert,
            },
        ];

        cache.process_tx(&datoms).unwrap();
        assert_eq!(cache.len(), 4);

        let person_age = cache.get("person/age").unwrap();
        assert_eq!(person_age.entity_id, 100);
        assert_eq!(person_age.value_type, ValueType::Long);
        assert_eq!(person_age.cardinality, Cardinality::One);
    }

    #[test]
    fn test_value_type_matches() {
        assert!(ValueType::String.matches(&DataType::String("hello".to_string())));
        assert!(ValueType::Long.matches(&DataType::Long(42)));
        assert!(ValueType::Boolean.matches(&DataType::Boolean(true)));

        assert!(!ValueType::String.matches(&DataType::Long(42)));
        assert!(!ValueType::Long.matches(&DataType::String("hello".to_string())));
    }

    #[test]
    fn test_enum_entity_not_added_to_cache() {
        let mut cache = SchemaCache::new();

        // An enum entity only has db/ident (no db/valueType), should NOT be a schema attribute
        let datoms = vec![Datom {
            entity: 50,
            attribute: "db/ident".to_string(),
            value: kw_ns("my.enum", "value"),
            tx: test_tx(),
            op: DatomOp::Assert,
        }];

        cache.process_tx(&datoms).unwrap();
        assert_eq!(cache.len(), 0);
    }

    fn bootstrapped_cache_with_person_name() -> SchemaCache {
        let mut cache = bootstrapped_cache();

        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(100));
        doc.insert("db/ident".to_string(), kw_ns("person", "name"));
        doc.insert("db/valueType".to_string(), kw_ns("db.type", "string"));
        doc.insert("db/cardinality".to_string(), DataType::Long(DB_CARDINALITY_ONE));
        cache.process_tx(&to_datoms(&[TxOp::Put(Document(doc))])).unwrap();

        cache
    }

    #[test]
    fn test_validate_tx_valid_put() {
        let cache = bootstrapped_cache_with_person_name();

        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(200));
        doc.insert("person/name".to_string(), DataType::String("Alice".to_string()));
        let ops = [TxOp::Put(Document(doc))];
        let result = cache.validate_tx(&to_datoms(&ops), &ops);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_tx_unknown_attribute() {
        let cache = bootstrapped_cache_with_person_name();

        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(200));
        doc.insert("person/age".to_string(), DataType::Long(30));
        let ops = [TxOp::Put(Document(doc))];
        let result = cache.validate_tx(&to_datoms(&ops), &ops);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unknown attribute: person/age"));
    }

    #[test]
    fn test_validate_tx_type_mismatch() {
        let cache = bootstrapped_cache_with_person_name();

        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(200));
        doc.insert("person/name".to_string(), DataType::Long(42));
        let ops = [TxOp::Put(Document(doc))];
        let result = cache.validate_tx(&to_datoms(&ops), &ops);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Type mismatch"));
    }

    #[test]
    fn test_validate_tx_schema_defining_tx() {
        let mut cache = bootstrapped_cache();

        // Defining a new attribute should validate against the bootstrap schema
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(100));
        doc.insert("db/ident".to_string(), kw_ns("person", "name"));
        doc.insert("db/valueType".to_string(), kw_ns("db.type", "string"));
        doc.insert("db/cardinality".to_string(), DataType::Long(DB_CARDINALITY_ONE));
        let ops = [TxOp::Put(Document(doc))];
        let result = cache.validate_tx(&to_datoms(&ops), &ops);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_tx_add_triple() {
        let cache = bootstrapped_cache_with_person_name();

        // Valid add
        let datoms = vec![Datom {
            entity: 200,
            attribute: "person/name".to_string(),
            value: DataType::String("Bob".to_string()),
            tx: test_tx(),
            op: DatomOp::Assert,
        }];
        assert!(cache.validate_tx(&datoms, &[]).is_ok());

        // Type mismatch in add
        let datoms = vec![Datom {
            entity: 200,
            attribute: "person/name".to_string(),
            value: DataType::Long(42),
            tx: test_tx(),
            op: DatomOp::Assert,
        }];
        assert!(cache.validate_tx(&datoms, &[]).is_err());
    }

    #[test]
    fn test_validate_tx_retract_triple() {
        let cache = bootstrapped_cache_with_person_name();

        // Valid retract
        let datoms = vec![Datom {
            entity: 200,
            attribute: "person/name".to_string(),
            value: DataType::String("Bob".to_string()),
            tx: test_tx(),
            op: DatomOp::Retract,
        }];
        assert!(cache.validate_tx(&datoms, &[]).is_ok());

        // Unknown attribute in retract
        let datoms = vec![Datom {
            entity: 200,
            attribute: "person/age".to_string(),
            value: DataType::Long(30),
            tx: test_tx(),
            op: DatomOp::Retract,
        }];
        assert!(cache.validate_tx(&datoms, &[]).is_err());
    }

    // --- Schema immutability tests ---

    #[test]
    fn test_cannot_redefine_schema_attribute() {
        let cache = bootstrapped_cache_with_person_name();

        // Try to redefine entity 100 (person/name) — should be rejected
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(100));
        doc.insert("db/ident".to_string(), kw_ns("person", "name"));
        doc.insert("db/valueType".to_string(), kw_ns("db.type", "long"));
        doc.insert("db/cardinality".to_string(), DataType::Long(DB_CARDINALITY_ONE));
        let ops = [TxOp::Put(Document(doc))];
        let result = cache.validate_tx(&to_datoms(&ops), &ops);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cannot modify schema attribute"));
    }

    #[test]
    fn test_cannot_retract_schema_attributes() {
        let cache = bootstrapped_cache_with_person_name();

        let datoms = vec![Datom {
            entity: 200,
            attribute: "db/ident".to_string(),
            value: kw_ns("person", "name"),
            tx: test_tx(),
            op: DatomOp::Retract,
        }];
        let result = cache.validate_tx(&datoms, &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cannot retract schema attribute"));
    }

    #[test]
    fn test_cannot_delete_schema_entity() {
        let cache = bootstrapped_cache_with_person_name();

        let ops = [TxOp::Delete(EntityId(100))];
        let result = cache.validate_tx(&[], &ops);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cannot delete schema entity"));
    }

    #[test]
    fn test_cannot_erase_schema_entity() {
        let cache = bootstrapped_cache_with_person_name();

        let ops = [TxOp::Erase(EntityId(100))];
        let result = cache.validate_tx(&[], &ops);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cannot erase schema entity"));
    }

    #[test]
    fn test_schema_missing_cardinality_defaults_to_one() {
        let mut cache = bootstrapped_cache();

        // db/ident + db/valueType present, db/cardinality missing → defaults to One
        let datoms = vec![
            Datom {
                entity: 100,
                attribute: "db/ident".to_string(),
                value: kw_ns("person", "name"),
                tx: test_tx(),
                op: DatomOp::Assert,
            },
            Datom {
                entity: 100,
                attribute: "db/valueType".to_string(),
                value: kw_ns("db.type", "string"),
                tx: test_tx(),
                op: DatomOp::Assert,
            },
        ];
        cache.process_tx(&datoms).unwrap();
        let attr = cache.get("person/name").unwrap();
        assert_eq!(attr.cardinality, Cardinality::One);
    }
}
