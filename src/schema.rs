use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{Error, Result};
use edn::kw;
use edn::symbols::Keyword;
use tokio::runtime::Handle;

use crate::ops::{DataType, Datom, DatomOp, TxOp};
use crate::parse::parse_query;
use crate::query::execute_query;

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

/// ident → entity_id (ALL named entities: enums + schema attrs)
pub type IdentMap = HashMap<String, i64>;
/// entity_id → ident (ALL named entities: enums + schema attrs)
pub type EntidMap = HashMap<i64, String>;
/// entity_id → Attribute (only schema attributes with db/valueType)
pub type AttributeMap = HashMap<i64, Attribute>;

/// A schema attribute's properties.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attribute {
    pub value_type: ValueType,
    pub multival: bool,
}

/// Builder for accumulating attribute properties from (e, a, v) assertions.
/// Each schema-related datom (db/valueType, db/cardinality) for the same entity
/// is witnessed onto the same builder. After all datoms are processed, build()
/// produces a validated Attribute.
#[derive(Debug, Default)]
pub struct AttributeBuilder {
    pub value_type: Option<ValueType>,
    pub multival: Option<bool>,
}

impl AttributeBuilder {
    /// Validate that all required fields are present for a new attribute.
    pub fn validate_install_attribute(&self) -> Result<()> {
        if self.value_type.is_none() {
            return Err(anyhow::anyhow!("db/valueType is required for schema attribute"));
        }
        if self.multival.is_none() {
            return Err(anyhow::anyhow!("db/cardinality is required for schema attribute"));
        }
        Ok(())
    }

    /// Build a validated Attribute from accumulated fields.
    pub fn build(self) -> Attribute {
        Attribute {
            value_type: self.value_type.expect("value_type must be set"),
            multival: self.multival.expect("multival must be set"),
        }
    }
}

/// Pre-validated schema changes, ready to apply after commit.
#[derive(Debug)]
pub struct SchemaUpdate {
    pub(crate) idents: Vec<(i64, String)>,
    pub(crate) attributes: Vec<(i64, Attribute)>,
}

impl SchemaUpdate {
    pub fn is_empty(&self) -> bool {
        self.idents.is_empty() && self.attributes.is_empty()
    }
}

/// The schema: bidirectional ident/entid maps + attribute definitions.
#[derive(Debug, Default)]
pub struct Schema {
    pub entid_map: EntidMap,
    pub ident_map: IdentMap,
    pub attribute_map: AttributeMap,
}

impl Schema {
    /// Two-step lookup: ident → entity_id via ident_map, then entity_id → Attribute via attribute_map.
    pub fn get_attribute(&self, ident: &str) -> Option<(i64, &Attribute)> {
        let eid = self.ident_map.get(ident)?;
        let attr = self.attribute_map.get(eid)?;
        Some((*eid, attr))
    }

    /// Check if an entity ID is a schema attribute (has an entry in attribute_map).
    pub fn is_schema_entity(&self, entity_id: i64) -> bool {
        self.attribute_map.contains_key(&entity_id)
    }

    /// Single-pass validation and schema change extraction (pre-commit, fallible).
    ///
    /// For each datom:
    /// 1. Type-check value against attribute's value_type, error on unknown attributes
    /// 2. Witness schema-related datoms into two streams:
    ///    - Ident stream: db/ident → pending (i64, String) pairs
    ///    - Attribute stream: db/valueType, db/cardinality → AttributeBuilder per entity
    pub fn validate_and_prepare(&self, datoms: &[Datom]) -> Result<SchemaUpdate> {
        let mut ident_updates: Vec<(i64, String)> = Vec::new();
        let mut builders: HashMap<i64, AttributeBuilder> = HashMap::new();

        for datom in datoms {
            if datom.op != DatomOp::Assert {
                continue;
            }

            // Reject modifications to existing schema entities
            if let Some(ident) = self.entid_map.get(&datom.entity) {
                return Err(anyhow::anyhow!(
                    "Cannot modify schema entity {} ({})", datom.entity, ident
                ));
            }

            // Type-check against known attributes. The `else if` fallback exists only for
            // the bootstrap transaction: db/ident, db/valueType, and db/cardinality are
            // installed BY the bootstrap tx itself, so they aren't in attribute_map yet when
            // we validate it.
            // TODO: Make the bootstrap process resemble more what Mentat does and remove the else if fallback
            if let Some((_eid, attr)) = self.get_attribute(&datom.attribute) {
                if !attr.value_type.matches(&datom.value) {
                    return Err(anyhow::anyhow!(
                        "Type mismatch for attribute {}: expected {}, got {:?}",
                        datom.attribute, attr.value_type, datom.value
                    ));
                }
            } else if !matches!(datom.attribute.as_str(), "db/ident" | "db/valueType" | "db/cardinality") {
                return Err(anyhow::anyhow!("Unknown attribute: {}", datom.attribute));
            }

            // Witness schema-related datoms.
            // NOTE: we do not currently require that an attribute entity (one with
            // db/valueType + db/cardinality) also has a db/ident. Bootstrap entities
            // always include one, but user-defined attributes could be installed without
            // a name. Not an issue for correctness, but maybe worth enforcing.
            match datom.attribute.as_str() {
                "db/ident" => {
                    // TODO: should match against a namespaced Keyword and use its
                    // namespace/name directly instead of string-stripping the colon prefix.
                    match &datom.value {
                        DataType::Keyword(kw) => {
                            let s = kw.to_string();
                            let ident = s.strip_prefix(':').unwrap_or(&s).to_string();
                            ident_updates.push((datom.entity, ident));
                        }
                        _ => return Err(anyhow::anyhow!("db/ident must be a Keyword")),
                    }
                }
                "db/valueType" => {
                    match &datom.value {
                        DataType::Keyword(kw) => {
                            let vt = ValueType::from_keyword(kw)?;
                            builders.entry(datom.entity).or_default().value_type = Some(vt);
                        }
                        _ => return Err(anyhow::anyhow!("db/valueType must be a Keyword")),
                    }
                }
                "db/cardinality" => {
                    match &datom.value {
                        DataType::Keyword(kw) => {
                            let card = Cardinality::from_keyword(kw)?;
                            builders.entry(datom.entity).or_default().multival = Some(card == Cardinality::Many);
                        }
                        _ => return Err(anyhow::anyhow!("db/cardinality must be a Keyword")),
                    }
                }
                _ => {}
            }
        }

        // Validate and build attributes from builders
        let mut attribute_updates: Vec<(i64, Attribute)> = Vec::new();
        for (entity_id, builder) in builders {
            // Only require full validation if the entity has db/valueType (is a real attribute).
            // Entities with only db/cardinality but no db/valueType would fail validation,
            // but that's the correct behavior.
            builder.validate_install_attribute().map_err(|e| {
                // Find the ident for better error messages
                let ident = ident_updates.iter()
                    .find(|(eid, _)| *eid == entity_id)
                    .map(|(_, name)| name.as_str())
                    .unwrap_or("<unknown>");
                anyhow::anyhow!("{} for '{}'", e, ident)
            })?;
            attribute_updates.push((entity_id, builder.build()));
        }

        Ok(SchemaUpdate {
            idents: ident_updates,
            attributes: attribute_updates,
        })
    }

    /// Apply pre-validated schema changes (post-commit, infallible).
    pub fn apply_schema_update(&mut self, update: SchemaUpdate) {
        for (eid, ident) in update.idents {
            self.ident_map.insert(ident.clone(), eid);
            self.entid_map.insert(eid, ident);
        }
        for (eid, attr) in update.attributes {
            self.attribute_map.insert(eid, attr);
        }
    }

    pub fn len(&self) -> usize {
        self.attribute_map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.attribute_map.is_empty()
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
        bootstrap_schema_attribute(DB_IDENT, kw!(:db/ident), "keyword"),
        bootstrap_schema_attribute(DB_VALUE_TYPE, kw!(:db/valueType), "keyword"),
        bootstrap_schema_attribute(DB_CARDINALITY, kw!(:db/cardinality), "keyword"),
        // Value type enum entities
        bootstrap_put(DB_TYPE_KEYWORD, kw!(:db.type/keyword)),
        bootstrap_put(DB_TYPE_STRING, kw!(:db.type/string)),
        bootstrap_put(DB_TYPE_LONG, kw!(:db.type/long)),
        bootstrap_put(DB_TYPE_REF, kw!(:db.type/ref)),
        bootstrap_put(DB_TYPE_BOOLEAN, kw!(:db.type/boolean)),
        bootstrap_put(DB_TYPE_DOUBLE, kw!(:db.type/double)),
        bootstrap_put(DB_TYPE_FLOAT, kw!(:db.type/float)),
        bootstrap_put(DB_TYPE_INSTANT, kw!(:db.type/instant)),
        bootstrap_put(DB_TYPE_UUID, kw!(:db.type/uuid)),
        bootstrap_put(DB_TYPE_BYTES, kw!(:db.type/bytes)),
        bootstrap_put(DB_TYPE_BIGINT, kw!(:db.type/bigint)),
        bootstrap_put(DB_TYPE_TUPLE, kw!(:db.type/tuple)),
        bootstrap_put(DB_TYPE_VECTOR, kw!(:db.type/vector)),
        bootstrap_put(DB_TYPE_MAP, kw!(:db.type/map)),
        // Cardinality enum entities
        bootstrap_put(DB_CARDINALITY_ONE, kw!(:db.cardinality/one)),
        bootstrap_put(DB_CARDINALITY_MANY, kw!(:db.cardinality/many)),
        // Transaction schema attributes
        bootstrap_schema_attribute(DB_TX_INSTANT, kw!(:db/txInstant), "instant"),
        bootstrap_schema_attribute(DB_TX_ID, kw!(:db/txId), "long"),
        bootstrap_schema_attribute(DB_TX_RESULT, kw!(:db/txResult), "keyword"),
        bootstrap_schema_attribute(DB_TX_ERROR, kw!(:db.tx/error), "string"),
        // Transaction result enum entities
        bootstrap_put(DB_TX_COMMITTED, kw!(:db.tx/committed)),
        bootstrap_put(DB_TX_ABORTED, kw!(:db.tx/aborted)),
    ]
}

/// Load the Schema from indices by querying with the Datalog engine.
/// Runs two queries:
/// 1. All entities with db/ident → populates ident_map/entid_map
/// 2. Entities with db/ident + db/valueType + db/cardinality → populates attribute_map
///
/// TODO: These two queries could be merged into a single query that fetches all
/// db/ident entities with optional db/valueType + db/cardinality. Query 2 re-fetches
/// ?e and ?ident that were already retrieved in query 1. This is only run at startup
/// so the inefficiency is minor, but a single-pass approach would be cleaner.
pub async fn load_schema_from_indices(slatedb: Arc<slatedb::Db>) -> Schema {
    // Build attribute map from bootstrap constants — sufficient to query schema entities
    let mut bootstrap_ident_map = HashMap::new();
    bootstrap_ident_map.insert("db/ident".to_string(), DB_IDENT);
    bootstrap_ident_map.insert("db/valueType".to_string(), DB_VALUE_TYPE);
    bootstrap_ident_map.insert("db/cardinality".to_string(), DB_CARDINALITY);
    let snapshot = slatedb.snapshot().await.expect("Failed to create snapshot");
    let handle = Handle::current();

    // Query 1: all entities with db/ident (populates ident_map/entid_map)
    let ident_query = parse_query(
        "[:find ?e ?ident :where [?e :db/ident ?ident]]"
    ).expect("Ident query parse failed");

    let snap_clone = snapshot.clone();
    let handle_clone = handle.clone();
    let ident_map_clone = bootstrap_ident_map.clone();
    let ident_results = tokio::task::spawn_blocking(move || {
        execute_query(&ident_query, snap_clone, handle_clone, &ident_map_clone, i64::MAX)
    })
    .await
    .expect("Ident query task failed")
    .expect("Ident query execution failed");

    // Query 2: entities with db/ident + db/valueType + db/cardinality (populates attribute_map)
    let attr_query = parse_query(
        "[:find ?e ?ident ?vt ?card :where [?e :db/ident ?ident] [?e :db/valueType ?vt] [?e :db/cardinality ?card]]"
    ).expect("Attribute query parse failed");

    let attr_results = tokio::task::spawn_blocking(move || {
        execute_query(&attr_query, snapshot, handle, &bootstrap_ident_map, i64::MAX)
    })
    .await
    .expect("Attribute query task failed")
    .expect("Attribute query execution failed");

    let mut schema = Schema::default();

    // Populate ident_map/entid_map from all entities with db/ident
    for row in ident_results {
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
        schema.ident_map.insert(ident.clone(), entity_id);
        schema.entid_map.insert(entity_id, ident);
    }

    // Populate attribute_map from entities with all three schema properties
    for row in attr_results {
        let entity_id = match &row[0] {
            DataType::Long(id) => *id,
            other => panic!("Expected Long for entity_id, got {:?}", other),
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

        schema.attribute_map.insert(entity_id, Attribute {
            value_type,
            multival: cardinality == Cardinality::Many,
        });
    }

    schema
}

/// Build a transaction that defines common test attributes.
/// Entity IDs are auto-assigned by resolve_entity_ids at transaction time.
#[cfg(any(test, feature = "test-helpers"))]
pub fn test_schema_tx() -> Vec<TxOp> {
    vec![
        schema_attribute(kw!(:name), "string"),
        schema_attribute(kw!(:age), "long"),
        schema_attribute(kw!(:email), "string"),
        // TODO: update this to ref once we support DataType::Ref
        schema_attribute(kw!(:follows), "long"),
        schema_attribute_with_cardinality(kw!(:tags), "string", "many"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::PartitionMap;
    use crate::ops::tx_ops_to_datoms;

    fn to_datoms(ops: &[TxOp]) -> Vec<Datom> {
        let mut pm = PartitionMap::new();
        let resolved = crate::partition::resolve_entity_ids(ops, &mut pm).unwrap();
        tx_ops_to_datoms(&resolved, 0_i64).unwrap()
    }

    fn bootstrapped_schema() -> Schema {
        let mut schema = Schema::default();
        let tx_ops = bootstrap_schema_tx();
        let datoms = tx_ops_to_datoms(&tx_ops, 0_i64).unwrap();
        let update = schema.validate_and_prepare(&datoms).unwrap();
        schema.apply_schema_update(update);
        schema
    }

    fn bootstrapped_schema_with_person_name() -> Schema {
        let mut schema = bootstrapped_schema();
        let ops = [schema_attribute(kw!(:name), "string")];
        let update = schema.validate_and_prepare(&to_datoms(&ops)).unwrap();
        schema.apply_schema_update(update);
        schema
    }

    #[test]
    fn test_schema_from_bootstrap() {
        let schema = bootstrapped_schema();
        // 7 schema attrs (3 core + 4 tx); enum entities go into ident_map but not attribute_map
        assert_eq!(schema.len(), 7);

        let (eid, attr) = schema.get_attribute("db/ident").unwrap();
        assert_eq!(eid, DB_IDENT);
        assert_eq!(attr.value_type, ValueType::Keyword);

        let (eid, attr) = schema.get_attribute("db/valueType").unwrap();
        assert_eq!(eid, DB_VALUE_TYPE);
        assert_eq!(attr.value_type, ValueType::Keyword);

        let (eid, attr) = schema.get_attribute("db/cardinality").unwrap();
        assert_eq!(eid, DB_CARDINALITY);
        assert_eq!(attr.value_type, ValueType::Keyword);

        // Enum entities are in ident_map but not attribute_map
        assert!(schema.ident_map.contains_key("db.type/string"));
        assert_eq!(schema.ident_map["db.type/string"], DB_TYPE_STRING);
    }

    #[test]
    fn test_apply_schema_update_user_attribute() {
        let schema = bootstrapped_schema_with_person_name();
        assert_eq!(schema.len(), 8);

        let (_eid, attr) = schema.get_attribute("name").unwrap();
        assert_eq!(attr.value_type, ValueType::String);
    }

    #[test]
    fn test_enum_entity_not_in_attribute_map() {
        let schema = bootstrapped_schema();
        // enum entities have db/ident but no db/valueType → not in attribute_map
        assert!(schema.get_attribute("db.type/string").is_none());
        // but they are in ident_map
        assert!(schema.ident_map.contains_key("db.type/string"));
    }

    #[test]
    fn test_validate_and_prepare_valid() {
        let schema = bootstrapped_schema_with_person_name();
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(200));
        doc.insert("name".to_string(), DataType::String("Alice".to_string()));
        assert!(schema.validate_and_prepare(&to_datoms(&[TxOp::Put(doc)])).is_ok());
    }

    #[test]
    fn test_validate_and_prepare_unknown_attribute() {
        let schema = bootstrapped_schema_with_person_name();
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(200));
        doc.insert("person/age".to_string(), DataType::Long(30));
        let err = schema.validate_and_prepare(&to_datoms(&[TxOp::Put(doc)])).unwrap_err();
        assert!(err.to_string().contains("Unknown attribute: person/age"));
    }

    #[test]
    fn test_validate_and_prepare_type_mismatch() {
        let schema = bootstrapped_schema_with_person_name();
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(200));
        doc.insert("name".to_string(), DataType::Long(42));
        let err = schema.validate_and_prepare(&to_datoms(&[TxOp::Put(doc)])).unwrap_err();
        assert!(err.to_string().contains("Type mismatch"));
    }

    #[test]
    fn test_validate_and_prepare_schema_defining_tx() {
        let schema = bootstrapped_schema();
        let ops = [schema_attribute(kw!(:name), "string")];
        assert!(schema.validate_and_prepare(&to_datoms(&ops)).is_ok());
    }

    #[test]
    fn test_schema_immutability() {
        let schema = bootstrapped_schema_with_person_name();
        let (name_id, _) = schema.get_attribute("name").unwrap();

        // Cannot redefine existing schema entity
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(name_id));
        doc.insert("db/ident".to_string(), DataType::Keyword(kw!(:name)));
        doc.insert("db/valueType".to_string(), DataType::Keyword(kw!(:db.type/long)));
        doc.insert("db/cardinality".to_string(), DataType::Keyword(kw!(:db.cardinality/one)));
        let err = schema.validate_and_prepare(&to_datoms(&[TxOp::Put(doc)])).unwrap_err();
        assert!(err.to_string().contains("Cannot modify schema entity"));
    }

    #[test]
    fn test_value_type_matches() {
        assert!(ValueType::String.matches(&DataType::String("hello".to_string())));
        assert!(ValueType::Long.matches(&DataType::Long(42)));
        assert!(!ValueType::String.matches(&DataType::Long(42)));
    }

    #[test]
    fn test_validate_and_prepare_parses_cardinality_many() {
        let schema = bootstrapped_schema();
        let ops = [schema_attribute_with_cardinality(kw!(:tags), "string", "many")];
        let update = schema.validate_and_prepare(&to_datoms(&ops)).unwrap();
        assert_eq!(update.attributes.len(), 1);
        assert!(update.attributes[0].1.multival);
    }

    #[test]
    fn test_validate_and_prepare_parses_cardinality_one() {
        let schema = bootstrapped_schema();
        let ops = [schema_attribute(kw!(:name), "string")];
        let update = schema.validate_and_prepare(&to_datoms(&ops)).unwrap();
        assert_eq!(update.attributes.len(), 1);
        assert!(!update.attributes[0].1.multival);
    }

    #[test]
    fn test_validate_and_prepare_missing_cardinality_errors() {
        let schema = bootstrapped_schema();
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(100));
        doc.insert("db/ident".to_string(), DataType::Keyword(kw!(:name)));
        doc.insert("db/valueType".to_string(), DataType::Keyword(kw!(:db.type/string)));
        // No db/cardinality
        let err = schema.validate_and_prepare(&to_datoms(&[TxOp::Put(doc)])).unwrap_err();
        assert!(err.to_string().contains("db/cardinality is required"));
    }
}
