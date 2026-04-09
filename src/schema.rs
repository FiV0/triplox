use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock};

use anyhow::{Error, Result};
use edn::kw;
use edn::symbols::Keyword;
use tokio::runtime::Handle;

use crate::ops::{DataType, Datom, DatomOp, EntityRef, TxOp};
use crate::query::execute_query;
use edn::query::ParsedQuery;

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
pub const DB_CARDINALITY_ONE: i64 = 30;
pub const DB_CARDINALITY_MANY: i64 = 31;

// Transaction schema attribute entities
pub const DB_TX_INSTANT: i64 = 40;
pub const DB_TX_ID: i64 = 41;
pub const DB_TX_RESULT: i64 = 42;
pub const DB_TX_ERROR: i64 = 43;

// Transaction result enum entities
pub const DB_TX_COMMITTED: i64 = 50;
pub const DB_TX_ABORTED: i64 = 51;

// --- Bootstrap data ---

/// All bootstrap ident→entid mappings. Populates ident_map/entid_map before any tx.
static V1_IDENTS: LazyLock<Vec<(Keyword, i64)>> = LazyLock::new(|| {
    vec![
        // Schema attribute entities
        (kw!(:db/ident), DB_IDENT),
        (kw!(:db/valueType), DB_VALUE_TYPE),
        (kw!(:db/cardinality), DB_CARDINALITY),
        // Value type enum entities
        (kw!(:db.type/keyword), DB_TYPE_KEYWORD),
        (kw!(:db.type/string), DB_TYPE_STRING),
        (kw!(:db.type/long), DB_TYPE_LONG),
        (kw!(:db.type/ref), DB_TYPE_REF),
        (kw!(:db.type/boolean), DB_TYPE_BOOLEAN),
        (kw!(:db.type/double), DB_TYPE_DOUBLE),
        (kw!(:db.type/float), DB_TYPE_FLOAT),
        (kw!(:db.type/instant), DB_TYPE_INSTANT),
        (kw!(:db.type/uuid), DB_TYPE_UUID),
        (kw!(:db.type/bytes), DB_TYPE_BYTES),
        (kw!(:db.type/bigint), DB_TYPE_BIGINT),
        (kw!(:db.type/tuple), DB_TYPE_TUPLE),
        (kw!(:db.type/vector), DB_TYPE_VECTOR),
        (kw!(:db.type/map), DB_TYPE_MAP),
        // Cardinality enum entities
        (kw!(:db.cardinality/one), DB_CARDINALITY_ONE),
        (kw!(:db.cardinality/many), DB_CARDINALITY_MANY),
        // Transaction schema attribute entities
        (kw!(:db/txInstant), DB_TX_INSTANT),
        (kw!(:db/txId), DB_TX_ID),
        (kw!(:db/txResult), DB_TX_RESULT),
        (kw!(:db.tx/error), DB_TX_ERROR),
        // Transaction result enum entities
        (kw!(:db.tx/committed), DB_TX_COMMITTED),
        (kw!(:db.tx/aborted), DB_TX_ABORTED),
    ]
});

/// Schema properties for bootstrap attributes using symbolic keywords.
/// (ident, value_type_ident, cardinality_ident)
static V1_ATTRIBUTES: LazyLock<Vec<(Keyword, Keyword, Keyword)>> = LazyLock::new(|| {
    vec![
        (kw!(:db/ident), kw!(:db.type/keyword), kw!(:db.cardinality/one)),
        (kw!(:db/valueType), kw!(:db.type/ref), kw!(:db.cardinality/one)),
        (kw!(:db/cardinality), kw!(:db.type/ref), kw!(:db.cardinality/one)),
        (kw!(:db/txInstant), kw!(:db.type/instant), kw!(:db.cardinality/one)),
        (kw!(:db/txId), kw!(:db.type/long), kw!(:db.cardinality/one)),
        (kw!(:db/txResult), kw!(:db.type/ref), kw!(:db.cardinality/one)),
        (kw!(:db.tx/error), kw!(:db.type/string), kw!(:db.cardinality/one)),
    ]
});

// --- Schema types ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    Keyword,
    String,
    Long,
    // Ref shares the same byte-level encoding as Long (same tag). Differentiated only at the
    // schema level: ref-typed attributes support entity joins and ident resolution.
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

impl std::fmt::Display for ValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueType::Keyword => write!(f, "keyword"),
            ValueType::String => write!(f, "string"),
            ValueType::Long => write!(f, "long"),
            ValueType::Ref => write!(f, "ref"),
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
    /// Map a value type entity ID to a ValueType.
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

    /// Map a ValueType to its entity ID.
    pub fn entity_id(&self) -> i64 {
        match self {
            ValueType::Keyword => DB_TYPE_KEYWORD,
            ValueType::String => DB_TYPE_STRING,
            ValueType::Long => DB_TYPE_LONG,
            ValueType::Ref => DB_TYPE_REF,
            ValueType::Boolean => DB_TYPE_BOOLEAN,
            ValueType::Double => DB_TYPE_DOUBLE,
            ValueType::Float => DB_TYPE_FLOAT,
            ValueType::Instant => DB_TYPE_INSTANT,
            ValueType::Uuid => DB_TYPE_UUID,
            ValueType::Bytes => DB_TYPE_BYTES,
            ValueType::BigInt => DB_TYPE_BIGINT,
            ValueType::Tuple => DB_TYPE_TUPLE,
            ValueType::Vector => DB_TYPE_VECTOR,
            ValueType::Map => DB_TYPE_MAP,
        }
    }

    /// Check if a DataType matches this ValueType.
    pub fn matches(&self, data: &DataType) -> bool {
        match self {
            // Ref values are stored as DataType::Long (same byte-level encoding).
            ValueType::Ref => data.value_type() == ValueType::Long,
            _ => *self == data.value_type(),
        }
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
    /// Map a cardinality entity ID to a Cardinality.
    pub fn from_entity_id(id: i64) -> Result<Self> {
        match id {
            DB_CARDINALITY_ONE => Ok(Cardinality::One),
            DB_CARDINALITY_MANY => Ok(Cardinality::Many),
            _ => Err(anyhow::anyhow!("Unknown cardinality entity ID: {}", id)),
        }
    }

    /// Map a Cardinality to its entity ID.
    pub fn entity_id(&self) -> i64 {
        match self {
            Cardinality::One => DB_CARDINALITY_ONE,
            Cardinality::Many => DB_CARDINALITY_MANY,
        }
    }
}

/// ident → entity_id (ALL named entities: enums + schema attrs)
pub type IdentMap = HashMap<Keyword, i64>;
/// entity_id → ident (ALL named entities: enums + schema attrs)
pub type EntidMap = HashMap<i64, Keyword>;
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
            return Err(anyhow::anyhow!(
                "db/valueType is required for schema attribute"
            ));
        }
        if self.multival.is_none() {
            return Err(anyhow::anyhow!(
                "db/cardinality is required for schema attribute"
            ));
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
    pub(crate) idents: Vec<(i64, Keyword)>,
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
    pub fn get_attribute(&self, ident: &Keyword) -> Option<(i64, &Attribute)> {
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
    ///    - Ident stream: db/ident → pending (i64, Keyword) pairs
    ///    - Attribute stream: db/valueType, db/cardinality → AttributeBuilder per entity
    pub fn validate_and_prepare(&self, datoms: &[Datom]) -> Result<SchemaUpdate> {
        let mut ident_updates: Vec<(i64, Keyword)> = Vec::new();
        let mut builders: HashMap<i64, AttributeBuilder> = HashMap::new();

        // TODO(#179): Schema immutability should be enforced in apply_schema_update,
        // similar to Mentat's approach where validation and schema mutation are
        // separate concerns.
        for datom in datoms {
            if datom.op != DatomOp::Assert {
                continue;
            }

            // Type-check against known attributes
            if let Some((_eid, attr)) = self.get_attribute(&datom.attribute) {
                if !attr.value_type.matches(&datom.value) {
                    return Err(anyhow::anyhow!(
                        "Type mismatch for attribute {}: expected {}, got {:?}",
                        datom.attribute,
                        attr.value_type,
                        datom.value
                    ));
                }
            } else {
                return Err(anyhow::anyhow!("Unknown attribute: {}", datom.attribute));
            }

            // Witness schema-related datoms
            match datom.attribute.components() {
                ("db", "ident") => match &datom.value {
                    DataType::Keyword(kw) => ident_updates.push((datom.entity, kw.clone())),
                    _ => return Err(anyhow::anyhow!("db/ident must be a Keyword")),
                },
                ("db", "valueType") => match &datom.value {
                    DataType::Long(id) => {
                        let vt = ValueType::from_entity_id(*id)?;
                        builders.entry(datom.entity).or_default().value_type = Some(vt);
                    }
                    _ => {
                        return Err(anyhow::anyhow!(
                            "db/valueType must be a Ref (Long entity ID)"
                        ))
                    }
                },
                ("db", "cardinality") => match &datom.value {
                    DataType::Long(id) => {
                        let card = Cardinality::from_entity_id(*id)?;
                        builders.entry(datom.entity).or_default().multival =
                            Some(card == Cardinality::Many);
                    }
                    _ => {
                        return Err(anyhow::anyhow!(
                            "db/cardinality must be a Ref (Long entity ID)"
                        ))
                    }
                },
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
                let ident = ident_updates
                    .iter()
                    .find(|(eid, _)| *eid == entity_id)
                    .map(|(_, kw)| kw.to_string())
                    .unwrap_or_else(|| "<unknown>".to_string());
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

// --- Bootstrap ---

/// Build a complete Schema directly from V1_IDENTS + V1_ATTRIBUTES, before any transaction.
pub fn bootstrap_schema() -> Schema {
    let mut schema = Schema::default();

    for (ident, eid) in V1_IDENTS.iter() {
        schema.ident_map.insert(ident.clone(), *eid);
        schema.entid_map.insert(*eid, ident.clone());
    }

    for (ident, vt_ident, card_ident) in V1_ATTRIBUTES.iter() {
        let eid = *schema
            .ident_map
            .get(ident)
            .expect("V1_ATTRIBUTES ident not in V1_IDENTS");
        let vt_id = *schema
            .ident_map
            .get(vt_ident)
            .expect("value type ident not in V1_IDENTS");
        let card_id = *schema
            .ident_map
            .get(card_ident)
            .expect("cardinality ident not in V1_IDENTS");
        schema.attribute_map.insert(
            eid,
            Attribute {
                value_type: ValueType::from_entity_id(vt_id)
                    .expect("invalid value type entity ID in V1_ATTRIBUTES"),
                multival: card_id == DB_CARDINALITY_MANY,
            },
        );
    }

    schema
}

/// Build the bootstrap schema transaction from V1_IDENTS + V1_ATTRIBUTES.
/// This is the first transaction on a fresh database.
pub fn bootstrap_schema_tx() -> Vec<TxOp> {
    let attrs = &*V1_ATTRIBUTES;
    let attr_idents: Vec<&Keyword> = attrs.iter().map(|(kw, _, _)| kw).collect();
    let idents = &*V1_IDENTS;

    let mut ops = Vec::new();

    // Schema attribute entities
    for (ident, vt_ident, card_ident) in attrs {
        let eid = idents.iter().find(|(kw, _)| kw == ident).unwrap().1;
        ops.push(TxOp::put(vec![
            (kw!(:db/id), DataType::Long(eid)),
            (kw!(:db/ident), DataType::Keyword(ident.clone())),
            (kw!(:db/valueType), DataType::Keyword(vt_ident.clone())),
            (
                kw!(:db/cardinality),
                DataType::Keyword(card_ident.clone()),
            ),
        ]));
    }

    // Enum entities
    for (ident, eid) in idents {
        if attr_idents.contains(&ident) {
            continue;
        }
        ops.push(TxOp::Add {
            entity: EntityRef::Id(*eid),
            attribute: kw!(:db/ident),
            value: DataType::Keyword(ident.clone()),
        });
    }

    ops
}

// --- Test helpers ---

/// Build a Put for a schema attribute without explicit db/id.
#[cfg(any(test, feature = "test-helpers"))]
fn schema_attribute_with_cardinality(ident: Keyword, value_type: &str, cardinality: &str) -> TxOp {
    TxOp::put(vec![
        (kw!(:db/ident), DataType::Keyword(ident)),
        (
            kw!(:db/valueType),
            DataType::Keyword(Keyword::namespaced("db.type", value_type)),
        ),
        (
            kw!(:db/cardinality),
            DataType::Keyword(Keyword::namespaced("db.cardinality", cardinality)),
        ),
    ])
}

#[cfg(any(test, feature = "test-helpers"))]
fn schema_attribute(ident: Keyword, value_type: &str) -> TxOp {
    schema_attribute_with_cardinality(ident, value_type, "one")
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
    let mut bootstrap_ident_map: IdentMap = HashMap::new();
    bootstrap_ident_map.insert(kw!(:db/ident), DB_IDENT);
    bootstrap_ident_map.insert(kw!(:db/valueType), DB_VALUE_TYPE);
    bootstrap_ident_map.insert(kw!(:db/cardinality), DB_CARDINALITY);
    let snapshot = slatedb.snapshot().await.expect("Failed to create snapshot");
    let handle = Handle::current();

    // Query 1: all entities with db/ident (populates ident_map/entid_map)
    let ident_query: ParsedQuery = "[:find ?e ?ident :where [?e :db/ident ?ident]]"
        .parse()
        .expect("Ident query parse failed");

    let snap_clone = snapshot.clone();
    let handle_clone = handle.clone();
    let ident_map_clone = bootstrap_ident_map.clone();
    let ident_results = tokio::task::spawn_blocking(move || {
        execute_query(
            &ident_query,
            snap_clone,
            handle_clone,
            &ident_map_clone,
            i64::MAX,
        )
    })
    .await
    .expect("Ident query task failed")
    .expect("Ident query execution failed");

    // Query 2: entities with db/ident + db/valueType + db/cardinality (populates attribute_map)
    let attr_query: ParsedQuery = "[:find ?e ?ident ?vt ?card :where [?e :db/ident ?ident] [?e :db/valueType ?vt] [?e :db/cardinality ?card]]"
        .parse()
        .expect("Attribute query parse failed");

    let attr_results = tokio::task::spawn_blocking(move || {
        execute_query(
            &attr_query,
            snapshot,
            handle,
            &bootstrap_ident_map,
            i64::MAX,
        )
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
            DataType::Keyword(kw) => kw.clone(),
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
            DataType::Long(id) => ValueType::from_entity_id(*id)
                .unwrap_or_else(|e| panic!("Invalid value type: {}", e)),
            other => panic!("Expected Long for valueType, got {:?}", other),
        };
        let cardinality = match &row[3] {
            DataType::Long(id) => Cardinality::from_entity_id(*id)
                .unwrap_or_else(|e| panic!("Invalid cardinality: {}", e)),
            other => panic!("Expected Long for cardinality, got {:?}", other),
        };

        schema.attribute_map.insert(
            entity_id,
            Attribute {
                value_type,
                multival: cardinality == Cardinality::Many,
            },
        );
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
        schema_attribute(kw!(:follows), "ref"),
        schema_attribute_with_cardinality(kw!(:tags), "string", "many"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::PartitionMap;
    use crate::tx;

    fn to_datoms(ops: &[TxOp], schema: &Schema) -> Vec<Datom> {
        let mut pm = PartitionMap::new();
        let expanded = tx::expand_tx_ops(ops, schema).unwrap();
        tx::resolve_tempids(&expanded, &mut pm).unwrap()
    }

    fn bootstrapped_schema() -> Schema {
        bootstrap_schema()
    }

    fn bootstrapped_schema_with_person_name() -> Schema {
        let mut schema = bootstrapped_schema();
        let ops = [schema_attribute(kw!(:name), "string")];
        let update = schema
            .validate_and_prepare(&to_datoms(&ops, &schema))
            .unwrap();
        schema.apply_schema_update(update);
        schema
    }

    #[test]
    fn test_schema_from_bootstrap() {
        let schema = bootstrapped_schema();
        // 7 schema attrs (3 core + 4 tx); enum entities go into ident_map but not attribute_map
        assert_eq!(schema.len(), 7);

        let (eid, attr) = schema.get_attribute(&kw!(:db/ident)).unwrap();
        assert_eq!(eid, DB_IDENT);
        assert_eq!(attr.value_type, ValueType::Keyword);

        let (eid, attr) = schema.get_attribute(&kw!(:db/valueType)).unwrap();
        assert_eq!(eid, DB_VALUE_TYPE);
        assert_eq!(attr.value_type, ValueType::Ref);

        let (eid, attr) = schema.get_attribute(&kw!(:db/cardinality)).unwrap();
        assert_eq!(eid, DB_CARDINALITY);
        assert_eq!(attr.value_type, ValueType::Ref);

        // Enum entities are in ident_map but not attribute_map
        assert!(schema.ident_map.contains_key(&kw!(:db.type/string)));
        assert_eq!(schema.ident_map[&kw!(:db.type/string)], DB_TYPE_STRING);
    }

    #[test]
    fn test_apply_schema_update_user_attribute() {
        let schema = bootstrapped_schema_with_person_name();
        assert_eq!(schema.len(), 8);

        let (_eid, attr) = schema.get_attribute(&kw!(:name)).unwrap();
        assert_eq!(attr.value_type, ValueType::String);
    }

    #[test]
    fn test_enum_entity_not_in_attribute_map() {
        let schema = bootstrapped_schema();
        // enum entities have db/ident but no db/valueType → not in attribute_map
        assert!(schema.get_attribute(&kw!(:db.type/string)).is_none());
        // but they are in ident_map
        assert!(schema.ident_map.contains_key(&kw!(:db.type/string)));
    }

    #[test]
    fn test_validate_and_prepare_valid() {
        let schema = bootstrapped_schema_with_person_name();
        let ops = [TxOp::put(vec![(kw!(:name), "Alice".into())])];
        assert!(schema
            .validate_and_prepare(&to_datoms(&ops, &schema))
            .is_ok());
    }

    #[test]
    fn test_validate_and_prepare_unknown_attribute() {
        let schema = bootstrapped_schema_with_person_name();
        let ops = [TxOp::put(vec![(kw!(:person/age), 30_i64.into())])];
        let err = schema
            .validate_and_prepare(&to_datoms(&ops, &schema))
            .unwrap_err();
        assert!(err.to_string().contains("Unknown attribute: :person/age"));
    }

    #[test]
    fn test_validate_and_prepare_type_mismatch() {
        let schema = bootstrapped_schema_with_person_name();
        let ops = [TxOp::put(vec![(kw!(:name), 42_i64.into())])];
        let err = schema
            .validate_and_prepare(&to_datoms(&ops, &schema))
            .unwrap_err();
        assert!(err.to_string().contains("Type mismatch"));
    }

    #[test]
    fn test_validate_and_prepare_schema_defining_tx() {
        let schema = bootstrapped_schema();
        let ops = [schema_attribute(kw!(:name), "string")];
        assert!(schema
            .validate_and_prepare(&to_datoms(&ops, &schema))
            .is_ok());
    }

    #[test]
    #[ignore]
    fn test_schema_immutability() {
        let schema = bootstrapped_schema_with_person_name();
        let (name_id, _) = schema.get_attribute(&kw!(:name)).unwrap();

        let ops = [TxOp::put(vec![
            (kw!(:db/id), DataType::Long(name_id)),
            (kw!(:db/ident), DataType::Keyword(kw!(:name))),
            (kw!(:db/valueType), DataType::Keyword(kw!(:db.type/long))),
            (
                kw!(:db/cardinality),
                DataType::Keyword(kw!(:db.cardinality/one)),
            ),
        ])];
        let err = schema
            .validate_and_prepare(&to_datoms(&ops, &schema))
            .unwrap_err();
        assert!(err.to_string().contains("Cannot modify schema entity"));
    }

    #[test]
    fn test_value_type_matches() {
        assert!(ValueType::String.matches(&DataType::String("hello".to_string())));
        assert!(ValueType::Long.matches(&DataType::Long(42)));
        assert!(!ValueType::String.matches(&DataType::Long(42)));
        // Ref accepts DataType::Long (same byte-level encoding)
        assert!(ValueType::Ref.matches(&DataType::Long(42)));
        assert!(!ValueType::Ref.matches(&DataType::String("x".into())));
    }

    #[test]
    fn test_value_type_from_entity_id() {
        assert_eq!(
            ValueType::from_entity_id(DB_TYPE_REF).unwrap(),
            ValueType::Ref
        );
        assert_eq!(
            ValueType::from_entity_id(DB_TYPE_STRING).unwrap(),
            ValueType::String
        );
        assert!(ValueType::from_entity_id(9999).is_err());
    }

    #[test]
    fn test_value_type_entity_id_roundtrip() {
        for vt in [
            ValueType::Keyword,
            ValueType::String,
            ValueType::Long,
            ValueType::Ref,
            ValueType::Boolean,
            ValueType::Double,
            ValueType::Float,
            ValueType::Instant,
            ValueType::Uuid,
            ValueType::Bytes,
            ValueType::BigInt,
            ValueType::Tuple,
            ValueType::Vector,
            ValueType::Map,
        ] {
            assert_eq!(ValueType::from_entity_id(vt.entity_id()).unwrap(), vt);
        }
    }

    #[test]
    fn test_cardinality_from_entity_id() {
        assert_eq!(
            Cardinality::from_entity_id(DB_CARDINALITY_ONE).unwrap(),
            Cardinality::One
        );
        assert_eq!(
            Cardinality::from_entity_id(DB_CARDINALITY_MANY).unwrap(),
            Cardinality::Many
        );
        assert!(Cardinality::from_entity_id(9999).is_err());
    }

    #[test]
    fn test_bootstrap_schema_consistency() {
        // Verify that bootstrap_schema() matches what bootstrap_schema_tx()
        // produces through the normal expand→resolve→validate→apply pipeline
        let bootstrap = bootstrap_schema();
        let tx_ops = bootstrap_schema_tx();
        let expanded = tx::expand_tx_ops(&tx_ops, &bootstrap).unwrap();
        let mut pm = PartitionMap::new();
        let datoms = tx::resolve_tempids(&expanded, &mut pm).unwrap();
        let update = bootstrap.validate_and_prepare(&datoms).unwrap();
        let mut schema_from_tx = Schema::default();
        schema_from_tx.apply_schema_update(update);
        assert_eq!(schema_from_tx.ident_map, bootstrap.ident_map);
        assert_eq!(schema_from_tx.attribute_map, bootstrap.attribute_map);
    }

    #[test]
    fn test_schema_ref_attribute_validation() {
        let mut schema = bootstrapped_schema();
        let ops = [schema_attribute(kw!(:follows), "ref")];
        let update = schema
            .validate_and_prepare(&to_datoms(&ops, &schema))
            .unwrap();
        schema.apply_schema_update(update);

        let (_eid, attr) = schema.get_attribute(&kw!(:follows)).unwrap();
        assert_eq!(attr.value_type, ValueType::Ref);

        // Long value accepted for ref-typed attribute
        let ops = [TxOp::put(vec![(kw!(:follows), 201_i64.into())])];
        assert!(schema
            .validate_and_prepare(&to_datoms(&ops, &schema))
            .is_ok());

        // String in ref position is interpreted as tempid (resolved to Long)
        let ops = [TxOp::put(vec![(
            kw!(:follows),
            DataType::String("some-tempid".to_string()),
        )])];
        let datoms = to_datoms(&ops, &schema);
        // After expand_tx_ops, the string becomes a TempRef which resolves to a Long,
        // so validate_and_prepare should accept it.
        assert!(schema.validate_and_prepare(&datoms).is_ok());
    }

    #[test]
    fn test_validate_and_prepare_parses_cardinality_many() {
        let schema = bootstrapped_schema();
        let ops = [schema_attribute_with_cardinality(
            kw!(:tags),
            "string",
            "many",
        )];
        let update = schema
            .validate_and_prepare(&to_datoms(&ops, &schema))
            .unwrap();
        assert_eq!(update.attributes.len(), 1);
        assert!(update.attributes[0].1.multival);
    }

    #[test]
    fn test_validate_and_prepare_parses_cardinality_one() {
        let schema = bootstrapped_schema();
        let ops = [schema_attribute(kw!(:name), "string")];
        let update = schema
            .validate_and_prepare(&to_datoms(&ops, &schema))
            .unwrap();
        assert_eq!(update.attributes.len(), 1);
        assert!(!update.attributes[0].1.multival);
    }

    #[test]
    fn test_validate_and_prepare_missing_cardinality_errors() {
        let schema = bootstrapped_schema();
        // Provide db/ident + db/valueType but no db/cardinality
        let ops = [TxOp::put(vec![
            (kw!(:db/ident), DataType::Keyword(kw!(:name))),
            (kw!(:db/valueType), DataType::Keyword(kw!(:db.type/string))),
            // No db/cardinality
        ])];
        let err = schema
            .validate_and_prepare(&to_datoms(&ops, &schema))
            .unwrap_err();
        assert!(err.to_string().contains("db/cardinality is required"));
    }
}
