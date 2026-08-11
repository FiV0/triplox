use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use anyhow::{Error, Result};
use edn::kw;
use edn::symbols::Keyword;
use tokio::runtime::Handle;

use crate::ops::{DataType, Datom, DatomOp, Entid, EntityRef, TxOp};
use crate::query::execute_query;
use edn::query::ParsedQuery;

// --- Reserved entity IDs ---
// Used as explicit db/id values in bootstrap_schema_tx().

// Schema attribute entities
pub const DB_IDENT: i64 = 1;
pub const DB_VALUE_TYPE: i64 = 2;
pub const DB_CARDINALITY: i64 = 3;
pub const DB_UNIQUE: i64 = 4;

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
pub const DB_TYPE_VECTOR: i64 = 21;
pub const DB_TYPE_MAP: i64 = 22;

// Cardinality enum entities
pub const DB_CARDINALITY_ONE: i64 = 30;
pub const DB_CARDINALITY_MANY: i64 = 31;

// Unique enum entities
pub const DB_UNIQUE_VALUE: i64 = 32;
pub const DB_UNIQUE_IDENTITY: i64 = 33;

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
        (kw!(:db/unique), DB_UNIQUE),
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
        (kw!(:db.type/vector), DB_TYPE_VECTOR),
        (kw!(:db.type/map), DB_TYPE_MAP),
        // Cardinality enum entities
        (kw!(:db.cardinality/one), DB_CARDINALITY_ONE),
        (kw!(:db.cardinality/many), DB_CARDINALITY_MANY),
        // Unique enum entities
        (kw!(:db.unique/value), DB_UNIQUE_VALUE),
        (kw!(:db.unique/identity), DB_UNIQUE_IDENTITY),
        // Transaction schema attribute entities
        (kw!(:db/txInstant), DB_TX_INSTANT),
        (kw!(:db/txId), DB_TX_ID),
        (kw!(:db/txResult), DB_TX_RESULT),
        (kw!(:db/txError), DB_TX_ERROR),
        // Transaction result enum entities
        (kw!(:db.tx/committed), DB_TX_COMMITTED),
        (kw!(:db.tx/aborted), DB_TX_ABORTED),
    ]
});

/// Schema properties for bootstrap attributes using symbolic keywords.
/// (ident, value_type_ident, cardinality_ident, unique_ident)
#[allow(clippy::type_complexity)]
static V1_ATTRIBUTES: LazyLock<Vec<(Keyword, Keyword, Keyword, Option<Keyword>)>> =
    LazyLock::new(|| {
        vec![
            (
                kw!(:db/ident),
                kw!(:db.type/keyword),
                kw!(:db.cardinality/one),
                Some(kw!(:db.unique/identity)),
            ),
            (
                kw!(:db/valueType),
                kw!(:db.type/ref),
                kw!(:db.cardinality/one),
                None,
            ),
            (
                kw!(:db/cardinality),
                kw!(:db.type/ref),
                kw!(:db.cardinality/one),
                None,
            ),
            (
                kw!(:db/unique),
                kw!(:db.type/ref),
                kw!(:db.cardinality/one),
                None,
            ),
            (
                kw!(:db/txInstant),
                kw!(:db.type/instant),
                kw!(:db.cardinality/one),
                None,
            ),
            (
                kw!(:db/txId),
                kw!(:db.type/long),
                kw!(:db.cardinality/one),
                None,
            ),
            (
                kw!(:db/txResult),
                kw!(:db.type/ref),
                kw!(:db.cardinality/one),
                None,
            ),
            (
                kw!(:db/txError),
                kw!(:db.type/string),
                kw!(:db.cardinality/one),
                None,
            ),
        ]
    });

// --- Schema types ---

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
            ValueType::Vector => DB_TYPE_VECTOR,
            ValueType::Map => DB_TYPE_MAP,
        }
    }

    /// Check if a DataType matches this ValueType.
    pub fn matches(&self, data: &DataType) -> bool {
        match self {
            // Ref values are stored as DataType::Long (same byte-level encoding).
            ValueType::Ref => value_type_of(data) == ValueType::Long,
            _ => *self == value_type_of(data),
        }
    }
}

/// Map a `DataType` value to its `ValueType` variant. Lives in the server
/// crate so `DataType` stays decoupled from the schema module.
pub fn value_type_of(dt: &DataType) -> ValueType {
    match dt {
        DataType::BigInt(_) => ValueType::BigInt,
        DataType::Boolean(_) => ValueType::Boolean,
        DataType::Bytes(_) => ValueType::Bytes,
        DataType::Double(_) => ValueType::Double,
        DataType::Float(_) => ValueType::Float,
        DataType::Instant(_) => ValueType::Instant,
        DataType::Keyword(_) => ValueType::Keyword,
        DataType::Long(_) => ValueType::Long,
        DataType::String(_) => ValueType::String,
        DataType::Uuid(_) => ValueType::Uuid,
        DataType::Vector(_) => ValueType::Vector,
        DataType::Map(_) => ValueType::Map,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Unique {
    Value,
    Identity,
}

impl std::fmt::Display for Unique {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Unique::Value => write!(f, "db.unique/value"),
            Unique::Identity => write!(f, "db.unique/identity"),
        }
    }
}

impl Unique {
    pub fn from_entity_id(id: i64) -> Result<Self> {
        match id {
            DB_UNIQUE_VALUE => Ok(Unique::Value),
            DB_UNIQUE_IDENTITY => Ok(Unique::Identity),
            _ => Err(anyhow::anyhow!("Unknown unique entity ID: {}", id)),
        }
    }

    pub fn entity_id(&self) -> i64 {
        match self {
            Unique::Value => DB_UNIQUE_VALUE,
            Unique::Identity => DB_UNIQUE_IDENTITY,
        }
    }
}

/// ident → entity_id (ALL named entities: enums + schema attrs)
pub type IdentMap = HashMap<Keyword, Entid>;
/// entity_id → ident (ALL named entities: enums + schema attrs)
pub type EntidMap = HashMap<Entid, Keyword>;
/// entity_id → Attribute (only schema attributes with db/valueType)
pub type AttributeMap = HashMap<Entid, Attribute>;

/// A schema attribute's properties.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Attribute {
    pub value_type: ValueType,
    pub multival: bool,
    pub unique: Option<Unique>,
}

/// Builder for accumulating attribute properties from (e, a, v) assertions.
/// Each schema-related datom (db/valueType, db/cardinality) for the same entity
/// is witnessed onto the same builder. After all datoms are processed, build()
/// produces a validated Attribute.
#[derive(Debug, Default)]
pub struct AttributeBuilder {
    pub value_type: Option<ValueType>,
    pub multival: Option<bool>,
    pub unique: Option<Option<Unique>>,
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
            unique: self.unique.unwrap_or(None),
        }
    }
}

/// Pre-validated schema changes, ready to apply after commit.
#[derive(Debug, Default)]
pub struct SchemaUpdate {
    pub(crate) idents: Vec<(Entid, Keyword)>,
    pub(crate) attributes: Vec<(Entid, Attribute)>,
}

impl SchemaUpdate {
    pub fn is_empty(&self) -> bool {
        self.idents.is_empty() && self.attributes.is_empty()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ValidationReport {
    pub schema_changes_detected: bool,
}

#[derive(Debug)]
struct AddRetractSet<T> {
    adds: HashSet<T>,
    retracts: HashSet<T>,
}

impl<T> Default for AddRetractSet<T> {
    fn default() -> Self {
        Self {
            adds: HashSet::new(),
            retracts: HashSet::new(),
        }
    }
}

type AEVValidationMap<'a> =
    HashMap<(Entid, &'a Attribute), HashMap<Entid, AddRetractSet<DataType>>>;

#[derive(Debug)]
enum ValidationConflict {
    TypeMismatch {
        entity: Entid,
        attribute: Keyword,
        value: DataType,
        expected: ValueType,
    },
    AddRetractConflict {
        entity: Entid,
        attribute: Keyword,
        values: Vec<DataType>,
    },
    CardinalityOneAddConflict {
        entity: Entid,
        attribute: Keyword,
        values: Vec<DataType>,
    },
}

fn is_schema_attribute(attribute: &Keyword) -> bool {
    matches!(
        attribute.components(),
        ("db", "ident" | "valueType" | "cardinality" | "unique")
    )
}

/// The schema: bidirectional ident/entid maps + attribute definitions.
#[derive(Debug, Clone, Default)]
pub struct Schema {
    pub entid_map: EntidMap,
    pub ident_map: IdentMap,
    pub attribute_map: AttributeMap,
}

impl Schema {
    /// Two-step lookup: ident → entity_id via ident_map, then entity_id → Attribute via attribute_map.
    pub fn get_attribute(&self, ident: &Keyword) -> Option<(Entid, &Attribute)> {
        let eid = self.ident_map.get(ident)?;
        let attr = self.attribute_map.get(eid)?;
        Some((*eid, attr))
    }

    /// Look up the keyword/ident for a given entity ID.
    pub fn get_ident(&self, entity_id: Entid) -> Option<&Keyword> {
        self.entid_map.get(&entity_id)
    }

    /// Check if an entity ID is a schema attribute (has an entry in attribute_map).
    pub fn is_schema_entity(&self, entity_id: Entid) -> bool {
        self.attribute_map.contains_key(&entity_id)
    }

    /// Validate resolved transaction datoms against the current schema.
    ///
    /// Runs before the card-one finalize rewrite so intra-tx conflicts are
    /// judged on user intent, not on mechanically rewritten datoms (#379).
    /// This step does general transaction validation only. Schema-related datoms
    /// are merely witnessed so the caller can decide whether a schema delta must
    /// be prepared in a separate pass.
    pub fn validate_datoms(&self, datoms: &[Datom]) -> Result<ValidationReport> {
        let (aev, schema_changes_detected) = self.build_aev_validation_map(datoms)?;

        let mut conflicts = self.type_mismatches(&aev);
        conflicts.extend(self.add_retract_conflicts(&aev));
        conflicts.extend(self.cardinality_conflicts(&aev));

        // TODO(#187): report all conflicts instead of only the first
        if let Some(conflict) = conflicts.first() {
            return Err(match conflict {
                ValidationConflict::TypeMismatch {
                    entity,
                    attribute,
                    value,
                    expected,
                } => anyhow::anyhow!(
                    "Type mismatch for attribute {} on entity {}: expected {}, got {:?}",
                    attribute, entity, expected, value
                ),
                ValidationConflict::AddRetractConflict {
                    entity,
                    attribute,
                    values,
                } => anyhow::anyhow!(
                    "Transaction cannot both assert and retract {:?} for attribute {} on entity {}",
                    values, attribute, entity
                ),
                ValidationConflict::CardinalityOneAddConflict {
                    entity,
                    attribute,
                    values,
                } => anyhow::anyhow!(
                    "Transaction cannot assert multiple values {:?} for cardinality-one attribute {} on entity {}",
                    values, attribute, entity
                ),
            });
        }

        Ok(ValidationReport {
            schema_changes_detected,
        })
    }

    /// Build a pre-validated schema delta from finalized transaction datoms.
    ///
    /// This step is responsible only for schema-related assertions and mutation
    /// rules. It assumes general datom validation has already succeeded.
    pub fn prepare_schema_update(&self, datoms: &[Datom]) -> Result<SchemaUpdate> {
        let mut ident_updates: Vec<(Entid, Keyword)> = Vec::new();
        let mut builders: HashMap<Entid, AttributeBuilder> = HashMap::new();

        for datom in datoms {
            if !is_schema_attribute(&datom.attribute) {
                continue;
            }

            if self.is_schema_entity(datom.entity) {
                let ident = self
                    .entid_map
                    .get(&datom.entity)
                    .map(|kw| kw.to_string())
                    .unwrap_or_else(|| "<unknown>".to_string());
                return Err(anyhow::anyhow!(
                    "Cannot modify schema entity {} ({})",
                    datom.entity,
                    ident
                ));
            }

            if datom.op != DatomOp::Assert {
                return Err(anyhow::anyhow!(
                    "Schema retractions are not supported for attribute {}",
                    datom.attribute
                ));
            }

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
                ("db", "unique") => match &datom.value {
                    DataType::Long(id) => {
                        let unique = Unique::from_entity_id(*id)?;
                        builders.entry(datom.entity).or_default().unique = Some(Some(unique));
                    }
                    _ => return Err(anyhow::anyhow!("db/unique must be a Ref (Long entity ID)")),
                },
                _ => {}
            }
        }

        let mut attribute_updates: Vec<(i64, Attribute)> = Vec::new();
        for (entity_id, builder) in builders {
            builder.validate_install_attribute().map_err(|e| {
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

    fn build_aev_validation_map(&self, datoms: &[Datom]) -> Result<(AEVValidationMap<'_>, bool)> {
        let mut aev: AEVValidationMap<'_> = HashMap::new();
        let mut schema_changes_detected = false;

        for datom in datoms {
            let (attribute_id, attr) = self
                .get_attribute(&datom.attribute)
                .ok_or_else(|| anyhow::anyhow!("Unknown attribute: {}", datom.attribute))?;

            if is_schema_attribute(&datom.attribute) {
                schema_changes_detected = true;
            }

            let entry = aev
                .entry((attribute_id, attr))
                .or_default()
                .entry(datom.entity)
                .or_default();

            match datom.op {
                DatomOp::Assert => {
                    entry.adds.insert(datom.value.clone());
                }
                DatomOp::Retract => {
                    entry.retracts.insert(datom.value.clone());
                }
            }
        }

        Ok((aev, schema_changes_detected))
    }

    fn type_mismatches(&self, aev: &AEVValidationMap<'_>) -> Vec<ValidationConflict> {
        let mut conflicts = Vec::new();
        for ((attribute_id, attribute), evs) in aev {
            let attribute_ident = self
                .entid_map
                .get(attribute_id)
                .cloned()
                .unwrap_or_else(|| kw!(:db/unknown));
            for (entity, values) in evs {
                for value in values.adds.iter().chain(values.retracts.iter()) {
                    if !attribute.value_type.matches(value) {
                        conflicts.push(ValidationConflict::TypeMismatch {
                            entity: *entity,
                            attribute: attribute_ident.clone(),
                            value: value.clone(),
                            expected: attribute.value_type,
                        });
                    }
                }
            }
        }
        conflicts
    }

    fn add_retract_conflicts(&self, aev: &AEVValidationMap<'_>) -> Vec<ValidationConflict> {
        let mut conflicts = Vec::new();
        for ((attribute_id, attribute), evs) in aev {
            if attribute.multival {
                continue;
            }
            let attribute_ident = self
                .entid_map
                .get(attribute_id)
                .cloned()
                .unwrap_or_else(|| kw!(:db/unknown));
            for (entity, values) in evs {
                let overlap: Vec<_> = values
                    .adds
                    .intersection(&values.retracts)
                    .cloned()
                    .collect();
                if !overlap.is_empty() {
                    conflicts.push(ValidationConflict::AddRetractConflict {
                        entity: *entity,
                        attribute: attribute_ident.clone(),
                        values: overlap,
                    });
                }
            }
        }
        conflicts
    }

    fn cardinality_conflicts(&self, aev: &AEVValidationMap<'_>) -> Vec<ValidationConflict> {
        let mut conflicts = Vec::new();
        for ((attribute_id, attribute), evs) in aev {
            if attribute.multival {
                continue;
            }
            let attribute_ident = self
                .entid_map
                .get(attribute_id)
                .cloned()
                .unwrap_or_else(|| kw!(:db/unknown));
            for (entity, values) in evs {
                if values.adds.len() > 1 {
                    conflicts.push(ValidationConflict::CardinalityOneAddConflict {
                        entity: *entity,
                        attribute: attribute_ident.clone(),
                        values: values.adds.iter().cloned().collect(),
                    });
                }
            }
        }
        conflicts
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

    for (ident, vt_ident, card_ident, unique_ident) in V1_ATTRIBUTES.iter() {
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
        let unique = unique_ident.as_ref().map(|ident| {
            let id = *schema
                .ident_map
                .get(ident)
                .expect("unique ident not in V1_IDENTS");
            Unique::from_entity_id(id).expect("invalid unique entity ID in V1_ATTRIBUTES")
        });
        schema.attribute_map.insert(
            eid,
            Attribute {
                value_type: ValueType::from_entity_id(vt_id)
                    .expect("invalid value type entity ID in V1_ATTRIBUTES"),
                multival: card_id == DB_CARDINALITY_MANY,
                unique,
            },
        );
    }

    schema
}

/// Build the bootstrap schema transaction from V1_IDENTS + V1_ATTRIBUTES.
/// This is the first transaction on a fresh database.
pub fn bootstrap_schema_tx() -> Vec<TxOp> {
    let attrs = &*V1_ATTRIBUTES;
    let attr_idents: Vec<&Keyword> = attrs.iter().map(|(kw, _, _, _)| kw).collect();
    let idents = &*V1_IDENTS;

    let mut ops = Vec::new();

    // Schema attribute entities
    for (ident, vt_ident, card_ident, unique_ident) in attrs {
        let eid = idents.iter().find(|(kw, _)| kw == ident).unwrap().1;
        let mut fields = vec![
            (kw!(:db/id), DataType::Long(eid)),
            (kw!(:db/ident), DataType::Keyword(ident.clone())),
            (kw!(:db/valueType), DataType::Keyword(vt_ident.clone())),
            (kw!(:db/cardinality), DataType::Keyword(card_ident.clone())),
        ];
        if let Some(unique_ident) = unique_ident {
            fields.push((kw!(:db/unique), DataType::Keyword(unique_ident.clone())));
        }
        ops.push(TxOp::put(fields));
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

/// Load the Schema from indices by querying with the Datalog engine.
/// Runs three sequential queries inside a single blocking task:
/// 1. All entities with db/ident → populates ident_map/entid_map
/// 2. Entities with db/ident + db/valueType + db/cardinality → populates attribute_map
/// 3. Entities with db/unique → enriches attribute_map with uniqueness
///
/// TODO: The three queries could be merged into a single query that fetches all
/// db/ident entities with optional db/valueType, db/cardinality, and db/unique.
/// Queries 2 and 3 re-fetch values already retrieved in query 1. This is only run
/// at startup so the inefficiency is minor, but a single-pass approach would be
/// cleaner. Could likely be done with an `optional` clause.
pub(crate) async fn load_schema_from_indices(slate: &crate::slate::SlateComponents) -> Schema {
    // Bootstrap metadata is sufficient to query schema entities.
    let query_schema = bootstrap_schema();
    let sdb = slate.db.clone();
    let range_stats = slate.range_stats.clone();
    let handle = Handle::current();

    let ident_query: ParsedQuery = "[:find ?e ?ident :where [?e :db/ident ?ident]]"
        .parse()
        .expect("Ident query parse failed");
    let attr_query: ParsedQuery = "[:find ?e ?ident ?vt ?card :where [?e :db/ident ?ident] [?e :db/valueType ?vt] [?e :db/cardinality ?card]]"
        .parse()
        .expect("Attribute query parse failed");
    let unique_query: ParsedQuery = "[:find ?e ?unique :where [?e :db/unique ?unique]]"
        .parse()
        .expect("Unique query parse failed");

    // Run all three queries in a single blocking task.
    let (ident_results, attr_results, unique_results) = tokio::task::spawn_blocking(move || {
        let idents = execute_query(
            &ident_query,
            &[],
            sdb.clone(),
            handle.clone(),
            &query_schema,
            i64::MAX,
            range_stats.clone(),
        )?;
        let attrs = execute_query(
            &attr_query,
            &[],
            sdb.clone(),
            handle.clone(),
            &query_schema,
            i64::MAX,
            range_stats.clone(),
        )?;
        let uniques = execute_query(
            &unique_query,
            &[],
            sdb,
            handle,
            &query_schema,
            i64::MAX,
            range_stats,
        )?;
        Ok::<_, anyhow::Error>((idents, attrs, uniques))
    })
    .await
    .expect("Schema-load task failed")
    .expect("Schema-load query failed");

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

    let mut unique_map = HashMap::new();
    for row in unique_results {
        let entity_id = match &row[0] {
            DataType::Long(id) => *id,
            other => panic!("Expected Long for entity_id, got {:?}", other),
        };
        let unique = match &row[1] {
            DataType::Long(id) => {
                Unique::from_entity_id(*id).unwrap_or_else(|e| panic!("Invalid unique: {}", e))
            }
            other => panic!("Expected Long for unique, got {:?}", other),
        };
        unique_map.insert(entity_id, unique);
    }

    // Populate attribute_map from entities with all required schema properties.
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
                unique: unique_map.remove(&entity_id),
            },
        );
    }

    schema
}

// --- Test helpers ---

/// Build a Put for a schema attribute without explicit db/id.
#[cfg(any(test, feature = "test-helpers"))]
fn schema_attribute_with_cardinality(ident: Keyword, value_type: &str, cardinality: &str) -> TxOp {
    schema_attribute_with_cardinality_and_unique(ident, value_type, cardinality, None)
}

#[cfg(any(test, feature = "test-helpers"))]
fn schema_attribute_with_cardinality_and_unique(
    ident: Keyword,
    value_type: &str,
    cardinality: &str,
    unique: Option<&str>,
) -> TxOp {
    let mut fields = vec![
        (kw!(:db/ident), DataType::Keyword(ident)),
        (
            kw!(:db/valueType),
            DataType::Keyword(Keyword::namespaced("db.type", value_type)),
        ),
        (
            kw!(:db/cardinality),
            DataType::Keyword(Keyword::namespaced("db.cardinality", cardinality)),
        ),
    ];
    if let Some(unique) = unique {
        fields.push((
            kw!(:db/unique),
            DataType::Keyword(Keyword::namespaced("db.unique", unique)),
        ));
    }
    TxOp::put(fields)
}

#[cfg(any(test, feature = "test-helpers"))]
fn schema_attribute(ident: Keyword, value_type: &str) -> TxOp {
    schema_attribute_with_cardinality(ident, value_type, "one")
}

#[cfg(any(test, feature = "test-helpers"))]
pub fn unique_identity_schema_attribute(ident: Keyword, value_type: &str) -> TxOp {
    schema_attribute_with_cardinality_and_unique(ident, value_type, "one", Some("identity"))
}

#[cfg(any(test, feature = "test-helpers"))]
pub fn unique_value_schema_attribute(ident: Keyword, value_type: &str) -> TxOp {
    schema_attribute_with_cardinality_and_unique(ident, value_type, "one", Some("value"))
}

/// Build a transaction that defines common test attributes.
/// Entity IDs are auto-assigned by resolve_entity_ids at transaction time.
#[cfg(any(test, feature = "test-helpers"))]
pub fn test_schema_tx() -> Vec<TxOp> {
    vec![
        schema_attribute(kw!(:name), "string"),
        schema_attribute(kw!(:age), "long"),
        unique_identity_schema_attribute(kw!(:email), "string"),
        schema_attribute(kw!(:follows), "ref"),
        schema_attribute_with_cardinality(kw!(:tags), "string", "many"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::PartitionMap;
    use crate::slate::in_memory_slate;
    use crate::tempids;
    use crate::tx;

    /// Skip lookup ref resolution for tests that have no DB and no lookup refs.
    fn expanded_to_tempids(datoms: Vec<tx::DatomExpanded>) -> Vec<tx::DatomWithTempids> {
        datoms
            .into_iter()
            .map(|d| tx::DatomWithTempids {
                entity: match d.entity {
                    tx::EntityExpanded::Id(id) => tx::IdOrTempId::Id(id),
                    tx::EntityExpanded::TempId(s) => tx::IdOrTempId::TempId(s),
                    tx::EntityExpanded::LookupRef(_, _) => {
                        panic!("test helper: unresolved lookup ref in entity position")
                    }
                },
                attribute: d.attribute,
                value: match d.value {
                    tx::ValueExpanded::Data(dt) => tx::ValueWithTempIds::Data(dt),
                    tx::ValueExpanded::TempRef(s) => tx::ValueWithTempIds::TempRef(s),
                    tx::ValueExpanded::LookupRef(_, _) => {
                        panic!("test helper: unresolved lookup ref in value position")
                    }
                },
                op: d.op,
            })
            .collect()
    }

    async fn to_datoms_result_async(ops: &[TxOp], schema: &Schema) -> Result<Vec<Datom>> {
        let slate = in_memory_slate().await;
        let mut pm = PartitionMap::new();
        let expanded = tx::expand_tx_ops(ops, schema).unwrap();
        let with_tempids = expanded_to_tempids(expanded);
        tempids::resolve_tempids(with_tempids, schema, &slate.db, &mut pm).await
    }

    async fn to_datoms_async(ops: &[TxOp], schema: &Schema) -> Vec<Datom> {
        to_datoms_result_async(ops, schema).await.unwrap()
    }

    fn to_datoms_result(ops: &[TxOp], schema: &Schema) -> Result<Vec<Datom>> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(to_datoms_result_async(ops, schema))
    }

    fn to_datoms(ops: &[TxOp], schema: &Schema) -> Vec<Datom> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(to_datoms_async(ops, schema))
    }

    fn bootstrapped_schema() -> Schema {
        bootstrap_schema()
    }

    fn bootstrapped_schema_with_person_name() -> Schema {
        let mut schema = bootstrapped_schema();
        let ops = [schema_attribute(kw!(:name), "string")];
        let datoms = to_datoms(&ops, &schema);
        let validation = schema.validate_datoms(&datoms).unwrap();
        assert!(validation.schema_changes_detected);
        let update = schema.prepare_schema_update(&datoms).unwrap();
        schema.apply_schema_update(update);
        schema
    }

    #[test]
    fn test_schema_from_bootstrap() {
        let schema = bootstrapped_schema();
        // 8 schema attrs (4 core + 4 tx); enum entities go into ident_map but not attribute_map
        assert_eq!(schema.len(), 8);

        let (eid, attr) = schema.get_attribute(&kw!(:db/ident)).unwrap();
        assert_eq!(eid, DB_IDENT);
        assert_eq!(attr.value_type, ValueType::Keyword);

        let (eid, attr) = schema.get_attribute(&kw!(:db/valueType)).unwrap();
        assert_eq!(eid, DB_VALUE_TYPE);
        assert_eq!(attr.value_type, ValueType::Ref);

        let (eid, attr) = schema.get_attribute(&kw!(:db/cardinality)).unwrap();
        assert_eq!(eid, DB_CARDINALITY);
        assert_eq!(attr.value_type, ValueType::Ref);

        let (eid, attr) = schema.get_attribute(&kw!(:db/unique)).unwrap();
        assert_eq!(eid, DB_UNIQUE);
        assert_eq!(attr.value_type, ValueType::Ref);

        let (_eid, attr) = schema.get_attribute(&kw!(:db/ident)).unwrap();
        assert_eq!(attr.unique, Some(Unique::Identity));

        // Enum entities are in ident_map but not attribute_map
        assert!(schema.ident_map.contains_key(&kw!(:db.type/string)));
        assert_eq!(schema.ident_map[&kw!(:db.type/string)], DB_TYPE_STRING);
    }

    #[test]
    fn test_apply_schema_update_user_attribute() {
        let schema = bootstrapped_schema_with_person_name();
        assert_eq!(schema.len(), 9);

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
    fn test_validate_datoms_valid() {
        let schema = bootstrapped_schema_with_person_name();
        let ops = [TxOp::Add {
            entity: "alice".into(),
            attribute: kw!(:name),
            value: "Alice".into(),
        }];
        let validation = schema.validate_datoms(&to_datoms(&ops, &schema)).unwrap();
        assert!(!validation.schema_changes_detected);
    }

    #[test]
    fn test_validate_datoms_unknown_attribute() {
        let schema = bootstrapped_schema_with_person_name();
        let datoms = [Datom {
            entity: 100,
            attribute: kw!(:person/age),
            value: DataType::Long(30),
            op: DatomOp::Assert,
        }];
        let err = schema.validate_datoms(&datoms).unwrap_err();
        assert!(err.to_string().contains("Unknown attribute: :person/age"));
    }

    #[test]
    fn test_validate_datoms_type_mismatch() {
        let schema = bootstrapped_schema_with_person_name();
        let ops = [TxOp::Add {
            entity: "person".into(),
            attribute: kw!(:name),
            value: 42_i64.into(),
        }];
        let err = schema
            .validate_datoms(&to_datoms(&ops, &schema))
            .unwrap_err();
        assert!(err.to_string().contains("Type mismatch"));
    }

    #[test]
    fn test_validate_datoms_schema_defining_tx_sets_flag() {
        let schema = bootstrapped_schema();
        let ops = [schema_attribute(kw!(:name), "string")];
        let validation = schema.validate_datoms(&to_datoms(&ops, &schema)).unwrap();
        assert!(validation.schema_changes_detected);
    }

    #[test]
    fn test_schema_immutability() {
        let schema = bootstrapped_schema_with_person_name();
        let (name_id, _) = schema.get_attribute(&kw!(:name)).unwrap();

        let ops = [TxOp::put([
            (kw!(:db/id), DataType::Long(name_id)),
            (kw!(:db/ident), DataType::Keyword(kw!(:name))),
            (kw!(:db/valueType), DataType::Long(DB_TYPE_LONG)),
            (kw!(:db/cardinality), DataType::Long(DB_CARDINALITY_ONE)),
        ])];
        let datoms = to_datoms(&ops, &schema);
        let validation = schema.validate_datoms(&datoms).unwrap();
        assert!(validation.schema_changes_detected);
        let err = schema.prepare_schema_update(&datoms).unwrap_err();
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

    #[tokio::test]
    async fn test_bootstrap_schema_consistency() {
        // Verify that bootstrap_schema() matches what bootstrap_schema_tx()
        // produces through the normal expand→resolve→validate→apply pipeline
        let bootstrap = bootstrap_schema();
        let tx_ops = bootstrap_schema_tx();
        let expanded = tx::expand_tx_ops(&tx_ops, &bootstrap).unwrap();
        let with_tempids = expanded_to_tempids(expanded);
        let mut pm = PartitionMap::new();
        let slate = in_memory_slate().await;
        let datoms = tempids::resolve_tempids(with_tempids, &bootstrap, &slate.db, &mut pm)
            .await
            .unwrap();
        let validation = bootstrap.validate_datoms(&datoms).unwrap();
        assert!(validation.schema_changes_detected);
        let update = Schema::default().prepare_schema_update(&datoms).unwrap();
        let mut schema_from_tx = Schema::default();
        schema_from_tx.apply_schema_update(update);
        assert_eq!(schema_from_tx.ident_map, bootstrap.ident_map);
        assert_eq!(schema_from_tx.attribute_map, bootstrap.attribute_map);
    }

    #[test]
    fn test_schema_ref_attribute_validation() {
        let mut schema = bootstrapped_schema();
        let ops = [schema_attribute(kw!(:follows), "ref")];
        let datoms = to_datoms(&ops, &schema);
        let validation = schema.validate_datoms(&datoms).unwrap();
        assert!(validation.schema_changes_detected);
        let update = schema.prepare_schema_update(&datoms).unwrap();
        schema.apply_schema_update(update);

        let (_eid, attr) = schema.get_attribute(&kw!(:follows)).unwrap();
        assert_eq!(attr.value_type, ValueType::Ref);

        // Long value accepted for ref-typed attribute
        let ops = [TxOp::Add {
            entity: "follower".into(),
            attribute: kw!(:follows),
            value: 201_i64.into(),
        }];
        let validation = schema.validate_datoms(&to_datoms(&ops, &schema)).unwrap();
        assert!(!validation.schema_changes_detected);

        // String in ref position must name a tempid introduced in entity position.
        let ops = [TxOp::Add {
            entity: "follower".into(),
            attribute: kw!(:follows),
            value: DataType::String("some-tempid".to_string()),
        }];
        let err = to_datoms_result(&ops, &schema).unwrap_err();
        assert!(
            err.to_string()
                .contains("Tempid some-tempid referenced only in value position"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_prepare_schema_update_parses_cardinality_many() {
        let schema = bootstrapped_schema();
        let ops = [schema_attribute_with_cardinality(
            kw!(:tags),
            "string",
            "many",
        )];
        let datoms = to_datoms(&ops, &schema);
        let validation = schema.validate_datoms(&datoms).unwrap();
        assert!(validation.schema_changes_detected);
        let update = schema.prepare_schema_update(&datoms).unwrap();
        assert_eq!(update.attributes.len(), 1);
        assert!(update.attributes[0].1.multival);
    }

    #[test]
    fn test_prepare_schema_update_parses_cardinality_one() {
        let schema = bootstrapped_schema();
        let ops = [schema_attribute(kw!(:name), "string")];
        let datoms = to_datoms(&ops, &schema);
        let validation = schema.validate_datoms(&datoms).unwrap();
        assert!(validation.schema_changes_detected);
        let update = schema.prepare_schema_update(&datoms).unwrap();
        assert_eq!(update.attributes.len(), 1);
        assert!(!update.attributes[0].1.multival);
    }

    #[test]
    fn test_prepare_schema_update_missing_cardinality_errors() {
        let schema = bootstrapped_schema();
        // Provide db/ident + db/valueType but no db/cardinality
        let ops = [TxOp::put([
            (kw!(:db/ident), DataType::Keyword(kw!(:name))),
            (kw!(:db/valueType), DataType::Long(DB_TYPE_STRING)),
            // No db/cardinality
        ])];
        let datoms = to_datoms(&ops, &schema);
        let validation = schema.validate_datoms(&datoms).unwrap();
        assert!(validation.schema_changes_detected);
        let err = schema.prepare_schema_update(&datoms).unwrap_err();
        assert!(err.to_string().contains("db/cardinality is required"));
    }

    #[test]
    fn test_validate_datoms_rejects_add_retract_conflict() {
        let schema = bootstrapped_schema_with_person_name();
        let ops = [
            TxOp::Add {
                entity: EntityRef::Id(200),
                attribute: kw!(:name),
                value: "Alice".into(),
            },
            TxOp::Retract {
                entity: EntityRef::Id(200),
                attribute: kw!(:name),
                value: "Alice".into(),
            },
        ];
        let err = schema
            .validate_datoms(&to_datoms(&ops, &schema))
            .unwrap_err();
        assert!(err.to_string().contains("both assert and retract"));
    }

    #[test]
    fn test_validate_datoms_rejects_cardinality_one_conflict() {
        let schema = bootstrapped_schema_with_person_name();
        let ops = [
            TxOp::Add {
                entity: EntityRef::Id(200),
                attribute: kw!(:name),
                value: "Alice".into(),
            },
            TxOp::Add {
                entity: EntityRef::Id(200),
                attribute: kw!(:name),
                value: "Bob".into(),
            },
        ];
        let err = schema
            .validate_datoms(&to_datoms(&ops, &schema))
            .unwrap_err();
        assert!(err.to_string().contains("multiple values"));
    }

    #[test]
    fn test_validate_datoms_allows_add_retract_overlap_for_cardinality_many() {
        let mut schema = bootstrapped_schema();
        let ops = [schema_attribute_with_cardinality(
            kw!(:tags),
            "string",
            "many",
        )];
        let datoms = to_datoms(&ops, &schema);
        let update = schema.prepare_schema_update(&datoms).unwrap();
        schema.apply_schema_update(update);

        let ops = [
            TxOp::Add {
                entity: EntityRef::Id(200),
                attribute: kw!(:tags),
                value: "rust".into(),
            },
            TxOp::Retract {
                entity: EntityRef::Id(200),
                attribute: kw!(:tags),
                value: "rust".into(),
            },
        ];
        let validation = schema.validate_datoms(&to_datoms(&ops, &schema)).unwrap();
        assert!(!validation.schema_changes_detected);
    }
}
