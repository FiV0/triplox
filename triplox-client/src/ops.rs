use anyhow::Result;
use chrono::{DateTime, Utc};
use edn::symbols::Keyword;
use edn::types::Value;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

pub type Entid = i64;

/// How to identify an entity in a transaction.
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Eq, Hash)]
pub enum EntityRef {
    Id(i64),
    TempId(String),
    Ident(Keyword),
    /// Accepted in the type but errors at expansion (not yet supported).
    LookupRef(Keyword, DataType),
}

impl From<i64> for EntityRef {
    fn from(v: i64) -> Self {
        EntityRef::Id(v)
    }
}

impl From<Keyword> for EntityRef {
    fn from(v: Keyword) -> Self {
        EntityRef::Ident(v)
    }
}

impl From<&str> for EntityRef {
    fn from(v: &str) -> Self {
        EntityRef::TempId(v.to_string())
    }
}

impl From<String> for EntityRef {
    fn from(v: String) -> Self {
        EntityRef::TempId(v)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum DataType {
    BigInt(i128),                    // Arbitrary large integers
    Boolean(bool),                   // Booleans (true or false)
    Bytes(Vec<u8>),                  // Binary data (as bytes)
    Double(f64),                     // Double precision floating point
    Float(f32),                      // Single precision floating point
    Instant(DateTime<Utc>),          // Timestamps or instants
    Keyword(Keyword),                // Keywords
    Long(i64),                       // Long integers (also used for Ref values; see ValueType::Ref)
    String(String),                  // Strings
    Uuid(Uuid),                      // Universally unique identifier
    Vector(Vec<DataType>),           // List (vector of DataTypes)
    Map(BTreeMap<String, DataType>), // Map (BTreeMap of string keys and DataType values)
}

impl Eq for DataType {}

impl std::hash::Hash for DataType {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            DataType::BigInt(v) => v.hash(state),
            DataType::Boolean(v) => v.hash(state),
            DataType::Bytes(v) => v.hash(state),
            DataType::Double(v) => v.to_bits().hash(state),
            DataType::Float(v) => v.to_bits().hash(state),
            DataType::Instant(v) => v.hash(state),
            DataType::Keyword(v) => v.hash(state),
            DataType::Long(v) => v.hash(state),
            DataType::String(v) => v.hash(state),
            DataType::Uuid(v) => v.hash(state),
            DataType::Vector(v) => v.hash(state),
            DataType::Map(v) => v.hash(state),
        }
    }
}

impl DataType {
    /// Variant name, for diagnostics. Stays inside this crate so the type
    /// stays decoupled from the server-side `ValueType`.
    fn variant_name(&self) -> &'static str {
        match self {
            DataType::BigInt(_) => "BigInt",
            DataType::Boolean(_) => "Boolean",
            DataType::Bytes(_) => "Bytes",
            DataType::Double(_) => "Double",
            DataType::Float(_) => "Float",
            DataType::Instant(_) => "Instant",
            DataType::Keyword(_) => "Keyword",
            DataType::Long(_) => "Long",
            DataType::String(_) => "String",
            DataType::Uuid(_) => "Uuid",
            DataType::Vector(_) => "Vector",
            DataType::Map(_) => "Map",
        }
    }

    /// Compare two DataType values. Returns an error if the types are incompatible
    /// or if floats are NaN.
    pub fn partial_compare(&self, other: &DataType) -> Result<std::cmp::Ordering> {
        use DataType::*;
        let nan_err = || anyhow::anyhow!("cannot compare NaN values");
        match (self, other) {
            (Long(a), Long(b)) => Ok(a.cmp(b)),
            (BigInt(a), BigInt(b)) => Ok(a.cmp(b)),
            (Double(a), Double(b)) => a.partial_cmp(b).ok_or_else(nan_err),
            (Float(a), Float(b)) => a.partial_cmp(b).ok_or_else(nan_err),
            (String(a), String(b)) => Ok(a.cmp(b)),
            (Boolean(a), Boolean(b)) => Ok(a.cmp(b)),
            (Instant(a), Instant(b)) => Ok(a.cmp(b)),
            // Cross-numeric promotion
            // NOTE: casting BigInt(i128) to f32/f64 may lose precision for large values.
            (Long(a), BigInt(b)) => Ok((*a as i128).cmp(b)),
            (BigInt(a), Long(b)) => Ok(a.cmp(&(*b as i128))),
            (Long(a), Double(b)) => (*a as f64).partial_cmp(b).ok_or_else(nan_err),
            (Double(a), Long(b)) => a.partial_cmp(&(*b as f64)).ok_or_else(nan_err),
            (Long(a), Float(b)) => (*a as f32).partial_cmp(b).ok_or_else(nan_err),
            (Float(a), Long(b)) => a.partial_cmp(&(*b as f32)).ok_or_else(nan_err),
            (BigInt(a), Float(b)) => (*a as f32).partial_cmp(b).ok_or_else(nan_err),
            (Float(a), BigInt(b)) => a.partial_cmp(&(*b as f32)).ok_or_else(nan_err),
            (BigInt(a), Double(b)) => (*a as f64).partial_cmp(b).ok_or_else(nan_err),
            (Double(a), BigInt(b)) => a.partial_cmp(&(*b as f64)).ok_or_else(nan_err),
            (Float(a), Double(b)) => (*a as f64).partial_cmp(b).ok_or_else(nan_err),
            (Double(a), Float(b)) => a.partial_cmp(&(*b as f64)).ok_or_else(nan_err),
            _ => Err(anyhow::anyhow!(
                "cannot compare {} with {}",
                self.variant_name(),
                other.variant_name()
            )),
        }
    }
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

impl_from_for_enum!(
    DataType,
    (BigInt, i128),
    (Boolean, bool),
    (Bytes, Vec<u8>),
    (Double, f64),
    (Float, f32),
    (Instant, DateTime<Utc>),
    (Keyword, Keyword),
    (Long, i64),
    (String, String),
    (Uuid, Uuid),
    (Vector, Vec<DataType>),
    (Map, BTreeMap<String, DataType>)
);

impl From<&str> for DataType {
    fn from(v: &str) -> Self {
        DataType::String(v.to_string())
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum TxOp {
    Put(BTreeMap<Keyword, DataType>),
    Add {
        entity: EntityRef,
        attribute: Keyword,
        value: DataType,
    },
    Retract {
        entity: EntityRef,
        attribute: Keyword,
        value: DataType,
    },
    Delete(EntityRef),
    Erase(EntityRef),
}

impl TxOp {
    /// Build a Put without explicit entity ID (auto-allocated).
    pub fn put(attrs: impl IntoIterator<Item = (Keyword, DataType)>) -> Self {
        TxOp::Put(attrs.into_iter().collect())
    }
}

/// Convert an EDN `Value` to a `DataType` for the value position of a TxOp.
///
/// Map keys here must be `Value::Text` because `DataType::Map` uses `String` keys.
/// (Top-level Put maps use `Value::Keyword` keys — that's handled in `tx_op_from_value`.)
pub fn value_to_data_type(value: Value) -> Result<DataType> {
    match value {
        Value::Boolean(b) => Ok(DataType::Boolean(b)),
        Value::Integer(i) => Ok(DataType::Long(i)),
        Value::BigInteger(bi) => bi
            .to_string()
            .parse::<i128>()
            .map(DataType::BigInt)
            .map_err(|_| anyhow::anyhow!("BigInt out of i128 range: {}", bi)),
        Value::Float(f) => Ok(DataType::Double(f.into_inner())),
        Value::Text(s) => Ok(DataType::String(s)),
        Value::Uuid(u) => Ok(DataType::Uuid(u)),
        Value::Instant(d) => Ok(DataType::Instant(d)),
        Value::Keyword(k) => Ok(DataType::Keyword(k)),
        Value::Vector(items) => items
            .into_iter()
            .map(value_to_data_type)
            .collect::<Result<Vec<_>>>()
            .map(DataType::Vector),
        Value::Map(m) => {
            let mut out = BTreeMap::new();
            for (k, v) in m {
                let key = match k {
                    Value::Text(s) => s,
                    other => anyhow::bail!("nested map keys must be strings, got {:?}", other),
                };
                out.insert(key, value_to_data_type(v)?);
            }
            Ok(DataType::Map(out))
        }
        Value::Nil => anyhow::bail!("nil is not a valid TxOp value"),
        Value::Set(_) => anyhow::bail!("set is not a valid TxOp value"),
        v @ Value::List(_) => anyhow::bail!("invalid TxOp value: {:?}", v),
        Value::PlainSymbol(s) => anyhow::bail!("symbol {} is not a valid TxOp value", s),
        Value::NamespacedSymbol(s) => anyhow::bail!("symbol {} is not a valid TxOp value", s),
    }
}

/// Convert an EDN `Value` to an `EntityRef`.
/// Accepts: integer (`Id`), text (`TempId`), namespaced keyword (`Ident`),
/// or `[:attr value]` 2-vector (`LookupRef`, Datomic-style).
pub fn value_to_entity_ref(value: Value) -> Result<EntityRef> {
    match value {
        Value::Integer(i) => Ok(EntityRef::Id(i)),
        Value::Text(s) => Ok(EntityRef::TempId(s)),
        Value::Keyword(k) => {
            if k.is_backward() {
                anyhow::bail!("reverse keyword {} not supported in entity position", k);
            }
            Ok(EntityRef::Ident(k))
        }
        Value::Vector(items) => {
            if items.len() != 2 {
                anyhow::bail!(
                    "lookup ref must be [:attr value] 2-vector, got {} elements",
                    items.len()
                );
            }
            let mut iter = items.into_iter();
            let attr = match iter.next().unwrap() {
                Value::Keyword(k) => k,
                other => anyhow::bail!("lookup ref attribute must be a keyword, got {:?}", other),
            };
            let v = value_to_data_type(iter.next().unwrap())?;
            Ok(EntityRef::LookupRef(attr, v))
        }
        other => anyhow::bail!(
            "expected entity ref (integer, string, keyword, or [:attr value] lookup ref), got {:?}",
            other
        ),
    }
}

/// Convert an EDN `Value` to a single `TxOp`.
///
/// Accepted forms:
/// - `[:db/add e a v]`     → `TxOp::Add`
/// - `[:db/retract e a v]` → `TxOp::Retract`
/// - `[:db/delete e]`      → `TxOp::Delete`
/// - `[:db/erase e]`       → `TxOp::Erase`
/// - `{:attr v ...}`       → `TxOp::Put` (`:db/id` is passed through as a normal entry)
pub fn tx_op_from_value(value: Value) -> Result<TxOp> {
    match value {
        Value::Vector(items) => {
            let mut iter = items.into_iter();
            let head = iter
                .next()
                .ok_or_else(|| anyhow::anyhow!("empty vector is not a valid TxOp"))?;
            let op_kw = match head {
                Value::Keyword(k) => k,
                other => {
                    anyhow::bail!("expected operation keyword (e.g. :db/add), got {:?}", other)
                }
            };
            match (op_kw.namespace(), op_kw.name()) {
                (Some("db"), name @ ("add" | "retract")) => {
                    let entity = match iter.next() {
                        Some(v) => value_to_entity_ref(v)?,
                        None => anyhow::bail!("{} missing entity", op_kw),
                    };
                    let attribute = match iter.next() {
                        Some(Value::Keyword(k)) => k,
                        Some(other) => {
                            anyhow::bail!("{} attribute must be a keyword, got {:?}", op_kw, other)
                        }
                        None => anyhow::bail!("{} missing attribute", op_kw),
                    };
                    let value = match iter.next() {
                        Some(v) => value_to_data_type(v)?,
                        None => anyhow::bail!("{} missing value", op_kw),
                    };
                    if iter.next().is_some() {
                        anyhow::bail!("{} takes exactly 3 arguments", op_kw);
                    }
                    Ok(if name == "add" {
                        TxOp::Add {
                            entity,
                            attribute,
                            value,
                        }
                    } else {
                        TxOp::Retract {
                            entity,
                            attribute,
                            value,
                        }
                    })
                }
                (Some("db"), name @ ("delete" | "erase")) => {
                    let entity = match iter.next() {
                        Some(v) => value_to_entity_ref(v)?,
                        None => anyhow::bail!("{} missing entity", op_kw),
                    };
                    if iter.next().is_some() {
                        anyhow::bail!("{} takes exactly 1 argument", op_kw);
                    }
                    Ok(if name == "delete" {
                        TxOp::Delete(entity)
                    } else {
                        TxOp::Erase(entity)
                    })
                }
                _ => anyhow::bail!("unknown TxOp operation: {}", op_kw),
            }
        }
        Value::Map(m) => {
            let mut out = BTreeMap::new();
            for (k, v) in m {
                let key = match k {
                    Value::Keyword(kw) => kw,
                    other => {
                        anyhow::bail!("Put map keys must be keywords, got {:?}", other)
                    }
                };
                out.insert(key, value_to_data_type(v)?);
            }
            Ok(TxOp::Put(out))
        }
        other => anyhow::bail!("expected TxOp (vector or map form), got {:?}", other),
    }
}

impl std::str::FromStr for TxOp {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let value = edn::parse::value(s)
            .map_err(|e| anyhow::anyhow!("EDN parse error: {}", e))?
            .without_spans();
        tx_op_from_value(value)
    }
}

/// A query input argument corresponding to an `:in` binding form.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryArg {
    Scalar(DataType),
    Collection(Vec<DataType>),
    // TODO: Tuple and Relation are not yet supported in the query engine
    Tuple(Vec<DataType>),
    Relation(Vec<Vec<DataType>>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode;
    use edn::kw;

    #[test]
    fn test_partial_compare_same_type() {
        use std::cmp::Ordering;
        assert_eq!(
            DataType::Long(1)
                .partial_compare(&DataType::Long(2))
                .unwrap(),
            Ordering::Less
        );
        assert_eq!(
            DataType::Long(2)
                .partial_compare(&DataType::Long(2))
                .unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            DataType::Long(3)
                .partial_compare(&DataType::Long(2))
                .unwrap(),
            Ordering::Greater
        );

        assert_eq!(
            DataType::String("a".into())
                .partial_compare(&DataType::String("b".into()))
                .unwrap(),
            Ordering::Less,
        );
        assert_eq!(
            DataType::Boolean(false)
                .partial_compare(&DataType::Boolean(true))
                .unwrap(),
            Ordering::Less,
        );
        assert_eq!(
            DataType::Double(1.5)
                .partial_compare(&DataType::Double(2.5))
                .unwrap(),
            Ordering::Less,
        );
    }

    #[test]
    fn test_partial_compare_cross_numeric() {
        use std::cmp::Ordering;
        assert_eq!(
            DataType::Long(10)
                .partial_compare(&DataType::BigInt(20))
                .unwrap(),
            Ordering::Less
        );
        assert_eq!(
            DataType::BigInt(20)
                .partial_compare(&DataType::Long(10))
                .unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            DataType::Long(5)
                .partial_compare(&DataType::Double(5.0))
                .unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            DataType::Double(3.0)
                .partial_compare(&DataType::Long(4))
                .unwrap(),
            Ordering::Less
        );

        // Float cross-numeric
        assert_eq!(
            DataType::Float(1.0)
                .partial_compare(&DataType::Long(2))
                .unwrap(),
            Ordering::Less
        );
        assert_eq!(
            DataType::Long(2)
                .partial_compare(&DataType::Float(1.0))
                .unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            DataType::Float(1.0)
                .partial_compare(&DataType::Double(1.0))
                .unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            DataType::Double(2.0)
                .partial_compare(&DataType::Float(1.0))
                .unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            DataType::Float(1.0)
                .partial_compare(&DataType::BigInt(2))
                .unwrap(),
            Ordering::Less
        );
        assert_eq!(
            DataType::BigInt(2)
                .partial_compare(&DataType::Float(1.0))
                .unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            DataType::BigInt(1)
                .partial_compare(&DataType::Double(2.0))
                .unwrap(),
            Ordering::Less
        );
        assert_eq!(
            DataType::Double(2.0)
                .partial_compare(&DataType::BigInt(1))
                .unwrap(),
            Ordering::Greater
        );
    }

    #[test]
    fn test_partial_compare_incompatible() {
        assert!(DataType::Long(1)
            .partial_compare(&DataType::String("a".into()))
            .is_err());
        assert!(DataType::Boolean(true)
            .partial_compare(&DataType::Long(1))
            .is_err());
    }

    #[test]
    fn test_partial_compare_nan() {
        // Same-type NaN
        assert!(DataType::Double(f64::NAN)
            .partial_compare(&DataType::Double(1.0))
            .is_err());
        assert!(DataType::Float(f32::NAN)
            .partial_compare(&DataType::Float(1.0))
            .is_err());
        // Cross-type NaN
        assert!(DataType::Float(f32::NAN)
            .partial_compare(&DataType::Long(1))
            .is_err());
        assert!(DataType::Float(f32::NAN)
            .partial_compare(&DataType::Double(1.0))
            .is_err());
        assert!(DataType::Float(f32::NAN)
            .partial_compare(&DataType::BigInt(1))
            .is_err());
        assert!(DataType::Double(f64::NAN)
            .partial_compare(&DataType::Long(1))
            .is_err());
        assert!(DataType::Double(f64::NAN)
            .partial_compare(&DataType::BigInt(1))
            .is_err());
    }

    #[test]
    fn test_op_put_bincode() {
        let op = TxOp::put([
            (kw!(:string), "string_value".into()),
            (kw!(:int), 1i64.into()),
        ]);
        let serialized = bincode::serialize(&op).unwrap();
        let deserialized: TxOp = bincode::deserialize(&serialized).unwrap();
        assert_eq!(op, deserialized);
    }

    #[test]
    fn test_op_put_accepts_array() {
        let op = TxOp::put([
            (kw!(:string), "string_value".into()),
            (kw!(:int), 1i64.into()),
        ]);
        let mut expected = BTreeMap::new();
        expected.insert(kw!(:string), "string_value".into());
        expected.insert(kw!(:int), 1i64.into());
        assert_eq!(op, TxOp::Put(expected));
    }

    #[test]
    fn test_op_add_bincode() {
        let op = TxOp::Add {
            entity: EntityRef::Id(1),
            attribute: kw!(:string),
            value: DataType::String("string_value".to_string()),
        };
        let serialized = bincode::serialize(&op).unwrap();
        let deserialized: TxOp = bincode::deserialize(&serialized).unwrap();
        assert_eq!(op, deserialized);
    }

    #[test]
    fn test_op_retract_bincode() {
        let op = TxOp::Retract {
            entity: EntityRef::Id(1),
            attribute: kw!(:string),
            value: DataType::String("string_value".to_string()),
        };
        let serialized = bincode::serialize(&op).unwrap();
        let deserialized: TxOp = bincode::deserialize(&serialized).unwrap();
        assert_eq!(op, deserialized);
    }

    #[test]
    fn test_op_delete_bincode() {
        let op = TxOp::Delete(EntityRef::Id(1));
        let serialized = bincode::serialize(&op).unwrap();
        let deserialized: TxOp = bincode::deserialize(&serialized).unwrap();
        assert_eq!(op, deserialized);
    }

    #[test]
    fn test_op_erase_bincode() {
        let op = TxOp::Erase(EntityRef::Id(1));
        let serialized = bincode::serialize(&op).unwrap();
        let deserialized: TxOp = bincode::deserialize(&serialized).unwrap();
        assert_eq!(op, deserialized);
    }

    #[test]
    fn test_entity_ref_from_impls() {
        assert_eq!(EntityRef::from(42_i64), EntityRef::Id(42));
        assert_eq!(
            EntityRef::from(kw!(:person/name)),
            EntityRef::Ident(kw!(:person/name))
        );
        assert_eq!(
            EntityRef::from("temp-1"),
            EntityRef::TempId("temp-1".to_string())
        );
    }

    #[test]
    fn test_parse_add_with_id() {
        let op: TxOp = "[:db/add 1 :user/name \"Alice\"]".parse().unwrap();
        assert_eq!(
            op,
            TxOp::Add {
                entity: EntityRef::Id(1),
                attribute: kw!(:user/name),
                value: DataType::String("Alice".to_string()),
            }
        );
    }

    #[test]
    fn test_parse_add_with_tempid() {
        let op: TxOp = "[:db/add \"alice\" :user/age 30]".parse().unwrap();
        assert_eq!(
            op,
            TxOp::Add {
                entity: EntityRef::TempId("alice".to_string()),
                attribute: kw!(:user/age),
                value: DataType::Long(30),
            }
        );
    }

    #[test]
    fn test_parse_add_with_ident() {
        let op: TxOp = "[:db/add :user/me :user/name \"Me\"]".parse().unwrap();
        assert_eq!(
            op,
            TxOp::Add {
                entity: EntityRef::Ident(kw!(:user/me)),
                attribute: kw!(:user/name),
                value: DataType::String("Me".to_string()),
            }
        );
    }

    #[test]
    fn test_parse_add_with_lookup_ref() {
        let op: TxOp = "[:db/add [:user/email \"a@b.c\"] :user/name \"A\"]"
            .parse()
            .unwrap();
        assert_eq!(
            op,
            TxOp::Add {
                entity: EntityRef::LookupRef(
                    kw!(:user/email),
                    DataType::String("a@b.c".to_string())
                ),
                attribute: kw!(:user/name),
                value: DataType::String("A".to_string()),
            }
        );
    }

    #[test]
    fn test_parse_retract() {
        let op: TxOp = "[:db/retract 7 :user/age 30]".parse().unwrap();
        assert_eq!(
            op,
            TxOp::Retract {
                entity: EntityRef::Id(7),
                attribute: kw!(:user/age),
                value: DataType::Long(30),
            }
        );
    }

    #[test]
    fn test_parse_delete() {
        let op: TxOp = "[:db/delete 42]".parse().unwrap();
        assert_eq!(op, TxOp::Delete(EntityRef::Id(42)));
    }

    #[test]
    fn test_parse_erase() {
        let op: TxOp = "[:db/erase \"tempid-1\"]".parse().unwrap();
        assert_eq!(op, TxOp::Erase(EntityRef::TempId("tempid-1".to_string())));
    }

    #[test]
    fn test_parse_put_no_id() {
        let op: TxOp = "{:user/name \"Alice\" :user/age 30}".parse().unwrap();
        let mut expected = BTreeMap::new();
        expected.insert(kw!(:user/name), DataType::String("Alice".to_string()));
        expected.insert(kw!(:user/age), DataType::Long(30));
        assert_eq!(op, TxOp::Put(expected));
    }

    #[test]
    fn test_parse_put_with_db_id_long() {
        let op: TxOp = "{:db/id 100 :user/name \"X\"}".parse().unwrap();
        let mut expected = BTreeMap::new();
        expected.insert(kw!(:db/id), DataType::Long(100));
        expected.insert(kw!(:user/name), DataType::String("X".to_string()));
        assert_eq!(op, TxOp::Put(expected));
    }

    #[test]
    fn test_parse_put_with_db_id_tempid() {
        let op: TxOp = "{:db/id \"alice\" :user/name \"Alice\"}".parse().unwrap();
        let mut expected = BTreeMap::new();
        expected.insert(kw!(:db/id), DataType::String("alice".to_string()));
        expected.insert(kw!(:user/name), DataType::String("Alice".to_string()));
        assert_eq!(op, TxOp::Put(expected));
    }

    #[test]
    fn test_parse_put_with_db_id_ident() {
        let op: TxOp = "{:db/id :user/me :user/name \"Me\"}".parse().unwrap();
        let mut expected = BTreeMap::new();
        expected.insert(kw!(:db/id), DataType::Keyword(kw!(:user/me)));
        expected.insert(kw!(:user/name), DataType::String("Me".to_string()));
        assert_eq!(op, TxOp::Put(expected));
    }

    #[test]
    fn test_parse_value_types() {
        let op: TxOp = "[:db/add 1 :a/b true]".parse().unwrap();
        assert!(matches!(
            op,
            TxOp::Add {
                value: DataType::Boolean(true),
                ..
            }
        ));
        let op: TxOp = "[:db/add 1 :a/b 3.14]".parse().unwrap();
        assert!(matches!(
            op,
            TxOp::Add {
                value: DataType::Double(_),
                ..
            }
        ));
        let op: TxOp = "[:db/add 1 :a/b :some/kw]".parse().unwrap();
        assert!(matches!(
            op,
            TxOp::Add {
                value: DataType::Keyword(_),
                ..
            }
        ));
        let op: TxOp = "[:db/add 1 :a/b [1 2 3]]".parse().unwrap();
        if let TxOp::Add {
            value: DataType::Vector(v),
            ..
        } = op
        {
            assert_eq!(v.len(), 3);
        } else {
            panic!("expected vector value");
        }
    }

    #[test]
    fn test_parse_errors() {
        // Wrong arity
        assert!("[:db/add 1 :a]".parse::<TxOp>().is_err());
        assert!("[:db/add 1 :a 2 3]".parse::<TxOp>().is_err());
        assert!("[:db/delete 1 2]".parse::<TxOp>().is_err());
        // Unknown op
        assert!("[:db/frobnicate 1]".parse::<TxOp>().is_err());
        // Bad shape
        assert!("42".parse::<TxOp>().is_err());
        assert!("[]".parse::<TxOp>().is_err());
        // Map key not a keyword
        assert!("{\"name\" \"Alice\"}".parse::<TxOp>().is_err());
        // Bad EDN
        assert!("[:db/add 1 :a".parse::<TxOp>().is_err());
        // Reverse keyword in entity position
        assert!("[:db/add :user/_friend :a 1]".parse::<TxOp>().is_err());
        // Set in value position
        assert!("[:db/add 1 :a #{1 2}]".parse::<TxOp>().is_err());
    }
}
