use anyhow::Result;
use chrono::{DateTime, Utc};
use edn::symbols::Keyword;
use edn::types::Value;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(
    feature = "dbsp",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, size_of::SizeOf)
)]
#[cfg_attr(feature = "dbsp", archive_attr(derive(Eq, PartialEq, Ord, PartialOrd)))]
pub enum ComparisonFamily {
    Numeric,
    String,
    Boolean,
    Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ComparisonError {
    #[error("cannot compare NaN values")]
    Nan,
    #[error("value type is not comparable")]
    Unsupported,
    #[error("cannot compare different value families")]
    MixedFamilies,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(
    feature = "dbsp",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, size_of::SizeOf)
)]
#[cfg_attr(feature = "dbsp", archive_attr(derive(Eq, PartialEq, Ord, PartialOrd)))]
struct BinaryMagnitude {
    highest_bit: i16,
    normalized_significand: u128,
}

impl BinaryMagnitude {
    fn new(significand: u128, exponent: i16) -> Self {
        debug_assert_ne!(significand, 0);
        let leading_zeros = significand.leading_zeros();
        Self {
            highest_bit: (127 - leading_zeros) as i16 + exponent,
            normalized_significand: significand << leading_zeros,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(
    feature = "dbsp",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, size_of::SizeOf)
)]
#[cfg_attr(feature = "dbsp", archive_attr(derive(Eq, PartialEq, Ord, PartialOrd)))]
struct NegativeMagnitude {
    reversed_highest_bit: i16,
    reversed_significand: u128,
}

impl From<BinaryMagnitude> for NegativeMagnitude {
    fn from(value: BinaryMagnitude) -> Self {
        Self {
            reversed_highest_bit: -value.highest_bit,
            reversed_significand: !value.normalized_significand,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(
    feature = "dbsp",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, size_of::SizeOf)
)]
#[cfg_attr(feature = "dbsp", archive_attr(derive(Eq, PartialEq, Ord, PartialOrd)))]
enum NumericKeyValue {
    NegativeInfinity,
    Negative(NegativeMagnitude),
    Zero,
    Positive(BinaryMagnitude),
    PositiveInfinity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(
    feature = "dbsp",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, size_of::SizeOf)
)]
#[cfg_attr(feature = "dbsp", archive_attr(derive(Eq, PartialEq, Ord, PartialOrd)))]
pub struct NumericKey(NumericKeyValue);

impl NumericKey {
    fn from_i128(value: i128) -> Self {
        Self(match value.cmp(&0) {
            Ordering::Less => {
                NumericKeyValue::Negative(BinaryMagnitude::new(value.unsigned_abs(), 0).into())
            }
            Ordering::Equal => NumericKeyValue::Zero,
            Ordering::Greater => NumericKeyValue::Positive(BinaryMagnitude::new(value as u128, 0)),
        })
    }

    fn from_f32(value: f32) -> Result<Self, ComparisonError> {
        let bits = value.to_bits();
        let negative = bits >> 31 != 0;
        let exponent = (bits >> 23) & 0xff;
        let fraction = bits & ((1 << 23) - 1);

        if exponent == 0xff {
            return if fraction == 0 {
                Ok(if negative {
                    Self(NumericKeyValue::NegativeInfinity)
                } else {
                    Self(NumericKeyValue::PositiveInfinity)
                })
            } else {
                Err(ComparisonError::Nan)
            };
        }
        if exponent == 0 && fraction == 0 {
            return Ok(Self(NumericKeyValue::Zero));
        }

        let (significand, exponent) = if exponent == 0 {
            (fraction as u128, -149)
        } else {
            (((1 << 23) | fraction) as u128, exponent as i16 - 127 - 23)
        };
        Ok(Self::from_magnitude(
            negative,
            BinaryMagnitude::new(significand, exponent),
        ))
    }

    fn from_f64(value: f64) -> Result<Self, ComparisonError> {
        let bits = value.to_bits();
        let negative = bits >> 63 != 0;
        let exponent = (bits >> 52) & 0x7ff;
        let fraction = bits & ((1_u64 << 52) - 1);

        if exponent == 0x7ff {
            return if fraction == 0 {
                Ok(if negative {
                    Self(NumericKeyValue::NegativeInfinity)
                } else {
                    Self(NumericKeyValue::PositiveInfinity)
                })
            } else {
                Err(ComparisonError::Nan)
            };
        }
        if exponent == 0 && fraction == 0 {
            return Ok(Self(NumericKeyValue::Zero));
        }

        let (significand, exponent) = if exponent == 0 {
            (fraction as u128, -1074)
        } else {
            (
                ((1_u64 << 52) | fraction) as u128,
                exponent as i16 - 1023 - 52,
            )
        };
        Ok(Self::from_magnitude(
            negative,
            BinaryMagnitude::new(significand, exponent),
        ))
    }

    fn from_magnitude(negative: bool, magnitude: BinaryMagnitude) -> Self {
        Self(if negative {
            NumericKeyValue::Negative(magnitude.into())
        } else {
            NumericKeyValue::Positive(magnitude)
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(
    feature = "dbsp",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, size_of::SizeOf)
)]
#[cfg_attr(feature = "dbsp", archive_attr(derive(Eq, PartialEq, Ord, PartialOrd)))]
pub struct InstantKey {
    seconds: i64,
    nanoseconds: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonKey<'a> {
    Numeric(NumericKey),
    String(&'a str),
    Boolean(bool),
    Instant(InstantKey),
}

impl ComparisonKey<'_> {
    pub fn family(&self) -> ComparisonFamily {
        match self {
            Self::Numeric(_) => ComparisonFamily::Numeric,
            Self::String(_) => ComparisonFamily::String,
            Self::Boolean(_) => ComparisonFamily::Boolean,
            Self::Instant(_) => ComparisonFamily::Instant,
        }
    }

    pub fn partial_compare(&self, other: &Self) -> Result<Ordering, ComparisonError> {
        match (self, other) {
            (Self::Numeric(left), Self::Numeric(right)) => Ok(left.cmp(right)),
            (Self::String(left), Self::String(right)) => Ok(left.cmp(right)),
            (Self::Boolean(left), Self::Boolean(right)) => Ok(left.cmp(right)),
            (Self::Instant(left), Self::Instant(right)) => Ok(left.cmp(right)),
            _ => Err(ComparisonError::MixedFamilies),
        }
    }

    pub fn into_owned(self) -> OwnedComparisonKey {
        match self {
            Self::Numeric(value) => OwnedComparisonKey::Numeric(value),
            Self::String(value) => OwnedComparisonKey::String(value.to_string()),
            Self::Boolean(value) => OwnedComparisonKey::Boolean(value),
            Self::Instant(value) => OwnedComparisonKey::Instant(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(
    feature = "dbsp",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, size_of::SizeOf)
)]
#[cfg_attr(feature = "dbsp", archive_attr(derive(Eq, PartialEq, Ord, PartialOrd)))]
pub enum OwnedComparisonKey {
    Numeric(NumericKey),
    String(String),
    Boolean(bool),
    Instant(InstantKey),
}

impl OwnedComparisonKey {
    pub fn family(&self) -> ComparisonFamily {
        match self {
            Self::Numeric(_) => ComparisonFamily::Numeric,
            Self::String(_) => ComparisonFamily::String,
            Self::Boolean(_) => ComparisonFamily::Boolean,
            Self::Instant(_) => ComparisonFamily::Instant,
        }
    }
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

    pub fn comparison_family(&self) -> Option<ComparisonFamily> {
        match self {
            Self::Long(_) | Self::BigInt(_) | Self::Double(_) | Self::Float(_) => {
                Some(ComparisonFamily::Numeric)
            }
            Self::String(_) => Some(ComparisonFamily::String),
            Self::Boolean(_) => Some(ComparisonFamily::Boolean),
            Self::Instant(_) => Some(ComparisonFamily::Instant),
            _ => None,
        }
    }

    pub fn comparison_key(&self) -> Result<ComparisonKey<'_>, ComparisonError> {
        match self {
            Self::Long(value) => Ok(ComparisonKey::Numeric(NumericKey::from_i128(
                *value as i128,
            ))),
            Self::BigInt(value) => Ok(ComparisonKey::Numeric(NumericKey::from_i128(*value))),
            Self::Double(value) => NumericKey::from_f64(*value).map(ComparisonKey::Numeric),
            Self::Float(value) => NumericKey::from_f32(*value).map(ComparisonKey::Numeric),
            Self::String(value) => Ok(ComparisonKey::String(value)),
            Self::Boolean(value) => Ok(ComparisonKey::Boolean(*value)),
            Self::Instant(value) => Ok(ComparisonKey::Instant(InstantKey {
                seconds: value.timestamp(),
                nanoseconds: value.timestamp_subsec_nanos(),
            })),
            _ => Err(ComparisonError::Unsupported),
        }
    }

    /// Compare two DataType values. Returns an error if the types are incompatible
    /// or if floats are NaN.
    pub fn partial_compare(&self, other: &DataType) -> Result<Ordering> {
        match (self.comparison_family(), other.comparison_family()) {
            (Some(left), Some(right)) if left == right => self
                .comparison_key()
                .map_err(anyhow::Error::new)?
                .partial_compare(&other.comparison_key().map_err(anyhow::Error::new)?)
                .map_err(anyhow::Error::new),
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
    RetractEntity(EntityRef),
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
/// - `[:db/retractEntity e]` → `TxOp::RetractEntity`
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
                (Some("db"), name @ ("retractEntity" | "erase")) => {
                    let entity = match iter.next() {
                        Some(v) => value_to_entity_ref(v)?,
                        None => anyhow::bail!("{} missing entity", op_kw),
                    };
                    if iter.next().is_some() {
                        anyhow::bail!("{} takes exactly 1 argument", op_kw);
                    }
                    Ok(if name == "retractEntity" {
                        TxOp::RetractEntity(entity)
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
        assert_eq!(
            DataType::Instant("2020-01-01T00:00:00Z".parse().unwrap())
                .partial_compare(&DataType::Instant("2021-01-01T00:00:00Z".parse().unwrap()))
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
    fn numeric_comparison_is_exact_across_representations() {
        use std::cmp::Ordering;

        assert_eq!(
            DataType::Long(16_777_217)
                .partial_compare(&DataType::Float(16_777_216.0))
                .unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            DataType::Long(9_007_199_254_740_993)
                .partial_compare(&DataType::Double(9_007_199_254_740_992.0))
                .unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            DataType::BigInt((1_i128 << 100) + 1)
                .partial_compare(&DataType::Double((1_u128 << 100) as f64))
                .unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            DataType::BigInt(i128::MIN)
                .partial_compare(&DataType::Double(-((1_u128 << 126) as f64)))
                .unwrap(),
            Ordering::Less
        );
        assert_eq!(
            DataType::BigInt(i128::MAX)
                .partial_compare(&DataType::Double((1_u128 << 126) as f64))
                .unwrap(),
            Ordering::Greater
        );
    }

    #[test]
    fn numeric_comparison_is_transitive_at_float_boundaries() {
        use std::cmp::Ordering;

        let lower = DataType::Float(16_777_216.0);
        let middle = DataType::Double(16_777_216.5);
        let upper = DataType::Long(16_777_217);

        assert_eq!(lower.partial_compare(&middle).unwrap(), Ordering::Less);
        assert_eq!(middle.partial_compare(&upper).unwrap(), Ordering::Less);
        assert_eq!(lower.partial_compare(&upper).unwrap(), Ordering::Less);
    }

    #[test]
    fn numeric_comparison_handles_special_non_nan_values() {
        use std::cmp::Ordering;

        assert_eq!(
            DataType::Double(-0.0)
                .partial_compare(&DataType::Long(0))
                .unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            DataType::Double(f64::from_bits(1))
                .partial_compare(&DataType::Long(0))
                .unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            DataType::Double(f64::NEG_INFINITY)
                .partial_compare(&DataType::BigInt(i128::MIN))
                .unwrap(),
            Ordering::Less
        );
        assert_eq!(
            DataType::Float(f32::INFINITY)
                .partial_compare(&DataType::Double(f64::MAX))
                .unwrap(),
            Ordering::Greater
        );
    }

    #[cfg(feature = "dbsp")]
    #[test]
    fn archived_numeric_key_order_matches_live_order() {
        let values = [
            DataType::Double(f64::NEG_INFINITY),
            DataType::BigInt(-((1_i128 << 100) + 1)),
            DataType::Double(-0.0),
            DataType::Double(f64::from_bits(1)),
            DataType::Double(9_007_199_254_740_992.0),
            DataType::Long(9_007_199_254_740_993),
            DataType::Double(f64::INFINITY),
        ];
        let keys = values
            .iter()
            .map(|value| value.comparison_key().unwrap().into_owned())
            .collect::<Vec<_>>();

        for pair in keys.windows(2) {
            let left = rkyv::to_bytes::<_, 256>(&pair[0]).unwrap();
            let right = rkyv::to_bytes::<_, 256>(&pair[1]).unwrap();
            let archived_left = unsafe { rkyv::archived_root::<OwnedComparisonKey>(&left) };
            let archived_right = unsafe { rkyv::archived_root::<OwnedComparisonKey>(&right) };

            assert_eq!(pair[0].cmp(&pair[1]), Ordering::Less);
            assert_eq!(archived_left.cmp(archived_right), Ordering::Less);
        }
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
    fn test_op_retract_entity_bincode() {
        let op = TxOp::RetractEntity(EntityRef::Id(1));
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
    fn test_parse_retract_entity() {
        let op: TxOp = "[:db/retractEntity 42]".parse().unwrap();
        assert_eq!(op, TxOp::RetractEntity(EntityRef::Id(42)));
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
        assert!("[:db/retractEntity 1 2]".parse::<TxOp>().is_err());
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
