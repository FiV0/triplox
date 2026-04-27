use anyhow::Result;
#[allow(unused_imports)]
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use edn::symbols::Keyword;
#[allow(unused_imports)]
use edn::symbols::NamespacedSymbol;
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use std::collections::{BTreeMap, BTreeSet};
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

// TODO maybe use also clock::Instant here
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum DataType {
    //BigDecimal(BigDecimal),          // Arbitrary precision decimal numbers
    BigInt(i128),  // Arbitrary large integers
    Boolean(bool), // Booleans (true or false)
    // TODO: use Bytes instead of Vec<u8> ?
    Bytes(Vec<u8>),         // Binary data (as bytes)
    Double(f64),            // Double precision floating point
    Float(f32),             // Single precision floating point
    Instant(DateTime<Utc>), // Timestamps or instants
    Keyword(Keyword),       // Keywords
    Long(i64),              // Long integers (also used for Ref values; see ValueType::Ref)
    String(String),         // Strings
    // Symbol(NamespacedSymbol),                  // Symbols (can be represented as strings)
    Uuid(Uuid), // Universally unique identifier
    // TODO
    //Uri(Uri),                        // URIs (could also be represented as strings)

    // Composite types
    Vector(Vec<DataType>), // List (vector of DataTypes)
    // TODO think about tradeoffs of using a BTreeSet vs a HashSet
    //Set(BTreeSet<DataType>),         // Set (BTreeSet of DataTypes)
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
    /// Return the ValueType corresponding to this DataType's variant.
    pub fn value_type(&self) -> crate::schema::ValueType {
        use crate::schema::ValueType;
        match self {
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
                "cannot compare {:?} with {:?}",
                self.value_type(),
                other.value_type()
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
    //(Set, BTreeSet<DataType>),
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
    pub fn put(attrs: Vec<(Keyword, DataType)>) -> Self {
        TxOp::Put(attrs.into_iter().collect())
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DatomOp {
    Assert,
    Retract,
}

/// A normalized fact: (entity, attribute, value, op).
/// The attribute is an unresolved keyword ident.
/// The tx_eid is not stored here — it's passed separately to write_index_entries.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Datom {
    pub entity: i64,
    pub attribute: Keyword,
    pub value: DataType,
    pub op: DatomOp,
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
        let op = TxOp::put(vec![
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
}
