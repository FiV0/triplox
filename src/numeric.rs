use anyhow::{anyhow, Result};

use crate::ops::DataType;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum NumericValue {
    Long(i64),
    Double(f64),
}

// The differences and reaon of being for `from_expression` and `from_aggregate` is the following:
// Expressions with incompatible types produces no results (tuples get filtered), hence the
// Option return value. On the other hand aggregates return an error when incompatible types
// are aggregated.
// TODO: Look into whether expressions should accept Float and BigInt like aggregates.

impl NumericValue {
    pub(crate) fn from_expression(value: &DataType) -> Option<Self> {
        match value {
            DataType::Long(value) => Some(Self::Long(*value)),
            DataType::Double(value) => Some(Self::Double(*value)),
            _ => None,
        }
    }

    pub(crate) fn from_aggregate(value: &DataType) -> Result<Self> {
        match value {
            DataType::Long(value) => Ok(Self::Long(*value)),
            DataType::Double(value) => Ok(Self::Double(*value)),
            DataType::Float(value) => Ok(Self::Double(*value as f64)),
            DataType::BigInt(value) => Ok(Self::Double(*value as f64)),
            other => Err(anyhow!(
                "cannot convert {:?} to numeric for aggregation",
                other
            )),
        }
    }

    pub(crate) fn as_f64(self) -> f64 {
        match self {
            Self::Long(value) => value as f64,
            Self::Double(value) => value,
        }
    }

    pub(crate) fn checked_add(self, other: Self) -> Option<Self> {
        match (self, other) {
            (Self::Long(left), Self::Long(right)) => left.checked_add(right).map(Self::Long),
            (left, right) => Some(Self::Double(left.as_f64() + right.as_f64())),
        }
    }

    pub(crate) fn checked_sub(self, other: Self) -> Option<Self> {
        match (self, other) {
            (Self::Long(left), Self::Long(right)) => left.checked_sub(right).map(Self::Long),
            (left, right) => Some(Self::Double(left.as_f64() - right.as_f64())),
        }
    }

    pub(crate) fn checked_mul(self, other: Self) -> Option<Self> {
        match (self, other) {
            (Self::Long(left), Self::Long(right)) => left.checked_mul(right).map(Self::Long),
            (left, right) => Some(Self::Double(left.as_f64() * right.as_f64())),
        }
    }

    pub(crate) fn checked_div(self, other: Self) -> Option<Self> {
        match (self, other) {
            (Self::Long(left), Self::Long(right)) => left.checked_div(right).map(Self::Long),
            (left, right) => {
                let right = right.as_f64();
                (right != 0.0).then(|| Self::Double(left.as_f64() / right))
            }
        }
    }

    pub(crate) fn checked_rem(self, other: Self) -> Option<Self> {
        match (self, other) {
            (Self::Long(left), Self::Long(right)) => left.checked_rem(right).map(Self::Long),
            (left, right) => {
                let right = right.as_f64();
                (right != 0.0).then(|| Self::Double(left.as_f64() % right))
            }
        }
    }

    pub(crate) fn into_data_type(self) -> DataType {
        match self {
            Self::Long(value) => DataType::Long(value),
            Self::Double(value) => DataType::Double(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expression_numeric_types_stay_narrow() {
        assert_eq!(
            NumericValue::from_expression(&DataType::Long(1)),
            Some(NumericValue::Long(1))
        );
        assert!(NumericValue::from_expression(&DataType::Float(1.0)).is_none());
    }

    #[test]
    fn aggregate_numeric_types_include_float_and_bigint() {
        assert_eq!(
            NumericValue::from_aggregate(&DataType::Float(1.5)).unwrap(),
            NumericValue::Double(1.5)
        );
        assert_eq!(
            NumericValue::from_aggregate(&DataType::BigInt(2)).unwrap(),
            NumericValue::Double(2.0)
        );
    }

    #[test]
    fn mixed_addition_promotes_to_double() {
        assert_eq!(
            NumericValue::Long(2).checked_add(NumericValue::Double(0.5)),
            Some(NumericValue::Double(2.5))
        );
    }
}
