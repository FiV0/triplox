use std::ops::{Add, AddAssign};

use anyhow::Result;
use dbsp::{
    algebra::{HasZero, MulByRef, F64},
    operator::{ConstantGenerator, Max, Min},
    typed_batch::OrdIndexedZSet,
    utils::Tup2,
    Circuit, OrdZSet, RootCircuit, Stream, ZWeight,
};

use crate::codec::{Decode, Encode};
use crate::incremental::EncodedRow;
use crate::numeric::NumericValue;
use crate::ops::{ComparisonError, ComparisonFamily, DataType, OwnedComparisonKey};
use crate::query::{AggregateFunc, FindPlan, Projection};

use super::{select_row_positions, OutputZSet, PlannedWhereStream};

// Grouped key -> Full row -> weight
type GroupedRows = Stream<RootCircuit, OrdIndexedZSet<EncodedRow, EncodedRow>>;
// Stream type of one aggregate expression
type AggregateStream = Stream<RootCircuit, OrdIndexedZSet<EncodedRow, AggregateOutput>>;
type ComparisonStream = Stream<RootCircuit, OrdIndexedZSet<EncodedRow, ComparisonValue>>;

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    size_of::SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    feldera_macros::IsNone,
    thiserror::Error,
)]
#[archive_attr(derive(Eq, PartialEq, Ord, PartialOrd))]
pub(in crate::incremental) enum AggregateError {
    #[error("sum: cannot aggregate non-numeric value")]
    SumNonNumeric,
    #[error("integer overflow in sum")]
    SumOverflow,
    #[error("cannot convert value to numeric for aggregation")]
    NonNumeric,
    #[error("aggregate value failed to decode")]
    Decode,
    #[error("cannot compare NaN values")]
    NanComparison,
    #[error("aggregate encountered uncomparable values")]
    UnsupportedComparison,
    #[error("aggregate encountered uncomparable values")]
    MixedComparison,
}

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    size_of::SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    feldera_macros::IsNone,
)]
#[archive_attr(derive(Eq, PartialEq, Ord, PartialOrd))]
pub(super) enum AggregateOutput {
    Value(Vec<u8>),
    Error(AggregateError),
}

impl Default for AggregateOutput {
    fn default() -> Self {
        Self::Value(Vec::new())
    }
}

pub(super) fn decode_aggregate_output(value: &AggregateOutput) -> Result<DataType> {
    match value {
        AggregateOutput::Value(value) => DataType::decode(value).map_err(anyhow::Error::from),
        AggregateOutput::Error(error) => Err(error.clone().into()),
    }
}

fn empty_global_aggregate(func: &AggregateFunc) -> Option<AggregateOutput> {
    match func {
        AggregateFunc::Count | AggregateFunc::CountDistinct | AggregateFunc::Sum => {
            Some(AggregateOutput::Value(DataType::Long(0).encode()))
        }
        AggregateFunc::Avg | AggregateFunc::Min | AggregateFunc::Max => None,
    }
}

#[derive(
    Clone,
    Debug,
    Default,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    size_of::SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    feldera_macros::IsNone,
)]
#[archive_attr(derive(Eq, PartialEq, Ord, PartialOrd))]
struct SumValue {
    long: i128,
    double: F64,
    non_long_count: i64,
}

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    size_of::SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    feldera_macros::IsNone,
)]
#[archive_attr(derive(Eq, PartialEq, Ord, PartialOrd))]
enum DbspSum {
    Value(SumValue),
    Error(AggregateError),
}

impl Default for DbspSum {
    fn default() -> Self {
        Self::Value(SumValue::default())
    }
}

impl DbspSum {
    fn from_encoded(value: &[u8]) -> Self {
        let Ok(value) = DataType::decode(value) else {
            return Self::Error(AggregateError::SumNonNumeric);
        };
        match NumericValue::from_aggregate(&value) {
            Ok(NumericValue::Long(value)) => Self::Value(SumValue {
                long: value as i128,
                ..SumValue::default()
            }),
            Ok(NumericValue::Double(value)) => Self::Value(SumValue {
                double: F64::new(value),
                non_long_count: 1,
                ..SumValue::default()
            }),
            Err(_) => Self::Error(AggregateError::SumNonNumeric),
        }
    }

    fn encode_result(&self) -> AggregateOutput {
        let value = match self {
            Self::Value(value) => value,
            Self::Error(error) => return AggregateOutput::Error(error.clone()),
        };
        if value.non_long_count != 0 {
            return AggregateOutput::Value(
                DataType::Double(value.long as f64 + value.double.into_inner()).encode(),
            );
        }
        match i64::try_from(value.long) {
            Ok(value) => AggregateOutput::Value(DataType::Long(value).encode()),
            Err(_) => AggregateOutput::Error(AggregateError::SumOverflow),
        }
    }
}

impl<'a> Add<&'a DbspSum> for &'a DbspSum {
    type Output = DbspSum;

    fn add(self, other: &'a DbspSum) -> Self::Output {
        match (self, other) {
            (DbspSum::Value(left), DbspSum::Value(right)) => DbspSum::Value(SumValue {
                long: left.long + right.long,
                double: left.double + right.double,
                non_long_count: left.non_long_count + right.non_long_count,
            }),
            (DbspSum::Error(left), DbspSum::Error(right)) => {
                DbspSum::Error(left.min(right).clone())
            }
            (DbspSum::Error(error), DbspSum::Value(_))
            | (DbspSum::Value(_), DbspSum::Error(error)) => DbspSum::Error(error.clone()),
        }
    }
}

impl AddAssign<&DbspSum> for DbspSum {
    fn add_assign(&mut self, other: &DbspSum) {
        *self = &*self + other;
    }
}

impl HasZero for DbspSum {
    fn is_zero(&self) -> bool {
        matches!(self, Self::Value(value) if value == &SumValue::default())
    }

    fn zero() -> Self {
        Self::default()
    }
}

impl MulByRef<ZWeight> for DbspSum {
    type Output = Self;

    fn mul_by_ref(&self, weight: &ZWeight) -> Self::Output {
        if *weight == 0 {
            return Self::default();
        }
        match self {
            Self::Value(value) => Self::Value(SumValue {
                long: value.long * *weight as i128,
                double: value.double * F64::new(*weight as f64),
                non_long_count: value.non_long_count * *weight,
            }),
            Self::Error(error) => Self::Error(error.clone()),
        }
    }
}

#[derive(
    Clone,
    Debug,
    Default,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    size_of::SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    feldera_macros::IsNone,
)]
#[archive_attr(derive(Eq, PartialEq, Ord, PartialOrd))]
struct AverageValue {
    sum: F64,
    count: i64,
}

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    size_of::SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    feldera_macros::IsNone,
)]
#[archive_attr(derive(Eq, PartialEq, Ord, PartialOrd))]
enum DbspAverage {
    Value(AverageValue),
    Error(AggregateError),
}

impl Default for DbspAverage {
    fn default() -> Self {
        Self::Value(AverageValue::default())
    }
}

impl DbspAverage {
    fn from_encoded(value: &[u8]) -> Self {
        let Ok(value) = DataType::decode(value) else {
            return Self::Error(AggregateError::NonNumeric);
        };
        match NumericValue::from_aggregate(&value) {
            Ok(value) => Self::Value(AverageValue {
                sum: F64::new(value.as_f64()),
                count: 1,
            }),
            Err(_) => Self::Error(AggregateError::NonNumeric),
        }
    }

    fn encode_result(&self) -> AggregateOutput {
        match self {
            Self::Value(value) => AggregateOutput::Value(
                DataType::Double(value.sum.into_inner() / value.count as f64).encode(),
            ),
            Self::Error(error) => AggregateOutput::Error(error.clone()),
        }
    }
}

impl<'a> Add<&'a DbspAverage> for &'a DbspAverage {
    type Output = DbspAverage;

    fn add(self, other: &'a DbspAverage) -> Self::Output {
        match (self, other) {
            (DbspAverage::Value(left), DbspAverage::Value(right)) => {
                DbspAverage::Value(AverageValue {
                    sum: left.sum + right.sum,
                    count: left.count + right.count,
                })
            }
            (DbspAverage::Error(left), DbspAverage::Error(right)) => {
                DbspAverage::Error(left.min(right).clone())
            }
            (DbspAverage::Error(error), DbspAverage::Value(_))
            | (DbspAverage::Value(_), DbspAverage::Error(error)) => {
                DbspAverage::Error(error.clone())
            }
        }
    }
}

impl AddAssign<&DbspAverage> for DbspAverage {
    fn add_assign(&mut self, other: &DbspAverage) {
        *self = &*self + other;
    }
}

impl HasZero for DbspAverage {
    fn is_zero(&self) -> bool {
        matches!(self, Self::Value(value) if value == &AverageValue::default())
    }

    fn zero() -> Self {
        Self::default()
    }
}

impl MulByRef<ZWeight> for DbspAverage {
    type Output = Self;

    fn mul_by_ref(&self, weight: &ZWeight) -> Self::Output {
        if *weight == 0 {
            return Self::default();
        }
        match self {
            Self::Value(value) => Self::Value(AverageValue {
                sum: value.sum * F64::new(*weight as f64),
                count: value.count * *weight,
            }),
            Self::Error(error) => Self::Error(error.clone()),
        }
    }
}

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    size_of::SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    feldera_macros::IsNone,
)]
#[archive_attr(derive(Eq, PartialEq, Ord, PartialOrd))]
enum ComparisonValue {
    Invalid(AggregateError),
    Valid {
        key: OwnedComparisonKey,
        encoded: Vec<u8>,
    },
}

impl Default for ComparisonValue {
    fn default() -> Self {
        Self::Invalid(AggregateError::Decode)
    }
}

impl ComparisonValue {
    fn from_encoded(encoded: &[u8]) -> Self {
        let Ok(value) = DataType::decode(encoded) else {
            return Self::Invalid(AggregateError::Decode);
        };
        let key = match value.comparison_key() {
            Ok(key) => key.into_owned(),
            Err(ComparisonError::Nan) => {
                return Self::Invalid(AggregateError::NanComparison);
            }
            Err(ComparisonError::Unsupported) => {
                return Self::Invalid(AggregateError::UnsupportedComparison);
            }
            Err(ComparisonError::MixedFamilies) => {
                return Self::Invalid(AggregateError::MixedComparison);
            }
        };
        Self::Valid {
            key,
            encoded: encoded.to_vec(),
        }
    }

    fn class(&self) -> ComparisonClass {
        match self {
            Self::Invalid(error) => ComparisonClass::Error(error.clone()),
            Self::Valid { key, .. } => ComparisonClass::Family(key.family()),
        }
    }

    fn encode_result(&self) -> AggregateOutput {
        match self {
            Self::Invalid(error) => AggregateOutput::Error(error.clone()),
            Self::Valid { encoded, .. } => AggregateOutput::Value(encoded.clone()),
        }
    }
}

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    size_of::SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    feldera_macros::IsNone,
)]
#[archive_attr(derive(Eq, PartialEq, Ord, PartialOrd))]
enum ComparisonClass {
    Error(AggregateError),
    Family(ComparisonFamily),
}

impl Default for ComparisonClass {
    fn default() -> Self {
        Self::Error(AggregateError::Decode)
    }
}

// DBSP min/max cannot synthesize mixed-family errors, so validate the converted values separately.
// See #474 for options that could collapse this into one aggregate operation.
fn validate_comparable_aggregate(
    values: &ComparisonStream,
    aggregate: AggregateStream,
) -> AggregateStream {
    let classes = values.map_index(|(group, value)| (group.clone(), value.class()));
    let first_class = classes.aggregate(Min);
    let class_count = classes.distinct_count();
    let validation = first_class.join_index(&class_count, |group, class, class_count| {
        let error = match class {
            ComparisonClass::Error(error) => Some(error.clone()),
            ComparisonClass::Family(_) if *class_count == 1 => None,
            ComparisonClass::Family(_) => Some(AggregateError::MixedComparison),
        };
        Some((group.clone(), error))
    });
    aggregate.join_index(&validation, |group, value, error| {
        let value = match error {
            Some(error) => AggregateOutput::Error(error.clone()),
            None => value.clone(),
        };
        Some((group.clone(), value))
    })
}

fn aggregate_stream(
    grouped: &GroupedRows,
    func: &AggregateFunc,
    input_position: usize,
) -> AggregateStream {
    match func {
        AggregateFunc::Count => grouped.weighted_count().map_index(|(group, count)| {
            (
                group.clone(),
                AggregateOutput::Value(DataType::Long(*count).encode()),
            )
        }),

        AggregateFunc::CountDistinct => grouped
            .map_index(move |(group, row)| (group.clone(), row[input_position].clone()))
            .distinct_count()
            .map_index(|(group, count)| {
                (
                    group.clone(),
                    AggregateOutput::Value(DataType::Long(*count).encode()),
                )
            }),

        AggregateFunc::Sum => grouped
            .map_index(move |(group, row)| {
                (group.clone(), DbspSum::from_encoded(&row[input_position]))
            })
            .aggregate_linear(|value| value.clone())
            .map_index(|(group, sum)| (group.clone(), sum.encode_result())),

        AggregateFunc::Avg => grouped
            .map_index(move |(group, row)| {
                (
                    group.clone(),
                    DbspAverage::from_encoded(&row[input_position]),
                )
            })
            .aggregate_linear(|value| value.clone())
            .map_index(|(group, average)| (group.clone(), average.encode_result())),

        AggregateFunc::Min => {
            let values = grouped.map_index(move |(group, row)| {
                (
                    group.clone(),
                    ComparisonValue::from_encoded(&row[input_position]),
                )
            });
            let minimum = values
                .aggregate(Min)
                .map_index(|(group, minimum)| (group.clone(), minimum.encode_result()));
            validate_comparable_aggregate(&values, minimum)
        }

        AggregateFunc::Max => {
            let values = grouped.map_index(move |(group, row)| {
                (
                    group.clone(),
                    ComparisonValue::from_encoded(&row[input_position]),
                )
            });
            let maximum = values
                .aggregate(Max)
                .map_index(|(group, maximum)| (group.clone(), maximum.encode_result()));
            validate_comparable_aggregate(&values, maximum)
        }
    }
}

pub(super) fn aggregate_find_stream(
    where_stream: PlannedWhereStream,
    plan: &FindPlan,
) -> Stream<RootCircuit, OutputZSet> {
    // Group complete input rows by the variables selected as the aggregate key.
    let group_positions = plan.group_key_indices.clone();
    let grouped = where_stream
        .stream
        .map_index(move |row| (select_row_positions(row, &group_positions), row.clone()));

    // Build one independent result stream for each aggregate projection.
    // Aggregate results are kept in the same order as they appear in plan.projections.
    let aggregate_streams = plan
        .projections
        .iter()
        .filter_map(|projection| match projection {
            Projection::Aggregate(func, input_position) => Some((
                func.clone(),
                aggregate_stream(&grouped, func, *input_position),
            )),
            Projection::GroupVar(_) => None,
        })
        .collect::<Vec<_>>();

    let combined = if plan.group_key_indices.is_empty() {
        // Seed the empty group so global aggregates produce a row for empty input.
        let singleton = OrdZSet::from_keys((), vec![Tup2(Vec::<Vec<u8>>::new(), 1)]);
        let mut combined = grouped
            .circuit()
            .add_source(ConstantGenerator::new(singleton))
            // The differentiate assures there are no changes after the first tick.
            .differentiate()
            .map_index(|group| (group.clone(), Vec::<AggregateOutput>::new()));
        for (func, aggregate) in aggregate_streams {
            let aggregate =
                aggregate.map_index(|(group, value)| (group.clone(), Some(value.clone())));
            // This needs to be a left_join because an empty global result has no group key. With left_join the left group is preserved.
            combined = combined.left_join_index(&aggregate, move |group, values, value| {
                let mut output = values.clone();
                output.push(value.clone().or_else(|| empty_global_aggregate(&func))?);
                Some((group.clone(), output))
            });
        }
        combined
    } else {
        // Join per-group aggregate streams into one vector of results.
        let mut streams = aggregate_streams.into_iter().map(|(_, stream)| stream);
        let first = streams
            .next()
            .expect("aggregate find plan must contain an aggregate");
        // Results remain indexed, because the group-key becomes important below.
        streams.fold(
            first.map_index(|(group, value)| (group.clone(), vec![value.clone()])),
            |combined, aggregate| {
                // This always triggers a join as the group keys *must* exist on both sides.
                combined.join_index(&aggregate, |group, values, value| {
                    let mut output = values.clone();
                    output.push(value.clone());
                    Some((group.clone(), output))
                })
            },
        )
    };

    // Restore group variables and aggregate results to find-clause order.
    let projections = plan.projections.clone();
    combined.map(move |(group, aggregate_values)| {
        let mut aggregate_position = 0;
        projections
            .iter()
            .map(|projection| match projection {
                Projection::GroupVar(group_position) => {
                    AggregateOutput::Value(group[*group_position].clone())
                }
                Projection::Aggregate(_, _) => {
                    let value = aggregate_values[aggregate_position].clone();
                    aggregate_position += 1;
                    value
                }
            })
            .collect()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_finalizers_return_typed_errors() {
        assert_eq!(
            DbspSum::Error(AggregateError::SumNonNumeric).encode_result(),
            AggregateOutput::Error(AggregateError::SumNonNumeric)
        );
        assert_eq!(
            DbspSum::Value(SumValue {
                long: i64::MAX as i128 + 1,
                ..SumValue::default()
            })
            .encode_result(),
            AggregateOutput::Error(AggregateError::SumOverflow)
        );
        assert_eq!(
            DbspAverage::Error(AggregateError::NonNumeric).encode_result(),
            AggregateOutput::Error(AggregateError::NonNumeric)
        );
        assert_eq!(
            ComparisonValue::from_encoded(&DataType::Double(f64::NAN).encode()).encode_result(),
            AggregateOutput::Error(AggregateError::NanComparison)
        );
        assert_eq!(
            ComparisonValue::from_encoded(&DataType::Bytes(vec![1]).encode()).encode_result(),
            AggregateOutput::Error(AggregateError::UnsupportedComparison)
        );
        assert_eq!(
            ComparisonValue::from_encoded(&[0xff]).encode_result(),
            AggregateOutput::Error(AggregateError::Decode)
        );
    }

    #[test]
    fn sum_error_is_terminal_aggregate_state() {
        let error = DbspSum::from_encoded(&DataType::String("bad".to_string()).encode());
        let value = DbspSum::from_encoded(&DataType::Long(10).encode());

        assert_eq!(
            (&error + &value).encode_result(),
            AggregateOutput::Error(AggregateError::SumNonNumeric)
        );
        assert_eq!(
            error.mul_by_ref(&-1).encode_result(),
            AggregateOutput::Error(AggregateError::SumNonNumeric)
        );
        assert!(error.mul_by_ref(&0).is_zero());
    }

    #[test]
    fn average_error_is_terminal_aggregate_state() {
        let error = DbspAverage::from_encoded(&DataType::String("bad".to_string()).encode());
        let value = DbspAverage::from_encoded(&DataType::Long(10).encode());

        assert_eq!(
            (&error + &value).encode_result(),
            AggregateOutput::Error(AggregateError::NonNumeric)
        );
        assert_eq!(
            error.mul_by_ref(&-1).encode_result(),
            AggregateOutput::Error(AggregateError::NonNumeric)
        );
        assert!(error.mul_by_ref(&0).is_zero());
    }
}
