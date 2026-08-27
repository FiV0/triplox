use std::ops::{Add, AddAssign, Div, Neg};

use anyhow::Result;
use dbsp::{
    algebra::{HasZero, MulByRef, F64},
    dynamic::{ClonableTrait, DowncastTrait, DynData, DynWeight},
    operator::dynamic::aggregate::AvgFactories,
    operator::{ConstantGenerator, Max, Min},
    typed_batch::OrdIndexedZSet,
    utils::Tup2,
    Circuit, OrdZSet, RootCircuit, Stream, ZWeight,
};

use crate::codec::{Decode, Encode};
use crate::incremental::EncodedRow;
use crate::numeric::NumericValue;
use crate::ops::DataType;
use crate::query::{AggregateFunc, FindPlan, Projection};

use super::{select_row_positions, OutputZSet, PlannedWhereStream};

// Grouped key -> Full row -> weight
type GroupedRows = Stream<RootCircuit, OrdIndexedZSet<EncodedRow, EncodedRow>>;
// Stream type of one aggregate expression
type AggregateStream = Stream<RootCircuit, OrdIndexedZSet<EncodedRow, AggregateOutput>>;

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
    #[error("min/max cannot compare aggregate value")]
    UnsupportedComparison,
    #[error("min/max cannot compare aggregate values")]
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
struct DbspSum {
    long: i128,
    double: F64,
    non_long_count: i64,
    error_count: i64,
}

impl DbspSum {
    fn from_encoded(value: &[u8]) -> Self {
        let Ok(value) = DataType::decode(value) else {
            return Self {
                error_count: 1,
                ..Self::default()
            };
        };
        match NumericValue::from_aggregate(&value) {
            Ok(NumericValue::Long(value)) => Self {
                long: value as i128,
                ..Self::default()
            },
            Ok(NumericValue::Double(value)) => Self {
                double: F64::new(value),
                non_long_count: 1,
                ..Self::default()
            },
            Err(_) => Self {
                error_count: 1,
                ..Self::default()
            },
        }
    }

    fn encode_result(&self) -> AggregateOutput {
        if self.error_count != 0 {
            return AggregateOutput::Error(AggregateError::SumNonNumeric);
        }
        if self.non_long_count != 0 {
            return AggregateOutput::Value(
                DataType::Double(self.long as f64 + self.double.into_inner()).encode(),
            );
        }
        match i64::try_from(self.long) {
            Ok(value) => AggregateOutput::Value(DataType::Long(value).encode()),
            Err(_) => AggregateOutput::Error(AggregateError::SumOverflow),
        }
    }
}

impl<'a> Add<&'a DbspSum> for &'a DbspSum {
    type Output = DbspSum;

    fn add(self, other: &'a DbspSum) -> Self::Output {
        DbspSum {
            long: self.long + other.long,
            double: self.double + other.double,
            non_long_count: self.non_long_count + other.non_long_count,
            error_count: self.error_count + other.error_count,
        }
    }
}

impl AddAssign<&DbspSum> for DbspSum {
    fn add_assign(&mut self, other: &DbspSum) {
        self.long += other.long;
        self.double += other.double;
        self.non_long_count += other.non_long_count;
        self.error_count += other.error_count;
    }
}

impl HasZero for DbspSum {
    fn is_zero(&self) -> bool {
        self == &Self::default()
    }

    fn zero() -> Self {
        Self::default()
    }
}

impl MulByRef<ZWeight> for DbspSum {
    type Output = Self;

    fn mul_by_ref(&self, weight: &ZWeight) -> Self::Output {
        Self {
            long: self.long * *weight as i128,
            double: self.double * F64::new(*weight as f64),
            non_long_count: self.non_long_count * *weight,
            error_count: self.error_count * *weight,
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
struct DbspAverage {
    value: F64,
    error_count: i64,
}

impl DbspAverage {
    fn from_encoded(value: &[u8]) -> Self {
        let Ok(value) = DataType::decode(value) else {
            return Self {
                error_count: 1,
                ..Self::default()
            };
        };
        match NumericValue::from_aggregate(&value) {
            Ok(value) => Self {
                value: F64::new(value.as_f64()),
                error_count: 0,
            },
            Err(_) => Self {
                error_count: 1,
                ..Self::default()
            },
        }
    }

    fn encode_result(&self) -> AggregateOutput {
        if self.error_count != 0 {
            AggregateOutput::Error(AggregateError::NonNumeric)
        } else {
            AggregateOutput::Value(DataType::Double(self.value.into_inner()).encode())
        }
    }
}

impl From<ZWeight> for DbspAverage {
    fn from(value: ZWeight) -> Self {
        Self {
            value: F64::new(value as f64),
            error_count: 0,
        }
    }
}

impl<'a> Add<&'a DbspAverage> for &'a DbspAverage {
    type Output = DbspAverage;

    fn add(self, other: &'a DbspAverage) -> Self::Output {
        DbspAverage {
            value: self.value + other.value,
            error_count: self.error_count + other.error_count,
        }
    }
}

impl AddAssign<&DbspAverage> for DbspAverage {
    fn add_assign(&mut self, other: &DbspAverage) {
        self.value += other.value;
        self.error_count += other.error_count;
    }
}

impl HasZero for DbspAverage {
    fn is_zero(&self) -> bool {
        self == &Self::default()
    }

    fn zero() -> Self {
        Self::default()
    }
}

impl Neg for DbspAverage {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self {
            value: -self.value,
            error_count: -self.error_count,
        }
    }
}

impl Neg for &DbspAverage {
    type Output = DbspAverage;

    fn neg(self) -> Self::Output {
        self.clone().neg()
    }
}

impl MulByRef<ZWeight> for DbspAverage {
    type Output = Self;

    fn mul_by_ref(&self, weight: &ZWeight) -> Self::Output {
        Self {
            value: self.value * F64::new(*weight as f64),
            error_count: self.error_count * *weight,
        }
    }
}

impl Div for DbspAverage {
    type Output = Self;

    fn div(self, denominator: Self) -> Self::Output {
        Self {
            value: self.value / denominator.value,
            error_count: self.error_count,
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
struct ComparableValue {
    rank: u8,
    sort_key: Vec<u8>,
    encoded: Vec<u8>,
    error: Option<AggregateError>,
}

impl ComparableValue {
    fn from_encoded(encoded: &[u8], minimum: bool) -> Self {
        let Ok(value) = DataType::decode(encoded) else {
            return Self::invalid(encoded, minimum, AggregateError::Decode);
        };
        let (family, sort_key) = match &value {
            DataType::Long(_) | DataType::Double(_) | DataType::Float(_) | DataType::BigInt(_) => {
                let numeric = NumericValue::from_aggregate(&value).expect("numeric value");
                if numeric.as_f64().is_nan() {
                    return Self::invalid(encoded, minimum, AggregateError::NanComparison);
                }
                (0, DataType::Double(numeric.as_f64()).encode())
            }
            DataType::String(_) => (1, encoded.to_vec()),
            DataType::Boolean(_) => (2, encoded.to_vec()),
            DataType::Instant(_) => (3, encoded.to_vec()),
            _ => return Self::invalid(encoded, minimum, AggregateError::UnsupportedComparison),
        };
        Self {
            rank: if minimum { family + 1 } else { family },
            sort_key,
            encoded: encoded.to_vec(),
            error: None,
        }
    }

    fn invalid(encoded: &[u8], minimum: bool, error: AggregateError) -> Self {
        Self {
            rank: if minimum { 0 } else { u8::MAX },
            sort_key: Vec::new(),
            encoded: encoded.to_vec(),
            error: Some(error),
        }
    }

    fn encode_result(&self) -> AggregateOutput {
        match &self.error {
            Some(error) => AggregateOutput::Error(error.clone()),
            None => AggregateOutput::Value(self.encoded.clone()),
        }
    }
}

fn comparable_family(encoded: &[u8]) -> u8 {
    match DataType::decode(encoded) {
        Ok(DataType::Long(_) | DataType::BigInt(_)) => 1,
        Ok(DataType::Double(value)) if !value.is_nan() => 1,
        Ok(DataType::Float(value)) if !value.is_nan() => 1,
        Ok(DataType::String(_)) => 2,
        Ok(DataType::Boolean(_)) => 3,
        Ok(DataType::Instant(_)) => 4,
        _ => u8::MAX,
    }
}

// DBSP's typed average wrapper fixes custom sum storage to ZWeight.
fn average_data_type(
    values: &Stream<RootCircuit, OrdIndexedZSet<EncodedRow, Vec<u8>>>,
) -> Stream<RootCircuit, OrdIndexedZSet<EncodedRow, DbspAverage>> {
    let factories: AvgFactories<_, DynData, DynWeight, _> =
        AvgFactories::new::<EncodedRow, DbspAverage, DbspAverage>();
    values
        .inner()
        .dyn_average::<DynData, DynWeight>(
            None,
            &factories,
            Box::new(|_key, value, weight, sum| unsafe {
                *sum.downcast_mut::<DbspAverage>() =
                    DbspAverage::from_encoded(value.downcast::<Vec<u8>>())
                        .mul_by_ref(weight.downcast::<ZWeight>());
            }),
            Box::new(|value, output| value.as_data_mut().move_to(output)),
        )
        .typed()
}

fn validate_comparable_aggregate(
    grouped: &GroupedRows,
    aggregate: AggregateStream,
    input_position: usize,
) -> AggregateStream {
    let family_count = grouped
        .map_index(move |(group, row)| (group.clone(), comparable_family(&row[input_position])))
        .distinct_count();
    aggregate.join_index(&family_count, |group, value, family_count| {
        let value = if *family_count == 1 {
            value.clone()
        } else {
            AggregateOutput::Error(AggregateError::MixedComparison)
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
        AggregateFunc::Avg => average_data_type(
            &grouped.map_index(move |(group, row)| (group.clone(), row[input_position].clone())),
        )
        .map_index(|(group, average)| (group.clone(), average.encode_result())),
        AggregateFunc::Min => {
            let values = grouped.map_index(move |(group, row)| {
                (
                    group.clone(),
                    ComparableValue::from_encoded(&row[input_position], true),
                )
            });
            let minimum = values
                .aggregate(Min)
                .map_index(|(group, minimum)| (group.clone(), minimum.encode_result()));
            validate_comparable_aggregate(grouped, minimum, input_position)
        }
        AggregateFunc::Max => {
            let values = grouped.map_index(move |(group, row)| {
                (
                    group.clone(),
                    ComparableValue::from_encoded(&row[input_position], false),
                )
            });
            let maximum = values
                .aggregate(Max)
                .map_index(|(group, maximum)| (group.clone(), maximum.encode_result()));
            validate_comparable_aggregate(grouped, maximum, input_position)
        }
    }
}

pub(super) fn aggregate_find_stream(
    where_stream: PlannedWhereStream,
    plan: &FindPlan,
) -> Stream<RootCircuit, OutputZSet> {
    let group_positions = plan.group_key_indices.clone();
    let grouped = where_stream
        .stream
        .map_index(move |row| (select_row_positions(row, &group_positions), row.clone()));
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
        let singleton = OrdZSet::from_keys((), vec![Tup2(Vec::<Vec<u8>>::new(), 1)]);
        let mut combined = grouped
            .circuit()
            .add_source(ConstantGenerator::new(singleton))
            .differentiate()
            .map_index(|group| (group.clone(), Vec::<AggregateOutput>::new()));
        for (func, aggregate) in aggregate_streams {
            let aggregate =
                aggregate.map_index(|(group, value)| (group.clone(), Some(value.clone())));
            combined = combined.left_join_index(&aggregate, move |group, values, value| {
                let mut output = values.clone();
                output.push(value.clone().or_else(|| empty_global_aggregate(&func))?);
                Some((group.clone(), output))
            });
        }
        combined
    } else {
        let mut streams = aggregate_streams.into_iter().map(|(_, stream)| stream);
        let first = streams
            .next()
            .expect("aggregate find plan must contain an aggregate");
        streams.fold(
            first.map_index(|(group, value)| (group.clone(), vec![value.clone()])),
            |combined, aggregate| {
                combined.join_index(&aggregate, |group, values, value| {
                    let mut output = values.clone();
                    output.push(value.clone());
                    Some((group.clone(), output))
                })
            },
        )
    };
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
            DbspSum {
                error_count: 1,
                ..DbspSum::default()
            }
            .encode_result(),
            AggregateOutput::Error(AggregateError::SumNonNumeric)
        );
        assert_eq!(
            DbspSum {
                long: i64::MAX as i128 + 1,
                ..DbspSum::default()
            }
            .encode_result(),
            AggregateOutput::Error(AggregateError::SumOverflow)
        );
        assert_eq!(
            DbspAverage {
                error_count: 1,
                ..DbspAverage::default()
            }
            .encode_result(),
            AggregateOutput::Error(AggregateError::NonNumeric)
        );
        assert_eq!(
            ComparableValue::from_encoded(&DataType::Double(f64::NAN).encode(), true)
                .encode_result(),
            AggregateOutput::Error(AggregateError::NanComparison)
        );
    }
}
