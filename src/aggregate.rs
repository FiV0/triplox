use std::collections::HashSet;

use anyhow::{anyhow, Result};

use crate::numeric::NumericValue;
use crate::ops::DataType;
use crate::query::AggregateFunc;

// TODO: use schema type info and type promotion rules (like Postgres) instead of
// eager f64 conversion — e.g. avg over integers should accumulate as (i64 sum, i64 count).

/// Trait for aggregate accumulators.
pub(crate) trait Accumulator {
    fn accumulate(&mut self, value: &DataType) -> Result<()>;
    fn finalize(&self) -> Result<DataType>;
}

struct CountAccumulator {
    count: i64,
}

impl Accumulator for CountAccumulator {
    fn accumulate(&mut self, _value: &DataType) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn finalize(&self) -> Result<DataType> {
        Ok(DataType::Long(self.count))
    }
}

struct CountDistinctAccumulator {
    seen: HashSet<Vec<u8>>,
}

impl Accumulator for CountDistinctAccumulator {
    fn accumulate(&mut self, value: &DataType) -> Result<()> {
        let bytes = bincode::serialize(value)?;
        self.seen.insert(bytes);
        Ok(())
    }

    fn finalize(&self) -> Result<DataType> {
        Ok(DataType::Long(self.seen.len() as i64))
    }
}

struct SumAccumulator {
    accum: Option<NumericValue>,
}

impl Accumulator for SumAccumulator {
    fn accumulate(&mut self, value: &DataType) -> Result<()> {
        let value = NumericValue::from_aggregate(value)
            .map_err(|_| anyhow!("sum: cannot aggregate non-numeric value {:?}", value))?;
        self.accum = Some(match self.accum {
            Some(accum) => accum
                .checked_add(value)
                .ok_or_else(|| anyhow!("integer overflow in sum"))?,
            None => value,
        });
        Ok(())
    }

    fn finalize(&self) -> Result<DataType> {
        match &self.accum {
            Some(value) => Ok(value.into_data_type()),
            None => Ok(DataType::Long(0)),
        }
    }
}

struct AvgAccumulator {
    sum: f64,
    count: i64,
}

impl Accumulator for AvgAccumulator {
    fn accumulate(&mut self, value: &DataType) -> Result<()> {
        self.sum += NumericValue::from_aggregate(value)?.as_f64();
        self.count += 1;
        Ok(())
    }

    fn finalize(&self) -> Result<DataType> {
        if self.count == 0 {
            return Err(anyhow!("avg: no values accumulated"));
        }
        Ok(DataType::Double(self.sum / self.count as f64))
    }
}

struct MinAccumulator {
    current: Option<DataType>,
}

impl Accumulator for MinAccumulator {
    fn accumulate(&mut self, value: &DataType) -> Result<()> {
        match &self.current {
            None => {
                self.current = Some(value.clone());
            }
            Some(current) => {
                let ord = current.partial_compare(value)?;
                if ord == std::cmp::Ordering::Greater {
                    self.current = Some(value.clone());
                }
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Result<DataType> {
        self.current
            .clone()
            .ok_or_else(|| anyhow!("min: no values accumulated"))
    }
}

struct MaxAccumulator {
    current: Option<DataType>,
}

impl Accumulator for MaxAccumulator {
    fn accumulate(&mut self, value: &DataType) -> Result<()> {
        match &self.current {
            None => {
                self.current = Some(value.clone());
            }
            Some(current) => {
                let ord = current.partial_compare(value)?;
                if ord == std::cmp::Ordering::Less {
                    self.current = Some(value.clone());
                }
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Result<DataType> {
        self.current
            .clone()
            .ok_or_else(|| anyhow!("max: no values accumulated"))
    }
}

pub(crate) fn make_accumulator(func: &AggregateFunc) -> Box<dyn Accumulator> {
    match func {
        AggregateFunc::Count => Box::new(CountAccumulator { count: 0 }),
        AggregateFunc::CountDistinct => Box::new(CountDistinctAccumulator {
            seen: HashSet::new(),
        }),
        AggregateFunc::Sum => Box::new(SumAccumulator { accum: None }),
        AggregateFunc::Avg => Box::new(AvgAccumulator { sum: 0.0, count: 0 }),
        AggregateFunc::Min => Box::new(MinAccumulator { current: None }),
        AggregateFunc::Max => Box::new(MaxAccumulator { current: None }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count() {
        let mut acc = make_accumulator(&AggregateFunc::Count);
        acc.accumulate(&DataType::Long(1)).unwrap();
        acc.accumulate(&DataType::Long(2)).unwrap();
        acc.accumulate(&DataType::Long(3)).unwrap();
        assert_eq!(acc.finalize().unwrap(), DataType::Long(3));
    }

    #[test]
    fn test_count_distinct() {
        let mut acc = make_accumulator(&AggregateFunc::CountDistinct);
        acc.accumulate(&DataType::Long(1)).unwrap();
        acc.accumulate(&DataType::Long(2)).unwrap();
        acc.accumulate(&DataType::Long(1)).unwrap();
        acc.accumulate(&DataType::Long(3)).unwrap();
        assert_eq!(acc.finalize().unwrap(), DataType::Long(3));
    }

    #[test]
    fn test_sum_long() {
        let mut acc = make_accumulator(&AggregateFunc::Sum);
        acc.accumulate(&DataType::Long(10)).unwrap();
        acc.accumulate(&DataType::Long(20)).unwrap();
        acc.accumulate(&DataType::Long(30)).unwrap();
        assert_eq!(acc.finalize().unwrap(), DataType::Long(60));
    }

    #[test]
    fn test_sum_double() {
        let mut acc = make_accumulator(&AggregateFunc::Sum);
        acc.accumulate(&DataType::Double(1.5)).unwrap();
        acc.accumulate(&DataType::Double(2.5)).unwrap();
        assert_eq!(acc.finalize().unwrap(), DataType::Double(4.0));
    }

    #[test]
    fn test_sum_mixed_promotes_to_double() {
        let mut acc = make_accumulator(&AggregateFunc::Sum);
        acc.accumulate(&DataType::Long(10)).unwrap();
        acc.accumulate(&DataType::Double(1.5)).unwrap();
        assert_eq!(acc.finalize().unwrap(), DataType::Double(11.5));
    }

    #[test]
    fn test_sum_non_numeric_error() {
        let mut acc = make_accumulator(&AggregateFunc::Sum);
        let result = acc.accumulate(&DataType::String("hello".into()));
        assert!(result.is_err());
    }

    #[test]
    fn test_avg() {
        let mut acc = make_accumulator(&AggregateFunc::Avg);
        acc.accumulate(&DataType::Long(10)).unwrap();
        acc.accumulate(&DataType::Long(20)).unwrap();
        acc.accumulate(&DataType::Long(30)).unwrap();
        assert_eq!(acc.finalize().unwrap(), DataType::Double(20.0));
    }

    #[test]
    fn test_min() {
        let mut acc = make_accumulator(&AggregateFunc::Min);
        acc.accumulate(&DataType::Long(30)).unwrap();
        acc.accumulate(&DataType::Long(10)).unwrap();
        acc.accumulate(&DataType::Long(20)).unwrap();
        assert_eq!(acc.finalize().unwrap(), DataType::Long(10));
    }

    #[test]
    fn test_max() {
        let mut acc = make_accumulator(&AggregateFunc::Max);
        acc.accumulate(&DataType::Long(10)).unwrap();
        acc.accumulate(&DataType::Long(30)).unwrap();
        acc.accumulate(&DataType::Long(20)).unwrap();
        assert_eq!(acc.finalize().unwrap(), DataType::Long(30));
    }

    #[test]
    fn test_min_string() {
        let mut acc = make_accumulator(&AggregateFunc::Min);
        acc.accumulate(&DataType::String("charlie".into())).unwrap();
        acc.accumulate(&DataType::String("alice".into())).unwrap();
        acc.accumulate(&DataType::String("bob".into())).unwrap();
        assert_eq!(acc.finalize().unwrap(), DataType::String("alice".into()));
    }

    #[test]
    fn test_max_double() {
        let mut acc = make_accumulator(&AggregateFunc::Max);
        acc.accumulate(&DataType::Double(1.5)).unwrap();
        acc.accumulate(&DataType::Double(3.5)).unwrap();
        acc.accumulate(&DataType::Double(2.5)).unwrap();
        assert_eq!(acc.finalize().unwrap(), DataType::Double(3.5));
    }

    #[test]
    fn test_count_empty() {
        let acc = make_accumulator(&AggregateFunc::Count);
        assert_eq!(acc.finalize().unwrap(), DataType::Long(0));
    }

    #[test]
    fn test_count_distinct_empty() {
        let acc = make_accumulator(&AggregateFunc::CountDistinct);
        assert_eq!(acc.finalize().unwrap(), DataType::Long(0));
    }

    #[test]
    fn test_sum_empty() {
        let acc = make_accumulator(&AggregateFunc::Sum);
        assert_eq!(acc.finalize().unwrap(), DataType::Long(0));
    }

    #[test]
    fn test_avg_empty_errors() {
        let acc = make_accumulator(&AggregateFunc::Avg);
        assert!(acc.finalize().is_err());
    }

    #[test]
    fn test_min_empty_errors() {
        let acc = make_accumulator(&AggregateFunc::Min);
        assert!(acc.finalize().is_err());
    }

    #[test]
    fn test_max_empty_errors() {
        let acc = make_accumulator(&AggregateFunc::Max);
        assert!(acc.finalize().is_err());
    }
}
