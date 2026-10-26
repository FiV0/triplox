use std::sync::Arc;

use anyhow::Error;
use bytes::Bytes;
use slatedb::{DbMetadataOps, DbReadOps};
use tokio::runtime::Handle;

use super::slate_iterator::{Extractor, Index};
use crate::slate::DEFAULT_SCAN_OPTIONS;
use crate::util::next_prefix;

/// Enumerates distinct components immediately after a scan prefix, ignoring temporal visibility.
pub(crate) struct DistinctPrefixIterator<M>
where
    M: DbMetadataOps + Send + Sync,
{
    inner: slatedb::DbIterator,
    current_key: Option<Bytes>,
    prefix: Bytes,
    handle: Handle,
    extractor: Extractor,
    range_stats: Arc<slatedb_estimates::RangeStats<M>>,
}

impl<M> DistinctPrefixIterator<M>
where
    M: DbMetadataOps + Send + Sync,
{
    /// The extractor must return the complete, prefix-free component following `prefix`.
    pub fn new<D>(
        prefix: &[u8],
        slate: &D,
        handle: Handle,
        extractor: Extractor,
        range_stats: Arc<slatedb_estimates::RangeStats<M>>,
    ) -> Result<Self, Error>
    where
        D: DbReadOps + Send + Sync,
    {
        let mut inner =
            handle.block_on(slate.scan_prefix_with_options(prefix, .., &DEFAULT_SCAN_OPTIONS))?;
        let current_key = handle.block_on(inner.next())?.map(|kv| kv.key);
        Ok(Self {
            inner,
            current_key,
            prefix: Bytes::copy_from_slice(prefix),
            handle,
            extractor,
            range_stats,
        })
    }
}

impl<M> Index for DistinctPrefixIterator<M>
where
    M: DbMetadataOps + Send + Sync,
{
    fn count(&self) -> Result<u64, Error> {
        Ok(self.handle.block_on(
            self.range_stats
                .estimate_key_count_with_prefix(&self.prefix),
        )?)
    }

    fn seek(&mut self, extension: Bytes) -> Result<(), Error> {
        let Some(current) = &self.current_key else {
            return Ok(());
        };
        let mut target = self.prefix.to_vec();
        target.extend_from_slice(&extension);
        if current.as_ref() >= target.as_slice() {
            return Ok(());
        }
        self.handle.block_on(self.inner.seek(Bytes::from(target)))?;
        self.current_key = self.handle.block_on(self.inner.next())?.map(|kv| kv.key);
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Bytes>, Error> {
        let Some(value) = self.get_value()? else {
            return Ok(None);
        };
        let mut group = self.prefix.to_vec();
        group.extend_from_slice(&value);
        let Some(target) = next_prefix(&group) else {
            self.current_key = None;
            return Ok(None);
        };
        self.handle.block_on(self.inner.seek(Bytes::from(target)))?;
        self.current_key = self.handle.block_on(self.inner.next())?.map(|kv| kv.key);
        self.get_value()
    }

    fn get_value(&self) -> Result<Option<Bytes>, Error> {
        Ok(self
            .current_key
            .as_ref()
            .map(|key| (self.extractor)(key.clone())))
    }

    fn has_next(&self) -> bool {
        self.current_key.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{self, Encode};
    use crate::index::IndexType;
    use crate::ops::DataType;
    use crate::slate::in_memory_slate;
    use crate::util::make_extractor;

    fn prefix(index: IndexType, attribute: i64) -> Vec<u8> {
        let mut prefix = vec![codec::index_type_to_prefix(index).unwrap()];
        prefix.extend_from_slice(&codec::encode_i64_bytes(attribute));
        prefix
    }

    fn key(prefix: &[u8], first: &DataType, second: &DataType, tx: i64, op: u8) -> Vec<u8> {
        let mut key = prefix.to_vec();
        key.extend_from_slice(&first.encode());
        key.extend_from_slice(&second.encode());
        key.extend_from_slice(&codec::encode_i64_bytes(tx));
        key.push(op);
        key
    }

    #[test]
    fn skips_trailing_components_and_versions_in_both_indexes() -> Result<(), Error> {
        let runtime = tokio::runtime::Runtime::new()?;
        for index in [IndexType::AEV, IndexType::AVE] {
            let components = runtime.block_on(in_memory_slate());
            let prefix = prefix(index, 42);
            let mut expected: Vec<Bytes> = match index {
                IndexType::AEV => [1, 2, 3]
                    .map(|id| Bytes::from(DataType::Long(id).encode()))
                    .to_vec(),
                _ => ["", "a", "a\0\u{1}", "aa"]
                    .map(|s| Bytes::from(DataType::String(s.into()).encode()))
                    .to_vec(),
            };
            runtime.block_on(async {
                for first in &expected {
                    for second in [DataType::Long(10), DataType::Long(20)] {
                        for (tx, op) in [(1, codec::ADD), (2, codec::RETRACT), (3, codec::ADD)] {
                            let mut key = prefix.clone();
                            key.extend_from_slice(first);
                            key.extend_from_slice(&second.encode());
                            key.extend_from_slice(&codec::encode_i64_bytes(tx));
                            key.push(op);
                            components.db.put(&key, b"").await?;
                        }
                    }
                }
                components
                    .db
                    .put(
                        &key(
                            &self::prefix(index, 43),
                            &DataType::Long(99),
                            &DataType::Long(99),
                            1,
                            codec::ADD,
                        ),
                        b"",
                    )
                    .await?;
                components
                    .db
                    .flush_with_options(slatedb::config::FlushOptions {
                        flush_type: slatedb::config::FlushType::MemTable,
                    })
                    .await?;
                Ok::<_, Error>(())
            })?;
            expected.sort();
            let physical_count = runtime.block_on(
                components
                    .range_stats
                    .estimate_key_count_with_prefix(&prefix),
            )?;
            let mut iterator = DistinctPrefixIterator::new(
                &prefix,
                components.db.as_ref(),
                runtime.handle().clone(),
                Box::new(move |key| make_extractor(1, index)(key)),
                components.range_stats.clone(),
            )?;
            assert_eq!(iterator.count()?, physical_count);
            let mut actual = Vec::new();
            while let Some(value) = iterator.get_value()? {
                actual.push(value);
                iterator.next()?;
            }
            assert_eq!(actual, expected);
            assert!(!iterator.has_next());
            assert_eq!(iterator.next()?, None);
            iterator.seek(expected[0].clone())?;
            assert_eq!(iterator.get_value()?, None);
        }
        Ok(())
    }

    #[test]
    fn seeks_forward_and_keeps_overshot_or_exhausted_position() -> Result<(), Error> {
        let runtime = tokio::runtime::Runtime::new()?;
        for index in [IndexType::AEV, IndexType::AVE] {
            let components = runtime.block_on(in_memory_slate());
            let prefix = prefix(index, 42);
            let mut values: Vec<_> = match index {
                IndexType::AEV => [1, 3, 5, 7]
                    .map(|id| Bytes::from(DataType::Long(id).encode()))
                    .to_vec(),
                _ => ["", "a\0", "a\u{1}", "aa"]
                    .map(|s| Bytes::from(DataType::String(s.into()).encode()))
                    .to_vec(),
            };
            values.sort();
            runtime.block_on(async {
                for value in &values {
                    let mut key = prefix.clone();
                    key.extend_from_slice(value);
                    key.extend_from_slice(&DataType::Long(1).encode());
                    key.extend_from_slice(&codec::encode_i64_bytes(1));
                    key.push(codec::RETRACT);
                    components.db.put(&key, b"").await?;
                }
                Ok::<_, Error>(())
            })?;
            let mut iterator = DistinctPrefixIterator::new(
                &prefix,
                components.db.as_ref(),
                runtime.handle().clone(),
                Box::new(move |key| make_extractor(1, index)(key)),
                components.range_stats.clone(),
            )?;
            iterator.seek(values[1].clone())?;
            assert_eq!(iterator.get_value()?, Some(values[1].clone()));
            iterator.seek(values[1].clone())?;
            iterator.seek(values[0].clone())?;
            assert_eq!(iterator.get_value()?, Some(values[1].clone()));
            let missing = Bytes::from(next_prefix(&values[1]).unwrap());
            iterator.seek(missing.clone())?;
            assert_eq!(iterator.get_value()?, Some(values[2].clone()));
            iterator.seek(missing)?;
            assert_eq!(iterator.next()?, Some(values[3].clone()));
            iterator.seek(Bytes::from_static(&[0xff]))?;
            assert!(!iterator.has_next());
            iterator.seek(values[0].clone())?;
            assert_eq!(iterator.next()?, None);
        }
        Ok(())
    }

    #[test]
    fn empty_scan_and_prefix_without_successor_exhaust() -> Result<(), Error> {
        let runtime = tokio::runtime::Runtime::new()?;
        let components = runtime.block_on(in_memory_slate());
        for populated in [false, true] {
            if populated {
                runtime.block_on(components.db.put(&[0xff, 0xff, 0], b""))?;
            }
            let mut iterator = DistinctPrefixIterator::new(
                &[0xff],
                components.db.as_ref(),
                runtime.handle().clone(),
                Box::new(|key| key.slice(1..2)),
                components.range_stats.clone(),
            )?;
            assert_eq!(iterator.has_next(), populated);
            assert_eq!(iterator.next()?, None);
            iterator.seek(Bytes::from_static(&[0]))?;
            assert_eq!(iterator.get_value()?, None);
        }
        Ok(())
    }
}
