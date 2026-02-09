use std::sync::Arc;

use anyhow::Error;
use bytes::Bytes;
use tokio::runtime::Handle;

use crate::algo::generic_join::{Extension, Prefix, PrefixExtender};
use crate::codec::index_type_to_prefix;
use crate::index::IndexType;
use crate::util::make_extractor;

use super::slate_iterator::{Extractor, Index, SlateIterator};

/// GenericPrefixExtender implements PrefixExtender using SlateDB with byte prefixes.
///
/// The caller is responsible for determining index types, attribute IDs, and level participation.
pub struct GenericPrefixExtender {
    slate: Arc<slatedb::DbSnapshot>,
    handle: Handle,
    index_types: Vec<IndexType>,      // e.g., [AV, AVE]
    constant_prefix: Vec<u8>,         // attr_bytes + serialized constant values from the pattern
    participating_levels: Vec<usize>, // Which join levels this participates in
}

impl GenericPrefixExtender {
    pub fn new(
        slate: Arc<slatedb::DbSnapshot>,
        handle: Handle,
        index_types: Vec<IndexType>,
        attribute_id: u64,
        constant_prefix: Vec<u8>,
        participating_levels: Vec<usize>,
    ) -> Self {
        let mut full_prefix = bincode::serialize(&attribute_id)
            .expect("Failed to serialize attribute_id");
        full_prefix.extend_from_slice(&constant_prefix);

        Self {
            slate,
            handle,
            index_types,
            constant_prefix: full_prefix,
            participating_levels,
        }
    }

    /// Get the pattern-internal level (how many of this pattern's variables we've already bound)
    fn pattern_level(&self, join_prefix: &Prefix) -> usize {
        self.participating_levels
            .iter()
            .filter(|&&level| level < join_prefix.len())
            .count()
    }

    /// Build SlateDB key prefix from join prefix
    ///
    /// Selects index type based on join depth and constructs the appropriate byte prefix.
    fn build_slate_prefix(&self, join_prefix: &Prefix) -> Result<Bytes, Error> {
        let pattern_level = self.pattern_level(join_prefix);
        let index_type = self.index_types[pattern_level];
        let codec = index_type_to_prefix(index_type)?;

        let mut key = vec![codec];
        key.extend_from_slice(&self.constant_prefix);

        // Append components from join prefix at participating levels
        for &level in self.participating_levels.iter() {
            if level >= join_prefix.len() {
                break;
            }
            key.extend_from_slice(&join_prefix[level]);
        }

        Ok(Bytes::from(key))
    }

    /// Create an extractor function for the current index type and position.
    ///
    /// Uses `make_extractor` from util.rs which knows the key layout for each index type.
    /// Position is pattern_level + 1 (since position 0 is always attribute, which is in the prefix).
    fn make_extractor_fn(&self, join_prefix: &Prefix) -> Extractor {
        let pattern_level = self.pattern_level(join_prefix);
        let index_type = self.index_types[pattern_level];
        // Position 0 = attribute (always in prefix), so we extract position 1 or 2
        let position = pattern_level + 1;

        Box::new(move |key: Bytes| make_extractor(position, index_type)(key))
    }
}

impl PrefixExtender for GenericPrefixExtender {
    fn count(&self, join_prefix: &Prefix) -> usize {
        // TODO: proper error handling
        let slate_prefix = self
            .build_slate_prefix(join_prefix)
            .unwrap_or_else(|e| panic!("Failed to build slate prefix: {}", e));
        let extractor = self.make_extractor_fn(join_prefix);

        let iter = SlateIterator::new(
            &slate_prefix,
            self.slate.as_ref(),
            self.handle.clone(),
            extractor,
        )
        .unwrap_or_else(|e| panic!("Failed to create SlateIterator: {}", e));

        iter.count().unwrap_or(0) as usize
    }

    fn propose(&self, join_prefix: &Prefix) -> Vec<Extension> {
        // TODO: proper error handling
        let slate_prefix = self
            .build_slate_prefix(join_prefix)
            .unwrap_or_else(|e| panic!("Failed to build slate prefix: {}", e));
        let extractor = self.make_extractor_fn(join_prefix);

        let mut iter = SlateIterator::new(
            &slate_prefix,
            self.slate.as_ref(),
            self.handle.clone(),
            extractor,
        )
        .unwrap_or_else(|e| panic!("Failed to create SlateIterator: {}", e));

        let mut extensions = Vec::new();
        while let Ok(Some(extension)) = iter.get_value() {
            extensions.push(extension);
            iter.next()
                .unwrap_or_else(|e| panic!("Failed to advance iterator: {}", e));
        }

        extensions
    }

    fn intersect(&self, join_prefix: &Prefix, extensions: &[Extension]) -> Vec<Extension> {
        // TODO: proper error handling
        let slate_prefix = self
            .build_slate_prefix(join_prefix)
            .unwrap_or_else(|e| panic!("Failed to build slate prefix: {}", e));
        let extractor = self.make_extractor_fn(join_prefix);

        let mut iter = SlateIterator::new(
            &slate_prefix,
            self.slate.as_ref(),
            self.handle.clone(),
            extractor,
        )
        .unwrap_or_else(|e| panic!("Failed to create SlateIterator: {}", e));

        let mut result = Vec::new();
        for ext in extensions {
            iter.seek(ext.clone())
                .unwrap_or_else(|e| panic!("Failed to seek iterator: {}", e));
            if !iter.has_next() {
                break;
            }
            if iter.get_value().unwrap_or(None).as_ref() == Some(ext) {
                result.push(ext.clone());
            }
        }
        result
    }

    fn participates_in_level(&self, level: usize) -> bool {
        self.participating_levels.contains(&level)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::slate::in_memory_slate;

    fn encode_u64(val: u64) -> Bytes {
        // Use bincode serialization to match how indexer stores entity IDs
        Bytes::from(bincode::serialize(&val).unwrap())
    }

    fn encode_string(s: &str) -> Bytes {
        Bytes::from(s.as_bytes().to_vec())
    }

    async fn insert_ave(
        slate: &slatedb::Db,
        attribute: u64,
        value: Bytes,
        entity: u64,
    ) -> anyhow::Result<()> {
        let mut key = vec![crate::codec::AVE];
        key.extend_from_slice(&bincode::serialize(&attribute)?);
        key.extend_from_slice(&value);
        key.extend_from_slice(&bincode::serialize(&entity)?);
        key.push(crate::codec::ADD);

        slate.put(&key, b"dummy_value").await?;
        Ok(())
    }

    async fn insert_av(slate: &slatedb::Db, attribute: u64, value: Bytes) -> anyhow::Result<()> {
        let mut key = vec![crate::codec::AV];
        key.extend_from_slice(&bincode::serialize(&attribute)?);
        key.extend_from_slice(&value);
        key.push(crate::codec::ADD);

        slate.put(&key, b"dummy_value").await?;
        Ok(())
    }

    #[test]
    fn test_participates_in_level() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let slate = runtime.block_on(in_memory_slate());
        let snapshot = runtime.block_on(slate.snapshot()).unwrap();

        let extender = GenericPrefixExtender::new(
            snapshot,
            runtime.handle().clone(),
            vec![IndexType::AV, IndexType::AVE],
            42,
            vec![],
            vec![0, 1],
        );

        assert!(extender.participates_in_level(0));
        assert!(extender.participates_in_level(1));
        assert!(!extender.participates_in_level(2));
    }

    #[test]
    fn test_count_with_av_index() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let slate = runtime.block_on(in_memory_slate());
        let attr_name = 42u64;

        runtime.block_on(insert_av(&slate, attr_name, encode_string("Alice")))?;
        runtime.block_on(insert_av(&slate, attr_name, encode_string("Bob")))?;
        runtime.block_on(insert_av(&slate, attr_name, encode_string("Charlie")))?;

        let snapshot = runtime.block_on(slate.snapshot()).unwrap();

        let extender = GenericPrefixExtender::new(
            snapshot,
            runtime.handle().clone(),
            vec![IndexType::AV],
            attr_name,
            vec![],
            vec![0],
        );

        let count = extender.count(&vec![]);
        // TODO: count() currently returns 100 as estimate_key_count is not on DbSnapshot
        assert_eq!(count, 100);

        Ok(())
    }

    #[test]
    fn test_propose_with_av_index() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let slate = runtime.block_on(in_memory_slate());
        let attr_name = 42u64;

        runtime.block_on(insert_av(&slate, attr_name, encode_string("Alice")))?;
        runtime.block_on(insert_av(&slate, attr_name, encode_string("Bob")))?;

        let snapshot = runtime.block_on(slate.snapshot()).unwrap();

        let extender = GenericPrefixExtender::new(
            snapshot,
            runtime.handle().clone(),
            vec![IndexType::AV],
            attr_name,
            vec![],
            vec![0],
        );

        let values = extender.propose(&vec![]);
        assert_eq!(values.len(), 2);
        assert!(values[0] < values[1]);

        Ok(())
    }

    #[test]
    fn test_multiple_index_types() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let slate = runtime.block_on(in_memory_slate());
        let attr_name = 42u64;

        // Insert into both AV and AVE indexes
        runtime.block_on(insert_av(&slate, attr_name, encode_string("Alice")))?;
        runtime.block_on(insert_av(&slate, attr_name, encode_string("Bob")))?;
        runtime.block_on(insert_ave(&slate, attr_name, encode_string("Alice"), 1))?;
        runtime.block_on(insert_ave(&slate, attr_name, encode_string("Bob"), 2))?;

        let snapshot = runtime.block_on(slate.snapshot()).unwrap();

        let extender = GenericPrefixExtender::new(
            snapshot,
            runtime.handle().clone(),
            vec![IndexType::AV, IndexType::AVE],
            attr_name,
            vec![],
            vec![0, 1],
        );

        let values = extender.propose(&vec![]);
        assert_eq!(values.len(), 2);

        let entities = extender.propose(&vec![encode_string("Alice")]);
        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0], encode_u64(1));

        Ok(())
    }

    #[test]
    fn test_intersect() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let slate = runtime.block_on(in_memory_slate());
        let attr_name = 42u64;

        runtime.block_on(insert_av(&slate, attr_name, encode_string("Alice")))?;
        runtime.block_on(insert_av(&slate, attr_name, encode_string("Bob")))?;

        let snapshot = runtime.block_on(slate.snapshot()).unwrap();

        let extender = GenericPrefixExtender::new(
            snapshot,
            runtime.handle().clone(),
            vec![IndexType::AV],
            attr_name,
            vec![],
            vec![0],
        );

        let candidates = vec![
            encode_string("Alice"),
            encode_string("Bob"),
            encode_string("Charlie"),
        ];

        let filtered = extender.intersect(&vec![], &candidates);
        assert_eq!(filtered.len(), 2);
        assert!(filtered.contains(&encode_string("Alice")));
        assert!(filtered.contains(&encode_string("Bob")));
        assert!(!filtered.contains(&encode_string("Charlie")));

        Ok(())
    }

    #[test]
    fn test_empty_results() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let slate = runtime.block_on(in_memory_slate());
        let attr_name = 42u64;

        let snapshot = runtime.block_on(slate.snapshot()).unwrap();

        let extender = GenericPrefixExtender::new(
            snapshot,
            runtime.handle().clone(),
            vec![IndexType::AV],
            attr_name,
            vec![],
            vec![0],
        );

        // Note: estimate_key_count may return non-zero even for empty ranges
        assert_eq!(extender.propose(&vec![]), Vec::<Bytes>::new());
        assert_eq!(
            extender.intersect(&vec![], &[encode_string("Alice")]),
            Vec::<Bytes>::new()
        );

        Ok(())
    }
}
