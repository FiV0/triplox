use std::sync::Arc;

use anyhow::Error;
use bytes::Bytes;
use tokio::runtime::Handle;

use crate::algo::generic_join::{Extension, Prefix, PrefixExtender};
use crate::codec::index_type_to_prefix;
use crate::index::IndexType;
use crate::util::extract_next_component;

use super::slate_iterator::{Index, SlateIterator};

/// GenericPrefixExtender implements PrefixExtender using SlateDB with byte prefixes.
///
/// The caller is responsible for determining index types, attribute IDs, and level participation.
pub struct GenericPrefixExtender {
    slate: Arc<slatedb::Db>,
    handle: Handle,
    index_types: Vec<IndexType>,      // e.g., [AV, AVE]
    attribute_id: u64,                // The attribute this pattern queries
    participating_levels: Vec<usize>, // Which join levels this participates in
}

impl GenericPrefixExtender {
    pub fn new(
        slate: Arc<slatedb::Db>,
        handle: Handle,
        index_types: Vec<IndexType>,
        attribute_id: u64,
        participating_levels: Vec<usize>,
    ) -> Self {
        Self {
            slate,
            handle,
            index_types,
            attribute_id,
            participating_levels,
        }
    }

    /// Build SlateDB key prefix from join prefix
    ///
    /// Selects index type based on join depth and constructs the appropriate byte prefix.
    fn build_slate_prefix(&self, join_prefix: &Prefix) -> Result<Bytes, Error> {
        // Index type is determined by current join depth
        let index_type = self.index_types[join_prefix.len()];
        let codec = index_type_to_prefix(index_type)?;

        let mut key = vec![codec];
        key.extend_from_slice(&self.attribute_id.to_be_bytes());

        // Append components from join prefix at participating levels
        for (idx, &level) in self.participating_levels.iter().enumerate() {
            if idx >= join_prefix.len() {
                break;
            }
            key.extend_from_slice(&join_prefix[level]);
        }

        Ok(Bytes::from(key))
    }
}

impl PrefixExtender for GenericPrefixExtender {
    fn count(&self, join_prefix: &Prefix) -> usize {
        // TODO: proper error handling
        let slate_prefix = self
            .build_slate_prefix(join_prefix)
            .unwrap_or_else(|e| panic!("Failed to build slate prefix: {}", e));

        let iter = SlateIterator::new(&slate_prefix, &self.slate, self.handle.clone())
            .unwrap_or_else(|e| panic!("Failed to create SlateIterator: {}", e));

        iter.count().unwrap_or(0) as usize
    }

    fn propose(&self, join_prefix: &Prefix) -> Vec<Extension> {
        // TODO: proper error handling
        let slate_prefix = self
            .build_slate_prefix(join_prefix)
            .unwrap_or_else(|e| panic!("Failed to build slate prefix: {}", e));

        let mut iter = SlateIterator::new(&slate_prefix, &self.slate, self.handle.clone())
            .unwrap_or_else(|e| panic!("Failed to create SlateIterator: {}", e));

        let mut extensions = Vec::new();
        while let Ok(Some(key)) = iter.get_value() {
            // Deduplications should happen lower level. Let's write a non historic version first.
            if let Ok(extension) = extract_next_component(&key, &slate_prefix) {
                extensions.push(extension);
            }
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

        // TODO: do a version with seek
        extensions
            .iter()
            .filter(|ext| {
                let mut full_key = slate_prefix.to_vec();
                full_key.extend_from_slice(ext);

                SlateIterator::new(&full_key, &self.slate, self.handle.clone())
                    .map(|iter| iter.has_next())
                    .unwrap_or(false)
            })
            .cloned()
            .collect()
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
        Bytes::from(val.to_be_bytes().to_vec())
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
        key.extend_from_slice(&attribute.to_be_bytes());
        key.extend_from_slice(&value);
        key.extend_from_slice(&entity.to_be_bytes());
        key.push(crate::codec::ADD);

        slate.put(&key, b"dummy_value").await?;
        Ok(())
    }

    async fn insert_av(slate: &slatedb::Db, attribute: u64, value: Bytes) -> anyhow::Result<()> {
        let mut key = vec![crate::codec::AV];
        key.extend_from_slice(&attribute.to_be_bytes());
        key.extend_from_slice(&value);
        key.push(crate::codec::ADD);

        slate.put(&key, b"dummy_value").await?;
        Ok(())
    }

    #[test]
    fn test_participates_in_level() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let slate = Arc::new(runtime.block_on(in_memory_slate()));

        let extender = GenericPrefixExtender::new(
            slate,
            runtime.handle().clone(),
            vec![IndexType::AV, IndexType::AVE],
            42,
            vec![0, 1],
        );

        assert!(extender.participates_in_level(0));
        assert!(extender.participates_in_level(1));
        assert!(!extender.participates_in_level(2));
    }

    #[test]
    fn test_count_with_av_index() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let slate = Arc::new(runtime.block_on(in_memory_slate()));
        let attr_name = 42u64;

        runtime.block_on(insert_av(&slate, attr_name, encode_string("Alice")))?;
        runtime.block_on(insert_av(&slate, attr_name, encode_string("Bob")))?;
        runtime.block_on(insert_av(&slate, attr_name, encode_string("Charlie")))?;

        let extender = GenericPrefixExtender::new(
            slate.clone(),
            runtime.handle().clone(),
            vec![IndexType::AV],
            attr_name,
            vec![0],
        );

        let count = extender.count(&vec![]);
        // estimate_key_count is an approximation, just verify it's non-zero
        assert!(count > 0, "count should be > 0, got {}", count);

        Ok(())
    }

    #[test]
    fn test_propose_with_av_index() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let slate = Arc::new(runtime.block_on(in_memory_slate()));
        let attr_name = 42u64;

        runtime.block_on(insert_av(&slate, attr_name, encode_string("Alice")))?;
        runtime.block_on(insert_av(&slate, attr_name, encode_string("Bob")))?;

        let extender = GenericPrefixExtender::new(
            slate.clone(),
            runtime.handle().clone(),
            vec![IndexType::AV],
            attr_name,
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
        let slate = Arc::new(runtime.block_on(in_memory_slate()));
        let attr_name = 42u64;

        // Insert into both AV and AVE indexes
        runtime.block_on(insert_av(&slate, attr_name, encode_string("Alice")))?;
        runtime.block_on(insert_av(&slate, attr_name, encode_string("Bob")))?;
        runtime.block_on(insert_ave(&slate, attr_name, encode_string("Alice"), 1))?;
        runtime.block_on(insert_ave(&slate, attr_name, encode_string("Bob"), 2))?;

        let extender = GenericPrefixExtender::new(
            slate.clone(),
            runtime.handle().clone(),
            vec![IndexType::AV, IndexType::AVE],
            attr_name,
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
        let slate = Arc::new(runtime.block_on(in_memory_slate()));
        let attr_name = 42u64;

        runtime.block_on(insert_av(&slate, attr_name, encode_string("Alice")))?;
        runtime.block_on(insert_av(&slate, attr_name, encode_string("Bob")))?;

        let extender = GenericPrefixExtender::new(
            slate.clone(),
            runtime.handle().clone(),
            vec![IndexType::AV],
            attr_name,
            vec![0],
        );

        let candidates = vec![
            encode_string("Alice"),
            encode_string("Charlie"),
            encode_string("Bob"),
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
        let slate = Arc::new(runtime.block_on(in_memory_slate()));
        let attr_name = 42u64;

        let extender = GenericPrefixExtender::new(
            slate.clone(),
            runtime.handle().clone(),
            vec![IndexType::AV],
            attr_name,
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
