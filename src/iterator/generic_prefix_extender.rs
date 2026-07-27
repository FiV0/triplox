use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::Error;
use bytes::Bytes;
use slatedb::{DbMetadataOps, DbReadOps};
use tokio::runtime::Handle;

use crate::algo::generic_join::{Extension, Prefix, PrefixExtender};
use crate::codec::{self, index_type_to_prefix};
use crate::index::IndexType;
use crate::util::make_extractor;

use super::slate_iterator::{Extractor, Index, SlateIterator};
use super::temporal_filter_iterator::TemporalFilterIterator;

// Materialize a range when scanning it is competitive with candidate-driven seeks.
const MATERIALIZE_INTERSECTION_RATIO: usize = 4;

// Query-local cache because one storage prefix can recur under many join prefixes.
#[derive(Default)]
struct PrefixCache {
    counts: Mutex<HashMap<Bytes, usize>>,
    values: Mutex<HashMap<Bytes, Arc<Vec<Extension>>>>,
}

impl PrefixCache {
    fn count_or_init(&self, prefix: &Bytes, load: impl FnOnce() -> usize) -> usize {
        if let Some(values) = self.values(prefix) {
            return values.len();
        }
        if let Some(count) = self.counts.lock().unwrap().get(prefix) {
            return *count;
        }

        let count = load();
        *self
            .counts
            .lock()
            .unwrap()
            .entry(prefix.clone())
            .or_insert(count)
    }

    fn values(&self, prefix: &Bytes) -> Option<Arc<Vec<Extension>>> {
        self.values.lock().unwrap().get(prefix).cloned()
    }

    fn values_or_init(
        &self,
        prefix: &Bytes,
        load: impl FnOnce() -> Vec<Extension>,
    ) -> Arc<Vec<Extension>> {
        if let Some(values) = self.values(prefix) {
            return values;
        }

        let loaded = Arc::new(load());
        self.values
            .lock()
            .unwrap()
            .entry(prefix.clone())
            .or_insert_with(|| loaded.clone())
            .clone()
    }
}

/// GenericPrefixExtender implements PrefixExtender using SlateDB with byte prefixes.
///
/// The caller is responsible for determining index types, attribute IDs, and level participation.
pub struct GenericPrefixExtender<D, M>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
    slate: Arc<D>,
    handle: Handle,
    range_stats: Arc<slatedb_estimates::RangeStats<M>>,
    index_types: Vec<IndexType>,      // e.g., [AV, AVE]
    constant_prefix: Vec<u8>,         // attr_bytes + serialized constant values from the pattern
    participating_levels: Vec<usize>, // Which join levels this participates in
    as_of: i64,                       // temporal filter: only see facts at or before this time
    prefix_cache: PrefixCache,
}

impl<D, M> GenericPrefixExtender<D, M>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        slate: Arc<D>,
        handle: Handle,
        range_stats: Arc<slatedb_estimates::RangeStats<M>>,
        index_types: Vec<IndexType>,
        attribute_id: i64,
        constant_prefix: Vec<u8>,
        participating_levels: Vec<usize>,
        as_of: i64,
    ) -> Self {
        debug_assert!(
            participating_levels.windows(2).all(|w| w[0] < w[1]),
            "participating_levels must be sorted strictly ascending, got {:?}",
            participating_levels
        );

        let mut full_prefix = Vec::new();
        codec::encode_i64(attribute_id, &mut full_prefix);
        full_prefix.extend_from_slice(&constant_prefix);

        Self {
            slate,
            handle,
            range_stats,
            index_types,
            constant_prefix: full_prefix,
            participating_levels,
            as_of,
            prefix_cache: PrefixCache::default(),
        }
    }

    fn is_atemporal(index_type: IndexType) -> bool {
        matches!(index_type, IndexType::AE | IndexType::AV)
    }

    fn should_materialize_intersection(
        index_type: IndexType,
        estimated_count: usize,
        candidate_count: usize,
    ) -> bool {
        Self::is_atemporal(index_type)
            || estimated_count <= candidate_count.saturating_mul(MATERIALIZE_INTERSECTION_RATIO)
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
    /// Returns both the prefix bytes and the resolved index type.
    fn build_slate_prefix(&self, join_prefix: &Prefix) -> Result<(Bytes, IndexType), Error> {
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

        Ok((Bytes::from(key), index_type))
    }

    /// Create the appropriate iterator for the given prefix and index type.
    ///
    /// AE/AV are atemporal indices — use SlateIterator (no temporal filtering).
    /// Full indices (EAV, AVE, AEV) — use TemporalFilterIterator.
    fn create_iterator(
        &self,
        slate_prefix: &Bytes,
        index_type: IndexType,
        extractor: Extractor,
    ) -> Box<dyn Index> {
        match index_type {
            IndexType::AE | IndexType::AV => Box::new(
                SlateIterator::new(
                    slate_prefix,
                    self.slate.as_ref(),
                    self.handle.clone(),
                    extractor,
                    self.range_stats.clone(),
                )
                .unwrap_or_else(|e| panic!("Failed to create SlateIterator: {}", e)),
            ),
            IndexType::EAV | IndexType::AVE | IndexType::AEV | IndexType::VAE => Box::new(
                TemporalFilterIterator::new(
                    slate_prefix,
                    self.slate.as_ref(),
                    self.handle.clone(),
                    extractor,
                    self.as_of,
                    self.range_stats.clone(),
                )
                .unwrap_or_else(|e| panic!("Failed to create TemporalFilterIterator: {}", e)),
            ),
        }
    }

    fn collect_extensions(
        &self,
        slate_prefix: &Bytes,
        index_type: IndexType,
        extractor: Extractor,
    ) -> Vec<Extension> {
        let mut iter = self.create_iterator(slate_prefix, index_type, extractor);
        let mut extensions = Vec::new();
        while let Ok(Some(extension)) = iter.get_value() {
            extensions.push(extension);
            iter.next()
                .unwrap_or_else(|e| panic!("Failed to advance iterator: {}", e));
        }
        extensions
    }

    fn estimate_count(&self, slate_prefix: &Bytes) -> usize {
        self.handle
            .block_on(
                self.range_stats
                    .estimate_key_count_with_prefix(slate_prefix),
            )
            .unwrap_or(0) as usize
    }

    fn values_for_prefix(
        &self,
        join_prefix: &Prefix,
        slate_prefix: &Bytes,
        index_type: IndexType,
    ) -> Arc<Vec<Extension>> {
        self.prefix_cache.values_or_init(slate_prefix, || {
            let values = self.collect_extensions(
                slate_prefix,
                index_type,
                self.make_extractor_fn(join_prefix),
            );
            debug_assert!(values.windows(2).all(|pair| pair[0] < pair[1]));
            values
        })
    }

    /// Create an extractor function for the current index type and position.
    ///
    /// Uses `make_extractor` from util.rs which knows the key layout for each index type.
    fn make_extractor_fn(&self, join_prefix: &Prefix) -> Extractor {
        let pattern_level = self.pattern_level(join_prefix);
        let index_type = self.index_types[pattern_level];
        let position = match index_type {
            // 3-component indices: if this is a single-variable pattern,
            // the variable is at position 2 (after attribute and the constant)
            IndexType::AVE | IndexType::AEV | IndexType::VAE
                if self.participating_levels.len() == 1 =>
            {
                2
            }
            _ => pattern_level + 1,
        };

        Box::new(move |key: Bytes| make_extractor(position, index_type)(key))
    }
}

impl<D, M> PrefixExtender for GenericPrefixExtender<D, M>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
    fn count(&self, join_prefix: &Prefix) -> usize {
        // TODO: proper error handling
        let (slate_prefix, _) = self
            .build_slate_prefix(join_prefix)
            .unwrap_or_else(|e| panic!("Failed to build slate prefix: {}", e));
        self.prefix_cache
            .count_or_init(&slate_prefix, || self.estimate_count(&slate_prefix))
    }

    fn propose(&self, join_prefix: &Prefix) -> Vec<Extension> {
        // TODO: proper error handling
        let (slate_prefix, index_type) = self
            .build_slate_prefix(join_prefix)
            .unwrap_or_else(|e| panic!("Failed to build slate prefix: {}", e));
        self.values_for_prefix(join_prefix, &slate_prefix, index_type)
            .as_ref()
            .clone()
    }

    fn intersect(&self, join_prefix: &Prefix, extensions: &[Extension]) -> Vec<Extension> {
        if extensions.is_empty() {
            return Vec::new();
        }

        // TODO: proper error handling
        let (slate_prefix, index_type) = self
            .build_slate_prefix(join_prefix)
            .unwrap_or_else(|e| panic!("Failed to build slate prefix: {}", e));

        if let Some(values) = self.prefix_cache.values(&slate_prefix) {
            return extensions
                .iter()
                .filter(|extension| values.binary_search(extension).is_ok())
                .cloned()
                .collect();
        }

        let estimated_count = self
            .prefix_cache
            .count_or_init(&slate_prefix, || self.estimate_count(&slate_prefix));
        if Self::should_materialize_intersection(index_type, estimated_count, extensions.len()) {
            let values = self.values_for_prefix(join_prefix, &slate_prefix, index_type);
            return extensions
                .iter()
                .filter(|extension| values.binary_search(extension).is_ok())
                .cloned()
                .collect();
        }

        let mut iter = self.create_iterator(
            &slate_prefix,
            index_type,
            self.make_extractor_fn(join_prefix),
        );

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
    use std::cell::Cell;

    use super::*;
    use crate::codec::Encode;
    use crate::ops::DataType;
    use crate::slate::in_memory_slate;

    fn encode_entity(val: i64) -> Bytes {
        Bytes::from(DataType::Long(val).encode())
    }

    fn encode_string(s: &str) -> Bytes {
        Bytes::from(DataType::String(s.to_string()).encode())
    }

    async fn insert_ave(
        slate: &slatedb::Db,
        attribute: i64,
        value: Bytes,
        entity: i64,
    ) -> anyhow::Result<()> {
        let mut key = vec![crate::codec::AVE];
        codec::encode_i64(attribute, &mut key);
        key.extend_from_slice(&value);
        key.extend_from_slice(&DataType::Long(entity).encode());
        key.extend_from_slice(&crate::codec::encode_i64_bytes(1000));
        key.push(crate::codec::ADD);

        slate.put(&key, b"dummy_value").await?;
        Ok(())
    }

    async fn insert_av(slate: &slatedb::Db, attribute: i64, value: Bytes) -> anyhow::Result<()> {
        // AV is atemporal — no timestamp or op suffix
        let mut key = vec![crate::codec::AV];
        codec::encode_i64(attribute, &mut key);
        key.extend_from_slice(&value);

        slate.put(&key, b"dummy_value").await?;
        Ok(())
    }

    #[test]
    fn test_participates_in_level() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let components = runtime.block_on(in_memory_slate());
        let slate = components.db.clone();
        let range_stats = components.range_stats.clone();
        let extender = GenericPrefixExtender::new(
            slate,
            runtime.handle().clone(),
            range_stats.clone(),
            vec![IndexType::AV, IndexType::AVE],
            42,
            vec![],
            vec![0, 1],
            2000_i64,
        );

        assert!(extender.participates_in_level(0));
        assert!(extender.participates_in_level(1));
        assert!(!extender.participates_in_level(2));
    }

    #[test]
    fn test_count_with_av_index() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let components = runtime.block_on(in_memory_slate());
        let slate = components.db.clone();
        let range_stats = components.range_stats.clone();
        let attr_name = 42i64;

        runtime.block_on(insert_av(&slate, attr_name, encode_string("Alice")))?;
        runtime.block_on(insert_av(&slate, attr_name, encode_string("Bob")))?;
        runtime.block_on(insert_av(&slate, attr_name, encode_string("Charlie")))?;

        // Flush memtable to SSTs so RangeStats can read the metadata
        runtime
            .block_on(slate.flush_with_options(slatedb::config::FlushOptions {
                flush_type: slatedb::config::FlushType::MemTable,
            }))
            .unwrap();

        let extender = GenericPrefixExtender::new(
            slate,
            runtime.handle().clone(),
            range_stats.clone(),
            vec![IndexType::AV],
            attr_name,
            vec![],
            vec![0],
            2000_i64,
        );

        let count = extender.count(&vec![]);
        assert_eq!(count, 3);

        Ok(())
    }

    #[test]
    fn test_propose_with_av_index() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let components = runtime.block_on(in_memory_slate());
        let slate = components.db.clone();
        let range_stats = components.range_stats.clone();
        let attr_name = 42i64;

        runtime.block_on(insert_av(&slate, attr_name, encode_string("Alice")))?;
        runtime.block_on(insert_av(&slate, attr_name, encode_string("Bob")))?;

        let extender = GenericPrefixExtender::new(
            slate,
            runtime.handle().clone(),
            range_stats.clone(),
            vec![IndexType::AV],
            attr_name,
            vec![],
            vec![0],
            2000_i64,
        );

        let values = extender.propose(&vec![]);
        assert_eq!(values.len(), 2);
        assert!(values[0] < values[1]);

        Ok(())
    }

    #[test]
    fn test_multiple_index_types() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let components = runtime.block_on(in_memory_slate());
        let slate = components.db.clone();
        let range_stats = components.range_stats.clone();
        let attr_name = 42i64;

        // Insert into both AV and AVE indexes
        runtime.block_on(insert_av(&slate, attr_name, encode_string("Alice")))?;
        runtime.block_on(insert_av(&slate, attr_name, encode_string("Bob")))?;
        runtime.block_on(insert_ave(&slate, attr_name, encode_string("Alice"), 1))?;
        runtime.block_on(insert_ave(&slate, attr_name, encode_string("Bob"), 2))?;

        let extender = GenericPrefixExtender::new(
            slate,
            runtime.handle().clone(),
            range_stats.clone(),
            vec![IndexType::AV, IndexType::AVE],
            attr_name,
            vec![],
            vec![0, 1],
            2000_i64,
        );

        let values = extender.propose(&vec![]);
        assert_eq!(values.len(), 2);

        let entities = extender.propose(&vec![encode_string("Alice")]);
        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0], encode_entity(1));

        Ok(())
    }

    #[test]
    fn test_intersect() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let components = runtime.block_on(in_memory_slate());
        let slate = components.db.clone();
        let range_stats = components.range_stats.clone();
        let attr_name = 42i64;

        runtime.block_on(insert_av(&slate, attr_name, encode_string("Alice")))?;
        runtime.block_on(insert_av(&slate, attr_name, encode_string("Bob")))?;

        let extender = GenericPrefixExtender::new(
            slate,
            runtime.handle().clone(),
            range_stats.clone(),
            vec![IndexType::AV],
            attr_name,
            vec![],
            vec![0],
            2000_i64,
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
    fn test_single_variable_ave_extracts_entity_not_value() -> anyhow::Result<()> {
        // Simulates pattern [?e "name" "alice"] — single variable, AVE index.
        // The constant_prefix includes the serialized value ("alice").
        // The extender should extract the entity (position 2 in AVE layout),
        // but make_extractor_fn computes position = pattern_level + 1 = 1,
        // which extracts the value (position 1) instead.
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let components = runtime.block_on(in_memory_slate());
        let slate = components.db.clone();
        let range_stats = components.range_stats.clone();
        let attr_name = 42i64;

        let value_bytes = Bytes::from(DataType::String("alice".to_string()).encode());
        runtime.block_on(insert_ave(&slate, attr_name, value_bytes.clone(), 1))?;

        // This mirrors how compile_pattern sets up a single-variable AVE pattern:
        // - index_types: [AVE]
        // - constant_prefix: serialized value bytes
        // - participating_levels: [0] (only one variable)
        let extender = GenericPrefixExtender::new(
            slate,
            runtime.handle().clone(),
            range_stats.clone(),
            vec![IndexType::AVE],
            attr_name,
            value_bytes.to_vec(),
            vec![0],
            2000_i64,
        );

        let proposed = extender.propose(&vec![]);
        assert_eq!(proposed.len(), 1);
        // Should extract entity (DataType::Long(1)), not the value (DataType::String("alice"))
        assert_eq!(proposed[0], encode_entity(1));

        Ok(())
    }

    #[test]
    fn test_empty_results() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let components = runtime.block_on(in_memory_slate());
        let slate = components.db.clone();
        let range_stats = components.range_stats.clone();
        let attr_name = 42i64;

        let extender = GenericPrefixExtender::new(
            slate,
            runtime.handle().clone(),
            range_stats.clone(),
            vec![IndexType::AV],
            attr_name,
            vec![],
            vec![0],
            2000_i64,
        );

        // Note: estimate_key_count may return non-zero even for empty ranges
        assert_eq!(extender.propose(&vec![]), Vec::<Bytes>::new());
        assert_eq!(
            extender.intersect(&vec![], &[encode_string("Alice")]),
            Vec::<Bytes>::new()
        );

        Ok(())
    }

    #[test]
    fn prefix_cache_initializes_count_and_values_once() {
        let cache = PrefixCache::default();
        let count_loads = Cell::new(0);
        let value_loads = Cell::new(0);
        let prefix = Bytes::from_static(b"prefix");

        assert_eq!(
            cache.count_or_init(&prefix, || {
                count_loads.set(count_loads.get() + 1);
                2
            }),
            2
        );
        assert_eq!(cache.count_or_init(&prefix, || 99), 2);

        let values = cache.values_or_init(&prefix, || {
            value_loads.set(value_loads.get() + 1);
            vec![encode_string("Alice"), encode_string("Bob")]
        });
        assert_eq!(values.len(), 2);
        assert_eq!(cache.values_or_init(&prefix, Vec::new), values);
        assert_eq!(cache.count_or_init(&prefix, || 99), values.len());
        assert_eq!(count_loads.get(), 1);
        assert_eq!(value_loads.get(), 1);
    }

    #[test]
    fn intersection_materialization_keeps_sparse_temporal_ranges_on_seek_path() {
        type Extender = GenericPrefixExtender<slatedb::Db, slatedb::Db>;

        assert!(Extender::should_materialize_intersection(
            IndexType::AE,
            10_000,
            1
        ));
        assert!(Extender::should_materialize_intersection(
            IndexType::AVE,
            8,
            2
        ));
        assert!(!Extender::should_materialize_intersection(
            IndexType::AVE,
            9,
            2
        ));
    }

    #[test]
    fn prefix_cache_initializes_values_once_per_slate_prefix() {
        let cache = PrefixCache::default();
        let loads = Cell::new(0);
        let alice = Bytes::from_static(b"alice");
        let bob = Bytes::from_static(b"bob");

        let alice_values = cache.values_or_init(&alice, || {
            loads.set(loads.get() + 1);
            vec![encode_entity(1)]
        });
        let same_alice_values = cache.values_or_init(&alice, || {
            loads.set(loads.get() + 1);
            Vec::new()
        });
        let bob_values = cache.values_or_init(&bob, || {
            loads.set(loads.get() + 1);
            vec![encode_entity(2)]
        });

        assert!(Arc::ptr_eq(&alice_values, &same_alice_values));
        assert_ne!(alice_values, bob_values);
        assert_eq!(loads.get(), 2);
    }
}
