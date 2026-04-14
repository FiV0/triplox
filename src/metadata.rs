use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use crate::partition::{extract_counter, extract_partition, make_entity_id};
use crate::schema::Schema;

/// Newtype wrapping partition counters. Deref/DerefMut to inner HashMap
/// so standard map methods work transparently.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartitionMap(HashMap<u32, i64>);

impl PartitionMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocate the next counter value for the given partition.
    /// Returns the current counter and increments it.
    pub fn allocate_counter(&mut self, partition: u32) -> i64 {
        let counter = self.0.entry(partition).or_insert(0);
        let value = *counter;
        *counter += 1;
        value
    }

    /// Allocate a new entity ID in the given partition.
    pub fn allocate_entid(&mut self, partition: u32) -> i64 {
        let counter = self.allocate_counter(partition);
        make_entity_id(partition, counter)
    }

    /// Check if an entity ID has been allocated (counter < next_counter for its partition).
    /// Used to validate explicit db/id values in transactions.
    pub fn contains_entid(&self, eid: i64) -> bool {
        let partition = extract_partition(eid);
        let counter = extract_counter(eid);
        match self.0.get(&partition) {
            Some(&next) => counter < next,
            None => false,
        }
    }
}

impl Deref for PartitionMap {
    type Target = HashMap<u32, i64>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PartitionMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
impl<const N: usize> From<[(u32, i64); N]> for PartitionMap {
    fn from(arr: [(u32, i64); N]) -> Self {
        PartitionMap(HashMap::from(arr))
    }
}

/// Groups all mutable metadata that evolves with each transaction.
#[derive(Debug)]
pub struct Metadata {
    pub generation: u64,
    pub partition_map: PartitionMap,
    pub schema: Schema,
}

impl Metadata {
    pub fn new(schema: Schema, partition_map: PartitionMap) -> Self {
        Self { generation: 0, partition_map, schema }
    }

    pub fn advance_generation(&mut self) {
        self.generation += 1;
    }
}
