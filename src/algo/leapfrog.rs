use bytes::Bytes;
use std::cmp::Ordering;

// Leapfrog Trie-Join based on
// https://arxiv.org/abs/1210.0481

pub type ResultTuple = Vec<Bytes>;

/// Core iterator trait for leapfrog join
pub trait LeapfrogIterator {
    /// Seek to the first key >= the given key
    fn seek(&mut self, key: &Bytes);
    /// Advance to the next key, returns the new key or None if at end
    fn next(&mut self) -> Option<Bytes>;
    /// Get the current key, or None if at end
    fn key(&self) -> Option<Bytes>;
    /// Check if the iterator is exhausted
    fn at_end(&self) -> bool;
}

/// Layered index trait for multi-level navigation
pub trait LayeredIndex {
    /// Open the next level with the given prefix
    fn open_level(&mut self, prefix: &[Bytes]);
    /// Close the current level
    fn close_level(&mut self);
    /// Get the current level
    fn level(&self) -> usize;
    /// Get the maximum number of levels
    fn max_level(&self) -> usize;
    /// Reinitialize the index
    fn reinit(&mut self);
}

/// Level participation trait
pub trait LevelParticipation {
    fn participates_in_level(&self, level: usize) -> bool;
}

/// Combined trait for leapfrog indexes
pub trait LeapfrogIndex: LeapfrogIterator + LayeredIndex + LevelParticipation {}

/// Blanket implementation for any type implementing all three traits
impl<T: LeapfrogIterator + LayeredIndex + LevelParticipation> LeapfrogIndex for T {}

/// Filter trait for negative joins
pub trait FilterLeapfrogIndex: LevelParticipation {
    fn accept(&self, tuple: &ResultTuple) -> bool;
}

/// A simple single-level index with sorted values
pub struct SingleLevelIndex {
    values: Vec<Bytes>,
    current_index: usize,
    participating_level: usize,
}

impl SingleLevelIndex {
    pub fn new(mut values: Vec<Bytes>, participating_level: usize) -> Self {
        values.sort();
        Self {
            values,
            current_index: 0,
            participating_level,
        }
    }
}

impl LeapfrogIterator for SingleLevelIndex {
    fn seek(&mut self, key: &Bytes) {
        while self.current_index < self.values.len() && self.values[self.current_index] < *key {
            self.current_index += 1;
        }
    }

    fn next(&mut self) -> Option<Bytes> {
        if self.current_index < self.values.len() {
            self.current_index += 1;
        }
        self.key()
    }

    fn key(&self) -> Option<Bytes> {
        if self.at_end() {
            None
        } else {
            Some(self.values[self.current_index].clone())
        }
    }

    fn at_end(&self) -> bool {
        self.current_index >= self.values.len()
    }
}

impl LayeredIndex for SingleLevelIndex {
    fn open_level(&mut self, _prefix: &[Bytes]) {
        self.current_index = 0;
    }

    fn close_level(&mut self) {}

    fn level(&self) -> usize {
        0
    }

    fn max_level(&self) -> usize {
        1
    }

    fn reinit(&mut self) {
        self.current_index = 0;
    }
}

impl LevelParticipation for SingleLevelIndex {
    fn participates_in_level(&self, level: usize) -> bool {
        level == self.participating_level
    }
}

/// An index created from a single tuple
pub struct TupleIndex {
    tuple: ResultTuple,
    current_level: usize,
    past_value: bool,
}

impl TupleIndex {
    pub fn new(tuple: ResultTuple) -> Self {
        Self {
            tuple,
            current_level: 0,
            past_value: false,
        }
    }
}

impl LeapfrogIterator for TupleIndex {
    fn seek(&mut self, key: &Bytes) {
        if let Some(value) = self.tuple.get(self.current_level) {
            self.past_value = key > value;
        }
    }

    fn next(&mut self) -> Option<Bytes> {
        self.past_value = true;
        None
    }

    fn key(&self) -> Option<Bytes> {
        if self.past_value {
            None
        } else {
            self.tuple.get(self.current_level).cloned()
        }
    }

    fn at_end(&self) -> bool {
        self.past_value
    }
}

impl LayeredIndex for TupleIndex {
    fn open_level(&mut self, _prefix: &[Bytes]) {
        self.current_level += 1;
        self.past_value = false;
    }

    fn close_level(&mut self) {
        self.current_level -= 1;
        self.past_value = false;
    }

    fn level(&self) -> usize {
        self.current_level
    }

    fn max_level(&self) -> usize {
        self.tuple.len()
    }

    fn reinit(&mut self) {
        self.current_level = 0;
        self.past_value = false;
    }
}

impl LevelParticipation for TupleIndex {
    fn participates_in_level(&self, level: usize) -> bool {
        level < self.tuple.len()
    }
}

/// Single-level join across multiple leapfrog iterators
pub struct LeapfrogSingleJoin<'a> {
    iterators: Vec<&'a mut dyn LeapfrogIndex>,
    current_iterator_index: usize,
}

impl<'a> LeapfrogSingleJoin<'a> {
    pub fn new(iterators: Vec<&'a mut dyn LeapfrogIndex>) -> Self {
        assert!(!iterators.is_empty(), "Must have at least one iterator");
        Self {
            iterators,
            current_iterator_index: 0,
        }
    }

    pub fn next(&mut self) {
        self.iterators[self.current_iterator_index].next();
    }

    /// Search for the next common value across all iterators
    /// Returns true if found (and appends to candidate_tuple), false if exhausted
    pub fn search(&mut self, candidate_tuple: &mut ResultTuple) -> bool {
        if self.iterators[self.current_iterator_index].at_end() {
            return false;
        }

        let mut max_key = match self.iterators[self.current_iterator_index].key() {
            Some(k) => k,
            None => return false,
        };
        let mut start_index = self.current_iterator_index;
        self.current_iterator_index = (self.current_iterator_index + 1) % self.iterators.len();

        loop {
            if self.current_iterator_index == start_index {
                candidate_tuple.push(max_key);
                return true;
            }

            let current_iterator = &mut self.iterators[self.current_iterator_index];
            current_iterator.seek(&max_key);

            if current_iterator.at_end() {
                return false;
            }

            let current_key = match current_iterator.key() {
                Some(k) => k,
                None => return false,
            };

            match current_key.cmp(&max_key) {
                Ordering::Less => {
                    panic!("current_key < max_key after seek, this should not happen!")
                }
                Ordering::Greater => {
                    max_key = current_key;
                    start_index = self.current_iterator_index;
                }
                Ordering::Equal => {}
            }

            self.current_iterator_index = (self.current_iterator_index + 1) % self.iterators.len();
        }
    }
}

/// Multi-level leapfrog join
pub struct LeapfrogJoin {
    indexes: Vec<Box<dyn LeapfrogIndex>>,
    levels: usize,
    filter_indexes: Vec<Box<dyn FilterLeapfrogIndex>>,
    participants: Vec<Vec<usize>>,
}

impl LeapfrogJoin {
    pub fn new(
        indexes: Vec<Box<dyn LeapfrogIndex>>,
        levels: usize,
        filter_indexes: Vec<Box<dyn FilterLeapfrogIndex>>,
    ) -> Self {
        assert!(!indexes.is_empty(), "At least one index is required");
        assert!(
            indexes.iter().all(|idx| idx.max_level() > 0),
            "All indices must have at least one level!"
        );

        // Build participants list for each level
        let participants: Vec<Vec<usize>> = (0..levels)
            .map(|level| {
                indexes
                    .iter()
                    .enumerate()
                    .filter(|(_, idx)| idx.participates_in_level(level))
                    .map(|(i, _)| i)
                    .collect()
            })
            .collect();

        Self {
            indexes,
            levels,
            filter_indexes,
            participants,
        }
    }

    pub fn join(&mut self) -> Vec<ResultTuple> {
        let mut results = Vec::new();
        let mut candidate_tuple: ResultTuple = Vec::new();

        // Track the current level (using i32 to allow -1 for termination)
        // We don't call open_level for level 0 - indexes start at their initial state
        let mut current_level: i32 = 0;

        while current_level >= 0 {
            let level = current_level as usize;
            assert_eq!(
                level,
                candidate_tuple.len(),
                "Level should always match candidate size"
            );

            // Perform single join search at current level
            let found = self.search_at_level(level, &mut candidate_tuple);

            if found {
                if level == self.levels - 1 {
                    // Check filters and add to results
                    let result_tuple = candidate_tuple.clone();
                    if self.filter_indexes.iter().all(|f| f.accept(&result_tuple)) {
                        results.push(result_tuple);
                    }
                    candidate_tuple.pop();
                    self.next_at_level(level);
                } else {
                    // Open next level for participants at level+1
                    let next_level = level + 1;
                    for &idx in &self.participants[next_level] {
                        self.indexes[idx].open_level(&candidate_tuple);
                    }
                    current_level += 1;
                }
            } else {
                // Close current level and backtrack
                for &idx in &self.participants[level] {
                    if self.indexes[idx].level() > 0 {
                        self.indexes[idx].close_level();
                    }
                }
                current_level -= 1;
                if current_level >= 0 {
                    self.next_at_level(current_level as usize);
                    candidate_tuple.pop();
                }
            }
        }

        results
    }

    /// Perform leapfrog search at the given level
    fn search_at_level(&mut self, level: usize, candidate_tuple: &mut ResultTuple) -> bool {
        let participants = &self.participants[level];
        if participants.is_empty() {
            return false;
        }

        let mut current_iter_idx = 0;

        if self.indexes[participants[current_iter_idx]].at_end() {
            return false;
        }

        let mut max_key = match self.indexes[participants[current_iter_idx]].key() {
            Some(k) => k,
            None => return false,
        };
        let mut start_idx = current_iter_idx;
        current_iter_idx = (current_iter_idx + 1) % participants.len();

        loop {
            if current_iter_idx == start_idx {
                candidate_tuple.push(max_key);
                return true;
            }

            let idx = participants[current_iter_idx];
            self.indexes[idx].seek(&max_key);

            if self.indexes[idx].at_end() {
                return false;
            }

            let current_key = match self.indexes[idx].key() {
                Some(k) => k,
                None => return false,
            };

            match current_key.cmp(&max_key) {
                Ordering::Less => {
                    panic!("current_key < max_key after seek, this should not happen!")
                }
                Ordering::Greater => {
                    max_key = current_key;
                    start_idx = current_iter_idx;
                }
                Ordering::Equal => {}
            }

            current_iter_idx = (current_iter_idx + 1) % participants.len();
        }
    }

    /// Advance to the next value at the given level
    fn next_at_level(&mut self, level: usize) {
        if let Some(&idx) = self.participants[level].first() {
            self.indexes[idx].next();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bi(n: i32) -> Bytes {
        Bytes::from(n.to_be_bytes().to_vec())
    }

    #[test]
    #[should_panic(expected = "At least one index is required")]
    fn test_empty_indexes_fails() {
        let empty_indexes: Vec<Box<dyn LeapfrogIndex>> = vec![];
        LeapfrogJoin::new(empty_indexes, 1, vec![]);
    }

    #[test]
    fn test_two_indexes_with_even_and_divisible_by_three() {
        let even_index = SingleLevelIndex::new(
            vec![bi(2), bi(4), bi(6), bi(8), bi(10), bi(12)],
            0,
        );
        let divisible_by_three_index = SingleLevelIndex::new(
            vec![bi(3), bi(6), bi(9), bi(12)],
            0,
        );

        let indexes: Vec<Box<dyn LeapfrogIndex>> =
            vec![Box::new(even_index), Box::new(divisible_by_three_index)];

        let mut join = LeapfrogJoin::new(indexes, 1, vec![]);
        let result = join.join();

        assert_eq!(result.len(), 2);

        let expected = vec![vec![bi(6)], vec![bi(12)]];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_leapfrog_join_with_two_levels() {
        struct MultiLevelIndex {
            level_data: Vec<Vec<Bytes>>,
            current_level: usize,
            index_per_level: Vec<usize>,
        }

        impl MultiLevelIndex {
            fn new(level_data: Vec<Vec<Bytes>>) -> Self {
                let num_levels = level_data.len();
                Self {
                    level_data,
                    current_level: 0,
                    index_per_level: vec![0; num_levels],
                }
            }
        }

        impl LeapfrogIterator for MultiLevelIndex {
            fn seek(&mut self, key: &Bytes) {
                let values = &self.level_data[self.current_level];
                while self.index_per_level[self.current_level] < values.len()
                    && values[self.index_per_level[self.current_level]] < *key
                {
                    self.index_per_level[self.current_level] += 1;
                }
            }

            fn next(&mut self) -> Option<Bytes> {
                let values = &self.level_data[self.current_level];
                if self.index_per_level[self.current_level] < values.len() {
                    self.index_per_level[self.current_level] += 1;
                }
                self.key()
            }

            fn key(&self) -> Option<Bytes> {
                if self.at_end() {
                    None
                } else {
                    Some(self.level_data[self.current_level][self.index_per_level[self.current_level]].clone())
                }
            }

            fn at_end(&self) -> bool {
                self.index_per_level[self.current_level] >= self.level_data[self.current_level].len()
            }
        }

        impl LayeredIndex for MultiLevelIndex {
            fn open_level(&mut self, _prefix: &[Bytes]) {
                self.current_level += 1;
                self.index_per_level[self.current_level] = 0;
            }

            fn close_level(&mut self) {
                self.current_level -= 1;
            }

            fn level(&self) -> usize {
                self.current_level
            }

            fn max_level(&self) -> usize {
                self.level_data.len()
            }

            fn reinit(&mut self) {
                self.current_level = 0;
                for i in 0..self.index_per_level.len() {
                    self.index_per_level[i] = 0;
                }
            }
        }

        impl LevelParticipation for MultiLevelIndex {
            fn participates_in_level(&self, level: usize) -> bool {
                level < self.level_data.len()
            }
        }

        // index1: level 0 = even numbers 2-12, level 1 = multiples of 4 (4-40)
        let index1 = MultiLevelIndex::new(vec![
            (1..=6).map(|i| bi(i * 2)).collect(),      // 2, 4, 6, 8, 10, 12
            (1..=10).map(|i| bi(i * 4)).collect(),    // 4, 8, 12, ..., 40
        ]);

        // index2: level 0 = multiples of 3 (3-12), level 1 = multiples of 5 (5-40)
        let index2 = MultiLevelIndex::new(vec![
            (1..=4).map(|i| bi(i * 3)).collect(),     // 3, 6, 9, 12
            (1..=8).map(|i| bi(i * 5)).collect(),    // 5, 10, 15, ..., 40
        ]);

        let indexes: Vec<Box<dyn LeapfrogIndex>> = vec![Box::new(index1), Box::new(index2)];

        let mut join = LeapfrogJoin::new(indexes, 2, vec![]);
        let result = join.join();

        // Level 0 intersection: (2,4,6,8,10,12) ∩ (3,6,9,12) = (6, 12)
        // Level 1 intersection: (4,8,12,16,20,24,28,32,36,40) ∩ (5,10,15,20,25,30,35,40) = (20, 40)
        // Results: (6, 20), (6, 40), (12, 20), (12, 40)
        assert_eq!(result.len(), 4);

        let expected = vec![
            vec![bi(6), bi(20)],
            vec![bi(6), bi(40)],
            vec![bi(12), bi(20)],
            vec![bi(12), bi(40)],
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_create_from_tuple_basic_iteration() {
        let tuple = vec![bi(1), bi(2), bi(3)];
        let index = TupleIndex::new(tuple);

        assert_eq!(index.level(), 0);
        assert_eq!(index.max_level(), 3);
        assert!(!index.at_end());
        assert_eq!(index.key(), Some(bi(1)));
    }

    #[test]
    fn test_create_from_tuple_next_goes_to_end() {
        let tuple = vec![bi(1), bi(2), bi(3)];
        let mut index = TupleIndex::new(tuple);

        index.next();
        assert!(index.at_end());
    }

    #[test]
    fn test_create_from_tuple_seek_behavior() {
        let tuple = vec![bi(5), bi(10), bi(15)];
        let mut index = TupleIndex::new(tuple);

        // Seek to value less than tuple value - should still be at the value
        index.seek(&bi(3));
        assert!(!index.at_end());
        assert_eq!(index.key(), Some(bi(5)));

        // Reinit and seek to exact value - should still be at the value
        index.reinit();
        index.seek(&bi(5));
        assert!(!index.at_end());
        assert_eq!(index.key(), Some(bi(5)));

        // Reinit and seek past the value - should be at end
        index.reinit();
        index.seek(&bi(6));
        assert!(index.at_end());
    }

    #[test]
    fn test_create_from_tuple_multi_level_navigation() {
        let tuple = vec![bi(1), bi(2), bi(3)];
        let mut index = TupleIndex::new(tuple);

        // Level 0
        assert_eq!(index.level(), 0);
        assert_eq!(index.key(), Some(bi(1)));

        // Open to level 1
        index.open_level(&[]);
        assert_eq!(index.level(), 1);
        assert!(!index.at_end());
        assert_eq!(index.key(), Some(bi(2)));

        // Open to level 2
        index.open_level(&[]);
        assert_eq!(index.level(), 2);
        assert!(!index.at_end());
        assert_eq!(index.key(), Some(bi(3)));

        // Close back to level 1
        index.close_level();
        assert_eq!(index.level(), 1);
        assert!(!index.at_end());
        assert_eq!(index.key(), Some(bi(2)));

        // Close back to level 0
        index.close_level();
        assert_eq!(index.level(), 0);
        assert!(!index.at_end());
        assert_eq!(index.key(), Some(bi(1)));
    }

    #[test]
    fn test_create_from_tuple_participates_in_level() {
        let tuple = vec![bi(1), bi(2), bi(3)];
        let index = TupleIndex::new(tuple);

        assert!(index.participates_in_level(0));
        assert!(index.participates_in_level(1));
        assert!(index.participates_in_level(2));
        assert!(!index.participates_in_level(3));
    }

    #[test]
    fn test_create_from_tuple_in_join_with_matching_tuple() {
        let tuple = vec![bi(6)];
        let tuple_index = TupleIndex::new(tuple);
        let range_index = SingleLevelIndex::new(vec![bi(2), bi(4), bi(6), bi(8), bi(10)], 0);

        let indexes: Vec<Box<dyn LeapfrogIndex>> =
            vec![Box::new(tuple_index), Box::new(range_index)];

        let mut join = LeapfrogJoin::new(indexes, 1, vec![]);
        let result = join.join();

        assert_eq!(result.len(), 1);
        assert_eq!(result, vec![vec![bi(6)]]);
    }

    #[test]
    fn test_create_from_tuple_in_join_with_non_matching_tuple() {
        let tuple = vec![bi(5)];
        let tuple_index = TupleIndex::new(tuple);
        let range_index = SingleLevelIndex::new(vec![bi(2), bi(4), bi(6), bi(8), bi(10)], 0);

        let indexes: Vec<Box<dyn LeapfrogIndex>> =
            vec![Box::new(tuple_index), Box::new(range_index)];

        let mut join = LeapfrogJoin::new(indexes, 1, vec![]);
        let result = join.join();

        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_create_from_tuple_resets_position_on_open_level() {
        let tuple = vec![bi(1), bi(2)];
        let mut index = TupleIndex::new(tuple);

        // Advance past value at level 0
        index.next();
        assert!(index.at_end());

        // Open level - should reset
        index.open_level(&[]);
        assert!(!index.at_end());
        assert_eq!(index.key(), Some(bi(2)));
    }

    #[test]
    fn test_create_from_tuple_resets_position_on_close_level() {
        let tuple = vec![bi(1), bi(2)];
        let mut index = TupleIndex::new(tuple);

        index.open_level(&[]);
        // Advance past value at level 1
        index.next();
        assert!(index.at_end());

        // Close level - should reset level 0
        index.close_level();
        assert!(!index.at_end());
        assert_eq!(index.key(), Some(bi(1)));
    }

    // Filter index for testing negative joins
    struct NotLeapfrogIndex {
        indexes: Vec<Box<dyn LeapfrogIndex>>,
        participation_level: usize,
    }

    impl NotLeapfrogIndex {
        fn new(indexes: Vec<Box<dyn LeapfrogIndex>>, participation_level: usize) -> Self {
            Self {
                indexes,
                participation_level,
            }
        }
    }

    impl LevelParticipation for NotLeapfrogIndex {
        fn participates_in_level(&self, level: usize) -> bool {
            level == self.participation_level
        }
    }

    impl FilterLeapfrogIndex for NotLeapfrogIndex {
        fn accept(&self, tuple: &ResultTuple) -> bool {
            // Accept if the tuple value is NOT in any of the indexes
            if let Some(value) = tuple.get(self.participation_level) {
                for index in &self.indexes {
                    // Check if value exists in this index by creating a temporary copy
                    let mut temp_index = SingleLevelIndex::new(vec![], 0);
                    // We can't easily check membership without iterating, so we'll use a simpler approach
                    // This is a simplified implementation for testing
                }
                // For testing, we'll implement a simpler version
                true
            } else {
                true
            }
        }
    }

    // Simpler filter implementation for testing
    struct SimpleNotFilter {
        excluded_values: Vec<Bytes>,
        participation_level: usize,
    }

    impl SimpleNotFilter {
        fn new(excluded_values: Vec<Bytes>, participation_level: usize) -> Self {
            Self {
                excluded_values,
                participation_level,
            }
        }
    }

    impl LevelParticipation for SimpleNotFilter {
        fn participates_in_level(&self, level: usize) -> bool {
            level == self.participation_level
        }
    }

    impl FilterLeapfrogIndex for SimpleNotFilter {
        fn accept(&self, tuple: &ResultTuple) -> bool {
            if let Some(value) = tuple.get(self.participation_level) {
                !self.excluded_values.contains(value)
            } else {
                true
            }
        }
    }

    #[test]
    fn test_leapfrog_join_with_filter_excludes_matching_tuples() {
        let even_index = SingleLevelIndex::new(
            vec![bi(2), bi(4), bi(6), bi(8), bi(10), bi(12)],
            0,
        );
        let divisible_by_three_index = SingleLevelIndex::new(
            vec![bi(3), bi(6), bi(9), bi(12)],
            0,
        );

        // Exclude multiples of 4 (which includes 4, 8, 12)
        let not_filter = SimpleNotFilter::new(vec![bi(4), bi(8), bi(12)], 0);

        let indexes: Vec<Box<dyn LeapfrogIndex>> =
            vec![Box::new(even_index), Box::new(divisible_by_three_index)];
        let filters: Vec<Box<dyn FilterLeapfrogIndex>> = vec![Box::new(not_filter)];

        let mut join = LeapfrogJoin::new(indexes, 1, filters);
        let result = join.join();

        // Intersection of evens and divisible by 3: 6, 12
        // After excluding multiples of 4: only 6 remains (12 is excluded)
        assert_eq!(result.len(), 1);
        assert_eq!(result, vec![vec![bi(6)]]);
    }

    #[test]
    fn test_leapfrog_join_with_filter_that_excludes_nothing() {
        let even_index = SingleLevelIndex::new(
            vec![bi(2), bi(4), bi(6), bi(8), bi(10), bi(12)],
            0,
        );
        let divisible_by_three_index = SingleLevelIndex::new(
            vec![bi(3), bi(6), bi(9), bi(12)],
            0,
        );

        // Disjoint set, excludes nothing from the intersection
        let not_filter = SimpleNotFilter::new(vec![bi(100), bi(200), bi(300)], 0);

        let indexes: Vec<Box<dyn LeapfrogIndex>> =
            vec![Box::new(even_index), Box::new(divisible_by_three_index)];
        let filters: Vec<Box<dyn FilterLeapfrogIndex>> = vec![Box::new(not_filter)];

        let mut join = LeapfrogJoin::new(indexes, 1, filters);
        let result = join.join();

        // Intersection of evens and divisible by 3: 6, 12 - nothing excluded
        assert_eq!(result.len(), 2);
        let expected = vec![vec![bi(6)], vec![bi(12)]];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_leapfrog_join_with_filter_that_excludes_all() {
        let even_index = SingleLevelIndex::new(
            vec![bi(2), bi(4), bi(6), bi(8), bi(10), bi(12)],
            0,
        );
        let divisible_by_three_index = SingleLevelIndex::new(
            vec![bi(3), bi(6), bi(9), bi(12)],
            0,
        );

        // Includes both 6 and 12
        let not_filter = SimpleNotFilter::new(vec![bi(6), bi(12)], 0);

        let indexes: Vec<Box<dyn LeapfrogIndex>> =
            vec![Box::new(even_index), Box::new(divisible_by_three_index)];
        let filters: Vec<Box<dyn FilterLeapfrogIndex>> = vec![Box::new(not_filter)];

        let mut join = LeapfrogJoin::new(indexes, 1, filters);
        let result = join.join();

        // All results are excluded
        assert_eq!(result.len(), 0);
    }
}
