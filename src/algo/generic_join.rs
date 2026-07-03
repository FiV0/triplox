use anyhow::Error;
use bytes::Bytes;
use std::collections::HashSet;

pub type Prefix = Vec<Bytes>;
pub type Extension = Bytes;
pub type ResultTuple = Vec<Bytes>;

// generic-join based on
// http://www.frankmcsherry.org/dataflow/relational/join/2015/04/11/genericjoin.html
// https://arxiv.org/abs/1310.3314

pub trait PrefixExtender {
    fn count(&self, prefix: &Prefix) -> Result<usize, Error>;
    fn propose(&self, prefix: &Prefix) -> Result<Vec<Extension>, Error>;
    fn intersect(&self, prefix: &Prefix, extensions: &[Extension])
        -> Result<Vec<Extension>, Error>;
    fn participates_in_level(&self, level: usize) -> bool;
}

/// Applies extensions to a prefix, creating new result tuples
pub fn apply_extensions(prefix: &Prefix, extensions: &[Extension]) -> Vec<ResultTuple> {
    extensions
        .iter()
        .map(|ext| {
            let mut tuple = prefix.clone();
            tuple.push(ext.clone());
            tuple
        })
        .collect()
}

/// A simple extender that participates in a single level with a fixed set of values
pub struct SingleLevelExtender {
    values: Vec<Bytes>,
    participates_in: usize,
}

impl SingleLevelExtender {
    pub fn new(values: Vec<Bytes>, participates_in: usize) -> Self {
        let mut values = values;
        values.sort();
        Self {
            values,
            participates_in,
        }
    }
}

impl PrefixExtender for SingleLevelExtender {
    fn count(&self, _prefix: &Prefix) -> Result<usize, Error> {
        Ok(self.values.len())
    }

    fn propose(&self, _prefix: &Prefix) -> Result<Vec<Extension>, Error> {
        Ok(self.values.clone())
    }

    fn intersect(
        &self,
        _prefix: &Prefix,
        extensions: &[Extension],
    ) -> Result<Vec<Extension>, Error> {
        let value_set: HashSet<_> = self.values.iter().collect();
        Ok(extensions
            .iter()
            .filter(|ext| value_set.contains(ext))
            .cloned()
            .collect())
    }

    fn participates_in_level(&self, level: usize) -> bool {
        level == self.participates_in
    }
}

/// An extender that extends a single tuple
pub struct TupleExtender {
    tuple: ResultTuple,
}

impl TupleExtender {
    pub fn new(tuple: ResultTuple) -> Self {
        Self { tuple }
    }

    fn is_prefix_matching(&self, prefix: &Prefix) -> bool {
        prefix.len() <= self.tuple.len()
            && prefix.iter().zip(self.tuple.iter()).all(|(a, b)| a == b)
    }
}

impl PrefixExtender for TupleExtender {
    fn count(&self, prefix: &Prefix) -> Result<usize, Error> {
        Ok(if self.is_prefix_matching(prefix) {
            1
        } else {
            0
        })
    }

    fn propose(&self, prefix: &Prefix) -> Result<Vec<Extension>, Error> {
        Ok(
            if self.is_prefix_matching(prefix) && prefix.len() < self.tuple.len() {
                vec![self.tuple[prefix.len()].clone()]
            } else {
                vec![]
            },
        )
    }

    fn intersect(
        &self,
        prefix: &Prefix,
        extensions: &[Extension],
    ) -> Result<Vec<Extension>, Error> {
        Ok(
            if self.is_prefix_matching(prefix) && prefix.len() < self.tuple.len() {
                let next_val = &self.tuple[prefix.len()];
                if extensions.contains(next_val) {
                    vec![next_val.clone()]
                } else {
                    vec![]
                }
            } else {
                vec![]
            },
        )
    }

    fn participates_in_level(&self, level: usize) -> bool {
        level < self.tuple.len()
    }
}

/// An extender with participating levels and a partial prefix
/// TODO: If we pass to set-semantics, this implementation can be delegated to the trie implementation.
pub struct FromPrefixExtender {
    participating_levels: Vec<usize>,
    level_set: HashSet<usize>,
    partial_prefix: Prefix,
}

impl FromPrefixExtender {
    pub fn new(participating_levels: Vec<usize>, partial_prefix: Prefix) -> Self {
        debug_assert!(
            participating_levels.windows(2).all(|w| w[0] < w[1]),
            "participating_levels must be sorted strictly ascending, got {:?}",
            participating_levels
        );
        let level_set = participating_levels.iter().cloned().collect();
        Self {
            participating_levels,
            level_set,
            partial_prefix,
        }
    }

    fn is_prefix_matching(&self, prefix: &Prefix) -> bool {
        for (i, &level) in self.participating_levels.iter().enumerate() {
            if level >= prefix.len() {
                break;
            }
            if self.partial_prefix[i] != prefix[level] {
                return false;
            }
        }
        true
    }

    fn next_level_index(&self, prefix: &Prefix) -> usize {
        let mut i = 0;
        for &level in &self.participating_levels {
            if level >= prefix.len() {
                break;
            }
            i += 1;
        }
        i
    }
}

impl PrefixExtender for FromPrefixExtender {
    fn count(&self, prefix: &Prefix) -> Result<usize, Error> {
        Ok(if self.is_prefix_matching(prefix) {
            1
        } else {
            0
        })
    }

    fn propose(&self, prefix: &Prefix) -> Result<Vec<Extension>, Error> {
        if !self.is_prefix_matching(prefix) {
            return Ok(vec![]);
        }
        let idx = self.next_level_index(prefix);
        Ok(if idx < self.partial_prefix.len() {
            vec![self.partial_prefix[idx].clone()]
        } else {
            vec![]
        })
    }

    fn intersect(
        &self,
        prefix: &Prefix,
        extensions: &[Extension],
    ) -> Result<Vec<Extension>, Error> {
        if !self.is_prefix_matching(prefix) {
            return Ok(vec![]);
        }
        let idx = self.next_level_index(prefix);
        Ok(
            if idx < self.partial_prefix.len() && extensions.contains(&self.partial_prefix[idx]) {
                vec![self.partial_prefix[idx].clone()]
            } else {
                vec![]
            },
        )
    }

    fn participates_in_level(&self, level: usize) -> bool {
        self.level_set.contains(&level)
    }
}

/// An extender with participating levels and multiple partial prefixes
pub struct FromPrefixesExtender {
    participating_levels: Vec<usize>,
    level_set: HashSet<usize>,
    partial_prefixes: Vec<Prefix>,
}

impl FromPrefixesExtender {
    pub fn new(participating_levels: Vec<usize>, partial_prefixes: Vec<Prefix>) -> Self {
        debug_assert!(
            participating_levels.windows(2).all(|w| w[0] < w[1]),
            "participating_levels must be sorted strictly ascending, got {:?}",
            participating_levels
        );
        let level_set = participating_levels.iter().cloned().collect();
        Self {
            participating_levels,
            level_set,
            partial_prefixes,
        }
    }

    fn matching_prefixes(&self, prefix: &Prefix) -> Vec<&Prefix> {
        self.partial_prefixes
            .iter()
            .filter(|partial_prefix| {
                for (i, &level) in self.participating_levels.iter().enumerate() {
                    if level >= prefix.len() {
                        break;
                    }
                    if partial_prefix[i] != prefix[level] {
                        return false;
                    }
                }
                true
            })
            .collect()
    }

    fn next_level_index(&self, prefix: &Prefix) -> usize {
        let mut i = 0;
        for &level in &self.participating_levels {
            if level >= prefix.len() {
                break;
            }
            i += 1;
        }
        i
    }
}

impl PrefixExtender for FromPrefixesExtender {
    fn count(&self, prefix: &Prefix) -> Result<usize, Error> {
        Ok(self.matching_prefixes(prefix).len())
    }

    fn propose(&self, prefix: &Prefix) -> Result<Vec<Extension>, Error> {
        let idx = self.next_level_index(prefix);
        let mut extensions: Vec<_> = self
            .matching_prefixes(prefix)
            .into_iter()
            .filter_map(|p| p.get(idx).cloned())
            .collect();
        extensions.sort();
        extensions.dedup();
        Ok(extensions)
    }

    fn intersect(
        &self,
        prefix: &Prefix,
        extensions: &[Extension],
    ) -> Result<Vec<Extension>, Error> {
        let idx = self.next_level_index(prefix);
        let valid_extensions: HashSet<_> = self
            .matching_prefixes(prefix)
            .into_iter()
            .filter_map(|p| p.get(idx))
            .collect();
        Ok(extensions
            .iter()
            .filter(|ext| valid_extensions.contains(ext))
            .cloned()
            .collect())
    }

    fn participates_in_level(&self, level: usize) -> bool {
        self.level_set.contains(&level)
    }
}

/// Performs a single-level join across multiple extenders
pub struct GenericSingleJoin<'a> {
    extenders: Vec<&'a dyn PrefixExtender>,
    prefixes: Vec<Prefix>,
}

impl<'a> GenericSingleJoin<'a> {
    pub fn new(extenders: Vec<&'a dyn PrefixExtender>, prefixes: Vec<Prefix>) -> Self {
        assert!(!extenders.is_empty(), "At least one extender is required");
        Self {
            extenders,
            prefixes,
        }
    }

    pub fn join(&self) -> Result<Vec<Prefix>, Error> {
        let mut results = Vec::new();

        for prefix in &self.prefixes {
            // Find the extender that proposes the least extensions
            let counts = self
                .extenders
                .iter()
                .map(|ext| ext.count(prefix))
                .collect::<Result<Vec<_>, _>>()?;
            let min_index = counts
                .iter()
                .enumerate()
                .min_by_key(|(_, count)| **count)
                .map(|(i, _)| i)
                .unwrap();

            // Propose extensions from that extender
            let mut extensions = self.extenders[min_index].propose(prefix)?;

            // Intersect with all other extenders
            for (i, extender) in self.extenders.iter().enumerate() {
                if i != min_index {
                    extensions = extender.intersect(prefix, &extensions)?;
                }
            }

            results.extend(apply_extensions(prefix, &extensions));
        }

        Ok(results)
    }
}

/// A prefix extender that pins the inner join to a fixed prefix and a set of candidate extensions.
///
/// Used by GenericNotPrefixExtender to constrain the inner join: at levels below the extension
/// level it matches exact prefix values, and at the extension level it proposes/intersects
/// from the candidate set.
pub struct PrefixAndExtensionsExtender {
    fixed_prefix: Prefix,
    fixed_extensions: Vec<Extension>,
    extension_set: HashSet<Extension>,
}

impl PrefixAndExtensionsExtender {
    pub fn new(fixed_prefix: Prefix, fixed_extensions: Vec<Extension>) -> Self {
        let extension_set = fixed_extensions.iter().cloned().collect();
        Self {
            fixed_prefix,
            fixed_extensions,
            extension_set,
        }
    }

    fn is_prefix_matching(&self, prefix: &Prefix) -> bool {
        prefix.len() <= self.fixed_prefix.len()
            && prefix
                .iter()
                .zip(self.fixed_prefix.iter())
                .all(|(a, b)| a == b)
    }
}

impl PrefixExtender for PrefixAndExtensionsExtender {
    fn count(&self, prefix: &Prefix) -> Result<usize, Error> {
        Ok(if self.is_prefix_matching(prefix) {
            if prefix.len() < self.fixed_prefix.len() {
                1
            } else {
                self.fixed_extensions.len()
            }
        } else {
            0
        })
    }

    fn propose(&self, prefix: &Prefix) -> Result<Vec<Extension>, Error> {
        Ok(if self.is_prefix_matching(prefix) {
            if prefix.len() < self.fixed_prefix.len() {
                vec![self.fixed_prefix[prefix.len()].clone()]
            } else {
                self.fixed_extensions.clone()
            }
        } else {
            vec![]
        })
    }

    fn intersect(
        &self,
        prefix: &Prefix,
        extensions: &[Extension],
    ) -> Result<Vec<Extension>, Error> {
        Ok(if self.is_prefix_matching(prefix) {
            if prefix.len() < self.fixed_prefix.len() {
                let next_val = &self.fixed_prefix[prefix.len()];
                if extensions.contains(next_val) {
                    vec![next_val.clone()]
                } else {
                    vec![]
                }
            } else {
                extensions
                    .iter()
                    .filter(|ext| self.extension_set.contains(*ext))
                    .cloned()
                    .collect()
            }
        } else {
            vec![]
        })
    }

    fn participates_in_level(&self, level: usize) -> bool {
        level <= self.fixed_prefix.len()
    }
}

/// Multi-level generic join
pub struct GenericJoin<'a> {
    extenders: Vec<&'a dyn PrefixExtender>,
    levels: usize,
}

impl<'a> GenericJoin<'a> {
    pub fn new(extenders: Vec<&'a dyn PrefixExtender>, levels: usize) -> Self {
        Self { extenders, levels }
    }

    pub fn join(&self) -> Result<Vec<ResultTuple>, Error> {
        let mut prefixes: Vec<Prefix> = vec![vec![]];

        // For every level, perform a single join with the extenders participating in that level
        for level in 0..self.levels {
            let level_extenders: Vec<_> = self
                .extenders
                .iter()
                .filter(|ext| ext.participates_in_level(level))
                .cloned()
                .collect();

            if level_extenders.is_empty() {
                continue;
            }

            let single_join = GenericSingleJoin::new(level_extenders, prefixes);
            prefixes = single_join.join()?;
        }

        Ok(prefixes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    fn bi(n: i32) -> Bytes {
        Bytes::from(n.to_be_bytes().to_vec())
    }

    #[test]
    #[should_panic(expected = "At least one extender is required")]
    fn test_empty_extenders_fails() {
        let empty_extenders: Vec<&dyn PrefixExtender> = vec![];
        let prefixes = vec![vec![]];
        GenericSingleJoin::new(empty_extenders, prefixes);
    }

    #[test]
    fn test_two_extenders_with_even_and_divisible_by_three() {
        let even_extender =
            SingleLevelExtender::new(vec![bi(2), bi(4), bi(6), bi(8), bi(10), bi(12)], 0);
        let divisible_by_three_extender =
            SingleLevelExtender::new(vec![bi(3), bi(6), bi(9), bi(12)], 0);

        let extenders: Vec<&dyn PrefixExtender> =
            vec![&even_extender, &divisible_by_three_extender];
        let prefixes = vec![vec![]];

        let join = GenericSingleJoin::new(extenders, prefixes);
        let result = join.join().unwrap();

        assert_eq!(result.len(), 2);

        let expected = vec![vec![bi(6)], vec![bi(12)]];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_generic_join_with_two_levels() {
        // First level: even numbers and numbers divisible by 3
        let even_extender =
            SingleLevelExtender::new(vec![bi(2), bi(4), bi(6), bi(8), bi(10), bi(12)], 0);
        let divisible_by_three_extender =
            SingleLevelExtender::new(vec![bi(3), bi(6), bi(9), bi(12)], 0);

        // Second level: multiples of 6 and 12
        let multiples_of_six = SingleLevelExtender::new(vec![bi(6), bi(12)], 1);

        let extenders: Vec<&dyn PrefixExtender> = vec![
            &even_extender,
            &divisible_by_three_extender,
            &multiples_of_six,
        ];

        let join = GenericJoin::new(extenders, 2);
        let result = join.join().unwrap();

        // First level produces: [6] and [12]
        // Second level adds 6 and 12 to each
        assert_eq!(result.len(), 4);

        let expected = vec![
            vec![bi(6), bi(6)],
            vec![bi(6), bi(12)],
            vec![bi(12), bi(6)],
            vec![bi(12), bi(12)],
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_from_prefix_extender_participates_in_correct_levels() {
        let extender = FromPrefixExtender::new(vec![0, 2], vec![b("a"), b("b")]);

        assert!(extender.participates_in_level(0));
        assert!(!extender.participates_in_level(1));
        assert!(extender.participates_in_level(2));
        assert!(!extender.participates_in_level(3));
    }

    #[test]
    fn test_from_prefix_extender_count_with_matching_prefix() {
        // Extender expects value "x" at level 0 and "y" at level 2
        let extender = FromPrefixExtender::new(vec![0, 2], vec![b("x"), b("y")]);

        // Empty prefix - no levels to check yet, so it matches
        assert_eq!(extender.count(&vec![]).unwrap(), 1);

        // Prefix with matching value at level 0
        assert_eq!(extender.count(&vec![b("x")]).unwrap(), 1);

        // Prefix with non-matching value at level 0
        assert_eq!(extender.count(&vec![b("z")]).unwrap(), 0);

        // Prefix with matching values at levels 0 and 2 (level 1 can be anything)
        assert_eq!(
            extender
                .count(&vec![b("x"), b("anything"), b("y")])
                .unwrap(),
            1
        );

        // Prefix with matching value at level 0 but non-matching at level 2
        assert_eq!(
            extender
                .count(&vec![b("x"), b("anything"), b("z")])
                .unwrap(),
            0
        );
    }

    #[test]
    fn test_from_prefix_extender_propose_with_matching_prefix() {
        // Extender expects value "x" at level 0 and "y" at level 2
        let extender = FromPrefixExtender::new(vec![0, 2], vec![b("x"), b("y")]);

        // Empty prefix - propose first value "x"
        assert_eq!(extender.propose(&vec![]).unwrap(), vec![b("x")]);

        // Prefix with matching value at level 0 - propose "y" (next value for level 2)
        assert_eq!(extender.propose(&vec![b("x")]).unwrap(), vec![b("y")]);

        // Prefix with non-matching value at level 0
        assert_eq!(
            extender.propose(&vec![b("z")]).unwrap(),
            Vec::<Bytes>::new()
        );

        // Prefix with matching value at level 0, arbitrary value at level 1
        assert_eq!(
            extender.propose(&vec![b("x"), b("anything")]).unwrap(),
            vec![b("y")]
        );
    }

    #[test]
    fn test_from_prefix_extender_intersect_with_matching_prefix() {
        // Extender expects value "x" at level 0 and "y" at level 2
        let extender = FromPrefixExtender::new(vec![0, 2], vec![b("x"), b("y")]);

        // Empty prefix, extensions contain the expected value
        assert_eq!(
            extender
                .intersect(&vec![], &[b("x"), b("a"), b("b")])
                .unwrap(),
            vec![b("x")]
        );

        // Empty prefix, extensions don't contain the expected value
        assert_eq!(
            extender
                .intersect(&vec![], &[b("a"), b("b"), b("c")])
                .unwrap(),
            Vec::<Bytes>::new()
        );

        // Matching prefix at level 0, extensions contain expected value "y"
        assert_eq!(
            extender
                .intersect(&vec![b("x")], &[b("y"), b("z")])
                .unwrap(),
            vec![b("y")]
        );

        // Matching prefix at level 0, extensions don't contain expected value
        assert_eq!(
            extender
                .intersect(&vec![b("x")], &[b("a"), b("b")])
                .unwrap(),
            Vec::<Bytes>::new()
        );

        // Non-matching prefix
        assert_eq!(
            extender
                .intersect(&vec![b("wrong")], &[b("x"), b("y")])
                .unwrap(),
            Vec::<Bytes>::new()
        );
    }

    #[test]
    fn test_from_prefix_extender_in_generic_join() {
        // Create a scenario with 3 levels
        // Level 0: values 1, 2, 3
        // Level 1: values "a", "b"
        // Level 2: values "x", "y"

        let level0_extender = SingleLevelExtender::new(vec![bi(1), bi(2), bi(3)], 0);
        let level1_extender = SingleLevelExtender::new(vec![b("a"), b("b")], 1);
        let level2_extender = SingleLevelExtender::new(vec![b("x"), b("y")], 2);

        // This extender constrains: level 0 = 2, level 2 = "x"
        let constraint_extender = FromPrefixExtender::new(vec![0, 2], vec![bi(2), b("x")]);

        let extenders: Vec<&dyn PrefixExtender> = vec![
            &level0_extender,
            &level1_extender,
            &level2_extender,
            &constraint_extender,
        ];

        let join = GenericJoin::new(extenders, 3);
        let result = join.join().unwrap();

        // Only tuples where level 0 = 2 and level 2 = "x" should remain
        // Level 1 can be "a" or "b"
        let expected = vec![vec![bi(2), b("a"), b("x")], vec![bi(2), b("b"), b("x")]];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_from_prefixes_extender_participates_in_correct_levels() {
        let extender =
            FromPrefixesExtender::new(vec![0, 2], vec![vec![b("a"), b("b")], vec![b("x"), b("y")]]);

        assert!(extender.participates_in_level(0));
        assert!(!extender.participates_in_level(1));
        assert!(extender.participates_in_level(2));
        assert!(!extender.participates_in_level(3));
    }

    #[test]
    fn test_from_prefixes_extender_count_with_matching_prefixes() {
        // Extender accepts two prefixes: ["a", "b"] or ["x", "y"] at levels [0, 2]
        let extender =
            FromPrefixesExtender::new(vec![0, 2], vec![vec![b("a"), b("b")], vec![b("x"), b("y")]]);

        // Empty prefix - both prefixes match
        assert_eq!(extender.count(&vec![]).unwrap(), 2);

        // Prefix with "a" at level 0 - only first prefix matches
        assert_eq!(extender.count(&vec![b("a")]).unwrap(), 1);

        // Prefix with "x" at level 0 - only second prefix matches
        assert_eq!(extender.count(&vec![b("x")]).unwrap(), 1);

        // Prefix with non-matching value at level 0
        assert_eq!(extender.count(&vec![b("z")]).unwrap(), 0);

        // Full match for first prefix
        assert_eq!(
            extender
                .count(&vec![b("a"), b("anything"), b("b")])
                .unwrap(),
            1
        );

        // Full match for second prefix
        assert_eq!(
            extender
                .count(&vec![b("x"), b("anything"), b("y")])
                .unwrap(),
            1
        );

        // Partial match at level 0 but mismatch at level 2
        assert_eq!(
            extender
                .count(&vec![b("a"), b("anything"), b("y")])
                .unwrap(),
            0
        );
    }

    #[test]
    fn test_from_prefixes_extender_propose_with_matching_prefixes() {
        let extender =
            FromPrefixesExtender::new(vec![0, 2], vec![vec![b("a"), b("b")], vec![b("x"), b("y")]]);

        // Empty prefix - propose first values from both prefixes (distinct, sorted)
        assert_eq!(extender.propose(&vec![]).unwrap(), vec![b("a"), b("x")]);

        // Prefix with "a" at level 0 - propose "b" from first prefix
        assert_eq!(extender.propose(&vec![b("a")]).unwrap(), vec![b("b")]);

        // Prefix with "x" at level 0 - propose "y" from second prefix
        assert_eq!(extender.propose(&vec![b("x")]).unwrap(), vec![b("y")]);

        // Non-matching prefix
        assert_eq!(
            extender.propose(&vec![b("z")]).unwrap(),
            Vec::<Bytes>::new()
        );

        // Prefix with "a" at level 0, anything at level 1 - propose "b"
        assert_eq!(
            extender.propose(&vec![b("a"), b("anything")]).unwrap(),
            vec![b("b")]
        );
    }

    #[test]
    fn test_from_prefixes_extender_intersect_with_matching_prefixes() {
        let extender =
            FromPrefixesExtender::new(vec![0, 2], vec![vec![b("a"), b("b")], vec![b("x"), b("y")]]);

        // Empty prefix, extensions contain both expected values
        assert_eq!(
            extender
                .intersect(&vec![], &[b("a"), b("x"), b("z")])
                .unwrap(),
            vec![b("a"), b("x")]
        );

        // Empty prefix, extensions contain only one expected value
        assert_eq!(
            extender.intersect(&vec![], &[b("a"), b("z")]).unwrap(),
            vec![b("a")]
        );

        // Empty prefix, extensions don't contain expected values
        assert_eq!(
            extender.intersect(&vec![], &[b("z"), b("w")]).unwrap(),
            Vec::<Bytes>::new()
        );

        // Matching prefix at level 0 for first prefix
        assert_eq!(
            extender
                .intersect(&vec![b("a")], &[b("b"), b("y"), b("z")])
                .unwrap(),
            vec![b("b")]
        );

        // Matching prefix at level 0 for second prefix
        assert_eq!(
            extender
                .intersect(&vec![b("x")], &[b("b"), b("y"), b("z")])
                .unwrap(),
            vec![b("y")]
        );

        // Non-matching prefix
        assert_eq!(
            extender
                .intersect(&vec![b("z")], &[b("a"), b("b"), b("x"), b("y")])
                .unwrap(),
            Vec::<Bytes>::new()
        );
    }

    #[test]
    fn test_from_prefixes_extender_in_generic_join() {
        // Create a scenario with 2 levels
        // Level 0: values "Ivan", "Petr", "Bob"
        // Level 1: values "Ivanov", "Petrov", "Smith"

        let level0_extender = SingleLevelExtender::new(vec![b("Bob"), b("Ivan"), b("Petr")], 0);
        let level1_extender =
            SingleLevelExtender::new(vec![b("Ivanov"), b("Petrov"), b("Smith")], 1);

        // This extender constrains to only allow: ["Ivan", "Ivanov"] or ["Petr", "Petrov"]
        let constraint_extender = FromPrefixesExtender::new(
            vec![0, 1],
            vec![vec![b("Ivan"), b("Ivanov")], vec![b("Petr"), b("Petrov")]],
        );

        let extenders: Vec<&dyn PrefixExtender> =
            vec![&level0_extender, &level1_extender, &constraint_extender];

        let join = GenericJoin::new(extenders, 2);
        let result = join.join().unwrap();

        // Only the two valid combinations should remain
        let expected = vec![vec![b("Ivan"), b("Ivanov")], vec![b("Petr"), b("Petrov")]];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_from_prefixes_extender_with_single_prefix_behaves_like_from_prefix_extender() {
        let single_extender = FromPrefixExtender::new(vec![0, 1], vec![b("a"), b("b")]);
        let multi_extender = FromPrefixesExtender::new(vec![0, 1], vec![vec![b("a"), b("b")]]);

        // Both should behave identically
        assert_eq!(
            single_extender.count(&vec![]).unwrap(),
            multi_extender.count(&vec![]).unwrap()
        );
        assert_eq!(
            single_extender.count(&vec![b("a")]).unwrap(),
            multi_extender.count(&vec![b("a")]).unwrap()
        );
        assert_eq!(
            single_extender.count(&vec![b("z")]).unwrap(),
            multi_extender.count(&vec![b("z")]).unwrap()
        );

        assert_eq!(
            single_extender.propose(&vec![]).unwrap(),
            multi_extender.propose(&vec![]).unwrap()
        );
        assert_eq!(
            single_extender.propose(&vec![b("a")]).unwrap(),
            multi_extender.propose(&vec![b("a")]).unwrap()
        );
        assert_eq!(
            single_extender.propose(&vec![b("z")]).unwrap(),
            multi_extender.propose(&vec![b("z")]).unwrap()
        );

        assert_eq!(
            single_extender
                .intersect(&vec![], &[b("a"), b("b"), b("c")])
                .unwrap(),
            multi_extender
                .intersect(&vec![], &[b("a"), b("b"), b("c")])
                .unwrap()
        );
    }
}
