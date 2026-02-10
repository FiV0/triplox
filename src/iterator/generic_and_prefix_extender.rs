use crate::algo::generic_join::{Extension, Prefix, PrefixExtender};

/// An AND-combinator extender that intersects results from multiple child extenders.
///
/// Mirrors hooray2's GenericAndPrefixExtender:
/// - count: minimum of all children's counts (intersection can't exceed smallest)
/// - propose: propose from child with minimum count, intersect with all others
/// - intersect: chain intersections through all children (sorted by count for efficiency)
/// - participates_in_level: true if any child participates
pub struct GenericAndPrefixExtender {
    children: Vec<Box<dyn PrefixExtender>>,
}

impl GenericAndPrefixExtender {
    pub fn new(children: Vec<Box<dyn PrefixExtender>>) -> Self {
        assert!(
            !children.is_empty(),
            "AND extender requires at least one child"
        );
        Self { children }
    }
}

impl PrefixExtender for GenericAndPrefixExtender {
    fn count(&self, prefix: &Prefix) -> usize {
        let next_level = prefix.len();
        self.children
            .iter()
            .filter(|c| c.participates_in_level(next_level))
            .map(|c| c.count(prefix))
            .min()
            .unwrap_or(0)
    }

    fn propose(&self, prefix: &Prefix) -> Vec<Extension> {
        let next_level = prefix.len();
        let participating: Vec<_> = self
            .children
            .iter()
            .filter(|c| c.participates_in_level(next_level))
            .collect();

        if participating.is_empty() {
            return vec![];
        }

        // Propose from the child with the smallest count
        let (min_idx, _) = participating
            .iter()
            .enumerate()
            .min_by_key(|(_, c)| c.count(prefix))
            .unwrap();

        let mut extensions = participating[min_idx].propose(prefix);

        // Intersect with all other participating children
        for (i, child) in participating.iter().enumerate() {
            if i != min_idx {
                extensions = child.intersect(prefix, &extensions);
                if extensions.is_empty() {
                    break;
                }
            }
        }

        extensions
    }

    fn intersect(&self, prefix: &Prefix, extensions: &[Extension]) -> Vec<Extension> {
        let next_level = prefix.len();
        let mut participating: Vec<_> = self
            .children
            .iter()
            .filter(|c| c.participates_in_level(next_level))
            .collect();

        // Sort by count so we intersect with the smallest first (fail fast)
        participating.sort_by_key(|c| c.count(prefix));

        let mut result = extensions.to_vec();
        for child in participating {
            result = child.intersect(prefix, &result);
            if result.is_empty() {
                break;
            }
        }
        result
    }

    fn participates_in_level(&self, level: usize) -> bool {
        self.children.iter().any(|c| c.participates_in_level(level))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::generic_join::{GenericJoin, SingleLevelExtender};
    use bytes::Bytes;

    fn bi(n: i32) -> Bytes {
        Bytes::from(n.to_be_bytes().to_vec())
    }

    #[test]
    fn test_and_extender_count_returns_minimum() {
        let ext1 = SingleLevelExtender::new(vec![bi(1), bi(2), bi(3)], 0);
        let ext2 = SingleLevelExtender::new(vec![bi(2), bi(3)], 0);
        let and_ext = GenericAndPrefixExtender::new(vec![Box::new(ext1), Box::new(ext2)]);
        assert_eq!(and_ext.count(&vec![]), 2);
    }

    #[test]
    fn test_and_extender_propose_returns_intersection() {
        // Even: {2,4,6,8,10,12}, Div3: {3,6,9,12}
        let even = SingleLevelExtender::new(vec![bi(2), bi(4), bi(6), bi(8), bi(10), bi(12)], 0);
        let div3 = SingleLevelExtender::new(vec![bi(3), bi(6), bi(9), bi(12)], 0);
        let and_ext = GenericAndPrefixExtender::new(vec![Box::new(even), Box::new(div3)]);

        let result = and_ext.propose(&vec![]);
        assert_eq!(result, vec![bi(6), bi(12)]);
    }

    #[test]
    fn test_and_extender_intersect_filters_correctly() {
        let ext1 = SingleLevelExtender::new(vec![bi(1), bi(3), bi(5)], 0);
        let ext2 = SingleLevelExtender::new(vec![bi(2), bi(3), bi(4)], 0);
        let and_ext = GenericAndPrefixExtender::new(vec![Box::new(ext1), Box::new(ext2)]);

        let candidates = vec![bi(1), bi(2), bi(3), bi(4), bi(5)];
        let result = and_ext.intersect(&vec![], &candidates);
        // Only 3 is in both ext1 and ext2
        assert_eq!(result, vec![bi(3)]);
    }

    #[test]
    fn test_and_extender_participates_in_level() {
        let ext1 = SingleLevelExtender::new(vec![bi(1)], 0);
        let ext2 = SingleLevelExtender::new(vec![bi(2)], 1);
        let and_ext = GenericAndPrefixExtender::new(vec![Box::new(ext1), Box::new(ext2)]);

        assert!(and_ext.participates_in_level(0));
        assert!(and_ext.participates_in_level(1));
        assert!(!and_ext.participates_in_level(2));
    }

    #[test]
    fn test_and_extender_in_generic_join() {
        // AND of {1,2,3,4,5,6} and {2,4,6,8} => intersection is {2,4,6}
        // Additional constraint: odd numbers {1,3,5} at level 1
        // Expected: pairs where level0 in {2,4,6} and level1 in {1,3,5}
        let and_ext1 = SingleLevelExtender::new(vec![bi(1), bi(2), bi(3), bi(4), bi(5), bi(6)], 0);
        let and_ext2 = SingleLevelExtender::new(vec![bi(2), bi(4), bi(6), bi(8)], 0);
        let and_extender =
            GenericAndPrefixExtender::new(vec![Box::new(and_ext1), Box::new(and_ext2)]);
        let odd_extender = SingleLevelExtender::new(vec![bi(1), bi(3), bi(5)], 1);

        let extenders: Vec<&dyn PrefixExtender> = vec![&and_extender, &odd_extender];
        let join = GenericJoin::new(extenders, 2);
        let result = join.join();

        assert_eq!(result.len(), 9); // 3 * 3
        assert_eq!(result[0], vec![bi(2), bi(1)]);
        assert_eq!(result[1], vec![bi(2), bi(3)]);
        assert_eq!(result[2], vec![bi(2), bi(5)]);
    }

    #[test]
    #[should_panic(expected = "AND extender requires at least one child")]
    fn test_and_extender_empty_children_panics() {
        GenericAndPrefixExtender::new(vec![]);
    }
}
