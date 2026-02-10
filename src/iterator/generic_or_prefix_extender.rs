use crate::algo::generic_join::{Extension, Prefix, PrefixExtender};

/// An OR-combinator extender that unifies multiple child extenders with union semantics.
///
/// All children must participate in the same levels (same free variables).
/// - count: sum of all children's counts (upper bound)
/// - propose: union of all children's proposals (sorted, deduplicated)
/// - intersect: union of all children's intersections (sorted, deduplicated)
/// - participates_in_level: delegates to the first child
pub struct GenericOrPrefixExtender {
    children: Vec<Box<dyn PrefixExtender>>,
}

impl GenericOrPrefixExtender {
    pub fn new(children: Vec<Box<dyn PrefixExtender>>) -> Self {
        assert!(
            !children.is_empty(),
            "OR extender requires at least one child"
        );
        Self { children }
    }
}

impl PrefixExtender for GenericOrPrefixExtender {
    fn count(&self, prefix: &Prefix) -> usize {
        self.children.iter().map(|c| c.count(prefix)).sum()
    }

    fn propose(&self, prefix: &Prefix) -> Vec<Extension> {
        let mut all: Vec<Extension> = self
            .children
            .iter()
            .flat_map(|c| c.propose(prefix))
            .collect();
        all.sort();
        all.dedup();
        all
    }

    fn intersect(&self, prefix: &Prefix, extensions: &[Extension]) -> Vec<Extension> {
        let mut all: Vec<Extension> = self
            .children
            .iter()
            .flat_map(|c| c.intersect(prefix, extensions))
            .collect();
        all.sort();
        all.dedup();
        all
    }

    fn participates_in_level(&self, level: usize) -> bool {
        self.children[0].participates_in_level(level)
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
    fn test_or_extender_count_sums_children() {
        let ext1 = SingleLevelExtender::new(vec![bi(1), bi(2), bi(3)], 0);
        let ext2 = SingleLevelExtender::new(vec![bi(4), bi(5)], 0);
        let or_ext = GenericOrPrefixExtender::new(vec![Box::new(ext1), Box::new(ext2)]);
        assert_eq!(or_ext.count(&vec![]), 5);
    }

    #[test]
    fn test_or_extender_propose_returns_sorted_union() {
        let ext1 = SingleLevelExtender::new(vec![bi(1), bi(3), bi(5)], 0);
        let ext2 = SingleLevelExtender::new(vec![bi(2), bi(3), bi(4)], 0);
        let or_ext = GenericOrPrefixExtender::new(vec![Box::new(ext1), Box::new(ext2)]);
        let proposed = or_ext.propose(&vec![]);
        assert_eq!(proposed, vec![bi(1), bi(2), bi(3), bi(4), bi(5)]);
    }

    #[test]
    fn test_or_extender_intersect_returns_union_of_matching() {
        let ext1 = SingleLevelExtender::new(vec![bi(1), bi(3)], 0);
        let ext2 = SingleLevelExtender::new(vec![bi(2), bi(4)], 0);
        let or_ext = GenericOrPrefixExtender::new(vec![Box::new(ext1), Box::new(ext2)]);
        let candidates = vec![bi(1), bi(2), bi(5)];
        let result = or_ext.intersect(&vec![], &candidates);
        assert_eq!(result, vec![bi(1), bi(2)]);
    }

    #[test]
    fn test_or_extender_participates_in_level() {
        let ext1 = SingleLevelExtender::new(vec![bi(1)], 0);
        let ext2 = SingleLevelExtender::new(vec![bi(2)], 0);
        let or_ext = GenericOrPrefixExtender::new(vec![Box::new(ext1), Box::new(ext2)]);
        assert!(or_ext.participates_in_level(0));
        assert!(!or_ext.participates_in_level(1));
    }

    #[test]
    fn test_or_extender_in_generic_join() {
        // OR of {1,2,3} and {3,4,5} => union is {1,2,3,4,5}
        // Constrained by even numbers => {2,4}
        let or_ext1 = SingleLevelExtender::new(vec![bi(1), bi(2), bi(3)], 0);
        let or_ext2 = SingleLevelExtender::new(vec![bi(3), bi(4), bi(5)], 0);
        let or_extender =
            GenericOrPrefixExtender::new(vec![Box::new(or_ext1), Box::new(or_ext2)]);
        let even_extender = SingleLevelExtender::new(vec![bi(2), bi(4), bi(6)], 0);

        let extenders: Vec<&dyn PrefixExtender> = vec![&or_extender, &even_extender];
        let join = GenericJoin::new(extenders, 1);
        let result = join.join();

        assert_eq!(result, vec![vec![bi(2)], vec![bi(4)]]);
    }

    #[test]
    #[should_panic(expected = "OR extender requires at least one child")]
    fn test_or_extender_empty_children_panics() {
        GenericOrPrefixExtender::new(vec![]);
    }
}
