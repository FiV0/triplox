use anyhow::Error;

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
    fn count(&self, prefix: &Prefix) -> Result<usize, Error> {
        let mut total: usize = 0;
        for child in &self.children {
            total = total.saturating_add(child.count(prefix)?);
        }
        Ok(total)
    }

    fn propose(&self, prefix: &Prefix) -> Result<Vec<Extension>, Error> {
        let mut all: Vec<Extension> = Vec::new();
        for child in &self.children {
            all.extend(child.propose(prefix)?);
        }
        all.sort();
        all.dedup();
        Ok(all)
    }

    fn intersect(
        &self,
        prefix: &Prefix,
        extensions: &[Extension],
    ) -> Result<Vec<Extension>, Error> {
        let mut all: Vec<Extension> = Vec::new();
        for child in &self.children {
            all.extend(child.intersect(prefix, extensions)?);
        }
        all.sort();
        all.dedup();
        Ok(all)
    }

    fn participates_in_level(&self, level: usize) -> bool {
        self.children[0].participates_in_level(level)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::generic_join::{GenericJoin, SingleLevelExtender};
    use crate::expr::Expr;
    use crate::iterator::GenericPredicatePrefixExtender;
    use crate::ops::DataType;
    use bytes::Bytes;
    use edn::query::ToVariable;

    fn bi(n: i32) -> Bytes {
        Bytes::from(n.to_be_bytes().to_vec())
    }

    #[test]
    fn test_or_extender_count_sums_children() {
        let ext1 = SingleLevelExtender::new(vec![bi(1), bi(2), bi(3)], 0);
        let ext2 = SingleLevelExtender::new(vec![bi(4), bi(5)], 0);
        let or_ext = GenericOrPrefixExtender::new(vec![Box::new(ext1), Box::new(ext2)]);
        assert_eq!(or_ext.count(&vec![]).unwrap(), 5);
    }

    #[test]
    fn test_or_extender_count_saturates_for_predicate_only_children() {
        let pred1 = GenericPredicatePrefixExtender::new(
            Expr::Literal(DataType::Boolean(true)),
            vec![],
            "?x".to_var(),
            0,
        );
        let pred2 = GenericPredicatePrefixExtender::new(
            Expr::Literal(DataType::Boolean(true)),
            vec![],
            "?x".to_var(),
            0,
        );
        let or_ext = GenericOrPrefixExtender::new(vec![Box::new(pred1), Box::new(pred2)]);

        assert_eq!(or_ext.count(&vec![]).unwrap(), usize::MAX);
    }

    #[test]
    fn test_or_extender_propose_returns_sorted_union() {
        let ext1 = SingleLevelExtender::new(vec![bi(1), bi(3), bi(5)], 0);
        let ext2 = SingleLevelExtender::new(vec![bi(2), bi(3), bi(4)], 0);
        let or_ext = GenericOrPrefixExtender::new(vec![Box::new(ext1), Box::new(ext2)]);
        let proposed = or_ext.propose(&vec![]).unwrap();
        assert_eq!(proposed, vec![bi(1), bi(2), bi(3), bi(4), bi(5)]);
    }

    #[test]
    fn test_or_extender_intersect_returns_union_of_matching() {
        let ext1 = SingleLevelExtender::new(vec![bi(1), bi(3)], 0);
        let ext2 = SingleLevelExtender::new(vec![bi(2), bi(4)], 0);
        let or_ext = GenericOrPrefixExtender::new(vec![Box::new(ext1), Box::new(ext2)]);
        let candidates = vec![bi(1), bi(2), bi(5)];
        let result = or_ext.intersect(&vec![], &candidates).unwrap();
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
        let or_extender = GenericOrPrefixExtender::new(vec![Box::new(or_ext1), Box::new(or_ext2)]);
        let even_extender = SingleLevelExtender::new(vec![bi(2), bi(4), bi(6)], 0);

        let extenders: Vec<&dyn PrefixExtender> = vec![&or_extender, &even_extender];
        let join = GenericJoin::new(extenders, 1);
        let result = join.join().unwrap();

        assert_eq!(result, vec![vec![bi(2)], vec![bi(4)]]);
    }

    #[test]
    #[should_panic(expected = "OR extender requires at least one child")]
    fn test_or_extender_empty_children_panics() {
        GenericOrPrefixExtender::new(vec![]);
    }
}
