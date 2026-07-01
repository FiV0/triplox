use std::cell::RefCell;

use crate::algo::generic_join::{Extension, Prefix, PrefixExtender};
use crate::algo::trie::Trie;

/// An OR-combinator extender that unifies multiple child extenders with union semantics.
///
/// Validation assures that all children participate in the same levels.
/// Child tries track which branches produced each OR-relevant prefix.
/// - count: sum of matching children's counts (upper bound)
/// - propose: union of matching children's proposals (sorted, deduplicated)
/// - intersect: union of matching children's intersections (sorted, deduplicated)
/// - participates_in_level: true when any child participates
pub struct GenericOrPrefixExtender {
    children: Vec<Box<dyn PrefixExtender>>,
    // TODO: We could pass to a @ must self trait for PrefixExtender, then the RefCell is not needed.
    // This would require a larger refactor (worth it?)
    child_tries: Vec<RefCell<Trie<Extension>>>,
    levels: Vec<usize>,
}

impl GenericOrPrefixExtender {
    pub fn new(children: Vec<Box<dyn PrefixExtender>>, num_levels: usize) -> Self {
        assert!(
            !children.is_empty(),
            "OR extender requires at least one child"
        );
        let child_tries = (0..children.len())
            .map(|_| RefCell::new(Trie::new()))
            .collect();
        let levels = (0..num_levels)
            .filter(|level| children[0].participates_in_level(*level))
            .collect();
        Self {
            children,
            child_tries,
            levels,
        }
    }

    fn relevant_prefix(&self, prefix: &Prefix) -> Prefix {
        self.levels
            .iter()
            .filter_map(|level| prefix.get(*level).cloned())
            .collect()
    }

    fn child_matches_prefix(&self, child_idx: usize, relevant_prefix: &Prefix) -> bool {
        self.child_tries[child_idx]
            .borrow()
            .contains_prefix(relevant_prefix.iter())
    }

    fn insert_child_extensions(
        &self,
        child_idx: usize,
        relevant_prefix: &Prefix,
        extensions: &[Extension],
    ) {
        let mut child_trie = self.child_tries[child_idx].borrow_mut();
        let Some(node) = child_trie.node_mut(relevant_prefix.iter()) else {
            return;
        };
        for extension in extensions {
            node.insert(extension.clone());
        }
    }
}

impl PrefixExtender for GenericOrPrefixExtender {
    fn count(&self, prefix: &Prefix) -> usize {
        let relevant_prefix = self.relevant_prefix(prefix);
        self.children
            .iter()
            .enumerate()
            .filter(|(idx, _)| self.child_matches_prefix(*idx, &relevant_prefix))
            .fold(0, |total, (_, child)| {
                total.saturating_add(child.count(prefix))
            })
    }

    fn propose(&self, prefix: &Prefix) -> Vec<Extension> {
        let relevant_prefix = self.relevant_prefix(prefix);
        let mut all = Vec::new();
        for (idx, child) in self.children.iter().enumerate() {
            if !self.child_matches_prefix(idx, &relevant_prefix) {
                continue;
            }
            let proposals = child.propose(prefix);
            self.insert_child_extensions(idx, &relevant_prefix, &proposals);
            all.extend(proposals);
        }
        all.sort();
        all.dedup();
        all
    }

    fn intersect(&self, prefix: &Prefix, extensions: &[Extension]) -> Vec<Extension> {
        let relevant_prefix = self.relevant_prefix(prefix);
        let mut all = Vec::new();
        for (idx, child) in self.children.iter().enumerate() {
            if !self.child_matches_prefix(idx, &relevant_prefix) {
                continue;
            }
            let child_extensions = child.intersect(prefix, extensions);
            self.insert_child_extensions(idx, &relevant_prefix, &child_extensions);
            all.extend(child_extensions);
        }
        all.sort();
        all.dedup();
        all
    }

    fn participates_in_level(&self, level: usize) -> bool {
        self.levels.contains(&level)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::generic_join::{FromPrefixExtender, GenericJoin, SingleLevelExtender};
    use crate::codec::Encode;
    use crate::expr::{BinaryExpr, BinaryOp, Expr};
    use crate::iterator::GenericPredicatePrefixExtender;
    use crate::ops::DataType;
    use bytes::Bytes;
    use edn::query::ToVariable;

    fn bi(n: i32) -> Bytes {
        Bytes::from(n.to_be_bytes().to_vec())
    }

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    fn long(n: i64) -> Bytes {
        Bytes::from(DataType::Long(n).encode())
    }

    fn lt_age_predicate(limit: i64) -> GenericPredicatePrefixExtender {
        GenericPredicatePrefixExtender::new(
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Variable("?age".to_var())),
                op: BinaryOp::Lt,
                right: Box::new(Expr::Literal(DataType::Long(limit))),
            }),
            vec![],
            "?age".to_var(),
            1,
        )
    }

    #[test]
    fn test_or_extender_intersect_does_not_leak_across_branch_prefixes() {
        let branch_a = crate::iterator::GenericAndPrefixExtender::new(vec![
            Box::new(FromPrefixExtender::new(vec![0], vec![b("a")])),
            Box::new(lt_age_predicate(30)),
        ]);
        let branch_b = crate::iterator::GenericAndPrefixExtender::new(vec![
            Box::new(FromPrefixExtender::new(vec![0], vec![b("b")])),
            Box::new(lt_age_predicate(40)),
        ]);
        let or_ext = GenericOrPrefixExtender::new(vec![Box::new(branch_a), Box::new(branch_b)], 2);

        assert_eq!(
            or_ext.intersect(&vec![], &[b("a"), b("b")]),
            vec![b("a"), b("b")]
        );
        assert_eq!(
            or_ext.intersect(&vec![b("a")], &[long(35)]),
            Vec::<Bytes>::new()
        );
        assert_eq!(or_ext.intersect(&vec![b("b")], &[long(35)]), vec![long(35)]);
    }

    #[test]
    fn test_or_extender_predicate_only_branches_can_filter_bound_values() {
        let branch_a =
            crate::iterator::GenericAndPrefixExtender::new(vec![Box::new(lt_age_predicate(30))]);
        let branch_b =
            crate::iterator::GenericAndPrefixExtender::new(vec![Box::new(lt_age_predicate(40))]);
        let or_ext = GenericOrPrefixExtender::new(vec![Box::new(branch_a), Box::new(branch_b)], 2);

        assert_eq!(or_ext.intersect(&vec![], &[b("entity")]), vec![b("entity")]);
        assert_eq!(or_ext.count(&vec![b("entity")]), usize::MAX);
        assert_eq!(
            or_ext.intersect(&vec![b("entity")], &[long(35)]),
            vec![long(35)]
        );
    }

    #[test]
    fn test_or_extender_count_sums_children() {
        let ext1 = SingleLevelExtender::new(vec![bi(1), bi(2), bi(3)], 0);
        let ext2 = SingleLevelExtender::new(vec![bi(4), bi(5)], 0);
        let or_ext = GenericOrPrefixExtender::new(vec![Box::new(ext1), Box::new(ext2)], 1);
        assert_eq!(or_ext.count(&vec![]), 5);
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
        let or_ext = GenericOrPrefixExtender::new(vec![Box::new(pred1), Box::new(pred2)], 1);

        assert_eq!(or_ext.count(&vec![]), usize::MAX);
    }

    #[test]
    fn test_or_extender_propose_returns_sorted_union() {
        let ext1 = SingleLevelExtender::new(vec![bi(1), bi(3), bi(5)], 0);
        let ext2 = SingleLevelExtender::new(vec![bi(2), bi(3), bi(4)], 0);
        let or_ext = GenericOrPrefixExtender::new(vec![Box::new(ext1), Box::new(ext2)], 1);
        let proposed = or_ext.propose(&vec![]);
        assert_eq!(proposed, vec![bi(1), bi(2), bi(3), bi(4), bi(5)]);
    }

    #[test]
    fn test_or_extender_intersect_returns_union_of_matching() {
        let ext1 = SingleLevelExtender::new(vec![bi(1), bi(3)], 0);
        let ext2 = SingleLevelExtender::new(vec![bi(2), bi(4)], 0);
        let or_ext = GenericOrPrefixExtender::new(vec![Box::new(ext1), Box::new(ext2)], 1);
        let candidates = vec![bi(1), bi(2), bi(5)];
        let result = or_ext.intersect(&vec![], &candidates);
        assert_eq!(result, vec![bi(1), bi(2)]);
    }

    #[test]
    fn test_or_extender_participates_in_level() {
        let ext1 = SingleLevelExtender::new(vec![bi(1)], 0);
        let ext2 = SingleLevelExtender::new(vec![bi(2)], 0);
        let or_ext = GenericOrPrefixExtender::new(vec![Box::new(ext1), Box::new(ext2)], 2);
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
            GenericOrPrefixExtender::new(vec![Box::new(or_ext1), Box::new(or_ext2)], 1);
        let even_extender = SingleLevelExtender::new(vec![bi(2), bi(4), bi(6)], 0);

        let extenders: Vec<&dyn PrefixExtender> = vec![&or_extender, &even_extender];
        let join = GenericJoin::new(extenders, 1);
        let result = join.join();

        assert_eq!(result, vec![vec![bi(2)], vec![bi(4)]]);
    }

    #[test]
    #[should_panic(expected = "OR extender requires at least one child")]
    fn test_or_extender_empty_children_panics() {
        GenericOrPrefixExtender::new(vec![], 0);
    }
}
