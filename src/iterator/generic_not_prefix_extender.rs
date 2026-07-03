use std::collections::HashSet;

use anyhow::{bail, Error};

use crate::algo::generic_join::{
    Extension, GenericJoin, Prefix, PrefixAndExtensionsExtender, PrefixExtender,
};

/// A prefix extender implementing NOT (negation) semantics for the generic join.
///
/// NOT never proposes candidates — it only filters (intersects) by removing extensions
/// that match its inner patterns. It participates at a single level (typically the last),
/// ensuring all variables used by its children are already bound.
///
/// Implements NOT (negation) by removing matching extensions.
pub struct GenericNotPrefixExtender {
    children: Vec<Box<dyn PrefixExtender>>,
    level: usize,
}

impl GenericNotPrefixExtender {
    pub fn new(children: Vec<Box<dyn PrefixExtender>>, level: usize) -> Self {
        Self { children, level }
    }
}

impl PrefixExtender for GenericNotPrefixExtender {
    fn count(&self, _prefix: &Prefix) -> Result<usize, Error> {
        Ok(usize::MAX)
    }

    fn propose(&self, _prefix: &Prefix) -> Result<Vec<Extension>, Error> {
        bail!("Propose should not be called on NOT prefix extender — variable is not bound by positive clauses")
    }

    fn intersect(
        &self,
        prefix: &Prefix,
        extensions: &[Extension],
    ) -> Result<Vec<Extension>, Error> {
        let pinning_extender =
            PrefixAndExtensionsExtender::new(prefix.clone(), extensions.to_vec());

        let mut all_extenders: Vec<&dyn PrefixExtender> =
            self.children.iter().map(|c| c.as_ref()).collect();
        all_extenders.push(&pinning_extender);

        let inner_join = GenericJoin::new(all_extenders, prefix.len() + 1);
        let extensions_to_remove: HashSet<Extension> = inner_join
            .join()?
            .into_iter()
            .filter_map(|tuple| tuple.last().cloned())
            .collect();

        Ok(extensions
            .iter()
            .filter(|ext| !extensions_to_remove.contains(*ext))
            .cloned()
            .collect())
    }

    fn participates_in_level(&self, level: usize) -> bool {
        level == self.level
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
    fn test_count_returns_max() {
        let not_ext = GenericNotPrefixExtender::new(vec![], 0);
        assert_eq!(not_ext.count(&vec![]).unwrap(), usize::MAX);
    }

    #[test]
    fn test_propose_errors() {
        let not_ext = GenericNotPrefixExtender::new(vec![], 0);
        let err = not_ext.propose(&vec![]).unwrap_err();
        assert!(err
            .to_string()
            .contains("Propose should not be called on NOT prefix extender"));
    }

    #[test]
    fn test_participates_in_level() {
        let not_ext = GenericNotPrefixExtender::new(vec![], 2);
        assert!(!not_ext.participates_in_level(0));
        assert!(!not_ext.participates_in_level(1));
        assert!(not_ext.participates_in_level(2));
        assert!(!not_ext.participates_in_level(3));
    }

    #[test]
    fn test_intersect_removes_matching_extensions() {
        // NOT child: values divisible by 3 at level 1
        let div3_extender: Box<dyn PrefixExtender> = Box::new(SingleLevelExtender::new(
            vec![bi(3), bi(6), bi(9), bi(12)],
            1,
        ));

        let not_ext = GenericNotPrefixExtender::new(vec![div3_extender], 1);

        // Prefix is [6] (level 0 already bound)
        // Extensions at level 1: 1..10
        let prefix = vec![bi(6)];
        let extensions: Vec<Bytes> = (1..=10).map(bi).collect();

        let result = not_ext.intersect(&prefix, &extensions).unwrap();

        // Should remove 3, 6, 9 — keep 1, 2, 4, 5, 7, 8, 10
        assert_eq!(result.len(), 7);
        assert!(!result.contains(&bi(3)));
        assert!(!result.contains(&bi(6)));
        assert!(!result.contains(&bi(9)));
        assert!(result.contains(&bi(1)));
        assert!(result.contains(&bi(5)));
        assert!(result.contains(&bi(10)));
    }

    #[test]
    fn test_intersect_with_two_children_conjunction() {
        // NOT with two children: even numbers at level 0, divisible by 3 at level 1
        // NOT removes extensions where BOTH conditions are met (conjunction inside NOT)
        let even_extender: Box<dyn PrefixExtender> = Box::new(SingleLevelExtender::new(
            vec![bi(2), bi(4), bi(6), bi(8), bi(10)],
            0,
        ));
        let div3_extender: Box<dyn PrefixExtender> = Box::new(SingleLevelExtender::new(
            vec![bi(3), bi(6), bi(9), bi(12)],
            1,
        ));

        let not_ext = GenericNotPrefixExtender::new(vec![even_extender, div3_extender], 1);

        let extensions: Vec<Bytes> = (1..=10).map(bi).collect();

        // Prefix [4] — even number, so div-by-3 extensions are removed
        let result_even = not_ext.intersect(&vec![bi(4)], &extensions).unwrap();
        assert_eq!(result_even.len(), 7); // removes 3, 6, 9

        // Prefix [3] — odd number, even_extender won't match, so nothing removed
        let result_odd = not_ext.intersect(&vec![bi(3)], &extensions).unwrap();
        assert_eq!(result_odd.len(), 10); // nothing removed
    }

    #[test]
    fn test_not_extender_in_generic_join() {
        // Level 0: numbers 1..5
        // Level 1: numbers 1..5
        // NOT at level 1: removes extensions divisible by 2
        let level0 = SingleLevelExtender::new((1..=5).map(bi).collect(), 0);
        let level1 = SingleLevelExtender::new((1..=5).map(bi).collect(), 1);
        let div2: Box<dyn PrefixExtender> =
            Box::new(SingleLevelExtender::new(vec![bi(2), bi(4)], 1));

        let not_ext = GenericNotPrefixExtender::new(vec![div2], 1);

        let extenders: Vec<&dyn PrefixExtender> = vec![&level0, &level1, &not_ext];
        let join = GenericJoin::new(extenders, 2);
        let result = join.join().unwrap();

        // Each of 5 prefixes should have 3 extensions (1, 3, 5) = 15 total
        assert_eq!(result.len(), 15);
        for tuple in &result {
            assert!(tuple[1] != bi(2) && tuple[1] != bi(4));
        }
    }
}
