use anyhow::{ensure, Result};
use bytes::Bytes;
use edn::query::Variable;

use crate::algo::trie::{Trie, TrieNode};
use crate::query::binding_bag::{BindingBag, BindingRow};
use crate::query::exec_pattern::{ExecPattern, PatternId, Proposal};

/// A pattern that matches a relation (set of rows) with a fixed set of variables.
///
/// Bound variables are read from the input in this relation's variable order, regardless of
/// their input column order or unrelated interleaved columns. For proposals, the bound variables
/// followed by the introduced variables must form a relation prefix. Validation requires the bound
/// variables themselves to form a relation prefix.
pub(crate) struct RelationPattern {
    id: PatternId,
    variables: Vec<Variable>,
    has_rows: bool,
    trie: Trie<Bytes>,
}

impl RelationPattern {
    pub(crate) fn new(id: PatternId, relation: BindingBag) -> Self {
        let has_rows = !relation.rows.is_empty();
        let variables = relation.variables;
        let mut trie = Trie::new();
        for row in relation.rows {
            trie.insert(row);
        }
        Self {
            id,
            variables,
            has_rows,
            trie,
        }
    }

    fn bound_prefix_len(&self, input: &BindingBag) -> Option<usize> {
        let mut prefix_len = 0;
        let mut missing_prefix = false;
        for variable in &self.variables {
            if input.variables.contains(variable) {
                if missing_prefix {
                    return None;
                }
                prefix_len += 1;
            } else {
                missing_prefix = true;
            }
        }
        Some(prefix_len)
    }

    fn prefix_indexes_or_none(
        &self,
        input: &BindingBag,
        added: &[Variable],
    ) -> Result<Option<Vec<usize>>> {
        let Some(prefix_len) = self.bound_prefix_len(input) else {
            return Ok(None);
        };
        if !self.variables[prefix_len..].starts_with(added) {
            return Ok(None);
        }
        self.variables[..prefix_len]
            .iter()
            .map(|variable| input.column_index(variable))
            .collect::<Result<Vec<_>>>()
            .map(Some)
    }

    fn trie_node_for(
        &self,
        input_row: &BindingRow,
        prefix_indexes: &[usize],
    ) -> Option<&TrieNode<Bytes>> {
        self.trie
            .node(prefix_indexes.iter().map(|index| &input_row[*index]))
    }

    fn count_extensions(node: &TrieNode<Bytes>, depth: usize) -> usize {
        if depth == 0 {
            return 1;
        }
        node.children()
            .map(|(_, child)| Self::count_extensions(child, depth - 1))
            .sum()
    }

    fn add_extensions(
        node: &TrieNode<Bytes>,
        depth: usize,
        values: &mut BindingRow,
        extensions: &mut Vec<BindingRow>,
    ) {
        if depth == 0 {
            extensions.push(values.clone());
            return;
        }
        for (value, child) in node.children() {
            values.push(value.clone());
            Self::add_extensions(child, depth - 1, values, extensions);
            values.pop().expect("extension value was just pushed");
        }
    }

    fn candidate_extensions(
        &self,
        input_row: &BindingRow,
        prefix_indexes: &[usize],
        depth: usize,
    ) -> Vec<BindingRow> {
        let Some(node) = self.trie_node_for(input_row, prefix_indexes) else {
            return Vec::new();
        };
        let mut values = Vec::with_capacity(depth);
        let mut extensions = Vec::new();
        Self::add_extensions(node, depth, &mut values, &mut extensions);
        extensions
    }
}

impl ExecPattern for RelationPattern {
    fn id(&self) -> PatternId {
        self.id
    }

    fn variables(&self) -> &[Variable] {
        &self.variables
    }

    fn count(
        &self,
        input: &BindingBag,
        added: &[Variable],
        proposals: &mut [Proposal],
    ) -> Result<()> {
        ensure!(
            proposals.len() == input.rows.len(),
            "Proposal sidecar has {} rows, expected {}",
            proposals.len(),
            input.rows.len()
        );
        if added.is_empty() {
            return Ok(());
        }
        let Some(prefix_indexes) = self.prefix_indexes_or_none(input, added)? else {
            return Ok(());
        };

        for (input_row, proposal) in input.rows.iter().zip(proposals) {
            let count = self
                .trie_node_for(input_row, &prefix_indexes)
                .map(|node| Self::count_extensions(node, added.len()))
                .unwrap_or(0);
            proposal.consider(self.id, count);
        }
        Ok(())
    }

    fn join(
        &self,
        input: &BindingBag,
        added: &[Variable],
        target_variables: &[Variable],
    ) -> Result<BindingBag> {
        if added.is_empty() {
            ensure!(
                target_variables == input.variables,
                "Relation validation must preserve the input layout"
            );
            let prefix_indexes = self.prefix_indexes_or_none(input, added)?.ok_or_else(|| {
                anyhow::anyhow!("Relation validation requires bound variables to form a prefix")
            })?;
            let matches = if self.has_rows {
                input
                    .rows
                    .iter()
                    .enumerate()
                    .filter_map(|(index, row)| {
                        self.trie_node_for(row, &prefix_indexes)
                            .is_some()
                            .then_some(index)
                    })
                    .collect()
            } else {
                Vec::new()
            };
            return input.select_rows(&matches);
        }

        let prefix_indexes = self.prefix_indexes_or_none(input, added)?.ok_or_else(|| {
            anyhow::anyhow!("Relation cannot propose the requested variables: {added:?}")
        })?;
        let extensions = input
            .rows
            .iter()
            .map(|row| self.candidate_extensions(row, &prefix_indexes, added.len()))
            .collect();
        input
            .extend_rows(added.to_vec(), extensions)?
            .reorder(target_variables)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use edn::query::ToVariable;

    use super::RelationPattern;
    use crate::query::binding_bag::BindingBag;
    use crate::query::exec_pattern::{ExecPattern, Proposal};

    fn bytes(value: &str) -> Bytes {
        Bytes::copy_from_slice(value.as_bytes())
    }

    fn binding_bag(variables: &[&str], rows: &[&[&str]]) -> BindingBag {
        BindingBag::new(
            variables.iter().map(|variable| variable.to_var()).collect(),
            rows.iter()
                .map(|row| row.iter().map(|value| bytes(value)).collect())
                .collect(),
        )
        .unwrap()
    }

    fn assert_bag_eq_unordered(actual: BindingBag, expected: BindingBag) {
        assert_eq!(actual.variables, expected.variables);
        let mut actual_rows = actual.rows;
        let mut expected_rows = expected.rows;
        actual_rows.sort();
        expected_rows.sort();
        assert_eq!(actual_rows, expected_rows);
    }

    #[test]
    fn count_updates_each_row_with_its_distinct_positive_candidate_count() {
        let pattern = RelationPattern::new(
            7,
            binding_bag(
                &["?x", "?y"],
                &[&["a", "1"], &["a", "1"], &["a", "2"], &["b", "3"]],
            ),
        );
        let input = binding_bag(&["?x"], &[&["a"], &["b"], &["c"]]);
        let mut proposals = vec![Proposal::default(); input.rows.len()];

        pattern
            .count(&input, &["?y".to_var()], &mut proposals)
            .unwrap();

        assert_eq!(proposals[0].proposer(), Some(7));
        assert_eq!(proposals[0].count(), 2);
        assert_eq!(proposals[1].proposer(), Some(7));
        assert_eq!(proposals[1].count(), 1);
        assert_eq!(proposals[2].proposer(), None);
    }

    #[test]
    fn count_is_a_noop_for_non_prefix_proposals_and_checks_sidecar_length() {
        let pattern = RelationPattern::new(7, binding_bag(&["?x", "?y"], &[&["a", "1"]]));
        let input = binding_bag(&["?y"], &[&["1"]]);
        let mut proposals = vec![Proposal::default()];

        pattern
            .count(&input, &["?x".to_var()], &mut proposals)
            .unwrap();
        assert_eq!(proposals, vec![Proposal::default()]);

        assert!(pattern.count(&input, &["?x".to_var()], &mut []).is_err());
    }

    #[test]
    fn multi_variable_non_prefix_proposals_are_ignored_and_rejected() {
        let pattern =
            RelationPattern::new(7, binding_bag(&["?x", "?y", "?z"], &[&["a", "1", "red"]]));
        let input = BindingBag::unit();
        let added = ["?y".to_var(), "?z".to_var()];
        let mut proposals = vec![Proposal::default()];

        pattern.count(&input, &added, &mut proposals).unwrap();

        assert_eq!(proposals, vec![Proposal::default()]);
        assert!(pattern.join(&input, &added, &added).is_err());
    }

    #[test]
    fn counts_and_proposes_distinct_multi_variable_prefixes_from_the_root() {
        let pattern = RelationPattern::new(
            7,
            binding_bag(
                &["?x", "?y", "?z"],
                &[
                    &["a", "1", "red"],
                    &["a", "1", "blue"],
                    &["a", "1", "red"],
                    &["a", "2", "green"],
                    &["b", "3", "black"],
                ],
            ),
        );
        let input = binding_bag(&["?outer"], &[&["seed"], &["seed"]]);
        let added = ["?x".to_var(), "?y".to_var()];
        let mut proposals = vec![Proposal::default(); input.rows.len()];

        pattern.count(&input, &added, &mut proposals).unwrap();

        assert_eq!(proposals[0].proposer(), Some(7));
        assert_eq!(proposals[0].count(), 3);
        assert_eq!(proposals[1].proposer(), Some(7));
        assert_eq!(proposals[1].count(), 3);

        let joined = pattern
            .join(
                &input,
                &added,
                &["?y".to_var(), "?outer".to_var(), "?x".to_var()],
            )
            .unwrap();
        assert_bag_eq_unordered(
            joined,
            binding_bag(
                &["?y", "?outer", "?x"],
                &[
                    &["1", "seed", "a"],
                    &["1", "seed", "a"],
                    &["2", "seed", "a"],
                    &["2", "seed", "a"],
                    &["3", "seed", "b"],
                    &["3", "seed", "b"],
                ],
            ),
        );
    }

    #[test]
    fn counts_and_proposes_multiple_variables_below_a_reordered_bound_prefix() {
        let pattern = RelationPattern::new(
            9,
            binding_bag(
                &["?w", "?x", "?y", "?z"],
                &[
                    &["a", "1", "m", "red"],
                    &["a", "1", "m", "blue"],
                    &["a", "1", "n", "green"],
                    &["b", "2", "o", "black"],
                ],
            ),
        );
        let input = binding_bag(
            &["?x", "?outer", "?w"],
            &[&["1", "first", "a"], &["9", "second", "a"]],
        );
        let added = ["?y".to_var(), "?z".to_var()];
        let mut proposals = vec![Proposal::default(); input.rows.len()];

        pattern.count(&input, &added, &mut proposals).unwrap();

        assert_eq!(proposals[0].proposer(), Some(9));
        assert_eq!(proposals[0].count(), 3);
        assert_eq!(proposals[1].proposer(), None);

        let joined = pattern
            .join(
                &input,
                &added,
                &[
                    "?outer".to_var(),
                    "?z".to_var(),
                    "?w".to_var(),
                    "?y".to_var(),
                    "?x".to_var(),
                ],
            )
            .unwrap();
        assert_bag_eq_unordered(
            joined,
            binding_bag(
                &["?outer", "?z", "?w", "?y", "?x"],
                &[
                    &["first", "red", "a", "m", "1"],
                    &["first", "blue", "a", "m", "1"],
                    &["first", "green", "a", "n", "1"],
                ],
            ),
        );
    }

    #[test]
    fn proposing_join_preserves_outer_columns_multiplicity_and_target_order() {
        let pattern = RelationPattern::new(
            7,
            binding_bag(&["?x", "?y"], &[&["a", "1"], &["a", "2"], &["b", "3"]]),
        );
        let input = binding_bag(
            &["?outer", "?x"],
            &[&["u", "a"], &["v", "a"], &["w", "missing"]],
        );

        let joined = pattern
            .join(
                &input,
                &["?y".to_var()],
                &["?y".to_var(), "?outer".to_var(), "?x".to_var()],
            )
            .unwrap();

        assert_bag_eq_unordered(
            joined,
            binding_bag(
                &["?y", "?outer", "?x"],
                &[
                    &["1", "u", "a"],
                    &["2", "u", "a"],
                    &["1", "v", "a"],
                    &["2", "v", "a"],
                ],
            ),
        );
    }

    #[test]
    fn validating_join_filters_prefixes_without_changing_layout_or_bag_semantics() {
        let pattern =
            RelationPattern::new(7, binding_bag(&["?x", "?y"], &[&["a", "1"], &["a", "2"]]));
        let input = binding_bag(&["?outer", "?x"], &[&["u", "a"], &["u", "a"], &["v", "b"]]);

        let validated = pattern.join(&input, &[], &input.variables).unwrap();

        assert_eq!(
            validated,
            binding_bag(&["?outer", "?x"], &[&["u", "a"], &["u", "a"]])
        );
        assert!(pattern
            .join(&input, &[], &["?x".to_var(), "?outer".to_var()],)
            .is_err());
    }

    #[test]
    fn validating_join_matches_complete_rows_independent_of_input_column_order() {
        let pattern = RelationPattern::new(
            7,
            binding_bag(
                &["?x", "?y", "?z"],
                &[&["a", "1", "red"], &["b", "2", "blue"]],
            ),
        );
        let input = binding_bag(
            &["?z", "?outer", "?x", "?y"],
            &[
                &["red", "first", "a", "1"],
                &["red", "first", "a", "1"],
                &["wrong", "second", "a", "1"],
            ],
        );

        let validated = pattern.join(&input, &[], &input.variables).unwrap();

        assert_eq!(
            validated,
            binding_bag(
                &["?z", "?outer", "?x", "?y"],
                &[&["red", "first", "a", "1"], &["red", "first", "a", "1"],],
            )
        );
    }

    #[test]
    fn zero_column_relations_validate_by_existence() {
        let input = binding_bag(&["?x"], &[&["a"], &["a"]]);
        let unit = RelationPattern::new(1, BindingBag::unit());
        let empty = RelationPattern::new(2, BindingBag::empty(vec![]).unwrap());

        assert_eq!(unit.join(&input, &[], &input.variables).unwrap(), input);
        assert_eq!(
            empty.join(&input, &[], &input.variables).unwrap(),
            BindingBag::empty(vec!["?x".to_var()]).unwrap()
        );
    }
}
