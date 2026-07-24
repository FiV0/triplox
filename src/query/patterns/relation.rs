use std::collections::HashSet;

use anyhow::{ensure, Result};
use bytes::Bytes;
use edn::query::Variable;

use crate::query::binding_bag::{BindingRow, BindingBag};
use crate::query::exec_pattern::{ExecPattern, PatternId, Proposal};

pub(crate) struct RelationPattern {
    id: PatternId,
    variables: Vec<Variable>,
    relation: BindingBag,
}

impl RelationPattern {
    pub(crate) fn new(id: PatternId, relation: BindingBag) -> Self {
        Self {
            id,
            variables: relation.variables.to_vec(),
            relation: relation.distinct(),
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

    fn proposal_index(&self, input: &BindingBag, added: &[Variable]) -> Option<usize> {
        if added.len() != 1 {
            return None;
        }
        let prefix_len = self.bound_prefix_len(input)?;
        self.variables
            .get(prefix_len)
            .filter(|variable| *variable == &added[0])
            .map(|_| prefix_len)
    }

    fn candidate_extensions(
        &self,
        input: &BindingBag,
        input_row: &BindingRow,
        proposal_index: usize,
    ) -> Result<Vec<BindingRow>> {
        let input_prefix_indexes = self.variables[..proposal_index]
            .iter()
            .map(|variable| input.column_index(variable))
            .collect::<Result<Vec<_>>>()?;
        let mut seen = HashSet::<Bytes>::new();
        let mut candidates = Vec::new();

        for relation_row in self.relation.rows {
            let matches_prefix =
                input_prefix_indexes
                    .iter()
                    .enumerate()
                    .all(|(relation_index, input_index)| {
                        relation_row[relation_index] == input_row[*input_index]
                    });
            if matches_prefix && seen.insert(relation_row[proposal_index].clone()) {
                candidates.push(vec![relation_row[proposal_index].clone()]);
            }
        }
        Ok(candidates)
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
        let Some(proposal_index) = self.proposal_index(input, added) else {
            return Ok(());
        };

        for (input_row, proposal) in input.rows.iter().zip(proposals) {
            let count = self
                .candidate_extensions(input, input_row, proposal_index)?
                .len();
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
            ensure!(
                self.bound_prefix_len(input).is_some(),
                "Relation validation requires bound variables to form a prefix"
            );
            return input.semijoin(&self.relation);
        }

        let proposal_index = self.proposal_index(input, added).ok_or_else(|| {
            anyhow::anyhow!("Relation cannot propose the requested variables: {added:?}")
        })?;
        let extensions = input
            .rows
            .iter()
            .map(|row| self.candidate_extensions(input, row, proposal_index))
            .collect::<Result<Vec<_>>>()?;
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

        assert_eq!(
            joined,
            binding_bag(
                &["?y", "?outer", "?x"],
                &[
                    &["1", "u", "a"],
                    &["2", "u", "a"],
                    &["1", "v", "a"],
                    &["2", "v", "a"],
                ],
            )
        );
    }

    #[test]
    fn validating_join_filters_prefixes_without_changing_layout_or_bag_semantics() {
        let pattern =
            RelationPattern::new(7, binding_bag(&["?x", "?y"], &[&["a", "1"], &["a", "2"]]));
        let input = binding_bag(&["?outer", "?x"], &[&["u", "a"], &["u", "a"], &["v", "b"]]);

        let validated = pattern.join(&input, &[], input.variables).unwrap();

        assert_eq!(
            validated,
            binding_bag(&["?outer", "?x"], &[&["u", "a"], &["u", "a"]])
        );
        assert!(pattern
            .join(&input, &[], &["?x".to_var(), "?outer".to_var()],)
            .is_err());
    }

    #[test]
    fn zero_column_relations_validate_by_existence() {
        let input = binding_bag(&["?x"], &[&["a"], &["a"]]);
        let unit = RelationPattern::new(1, BindingBag::unit());
        let empty = RelationPattern::new(2, BindingBag::empty(vec![]).unwrap());

        assert_eq!(unit.join(&input, &[], input.variables).unwrap(), input);
        assert_eq!(
            empty.join(&input, &[], input.variables).unwrap(),
            BindingBag::empty(vec!["?x".to_var()]).unwrap()
        );
    }
}
