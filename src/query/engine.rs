use std::collections::HashMap;

use anyhow::{ensure, Context, Result};

use super::binding_bag::BindingBag;
use super::exec_pattern::{ExecPattern, PatternIndex, Proposal};
use super::stage::Stage;

// generic-join based on
// http://www.frankmcsherry.org/dataflow/relational/join/2015/04/11/genericjoin.html
// https://arxiv.org/abs/1310.3314
//
// The ExecPattern interface very much inspired by
// https://github.com/frankmcsherry/datatoad

pub(crate) struct GenericJoinEngine;

impl GenericJoinEngine {
    fn validate_all<'a>(
        mut input: BindingBag,
        validators: impl IntoIterator<Item = &'a dyn ExecPattern>,
    ) -> Result<BindingBag> {
        for validator in validators {
            let validated = validator
                .join(&input, &[], &input.variables)
                .with_context(|| {
                    format!("Pattern {} failed during validation", validator.index())
                })?;
            ensure!(
                validated.variables == input.variables,
                "Pattern {} changed the layout during validation",
                validator.index()
            );
            input = validated;
        }
        Ok(input)
    }

    fn verify_proposed_layout(
        proposer: PatternIndex,
        proposed: &BindingBag,
        stage: &Stage,
    ) -> Result<()> {
        ensure!(
            proposed.variables == stage.target_variables(),
            "Pattern {proposer} proposed layout {:?}, expected {:?}",
            proposed.variables,
            stage.target_variables()
        );
        Ok(())
    }

    fn execute_proposing_stage(stage: &Stage, input: &BindingBag) -> Result<BindingBag> {
        if stage.proposers().len() == 1 {
            let proposer = stage
                .proposers()
                .next()
                .expect("proposing stages have at least one proposer");
            let proposed = proposer
                .join(input, stage.added(), stage.target_variables())
                .with_context(|| format!("Pattern {} failed during proposal", proposer.index()))?;
            Self::verify_proposed_layout(proposer.index(), &proposed, stage)?;
            let validators = stage
                .participants()
                .iter()
                .filter(|participant| participant.index() != proposer.index())
                .map(|participant| participant.as_ref());
            return Self::validate_all(proposed, validators);
        }

        let mut proposals = vec![Proposal::default(); input.rows.len()];
        for proposer in stage.proposers() {
            proposer
                .count(input, stage.added(), &mut proposals)
                .with_context(|| {
                    format!(
                        "Pattern {} failed while counting proposals",
                        proposer.index()
                    )
                })?;
        }
        let mut shards: HashMap<PatternIndex, Vec<usize>> = HashMap::new();
        for (row_index, proposal) in proposals.iter().enumerate() {
            let Some(proposer) = proposal.proposer() else {
                continue;
            };
            shards.entry(proposer).or_default().push(row_index);
        }
        let mut ordered_shards: Vec<_> = shards.into_iter().collect();
        // Multi-proposer output is grouped by proposer in first-occurrence order.
        ordered_shards.sort_unstable_by_key(|(_, row_indexes)| row_indexes[0]);

        let mut result = BindingBag::empty(stage.target_variables().to_vec())?;
        for (proposer_index, row_indexes) in ordered_shards {
            let proposer = stage
                .proposers()
                .find(|proposer| proposer.index() == proposer_index)
                .ok_or_else(|| anyhow::anyhow!("Unknown proposer index {proposer_index}"))?;
            let shard = input.select_rows(&row_indexes)?;
            let proposed = proposer
                .join(&shard, stage.added(), stage.target_variables())
                .with_context(|| format!("Pattern {proposer_index} failed during proposal"))?;
            Self::verify_proposed_layout(proposer_index, &proposed, stage)?;

            let validators = stage
                .participants()
                .iter()
                .filter(|participant| participant.index() != proposer_index)
                .map(|participant| participant.as_ref());
            let validated = Self::validate_all(proposed, validators)?;
            result = result.union(&validated)?;
        }
        Ok(result)
    }

    fn execute_stage(stage: &Stage, input: BindingBag) -> Result<BindingBag> {
        stage.validate_input(&input)?;
        let result = if stage.added().is_empty() {
            Self::validate_all(
                input,
                stage
                    .participants()
                    .iter()
                    .map(|participant| participant.as_ref()),
            )?
            .reorder(stage.target_variables())?
        } else {
            Self::execute_proposing_stage(stage, &input)?
        };
        ensure!(
            result.variables == stage.target_variables(),
            "Stage produced layout {:?}, expected {:?}",
            result.variables,
            stage.target_variables()
        );
        Ok(result)
    }

    pub(crate) fn execute(stages: &[Stage], mut input: BindingBag) -> Result<BindingBag> {
        for stage in stages {
            input = Self::execute_stage(stage, input)?;
        }
        Ok(input)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};

    use anyhow::{ensure, Result};
    use bytes::Bytes;
    use edn::query::{ToVariable, Variable};

    use super::GenericJoinEngine;
    use crate::query::binding_bag::{BindingBag, BindingRow};
    use crate::query::exec_pattern::{ExecPattern, PatternIndex, Proposal};
    use crate::query::stage::Stage;

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

    fn row(values: &[&str]) -> BindingRow {
        values.iter().map(|value| bytes(value)).collect()
    }

    // Configurable proposer used to control per-row counts, extensions, and observed inputs.
    // TODO: Can these tests avoid stateful helpers and mutexes?
    struct MapPattern {
        index: PatternIndex,
        variables: Vec<Variable>,
        // Proposal count reported for each input row.
        counts: HashMap<BindingRow, usize>,
        // Extensions returned for each input row during proposal.
        values: HashMap<BindingRow, Vec<BindingRow>>,
        // Rows that deliberately violate the count contract by receiving no proposal.
        skipped_inputs: HashSet<BindingRow>,
        // Alternate proposer identifier used to test unknown proposer handling.
        proposer_override: Option<PatternIndex>,
        // Input rows observed when this pattern is selected to propose.
        proposed_inputs: Mutex<Vec<BindingRow>>,
        // Added-variable layouts observed across join calls.
        join_added: Mutex<Vec<Vec<Variable>>>,
    }

    impl MapPattern {
        fn new(
            index: PatternIndex,
            variables: &[&str],
            counts: &[(&[&str], usize)],
            values: &[(&[&str], &[&[&str]])],
        ) -> Self {
            Self {
                index,
                variables: variables.iter().map(|variable| variable.to_var()).collect(),
                counts: counts
                    .iter()
                    .map(|(key, count)| (row(key), *count))
                    .collect(),
                values: values
                    .iter()
                    .map(|(key, extensions)| {
                        (
                            row(key),
                            extensions.iter().map(|extension| row(extension)).collect(),
                        )
                    })
                    .collect(),
                skipped_inputs: HashSet::new(),
                proposer_override: None,
                proposed_inputs: Mutex::new(Vec::new()),
                join_added: Mutex::new(Vec::new()),
            }
        }

        fn with_proposer_override(mut self, proposer: PatternIndex) -> Self {
            self.proposer_override = Some(proposer);
            self
        }

        fn without_proposals_for(mut self, inputs: &[&[&str]]) -> Self {
            self.skipped_inputs = inputs.iter().map(|input| row(input)).collect();
            self
        }
    }

    impl ExecPattern for MapPattern {
        fn index(&self) -> PatternIndex {
            self.index
        }

        fn variables(&self) -> &[Variable] {
            &self.variables
        }

        fn count(
            &self,
            input: &BindingBag,
            _added: &[Variable],
            proposals: &mut [Proposal],
        ) -> Result<()> {
            ensure!(
                proposals.len() == input.rows.len(),
                "wrong proposal sidecar length"
            );
            for (input_row, proposal) in input.rows.iter().zip(proposals) {
                if self.skipped_inputs.contains(input_row) {
                    continue;
                }
                let count = self
                    .counts
                    .get(input_row)
                    .copied()
                    .unwrap_or_else(|| self.values.get(input_row).map_or(0, Vec::len));
                proposal.consider(self.proposer_override.unwrap_or(self.index), count);
            }
            Ok(())
        }

        fn join(
            &self,
            input: &BindingBag,
            added: &[Variable],
            target_variables: &[Variable],
        ) -> Result<BindingBag> {
            self.join_added.lock().unwrap().push(added.to_vec());
            if added.is_empty() {
                return Ok(input.clone());
            }
            self.proposed_inputs
                .lock()
                .unwrap()
                .extend(input.rows.iter().cloned());
            let extensions = input
                .rows
                .iter()
                .map(|input_row| self.values.get(input_row).cloned().unwrap_or_default())
                .collect();
            input
                .extend_rows(added.to_vec(), extensions)?
                .reorder(target_variables)
        }
    }

    // Single-proposer test pattern that fails if the engine calls `count`.
    struct JoinOnlyPattern(MapPattern);

    impl ExecPattern for JoinOnlyPattern {
        fn index(&self) -> PatternIndex {
            self.0.index()
        }

        fn variables(&self) -> &[Variable] {
            self.0.variables()
        }

        fn join(
            &self,
            input: &BindingBag,
            added: &[Variable],
            target_variables: &[Variable],
        ) -> Result<BindingBag> {
            self.0.join(input, added, target_variables)
        }
    }

    // Validator-only pattern that filters rows and fails if used as a proposer.
    struct FilterPattern {
        index: PatternIndex,
        variables: Vec<Variable>,
        allowed_first_column: Bytes,
        join_added: Mutex<Vec<Vec<Variable>>>,
    }

    impl ExecPattern for FilterPattern {
        fn index(&self) -> PatternIndex {
            self.index
        }

        fn variables(&self) -> &[Variable] {
            &self.variables
        }

        fn join(
            &self,
            input: &BindingBag,
            added: &[Variable],
            _target_variables: &[Variable],
        ) -> Result<BindingBag> {
            self.join_added.lock().unwrap().push(added.to_vec());
            ensure!(added.is_empty(), "filter cannot propose");
            let rows: Vec<usize> = input
                .rows
                .iter()
                .enumerate()
                .filter(|(_, row)| row.first() == Some(&self.allowed_first_column))
                .map(|(index, _)| index)
                .collect();
            input.select_rows(&rows)
        }
    }

    struct WrongLayoutPattern {
        index: PatternIndex,
        variables: Vec<Variable>,
        output: BindingBag,
    }

    impl ExecPattern for WrongLayoutPattern {
        fn index(&self) -> PatternIndex {
            self.index
        }

        fn variables(&self) -> &[Variable] {
            &self.variables
        }

        fn count(
            &self,
            input: &BindingBag,
            _added: &[Variable],
            proposals: &mut [Proposal],
        ) -> Result<()> {
            for proposal in proposals.iter_mut().take(input.rows.len()) {
                proposal.consider(self.index, 1);
            }
            Ok(())
        }

        fn join(
            &self,
            _input: &BindingBag,
            _added: &[Variable],
            _target_variables: &[Variable],
        ) -> Result<BindingBag> {
            Ok(self.output.clone())
        }
    }

    #[test]
    fn executes_stage_layout_transitions_sequentially() {
        let first = Arc::new(MapPattern::new(
            0,
            &["?e", "?x"],
            &[],
            &[(&["a"], &[&["1"]])],
        ));
        let second = Arc::new(MapPattern::new(
            1,
            &["?x", "?y"],
            &[],
            &[(&["a", "1"], &[&["2"]])],
        ));
        let stages = vec![
            Stage::new(
                vec!["?x".to_var()],
                vec![first],
                vec![0],
                vec!["?e".to_var(), "?x".to_var()],
            )
            .unwrap(),
            Stage::new(
                vec!["?y".to_var()],
                vec![second],
                vec![0],
                vec!["?e".to_var(), "?x".to_var(), "?y".to_var()],
            )
            .unwrap(),
        ];

        let result = GenericJoinEngine::execute(&stages, binding_bag(&["?e"], &[&["a"]])).unwrap();

        assert_eq!(
            result,
            binding_bag(&["?e", "?x", "?y"], &[&["a", "1", "2"]])
        );
    }

    #[test]
    fn chooses_cheapest_proposers_and_groups_results_by_first_occurrence() {
        let first = Arc::new(MapPattern::new(
            1,
            &["?e", "?x"],
            &[(&["a"], 1), (&["b"], 2), (&["c"], 1)],
            &[(&["a"], &[&["10"]]), (&["c"], &[&["30"]])],
        ));
        let second = Arc::new(MapPattern::new(
            2,
            &["?e", "?x"],
            &[(&["a"], 2), (&["b"], 1), (&["c"], 1)],
            &[(&["b"], &[&["20"]])],
        ));
        let stage = Stage::new(
            vec!["?x".to_var()],
            vec![first.clone(), second.clone()],
            vec![0, 1],
            vec!["?e".to_var(), "?x".to_var()],
        )
        .unwrap();

        let result =
            GenericJoinEngine::execute(&[stage], binding_bag(&["?e"], &[&["a"], &["b"], &["c"]]))
                .unwrap();

        assert_eq!(
            *first.proposed_inputs.lock().unwrap(),
            vec![row(&["a"]), row(&["c"])]
        );
        assert_eq!(*second.proposed_inputs.lock().unwrap(), vec![row(&["b"])]);
        assert_eq!(
            result,
            binding_bag(&["?e", "?x"], &[&["a", "10"], &["c", "30"], &["b", "20"]])
        );
    }

    #[test]
    fn single_proposer_bypasses_counting_and_runs_validators() {
        let proposer = Arc::new(JoinOnlyPattern(MapPattern::new(
            0,
            &["?e", "?x"],
            &[],
            &[(&["a"], &[&["1"]])],
        )));
        let validator = Arc::new(FilterPattern {
            index: 1,
            variables: vec!["?e".to_var(), "?x".to_var()],
            allowed_first_column: bytes("a"),
            join_added: Mutex::new(Vec::new()),
        });
        let proposing = Stage::new(
            vec!["?x".to_var()],
            vec![proposer, validator.clone()],
            vec![0],
            vec!["?e".to_var(), "?x".to_var()],
        )
        .unwrap();
        let filtering: Arc<dyn ExecPattern> = Arc::new(FilterPattern {
            index: 2,
            variables: vec!["?e".to_var()],
            allowed_first_column: bytes("a"),
            join_added: Mutex::new(Vec::new()),
        });
        let validation = Stage::new(
            vec![],
            vec![filtering],
            vec![],
            vec!["?x".to_var(), "?e".to_var()],
        )
        .unwrap();

        let result =
            GenericJoinEngine::execute(&[proposing, validation], binding_bag(&["?e"], &[&["a"]]))
                .unwrap();

        assert_eq!(
            *validator.join_added.lock().unwrap(),
            vec![Vec::<Variable>::new()]
        );
        assert_eq!(result, binding_bag(&["?x", "?e"], &[&["1", "a"]]));
    }

    #[test]
    fn multi_proposer_stage_never_counts_validators() {
        let first = Arc::new(MapPattern::new(
            0,
            &["?e", "?x"],
            &[(&["a"], 1), (&["b"], 2)],
            &[(&["a"], &[&["1"]])],
        ));
        let second = Arc::new(MapPattern::new(
            1,
            &["?e", "?x"],
            &[(&["a"], 2), (&["b"], 1)],
            &[(&["b"], &[&["2"]])],
        ));
        let validator: Arc<dyn ExecPattern> = Arc::new(FilterPattern {
            index: 2,
            variables: vec!["?e".to_var(), "?x".to_var()],
            allowed_first_column: bytes("a"),
            join_added: Mutex::new(Vec::new()),
        });
        let stage = Stage::new(
            vec!["?x".to_var()],
            vec![first, second, validator],
            vec![0, 1],
            vec!["?e".to_var(), "?x".to_var()],
        )
        .unwrap();

        let result =
            GenericJoinEngine::execute(&[stage], binding_bag(&["?e"], &[&["a"], &["b"]])).unwrap();

        assert_eq!(result, binding_bag(&["?e", "?x"], &[&["a", "1"]]));
    }

    #[test]
    fn proposes_multiple_variables_in_target_order() {
        let proposer = Arc::new(MapPattern::new(
            0,
            &["?e", "?x", "?y"],
            &[],
            &[(&["a"], &[&["1", "2"]])],
        ));
        let stage = Stage::new(
            vec!["?x".to_var(), "?y".to_var()],
            vec![proposer],
            vec![0],
            vec!["?y".to_var(), "?e".to_var(), "?x".to_var()],
        )
        .unwrap();

        let result = GenericJoinEngine::execute(&[stage], binding_bag(&["?e"], &[&["a"]])).unwrap();

        assert_eq!(
            result,
            binding_bag(&["?y", "?e", "?x"], &[&["2", "a", "1"]])
        );
    }

    #[test]
    fn zero_count_proposals_with_no_extensions_produce_no_rows() {
        let first = Arc::new(MapPattern::new(
            0,
            &["?e", "?x"],
            &[(&["a"], 0), (&["b"], 0)],
            &[],
        ));
        let second = Arc::new(MapPattern::new(
            1,
            &["?e", "?x"],
            &[(&["a"], 0), (&["b"], 0)],
            &[],
        ));
        let stage = Stage::new(
            vec!["?x".to_var()],
            vec![first, second],
            vec![0, 1],
            vec!["?e".to_var(), "?x".to_var()],
        )
        .unwrap();

        let result =
            GenericJoinEngine::execute(&[stage], binding_bag(&["?e"], &[&["a"], &["b"]])).unwrap();

        assert_eq!(
            result,
            BindingBag::empty(vec!["?e".to_var(), "?x".to_var()]).unwrap()
        );
    }

    #[test]
    fn drops_rows_without_a_proposer_before_join() {
        let first = Arc::new(
            MapPattern::new(0, &["?e", "?x"], &[(&["a"], 1)], &[(&["a"], &[&["1"]])])
                .without_proposals_for(&[&["b"]]),
        );
        let second = Arc::new(
            MapPattern::new(1, &["?e", "?x"], &[(&["a"], 2)], &[]).without_proposals_for(&[&["b"]]),
        );
        let stage = Stage::new(
            vec!["?x".to_var()],
            vec![first.clone(), second.clone()],
            vec![0, 1],
            vec!["?e".to_var(), "?x".to_var()],
        )
        .unwrap();

        let result =
            GenericJoinEngine::execute(&[stage], binding_bag(&["?e"], &[&["a"], &["b"]])).unwrap();

        assert_eq!(*first.proposed_inputs.lock().unwrap(), vec![row(&["a"])]);
        assert!(second.proposed_inputs.lock().unwrap().is_empty());
        assert_eq!(result, binding_bag(&["?e", "?x"], &[&["a", "1"]]));
    }

    #[test]
    fn rejects_unknown_proposers_and_invalid_stage_inputs() {
        let invalid =
            Arc::new(MapPattern::new(0, &["?x"], &[(&[], 1)], &[]).with_proposer_override(2));
        let other = Arc::new(MapPattern::new(1, &["?x"], &[(&[], 2)], &[]));
        let validator: Arc<dyn ExecPattern> = Arc::new(FilterPattern {
            index: 2,
            variables: vec!["?x".to_var()],
            allowed_first_column: bytes("unused"),
            join_added: Mutex::new(Vec::new()),
        });
        let stage = Stage::new(
            vec!["?x".to_var()],
            vec![invalid, other, validator],
            vec![0, 1],
            vec!["?x".to_var()],
        )
        .unwrap();
        assert!(GenericJoinEngine::execute(&[stage], BindingBag::unit())
            .unwrap_err()
            .to_string()
            .contains("Unknown proposer index 2"));

        let pattern: Arc<dyn ExecPattern> = Arc::new(MapPattern::new(2, &["?x"], &[], &[]));
        let invalid_stage = Stage::new(
            vec!["?x".to_var()],
            vec![pattern],
            vec![0],
            vec!["?x".to_var()],
        )
        .unwrap();
        assert!(GenericJoinEngine::execute(
            &[invalid_stage],
            binding_bag(&["?already"], &[&["a"]]),
        )
        .is_err());
    }

    #[test]
    fn rejects_proposer_and_validator_layout_changes() {
        let wrong_proposer: Arc<dyn ExecPattern> = Arc::new(WrongLayoutPattern {
            index: 0,
            variables: vec!["?e".to_var(), "?x".to_var()],
            output: binding_bag(&["?x", "?e"], &[&["1", "a"]]),
        });
        let proposal_stage = Stage::new(
            vec!["?x".to_var()],
            vec![wrong_proposer],
            vec![0],
            vec!["?e".to_var(), "?x".to_var()],
        )
        .unwrap();
        assert!(
            GenericJoinEngine::execute(&[proposal_stage], binding_bag(&["?e"], &[&["a"]]),)
                .unwrap_err()
                .to_string()
                .contains("proposed layout")
        );

        let wrong_validator: Arc<dyn ExecPattern> = Arc::new(WrongLayoutPattern {
            index: 1,
            variables: vec!["?e".to_var()],
            output: BindingBag::unit(),
        });
        let validation_stage =
            Stage::new(vec![], vec![wrong_validator], vec![], vec!["?e".to_var()]).unwrap();
        assert!(
            GenericJoinEngine::execute(&[validation_stage], binding_bag(&["?e"], &[&["a"]]),)
                .unwrap_err()
                .to_string()
                .contains("changed the layout")
        );
    }
}
