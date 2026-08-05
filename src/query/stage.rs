use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{ensure, Result};
use edn::query::Variable;

use super::binding_bag::BindingBag;
use super::exec_pattern::ExecPattern;

fn ensure_unique(label: &str, variables: &[Variable]) -> Result<()> {
    let mut seen = HashSet::with_capacity(variables.len());
    for variable in variables {
        ensure!(
            seen.insert(variable),
            "{label} variables must be unique: {variable}"
        );
    }
    Ok(())
}

pub(crate) struct Stage {
    added: Vec<Variable>,
    participants: Vec<Arc<dyn ExecPattern>>,
    target_variables: Vec<Variable>,
}

impl Stage {
    pub(crate) fn new(
        added: Vec<Variable>,
        participants: Vec<Arc<dyn ExecPattern>>,
        target_variables: Vec<Variable>,
    ) -> Result<Self> {
        ensure_unique("Added", &added)?;
        ensure_unique("Target", &target_variables)?;
        ensure!(
            !participants.is_empty(),
            "Stage participants must not be empty"
        );

        for variable in &added {
            ensure!(
                target_variables.contains(variable),
                "Added variable {variable} is missing from the target layout"
            );
        }

        let mut participant_indexes = HashSet::with_capacity(participants.len());
        for participant in &participants {
            ensure!(
                participant_indexes.insert(participant.index()),
                "Stage participant indexes must be distinct: {}",
                participant.index()
            );
        }

        Ok(Self {
            added,
            participants,
            target_variables,
        })
    }

    pub(crate) fn added(&self) -> &[Variable] {
        &self.added
    }

    pub(crate) fn participants(&self) -> &[Arc<dyn ExecPattern>] {
        &self.participants
    }

    pub(crate) fn target_variables(&self) -> &[Variable] {
        &self.target_variables
    }

    // TODO: Is this necessary? Currently it's only used in tests.
    pub(crate) fn validate_input(&self, input: &BindingBag) -> Result<()> {
        for variable in &self.added {
            ensure!(
                !input.variables.contains(variable),
                "Stage cannot add already-bound variable {variable}"
            );
        }

        let expected: HashSet<&Variable> =
            input.variables.iter().chain(self.added.iter()).collect();
        let target: HashSet<&Variable> = self.target_variables.iter().collect();
        ensure!(
            expected == target,
            "Stage target layout must equal the input variables plus added variables"
        );
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) enum ParticipantTemplate {
    Pattern(Arc<dyn ExecPattern>),
    Incoming,
}

#[derive(Clone)]
pub(crate) struct StageTemplate {
    added: Vec<Variable>,
    participants: Vec<ParticipantTemplate>,
    target_variables: Vec<Variable>,
}

impl StageTemplate {
    pub(crate) fn new(
        added: Vec<Variable>,
        participants: Vec<ParticipantTemplate>,
        target_variables: Vec<Variable>,
    ) -> Result<Self> {
        ensure_unique("Added", &added)?;
        ensure_unique("Target", &target_variables)?;
        ensure!(
            !participants.is_empty(),
            "Stage template participants must not be empty"
        );
        for variable in &added {
            ensure!(
                target_variables.contains(variable),
                "Added variable {variable} is missing from the template target layout"
            );
        }

        let mut pattern_indexes = HashSet::new();
        let mut has_incoming = false;
        for participant in &participants {
            match participant {
                ParticipantTemplate::Pattern(pattern) => ensure!(
                    pattern_indexes.insert(pattern.index()),
                    "Stage template pattern indexes must be distinct: {}",
                    pattern.index()
                ),
                ParticipantTemplate::Incoming => ensure!(
                    !std::mem::replace(&mut has_incoming, true),
                    "A stage template can contain Incoming only once"
                ),
            }
        }
        Ok(Self {
            added,
            participants,
            target_variables,
        })
    }

    pub(crate) fn added(&self) -> &[Variable] {
        &self.added
    }

    pub(crate) fn participants(&self) -> &[ParticipantTemplate] {
        &self.participants
    }

    pub(crate) fn target_variables(&self) -> &[Variable] {
        &self.target_variables
    }

    pub(crate) fn contains_incoming(&self) -> bool {
        self.participants
            .iter()
            .any(|participant| matches!(participant, ParticipantTemplate::Incoming))
    }

    pub(crate) fn materialize(&self, incoming: &Arc<dyn ExecPattern>) -> Result<Stage> {
        let participants = self
            .participants
            .iter()
            .map(|participant| match participant {
                ParticipantTemplate::Pattern(pattern) => Arc::clone(pattern),
                ParticipantTemplate::Incoming => Arc::clone(incoming),
            })
            .collect();
        Stage::new(
            self.added.clone(),
            participants,
            self.target_variables.clone(),
        )
    }

    pub(crate) fn into_stage(self) -> Result<Stage> {
        let participants = self
            .participants
            .into_iter()
            .map(|participant| match participant {
                ParticipantTemplate::Pattern(pattern) => Ok(pattern),
                ParticipantTemplate::Incoming => {
                    Err(anyhow::anyhow!("Top-level stage cannot contain Incoming"))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Stage::new(self.added, participants, self.target_variables)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use edn::query::{ToVariable, Variable};

    use super::{ParticipantTemplate, Stage, StageTemplate};
    use crate::query::binding_bag::BindingBag;
    use crate::query::exec_pattern::{ExecPattern, PatternIndex, Proposal};

    struct TestPattern {
        index: PatternIndex,
        variables: Vec<Variable>,
    }

    impl TestPattern {
        fn shared(index: PatternIndex, variables: &[&str]) -> Arc<dyn ExecPattern> {
            Arc::new(Self {
                index,
                variables: variables.iter().map(|variable| variable.to_var()).collect(),
            })
        }
    }

    impl ExecPattern for TestPattern {
        fn index(&self) -> PatternIndex {
            self.index
        }

        fn variables(&self) -> &[Variable] {
            &self.variables
        }

        fn count(
            &self,
            _input: &BindingBag,
            _added: &[Variable],
            _proposals: &mut [Proposal],
        ) -> Result<()> {
            Ok(())
        }

        fn join(
            &self,
            input: &BindingBag,
            _added: &[Variable],
            target_variables: &[Variable],
        ) -> Result<BindingBag> {
            input.reorder(target_variables)
        }
    }

    #[test]
    fn stage_construction_enforces_static_invariants() {
        let pattern = TestPattern::shared(1, &["?x"]);

        assert!(Stage::new(vec![], vec![], vec![]).is_err());
        assert!(Stage::new(
            vec!["?x".to_var(), "?x".to_var()],
            vec![Arc::clone(&pattern)],
            vec!["?x".to_var()],
        )
        .is_err());
        assert!(Stage::new(
            vec!["?x".to_var()],
            vec![Arc::clone(&pattern)],
            vec!["?x".to_var(), "?x".to_var()],
        )
        .is_err());
        assert!(Stage::new(
            vec!["?missing".to_var()],
            vec![Arc::clone(&pattern)],
            vec!["?x".to_var()],
        )
        .is_err());
        assert!(Stage::new(
            vec!["?x".to_var()],
            vec![Arc::clone(&pattern), pattern],
            vec!["?x".to_var()],
        )
        .is_err());
    }

    #[test]
    fn stage_validates_each_input_layout_transition() {
        let stage = Stage::new(
            vec!["?y".to_var()],
            vec![TestPattern::shared(1, &["?x", "?y"])],
            vec!["?y".to_var(), "?x".to_var()],
        )
        .unwrap();

        let input = BindingBag::empty(vec!["?x".to_var()]).unwrap();
        assert!(stage.validate_input(&input).is_ok());

        let already_added = BindingBag::empty(vec!["?x".to_var(), "?y".to_var()]).unwrap();
        assert!(stage.validate_input(&already_added).is_err());

        let wrong_input = BindingBag::empty(vec!["?z".to_var()]).unwrap();
        assert!(stage.validate_input(&wrong_input).is_err());
    }

    #[test]
    fn stage_template_materializes_every_incoming_slot_with_the_same_pattern() {
        let first = TestPattern::shared(1, &["?x"]);
        let incoming = TestPattern::shared(9, &["?x", "?y"]);
        let templates = [
            StageTemplate::new(
                vec!["?x".to_var()],
                vec![
                    ParticipantTemplate::Incoming,
                    ParticipantTemplate::Pattern(first),
                ],
                vec!["?x".to_var()],
            )
            .unwrap(),
            StageTemplate::new(
                vec!["?y".to_var()],
                vec![ParticipantTemplate::Incoming],
                vec!["?x".to_var(), "?y".to_var()],
            )
            .unwrap(),
        ];

        let stages = templates
            .iter()
            .map(|template| template.materialize(&incoming))
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(stages[0].participants()[0].index(), 9);
        assert_eq!(stages[1].participants()[0].index(), 9);
        assert!(Arc::ptr_eq(
            &stages[0].participants()[0],
            &stages[1].participants()[0]
        ));
    }

    #[test]
    fn stage_template_rejects_duplicate_slots_and_top_level_incoming() {
        let pattern = TestPattern::shared(1, &["?x"]);
        assert!(StageTemplate::new(vec![], vec![], vec![]).is_err());
        assert!(StageTemplate::new(
            vec![],
            vec![
                ParticipantTemplate::Pattern(Arc::clone(&pattern)),
                ParticipantTemplate::Pattern(pattern),
            ],
            vec![],
        )
        .is_err());
        assert!(StageTemplate::new(
            vec![],
            vec![ParticipantTemplate::Incoming, ParticipantTemplate::Incoming,],
            vec![],
        )
        .is_err());
        assert!(
            StageTemplate::new(vec![], vec![ParticipantTemplate::Incoming], vec![],)
                .unwrap()
                .into_stage()
                .is_err()
        );
    }
}
