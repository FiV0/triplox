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

fn ensure_proposer_positions(
    added: &[Variable],
    participant_count: usize,
    proposer_positions: &[usize],
) -> Result<()> {
    for positions in proposer_positions.windows(2) {
        ensure!(
            positions[0] < positions[1],
            "Stage proposer positions must be unique and ordered"
        );
    }
    for position in proposer_positions {
        ensure!(
            *position < participant_count,
            "Stage proposer position {position} is out of bounds for {participant_count} participants"
        );
    }
    ensure!(
        added.is_empty() == proposer_positions.is_empty(),
        "A stage must have proposers if and only if it adds variables"
    );
    Ok(())
}

pub(crate) struct Stage {
    added: Vec<Variable>,
    participants: Vec<Arc<dyn ExecPattern>>,
    proposer_positions: Vec<usize>,
    target_variables: Vec<Variable>,
}

impl Stage {
    pub(crate) fn new(
        added: Vec<Variable>,
        participants: Vec<Arc<dyn ExecPattern>>,
        proposer_positions: Vec<usize>,
        target_variables: Vec<Variable>,
    ) -> Result<Self> {
        ensure_unique("Added", &added)?;
        ensure_unique("Target", &target_variables)?;
        ensure!(
            !participants.is_empty(),
            "Stage participants must not be empty"
        );
        ensure_proposer_positions(&added, participants.len(), &proposer_positions)?;

        for variable in &added {
            ensure!(
                target_variables.contains(variable),
                "Added variable {variable} is missing from the target layout"
            );
        }

        let mut participant_ids = HashSet::with_capacity(participants.len());
        for participant in &participants {
            ensure!(
                participant_ids.insert(participant.id()),
                "Stage participant ids must be distinct: {}",
                participant.id()
            );
        }

        Ok(Self {
            added,
            participants,
            proposer_positions,
            target_variables,
        })
    }

    pub(crate) fn added(&self) -> &[Variable] {
        &self.added
    }

    pub(crate) fn participants(&self) -> &[Arc<dyn ExecPattern>] {
        &self.participants
    }

    pub(crate) fn proposers(&self) -> impl ExactSizeIterator<Item = &Arc<dyn ExecPattern>> {
        self.proposer_positions
            .iter()
            .map(|position| &self.participants[*position])
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use edn::query::{ToVariable, Variable};

    use super::Stage;
    use crate::query::binding_bag::BindingBag;
    use crate::query::exec_pattern::{ExecPattern, PatternId, Proposal};

    struct TestPattern {
        id: PatternId,
        variables: Vec<Variable>,
    }

    impl TestPattern {
        fn shared(id: PatternId, variables: &[&str]) -> Arc<dyn ExecPattern> {
            Arc::new(Self {
                id,
                variables: variables.iter().map(|variable| variable.to_var()).collect(),
            })
        }
    }

    impl ExecPattern for TestPattern {
        fn id(&self) -> PatternId {
            self.id
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

        assert!(Stage::new(vec![], vec![], vec![], vec![]).is_err());
        assert!(Stage::new(
            vec!["?x".to_var(), "?x".to_var()],
            vec![Arc::clone(&pattern)],
            vec![0],
            vec!["?x".to_var()],
        )
        .is_err());
        assert!(Stage::new(
            vec!["?x".to_var()],
            vec![Arc::clone(&pattern)],
            vec![0],
            vec!["?x".to_var(), "?x".to_var()],
        )
        .is_err());
        assert!(Stage::new(
            vec!["?missing".to_var()],
            vec![Arc::clone(&pattern)],
            vec![0],
            vec!["?x".to_var()],
        )
        .is_err());
        assert!(Stage::new(
            vec!["?x".to_var()],
            vec![Arc::clone(&pattern), pattern],
            vec![0],
            vec!["?x".to_var()],
        )
        .is_err());
    }

    #[test]
    fn stage_construction_enforces_proposer_roles() {
        let first = TestPattern::shared(1, &["?x"]);
        let second = TestPattern::shared(2, &["?x"]);
        let participants = || vec![Arc::clone(&first), Arc::clone(&second)];

        // A stage that adds variables must declare at least one proposer.
        assert!(Stage::new(
            vec!["?x".to_var()],
            participants(),
            vec![],
            vec!["?x".to_var()],
        )
        .is_err());
        // A stage that adds no variables must not declare a proposer.
        assert!(Stage::new(vec![], participants(), vec![0], vec![]).is_err());
        // Every proposer position must refer to an existing participant.
        assert!(Stage::new(
            vec!["?x".to_var()],
            participants(),
            vec![2],
            vec!["?x".to_var()],
        )
        .is_err());
        // Each participant can be declared as a proposer only once.
        assert!(Stage::new(
            vec!["?x".to_var()],
            participants(),
            vec![0, 0],
            vec!["?x".to_var()],
        )
        .is_err());
        // Proposer positions must follow participant order.
        assert!(Stage::new(
            vec!["?x".to_var()],
            participants(),
            vec![1, 0],
            vec!["?x".to_var()],
        )
        .is_err());
    }

    #[test]
    fn stage_validates_each_input_layout_transition() {
        let stage = Stage::new(
            vec!["?y".to_var()],
            vec![TestPattern::shared(1, &["?x", "?y"])],
            vec![0],
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
}
