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

        let mut participant_ids = HashSet::with_capacity(participants.len());
        for participant in &participants {
            ensure!(
                participant_ids.insert(participant.id()),
                "Stage participant IDs must be distinct: {}",
                participant.id()
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
}
