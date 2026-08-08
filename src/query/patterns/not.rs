use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use edn::query::Variable;

use super::relation::RelationPattern;
use crate::query::binding_bag::BindingBag;
use crate::query::engine::GenericJoinEngine;
use crate::query::exec_pattern::{ExecPattern, PatternIndex, Proposal};
use crate::query::stage::StageTemplate;

pub(crate) struct NotPattern {
    index: PatternIndex,
    variables: Vec<Variable>,
    body: Vec<StageTemplate>,
}

impl NotPattern {
    pub(crate) fn new(
        index: PatternIndex,
        variables: Vec<Variable>,
        body: Vec<StageTemplate>,
    ) -> Result<Self> {
        let variables_set: HashSet<&Variable> = variables.iter().collect();
        ensure!(
            variables_set.len() == variables.len(),
            "NOT variables must be unique"
        );
        let final_stage = body
            .last()
            .ok_or_else(|| anyhow::anyhow!("NOT body has no stages"))?;
        ensure!(
            final_stage
                .target_variables()
                .iter()
                .collect::<HashSet<_>>()
                == variables_set,
            "NOT body does not produce the NOT variable set"
        );
        ensure!(
            body.iter().any(StageTemplate::contains_incoming),
            "NOT body has no Incoming participant"
        );
        Ok(Self {
            index,
            variables,
            body,
        })
    }

    fn execute_body(&self, input: &BindingBag) -> Result<BindingBag> {
        let projected = input.project(&self.variables)?;
        let incoming: Arc<dyn ExecPattern> = Arc::new(RelationPattern::new(self.index, projected));
        let stages = self
            .body
            .iter()
            .map(|template| template.materialize(&incoming))
            .collect::<Result<Vec<_>>>()
            .with_context(|| {
                format!("NOT pattern {} failed to materialize its body", self.index)
            })?;
        GenericJoinEngine::execute(&stages, BindingBag::unit())
            .with_context(|| format!("NOT pattern {} body failed", self.index))?
            .reorder(&self.variables)
    }
}

impl ExecPattern for NotPattern {
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
            "NOT pattern {} received {} proposals for {} input rows",
            self.index,
            proposals.len(),
            input.rows.len()
        );
        Ok(())
    }

    fn join(
        &self,
        input: &BindingBag,
        added: &[Variable],
        target_variables: &[Variable],
    ) -> Result<BindingBag> {
        ensure!(
            added.is_empty(),
            "NOT pattern {} cannot propose variables: {added:?}",
            self.index
        );
        ensure!(
            target_variables == input.variables,
            "NOT pattern {} validation must preserve the input layout",
            self.index
        );
        ensure!(
            self.variables
                .iter()
                .all(|variable| input.variables.contains(variable)),
            "NOT pattern {} requires every correlated variable to be bound",
            self.index
        );
        input.antijoin(&self.execute_body(input)?)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use edn::query::ToVariable;

    use super::NotPattern;
    use crate::query::binding_bag::BindingBag;
    use crate::query::exec_pattern::ExecPattern;
    use crate::query::patterns::relation::RelationPattern;
    use crate::query::stage::{ParticipantTemplate, StageTemplate};

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
    fn antijoins_matches_without_losing_unrelated_columns_or_multiplicity() {
        let blocked: Arc<dyn ExecPattern> =
            Arc::new(RelationPattern::new(1, binding_bag(&["?x"], &[&["b"]])));
        let body = vec![StageTemplate::new(
            vec!["?x".to_var()],
            vec![
                ParticipantTemplate::Incoming,
                ParticipantTemplate::Pattern(blocked),
            ],
            vec!["?x".to_var()],
        )
        .unwrap()];
        let pattern = NotPattern::new(100, vec!["?x".to_var()], body).unwrap();
        let input = binding_bag(
            &["?outer", "?x"],
            &[&["u", "a"], &["u", "a"], &["u", "b"], &["v", "b"]],
        );

        assert_eq!(
            pattern.join(&input, &[], &input.variables).unwrap(),
            binding_bag(&["?outer", "?x"], &[&["u", "a"], &["u", "a"]])
        );
    }

    #[test]
    fn never_proposes_and_requires_the_planned_validation_layout() {
        let body = vec![StageTemplate::new(
            vec!["?x".to_var()],
            vec![ParticipantTemplate::Incoming],
            vec!["?x".to_var()],
        )
        .unwrap()];
        let pattern = NotPattern::new(100, vec!["?x".to_var()], body).unwrap();
        let input = binding_bag(&["?x"], &[&["a"]]);

        assert!(pattern
            .join(&input, &["?y".to_var()], &input.variables)
            .is_err());
        assert!(pattern.join(&input, &[], &["?other".to_var()]).is_err());
        assert!(pattern.join(&BindingBag::unit(), &[], &[]).is_err());
    }
}
