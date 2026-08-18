use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use edn::query::Variable;
use slatedb::{DbMetadataOps, DbReadOps};

use super::ensure_unique;
use super::relation::RelationPattern;
use super::triple::DbValue;
use crate::query::binding_bag::BindingBag;
use crate::query::engine::GenericJoinEngine;
use crate::query::exec_pattern::{ExecPattern, PatternId};
use crate::query::plan::LogicalPlan;

pub(crate) struct OrPattern<D, M>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
    id: PatternId,
    variables: Vec<Variable>,
    incoming_variables: Vec<Variable>,
    branch_plans: Vec<LogicalPlan>,
    db: Arc<DbValue<D, M>>,
}

impl<D, M> OrPattern<D, M>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
    pub(crate) fn new(
        id: PatternId,
        variables: Vec<Variable>,
        branch_plans: Vec<LogicalPlan>,
        db: Arc<DbValue<D, M>>,
    ) -> Result<Self> {
        ensure_unique("OR", &variables)?;
        let incoming_variables = branch_plans
            .first()
            .and_then(LogicalPlan::incoming_variables)
            .ok_or_else(|| anyhow::anyhow!("OR must contain at least one nested branch"))?
            .to_vec();
        ensure_unique("OR incoming", &incoming_variables)?;
        ensure!(
            incoming_variables
                .iter()
                .all(|variable| variables.contains(variable)),
            "OR incoming variables must be OR variables"
        );

        let expected: HashSet<&Variable> = variables.iter().collect();
        for (branch_index, branch_plan) in branch_plans.iter().enumerate() {
            ensure!(
                branch_plan
                    .output_variables()
                    .iter()
                    .collect::<HashSet<_>>()
                    == expected,
                "OR branch {branch_index} does not produce the OR variable set"
            );
            ensure!(
                branch_plan.incoming_variables() == Some(incoming_variables.as_slice()),
                "OR branch {branch_index} has a different incoming layout"
            );
        }

        Ok(Self {
            id,
            variables,
            incoming_variables,
            branch_plans,
            db,
        })
    }

    fn validate_invocation(&self, input: &BindingBag) -> Result<()> {
        for variable in &self.incoming_variables {
            ensure!(
                input.variables.contains(variable),
                "OR pattern {} requires incoming variable {variable}",
                self.id
            );
        }
        Ok(())
    }

    fn execute_branches(&self, input: &BindingBag) -> Result<BindingBag> {
        self.validate_invocation(input)?;
        let projected = input.project(&self.incoming_variables)?;
        let incoming: Arc<dyn ExecPattern> = Arc::new(RelationPattern::new(self.id, projected));
        let mut result = BindingBag::empty(self.variables.clone())?;

        for (branch_index, branch_plan) in self.branch_plans.iter().enumerate() {
            let stages = branch_plan
                .materialize(Arc::clone(&self.db), Some(Arc::clone(&incoming)))
                .with_context(|| {
                    format!(
                        "OR pattern {} failed to materialize branch {branch_index}",
                        self.id
                    )
                })?;
            let branch = GenericJoinEngine::execute(&stages, BindingBag::unit())
                .with_context(|| format!("OR pattern {} failed in branch {branch_index}", self.id))?
                .reorder(&self.variables)?;
            result = result.distinct_union(&branch)?;
        }
        Ok(result)
    }
}

impl<D, M> ExecPattern for OrPattern<D, M>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
    fn id(&self) -> PatternId {
        self.id
    }

    fn variables(&self) -> &[Variable] {
        &self.variables
    }

    // The engine bypasses `count` when OR is a stage's sole proposer.
    // TODO: Propose for disjunctions using at least the summed branch-count upper bound.

    fn join(
        &self,
        input: &BindingBag,
        added: &[Variable],
        target_variables: &[Variable],
    ) -> Result<BindingBag> {
        if added.is_empty() {
            ensure!(
                target_variables == input.variables,
                "OR pattern {} validation must preserve the input layout",
                self.id
            );
            ensure!(
                self.variables
                    .iter()
                    .all(|variable| input.variables.contains(variable)),
                "OR pattern {} validation requires every OR variable to be bound",
                self.id
            );
            input.semijoin(&self.execute_branches(input)?)
        } else {
            let expected_added: HashSet<&Variable> = self
                .variables
                .iter()
                .filter(|variable| !input.variables.contains(*variable))
                .collect();
            ensure!(
                added.iter().collect::<HashSet<_>>() == expected_added,
                "OR pattern {} must propose every missing OR variable",
                self.id
            );
            input
                .natural_join(&self.execute_branches(input)?)?
                .reorder(target_variables)
        }
    }
}
