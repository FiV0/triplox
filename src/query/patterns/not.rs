use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use edn::query::Variable;
use slatedb::{DbMetadataOps, DbReadOps};

use super::ensure_unique;
use super::relation::RelationPattern;
use crate::db::DB;
use crate::query::binding_bag::BindingBag;
use crate::query::engine::GenericJoinEngine;
use crate::query::exec_pattern::{ExecPattern, PatternId};
use crate::query::plan::LogicalPlan;

pub(crate) struct NotPattern<D, M>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
    id: PatternId,
    variables: Vec<Variable>,
    incoming_variables: Vec<Variable>,
    logical_plan: LogicalPlan,
    db: Arc<DB<D, M>>,
}

impl<D, M> NotPattern<D, M>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
    pub(crate) fn new(
        id: PatternId,
        variables: Vec<Variable>,
        logical_plan: LogicalPlan,
        db: Arc<DB<D, M>>,
    ) -> Result<Self> {
        ensure_unique("NOT", &variables)?;
        let variables_set: HashSet<&Variable> = variables.iter().collect();
        let incoming_variables = logical_plan
            .incoming_variables()
            .ok_or_else(|| anyhow::anyhow!("NOT logical plan must be nested"))?
            .to_vec();
        ensure!(
            incoming_variables.iter().collect::<HashSet<_>>() == variables_set,
            "NOT incoming variables must be the NOT variable set"
        );
        ensure!(
            logical_plan
                .output_variables()
                .iter()
                .collect::<HashSet<_>>()
                == variables_set,
            "NOT body does not produce the NOT variable set"
        );
        Ok(Self {
            id,
            variables,
            incoming_variables,
            logical_plan,
            db,
        })
    }

    fn execute_logical_plan(&self, input: &BindingBag) -> Result<BindingBag> {
        let projected = input.project(&self.incoming_variables)?;
        let incoming: Arc<dyn ExecPattern> = Arc::new(RelationPattern::new(self.id, projected));
        let stages = self
            .logical_plan
            .materialize(Arc::clone(&self.db), Some(incoming))
            .with_context(|| {
                format!(
                    "NOT pattern {} failed to materialize its logical plan",
                    self.id
                )
            })?;
        GenericJoinEngine::execute(&stages, BindingBag::unit())
            .with_context(|| format!("NOT pattern {} logical plan failed", self.id))?
            .reorder(&self.variables)
    }
}

impl<D, M> ExecPattern for NotPattern<D, M>
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

    fn join(
        &self,
        input: &BindingBag,
        added: &[Variable],
        target_variables: &[Variable],
    ) -> Result<BindingBag> {
        ensure!(
            added.is_empty(),
            "NOT pattern {} cannot propose variables: {added:?}",
            self.id
        );
        ensure!(
            target_variables == input.variables,
            "NOT pattern {} validation must preserve the input layout",
            self.id
        );
        ensure!(
            self.variables
                .iter()
                .all(|variable| input.variables.contains(variable)),
            "NOT pattern {} requires every correlated variable to be bound",
            self.id
        );
        input.antijoin(&self.execute_logical_plan(input)?)
    }
}
