use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use bytes::Bytes;
use edn::query::{PatternNonValuePlace, PatternValuePlace, Variable};
use slatedb::{DbMetadataOps, DbReadOps};
use tokio::runtime::Handle;

use super::binding_bag::BindingBag;
use super::exec_pattern::{ExecPattern, PatternId};
use super::patterns::function::FunctionPattern;
use super::patterns::not::NotPattern;
use super::patterns::or::OrPattern;
use super::patterns::predicate::PredicatePattern;
use super::patterns::relation::RelationPattern;
use super::patterns::triple::{TriplePattern, TripleTerm};
use super::plan::{
    Descriptor, DescriptorKind, LogicalPlan, LogicalScope, LogicalStage, NestedScopes,
    ParticipantRef,
};
use super::stage::{ParticipantTemplate, Stage, StageTemplate};
use crate::codec::Encode;
use crate::query::{
    non_value_place_to_datatype, resolve_attribute_from_pattern, value_place_to_datatype,
};
use crate::schema::IdentMap;

pub(crate) struct ExecutablePlan {
    variable_order: Vec<Variable>,
    stages: Vec<Stage>,
}

impl ExecutablePlan {
    pub(crate) fn variable_order(&self) -> &[Variable] {
        &self.variable_order
    }

    pub(crate) fn stages(&self) -> &[Stage] {
        &self.stages
    }
}

fn encoded_term(value: crate::ops::DataType) -> TripleTerm {
    TripleTerm::Constant(Bytes::from(value.encode()))
}

fn entity_term(place: &PatternNonValuePlace) -> Result<TripleTerm> {
    match place {
        PatternNonValuePlace::Variable(variable) => Ok(TripleTerm::Variable(variable.clone())),
        PatternNonValuePlace::Placeholder => Ok(TripleTerm::Placeholder),
        PatternNonValuePlace::Entid(_) | PatternNonValuePlace::Ident(_) => {
            non_value_place_to_datatype(place)
                .map(encoded_term)
                .ok_or_else(|| anyhow::anyhow!("Unsupported triple entity term: {place:?}"))
        }
    }
}

fn value_term(place: &PatternValuePlace) -> Result<TripleTerm> {
    match place {
        PatternValuePlace::Variable(variable) => Ok(TripleTerm::Variable(variable.clone())),
        PatternValuePlace::Placeholder => Ok(TripleTerm::Placeholder),
        PatternValuePlace::EntidOrInteger(_)
        | PatternValuePlace::IdentOrKeyword(_)
        | PatternValuePlace::Constant(_) => value_place_to_datatype(place)
            .map(encoded_term)
            .ok_or_else(|| anyhow::anyhow!("Unsupported triple value term: {place:?}")),
    }
}

struct RuntimeAssembler<'a, D, M>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
    plan: &'a LogicalPlan,
    slate: &'a Arc<D>,
    handle: &'a Handle,
    ident_map: &'a IdentMap,
    as_of: i64,
    range_stats: &'a Arc<slatedb_estimates::RangeStats<M>>,
}

impl<D, M> RuntimeAssembler<'_, D, M>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
    fn assemble_triple(
        &self,
        descriptor: &Descriptor,
        pattern: &edn::query::Pattern,
    ) -> Result<Arc<dyn ExecPattern>> {
        let attribute = resolve_attribute_from_pattern(&pattern.attribute, self.ident_map)
            .with_context(|| format!("Failed to resolve triple pattern {}", descriptor.id()))?;
        Ok(Arc::new(TriplePattern::new(
            descriptor.id(),
            entity_term(&pattern.entity)?,
            attribute,
            value_term(&pattern.value)?,
            Arc::clone(self.slate),
            self.handle.clone(),
            self.as_of,
            Arc::clone(self.range_stats),
        )?))
    }

    fn assemble_relation(
        &self,
        descriptor: &Descriptor,
        rows: &[Vec<crate::ops::DataType>],
    ) -> Result<Arc<dyn ExecPattern>> {
        let encoded_rows = rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|value| Bytes::from(value.encode()))
                    .collect()
            })
            .collect();
        let relation = BindingBag::new(descriptor.variables().to_vec(), encoded_rows)
            .with_context(|| format!("Invalid relation descriptor {}", descriptor.id()))?;
        Ok(Arc::new(RelationPattern::new(descriptor.id(), relation)))
    }

    fn nested_scopes(&self, descriptor: &Descriptor) -> Result<&NestedScopes> {
        self.plan
            .nested_scopes()
            .get(&descriptor.id())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Composite descriptor {} has no nested logical scope",
                    descriptor.id()
                )
            })
    }

    fn assemble_or(
        &self,
        descriptor: &Descriptor,
        branches: &[Vec<Descriptor>],
    ) -> Result<Arc<dyn ExecPattern>> {
        let NestedScopes::Or(branch_scopes) = self.nested_scopes(descriptor)? else {
            anyhow::bail!("Descriptor {} expected OR nested scopes", descriptor.id());
        };
        ensure!(
            branches.len() == branch_scopes.len(),
            "OR descriptor {} has {} branches but {} planned scopes",
            descriptor.id(),
            branches.len(),
            branch_scopes.len()
        );

        let mut incoming_variables = None;
        let mut branch_templates = Vec::with_capacity(branches.len());
        for (branch_index, (branch, scope)) in branches.iter().zip(branch_scopes).enumerate() {
            let incoming = scope.incoming_variables().ok_or_else(|| {
                anyhow::anyhow!(
                    "OR descriptor {} branch {branch_index} has no incoming layout",
                    descriptor.id()
                )
            })?;
            if let Some(expected) = incoming_variables.as_deref() {
                ensure!(
                    incoming == expected,
                    "OR descriptor {} branches have different incoming layouts",
                    descriptor.id()
                );
            } else {
                incoming_variables = Some(incoming.to_vec());
            }
            branch_templates.push(self.assemble_scope(scope, branch).with_context(|| {
                format!(
                    "Failed to assemble OR descriptor {} branch {branch_index}",
                    descriptor.id()
                )
            })?);
        }

        Ok(Arc::new(OrPattern::new(
            descriptor.id(),
            descriptor.variables().to_vec(),
            incoming_variables.unwrap_or_default(),
            branch_templates,
        )?))
    }

    fn assemble_not(
        &self,
        descriptor: &Descriptor,
        children: &[Descriptor],
    ) -> Result<Arc<dyn ExecPattern>> {
        let NestedScopes::Not(scope) = self.nested_scopes(descriptor)? else {
            anyhow::bail!("Descriptor {} expected NOT nested scope", descriptor.id());
        };
        let incoming = scope.incoming_variables().ok_or_else(|| {
            anyhow::anyhow!("NOT descriptor {} has no incoming layout", descriptor.id())
        })?;
        ensure!(
            incoming == descriptor.variables(),
            "NOT descriptor {} incoming layout does not contain every correlated variable",
            descriptor.id()
        );
        Ok(Arc::new(NotPattern::new(
            descriptor.id(),
            descriptor.variables().to_vec(),
            self.assemble_scope(scope, children).with_context(|| {
                format!("Failed to assemble NOT descriptor {}", descriptor.id())
            })?,
        )?))
    }

    fn assemble_descriptor(&self, descriptor: &Descriptor) -> Result<Arc<dyn ExecPattern>> {
        let pattern: Arc<dyn ExecPattern> = match descriptor.kind() {
            DescriptorKind::Triple(triple) => self.assemble_triple(descriptor, triple)?,
            DescriptorKind::Relation { rows } => self.assemble_relation(descriptor, rows)?,
            DescriptorKind::Predicate { expression } => {
                Arc::new(PredicatePattern::new(descriptor.id(), expression.clone()))
            }
            DescriptorKind::Function {
                expression, output, ..
            } => Arc::new(FunctionPattern::new(
                descriptor.id(),
                expression.clone(),
                output.clone(),
            )?),
            DescriptorKind::Or { branches } => self.assemble_or(descriptor, branches)?,
            DescriptorKind::Not { children } => self.assemble_not(descriptor, children)?,
        };
        ensure!(
            pattern.variables() == descriptor.variables(),
            "Runtime pattern {} variables {:?} do not match descriptor variables {:?}",
            descriptor.id(),
            pattern.variables(),
            descriptor.variables()
        );
        Ok(pattern)
    }

    fn assemble_patterns(
        &self,
        descriptors: &[Descriptor],
    ) -> Result<HashMap<PatternId, Arc<dyn ExecPattern>>> {
        let mut patterns = HashMap::with_capacity(descriptors.len());
        for descriptor in descriptors {
            let pattern = self
                .assemble_descriptor(descriptor)
                .with_context(|| format!("Failed to assemble descriptor {}", descriptor.id()))?;
            ensure!(
                patterns.insert(descriptor.id(), pattern).is_none(),
                "Duplicate descriptor ID {} in one scope",
                descriptor.id()
            );
        }
        Ok(patterns)
    }

    fn validate_roles(&self, stage: &LogicalStage) -> Result<()> {
        let mut proposers = Vec::with_capacity(stage.proposers().len());
        for proposer in stage.proposers() {
            ensure!(
                !proposers.contains(proposer),
                "Logical stage contains duplicate proposer {proposer:?}"
            );
            ensure!(
                stage.participants().contains(proposer),
                "Logical stage proposer {proposer:?} is not a participant"
            );
            proposers.push(proposer.clone());
        }
        if stage.added().is_empty() {
            ensure!(
                stage.proposers().is_empty(),
                "Validation-only logical stage cannot have proposers"
            );
        } else {
            ensure!(
                !stage.proposers().is_empty(),
                "Proposing logical stage must have a proposer"
            );
        }
        Ok(())
    }

    fn assemble_stage(
        &self,
        stage: &LogicalStage,
        patterns: &HashMap<PatternId, Arc<dyn ExecPattern>>,
    ) -> Result<StageTemplate> {
        self.validate_roles(stage)?;
        let participants = stage
            .participants()
            .iter()
            .map(|participant| match participant {
                ParticipantRef::Pattern(id) => patterns
                    .get(id)
                    .cloned()
                    .map(ParticipantTemplate::Pattern)
                    .ok_or_else(|| anyhow::anyhow!("Unknown logical stage pattern ID {id}")),
                ParticipantRef::Incoming => Ok(ParticipantTemplate::Incoming),
            })
            .collect::<Result<Vec<_>>>()?;
        StageTemplate::new(
            stage.added().to_vec(),
            participants,
            stage.target_variables().to_vec(),
        )
    }

    fn assemble_scope(
        &self,
        scope: &LogicalScope,
        descriptors: &[Descriptor],
    ) -> Result<Vec<StageTemplate>> {
        let patterns = self.assemble_patterns(descriptors)?;
        scope
            .stages()
            .iter()
            .enumerate()
            .map(|(stage_index, stage)| {
                self.assemble_stage(stage, &patterns)
                    .with_context(|| format!("Failed to assemble logical stage {stage_index}"))
            })
            .collect()
    }

    fn assemble(self) -> Result<ExecutablePlan> {
        ensure!(
            self.plan.root_scope().incoming_variables().is_none(),
            "Top-level logical scope cannot have incoming variables"
        );
        let templates = self.assemble_scope(self.plan.root_scope(), self.plan.descriptors())?;
        let stages = templates
            .into_iter()
            .enumerate()
            .map(|(stage_index, template)| {
                template
                    .into_stage()
                    .with_context(|| format!("Failed to materialize root stage {stage_index}"))
            })
            .collect::<Result<Vec<_>>>()?;
        let final_variables = stages
            .last()
            .map(Stage::target_variables)
            .unwrap_or_default();
        ensure!(
            final_variables == self.plan.variable_order(),
            "Executable plan produces variables {final_variables:?}, expected {:?}",
            self.plan.variable_order()
        );
        Ok(ExecutablePlan {
            variable_order: self.plan.variable_order().to_vec(),
            stages,
        })
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn assemble_plan<D, M>(
    plan: &LogicalPlan,
    slate: Arc<D>,
    handle: Handle,
    ident_map: &IdentMap,
    as_of: i64,
    range_stats: Arc<slatedb_estimates::RangeStats<M>>,
) -> Result<ExecutablePlan>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
    RuntimeAssembler {
        plan,
        slate: &slate,
        handle: &handle,
        ident_map,
        as_of,
        range_stats: &range_stats,
    }
    .assemble()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use anyhow::Result;
    use edn::kw;
    use edn::query::ToVariable;

    use super::assemble_plan;
    use crate::codec::{Decode, Encode};
    use crate::ops::{DataType, QueryArg};
    use crate::query::binding_bag::BindingBag;
    use crate::query::engine::GenericJoinEngine;
    use crate::query::plan::build_logical_plan;
    use crate::slate::in_memory_slate;

    #[test]
    fn root_stages_share_each_descriptor_pattern() -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        let components = runtime.block_on(in_memory_slate());
        let query = edn::parse::parse_query("[:find ?e ?v :where [?e :name ?v]]").unwrap();
        let logical = build_logical_plan(&query, &[])?;
        let executable = assemble_plan(
            &logical,
            components.db,
            runtime.handle().clone(),
            &HashMap::from([(kw!(:name), 42)]),
            0,
            components.range_stats,
        )?;

        assert_eq!(executable.variable_order(), &["?e".to_var(), "?v".to_var()]);
        assert_eq!(executable.stages().len(), 2);
        let first = &executable.stages()[0].participants()[0];
        let second = &executable.stages()[1].participants()[0];
        assert_eq!(first.id(), logical.descriptors()[0].id());
        assert!(Arc::ptr_eq(first, second));
        Ok(())
    }

    #[test]
    fn assembles_and_executes_relations_functions_or_and_not() -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        let components = runtime.block_on(in_memory_slate());
        let query = edn::parse::parse_query(
            "[:find ?x ?y
              :in [?x ...]
              :where
              [(+ ?x 1) ?y]
              (or [(= ?x 1)] [(= ?x 2)])
              (not [(= ?x 2)])]",
        )
        .unwrap();
        let arguments = [QueryArg::Collection(vec![
            DataType::Long(0),
            DataType::Long(1),
            DataType::Long(1),
            DataType::Long(2),
        ])];
        let logical = build_logical_plan(&query, &arguments)?;
        let executable = assemble_plan(
            &logical,
            components.db,
            runtime.handle().clone(),
            &HashMap::new(),
            0,
            components.range_stats,
        )?;

        let result = GenericJoinEngine::execute(executable.stages(), BindingBag::unit())?;
        assert_eq!(result.variables, ["?x".to_var(), "?y".to_var()]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(DataType::decode(&result.rows[0][0])?, DataType::Long(1));
        assert_eq!(DataType::decode(&result.rows[0][1])?, DataType::Long(2));
        Ok(())
    }

    #[test]
    fn reports_unknown_attributes_during_runtime_assembly() -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        let components = runtime.block_on(in_memory_slate());
        let query = edn::parse::parse_query("[:find ?e :where [?e :missing \"value\"]]").unwrap();
        let logical = build_logical_plan(&query, &[])?;

        let error = assemble_plan(
            &logical,
            components.db,
            runtime.handle().clone(),
            &HashMap::new(),
            0,
            components.range_stats,
        )
        .err()
        .expect("assembly should fail");

        assert!(format!("{error:#}").contains("Unknown attribute: :missing"));
        Ok(())
    }

    #[test]
    fn relation_values_are_encoded_without_runtime_decoding() -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        let components = runtime.block_on(in_memory_slate());
        let query = edn::parse::parse_query("[:find ?x :in [?x ...] :where [(>= ?x 0)]]").unwrap();
        let arguments = [QueryArg::Collection(vec![
            DataType::Long(1),
            DataType::Long(1),
        ])];
        let logical = build_logical_plan(&query, &arguments)?;
        let executable = assemble_plan(
            &logical,
            components.db,
            runtime.handle().clone(),
            &HashMap::new(),
            0,
            components.range_stats,
        )?;

        let result = GenericJoinEngine::execute(executable.stages(), BindingBag::unit())?;
        assert_eq!(
            result.rows,
            &[vec![bytes::Bytes::from(DataType::Long(1).encode())]]
        );
        Ok(())
    }
}
