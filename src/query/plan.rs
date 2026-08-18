use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{bail, ensure, Context, Result};
use bytes::Bytes;
use edn::query::{
    Binding, Pattern, PatternNonValuePlace, PatternValuePlace, Variable, WhereClause,
};
use itertools::Itertools;
use slatedb::{DbMetadataOps, DbReadOps};

use crate::codec::Encode;
use crate::expr::{expr_variables, Expr};
use crate::ops::{DataType, QueryArg};
use crate::query::{
    convert_predicate, convert_where_fn, non_value_place_to_datatype, pattern_variables,
    query_variable_order, resolve_attribute_from_pattern, value_place_to_datatype,
};

use super::binding_bag::BindingBag;
use super::exec_pattern::{ExecPattern, PatternId};
use super::patterns::function::FunctionPattern;
use super::patterns::not::NotPattern;
use super::patterns::or::OrPattern;
use super::patterns::predicate::PredicatePattern;
use super::patterns::relation::RelationPattern;
use super::patterns::triple::{DbValue, TriplePattern, TripleTerm};
use super::stage::Stage;

//////////////////////////////////
// Descriptors
//////////////////////////////////

#[derive(Clone, Debug, PartialEq)]
struct Descriptor {
    id: PatternId,
    variables: Vec<Variable>,
    kind: DescriptorKind,
}

fn relation_bound_prefix_len(variables: &[Variable], bound: &HashSet<Variable>) -> Option<usize> {
    let prefix_len = variables
        .iter()
        .take_while(|variable| bound.contains(*variable))
        .count();
    if variables[prefix_len..]
        .iter()
        .any(|variable| bound.contains(variable))
    {
        return None;
    }
    Some(prefix_len)
}

fn relation_groundable(variables: &[Variable], bound: &HashSet<Variable>) -> Vec<Variable> {
    relation_bound_prefix_len(variables, bound)
        .and_then(|prefix_len| variables.get(prefix_len))
        .cloned()
        .into_iter()
        .collect()
}

fn branch_derives(
    descriptors: &[Descriptor],
    initial_bound: &HashSet<Variable>,
    required: &[Variable],
) -> bool {
    let mut bound = initial_bound.clone();
    loop {
        let mut changed = false;
        for descriptor in descriptors {
            for variable in descriptor.groundable(&bound) {
                changed |= bound.insert(variable);
            }
        }
        if !changed {
            break;
        }
    }
    required.iter().all(|variable| bound.contains(variable))
}

impl Descriptor {
    fn groundable(&self, bound: &HashSet<Variable>) -> Vec<Variable> {
        match &self.kind {
            DescriptorKind::Triple(_) => self
                .variables
                .iter()
                .filter(|variable| !bound.contains(*variable))
                .cloned()
                .collect(),
            DescriptorKind::Relation { .. } => relation_groundable(&self.variables, bound),
            DescriptorKind::Predicate { .. } | DescriptorKind::Not { .. } => Vec::new(),
            DescriptorKind::Function {
                input_variables,
                output,
                ..
            } => {
                if !bound.contains(output)
                    && input_variables
                        .iter()
                        .all(|variable| bound.contains(variable))
                {
                    vec![output.clone()]
                } else {
                    Vec::new()
                }
            }
            DescriptorKind::Or { branches } => {
                let missing: Vec<Variable> = self
                    .variables
                    .iter()
                    .filter(|variable| !bound.contains(*variable))
                    .cloned()
                    .collect();
                if missing.is_empty()
                    || !branches
                        .iter()
                        .all(|branch| branch_derives(branch, bound, &missing))
                {
                    Vec::new()
                } else {
                    missing
                }
            }
        }
    }

    fn is_or(&self) -> bool {
        matches!(self.kind, DescriptorKind::Or { .. })
    }
}

#[derive(Clone, Debug, PartialEq)]
enum DescriptorKind {
    Triple(Pattern),
    Relation {
        rows: Vec<Vec<DataType>>,
    },
    Predicate {
        expression: Expr,
    },
    Function {
        expression: Expr,
        input_variables: Vec<Variable>,
        output: Variable,
    },
    Or {
        branches: Vec<Vec<Descriptor>>,
    },
    Not {
        children: Vec<Descriptor>,
    },
}

struct DescriptorBuilder {
    next_id: PatternId,
}

impl DescriptorBuilder {
    fn new() -> Self {
        Self { next_id: 0 }
    }

    fn allocate_id(&mut self) -> PatternId {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn relation(&mut self, binding: &Binding, argument: &QueryArg) -> Descriptor {
        let (variable, rows) = match (binding, argument) {
            (Binding::BindScalar(variable), QueryArg::Scalar(value)) => {
                (variable.clone(), vec![vec![value.clone()]])
            }
            (Binding::BindColl(variable), QueryArg::Collection(values)) => (
                variable.clone(),
                values.iter().cloned().map(|value| vec![value]).collect(),
            ),
            _ => unreachable!("query validation rejects unsupported input bindings"),
        };
        Descriptor {
            id: self.allocate_id(),
            variables: vec![variable],
            kind: DescriptorKind::Relation { rows },
        }
    }

    fn triple(&mut self, pattern: &Pattern) -> Descriptor {
        Descriptor {
            id: self.allocate_id(),
            variables: pattern_variables(pattern),
            kind: DescriptorKind::Triple(pattern.clone()),
        }
    }

    fn branch(&mut self, branch: &edn::query::OrWhereClause) -> Result<Vec<Descriptor>> {
        match branch {
            edn::query::OrWhereClause::Clause(clause) => Ok(vec![self.where_clause(clause)?]),
            edn::query::OrWhereClause::And(children) => self.where_clauses(children),
        }
    }

    fn where_clause(&mut self, clause: &WhereClause) -> Result<Descriptor> {
        match clause {
            WhereClause::Pattern(pattern) => Ok(self.triple(pattern)),
            WhereClause::Pred(predicate) => {
                let expression = convert_predicate(predicate)?;
                Ok(Descriptor {
                    id: self.allocate_id(),
                    variables: expr_variables(&expression),
                    kind: DescriptorKind::Predicate { expression },
                })
            }
            WhereClause::WhereFn(function) => {
                let function = convert_where_fn(function)?;
                let input_variables = function.input_variables();
                let variables = input_variables
                    .iter()
                    .cloned()
                    .chain(std::iter::once(function.output.clone()))
                    .unique()
                    .collect();
                Ok(Descriptor {
                    id: self.allocate_id(),
                    variables,
                    kind: DescriptorKind::Function {
                        expression: function.expr,
                        input_variables,
                        output: function.output,
                    },
                })
            }
            WhereClause::OrJoin(or) => {
                let id = self.allocate_id();
                let branches = or
                    .clauses
                    .iter()
                    .map(|branch| self.branch(branch))
                    .collect::<Result<Vec<_>>>()?;
                let variables = branches[0]
                    .iter()
                    .flat_map(|descriptor| descriptor.variables.iter().cloned())
                    .unique()
                    .collect();
                Ok(Descriptor {
                    id,
                    variables,
                    kind: DescriptorKind::Or { branches },
                })
            }
            WhereClause::NotJoin(not) => {
                let id = self.allocate_id();
                let children = self.where_clauses(&not.clauses)?;
                let variables = children
                    .iter()
                    .flat_map(|descriptor| descriptor.variables.iter().cloned())
                    .unique()
                    .collect();
                Ok(Descriptor {
                    id,
                    variables,
                    kind: DescriptorKind::Not { children },
                })
            }
            WhereClause::RuleExpr | WhereClause::TypeAnnotation(_) => {
                unreachable!("query validation rejects unsupported where clauses")
            }
        }
    }

    fn where_clauses(&mut self, clauses: &[WhereClause]) -> Result<Vec<Descriptor>> {
        clauses
            .iter()
            .map(|clause| self.where_clause(clause))
            .collect()
    }
}

//////////////////////////////////
// Logical Plan
//////////////////////////////////

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LogicalDescriptor {
    id: PatternId,
    variables: Vec<Variable>,
    kind: LogicalDescriptorKind,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum LogicalDescriptorKind {
    Triple(Pattern),
    Relation {
        rows: Vec<Vec<DataType>>,
    },
    Predicate {
        expression: Expr,
    },
    Function {
        expression: Expr,
        input_variables: Vec<Variable>,
        output: Variable,
    },
    Or {
        branches: Vec<LogicalPlan>,
    },
    Not {
        children: LogicalPlan,
    },
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum ParticipantRef {
    Pattern(PatternId),
    Incoming,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LogicalStage {
    added: Vec<Variable>,
    proposers: Vec<ParticipantRef>,
    participants: Vec<ParticipantRef>,
    target_variables: Vec<Variable>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LogicalPlan {
    incoming_variables: Option<Vec<Variable>>,
    descriptors: Vec<LogicalDescriptor>,
    stages: Vec<LogicalStage>,
}

fn encoded_term(value: DataType) -> TripleTerm {
    TripleTerm::Constant(Bytes::from(value.encode()))
}

fn entity_term(place: &PatternNonValuePlace) -> Result<TripleTerm> {
    match place {
        PatternNonValuePlace::Variable(variable) => Ok(TripleTerm::Variable(variable.clone())),
        PatternNonValuePlace::Placeholder => {
            bail!("Triple entity placeholders are not executable")
        }
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
        PatternValuePlace::Placeholder => {
            bail!("Triple value placeholders are not executable")
        }
        PatternValuePlace::EntidOrInteger(_)
        | PatternValuePlace::IdentOrKeyword(_)
        | PatternValuePlace::Constant(_) => value_place_to_datatype(place)
            .map(encoded_term)
            .ok_or_else(|| anyhow::anyhow!("Unsupported triple value term: {place:?}")),
    }
}

impl LogicalDescriptor {
    fn materialize<D, M>(&self, db: Arc<DbValue<D, M>>) -> Result<Arc<dyn ExecPattern>>
    where
        D: DbReadOps + Send + Sync + 'static,
        M: DbMetadataOps + Send + Sync + 'static,
    {
        match &self.kind {
            LogicalDescriptorKind::Triple(pattern) => {
                let attribute = resolve_attribute_from_pattern(&pattern.attribute, db.ident_map())
                    .with_context(|| format!("Failed to resolve triple pattern {}", self.id))?;
                Ok(Arc::new(TriplePattern::new(
                    self.id,
                    entity_term(&pattern.entity)?,
                    attribute,
                    value_term(&pattern.value)?,
                    db,
                )?))
            }
            LogicalDescriptorKind::Relation { rows } => {
                let encoded_rows = rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|value| Bytes::from(value.encode()))
                            .collect()
                    })
                    .collect();
                let relation = BindingBag::new(self.variables.clone(), encoded_rows)
                    .with_context(|| format!("Invalid relation descriptor {}", self.id))?;
                Ok(Arc::new(RelationPattern::new(self.id, relation)))
            }
            LogicalDescriptorKind::Predicate { expression } => {
                Ok(Arc::new(PredicatePattern::new(self.id, expression.clone())))
            }
            LogicalDescriptorKind::Function {
                expression, output, ..
            } => Ok(Arc::new(FunctionPattern::new(
                self.id,
                expression.clone(),
                output.clone(),
            )?)),
            LogicalDescriptorKind::Or { branches } => Ok(Arc::new(OrPattern::new(
                self.id,
                self.variables.clone(),
                branches.clone(),
                db,
            )?)),
            LogicalDescriptorKind::Not { children } => Ok(Arc::new(NotPattern::new(
                self.id,
                self.variables.clone(),
                children.clone(),
                db,
            )?)),
        }
    }
}

impl LogicalStage {
    pub(crate) fn target_variables(&self) -> &[Variable] {
        &self.target_variables
    }

    fn proposer_positions(&self) -> Result<Vec<usize>> {
        let mut proposers = HashSet::with_capacity(self.proposers.len());
        for proposer in &self.proposers {
            ensure!(
                proposers.insert(*proposer),
                "Logical stage contains duplicate proposer {proposer:?}"
            );
            ensure!(
                self.participants.contains(proposer),
                "Logical stage proposer {proposer:?} is not a participant"
            );
        }
        Ok(self
            .participants
            .iter()
            .enumerate()
            .filter_map(|(position, participant)| {
                proposers.contains(participant).then_some(position)
            })
            .collect())
    }

    fn materialize(
        &self,
        patterns: &HashMap<PatternId, Arc<dyn ExecPattern>>,
        incoming: Option<&Arc<dyn ExecPattern>>,
    ) -> Result<Stage> {
        let participants = self
            .participants
            .iter()
            .map(|participant| match participant {
                ParticipantRef::Pattern(id) => patterns
                    .get(id)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("Unknown logical stage pattern id {id}")),
                ParticipantRef::Incoming => incoming
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("Root logical stage cannot contain Incoming")),
            })
            .collect::<Result<Vec<_>>>()?;
        Stage::new(
            self.added.clone(),
            participants,
            self.proposer_positions()?,
            self.target_variables.clone(),
        )
    }
}

impl LogicalPlan {
    pub(crate) fn incoming_variables(&self) -> Option<&[Variable]> {
        self.incoming_variables.as_deref()
    }

    pub(crate) fn output_variables(&self) -> &[Variable] {
        self.stages
            .last()
            .map(LogicalStage::target_variables)
            .or_else(|| self.incoming_variables())
            .unwrap_or_default()
    }

    pub(crate) fn materialize<D, M>(
        &self,
        db: Arc<DbValue<D, M>>,
        incoming: Option<Arc<dyn ExecPattern>>,
    ) -> Result<Vec<Stage>>
    where
        D: DbReadOps + Send + Sync + 'static,
        M: DbMetadataOps + Send + Sync + 'static,
    {
        match (&self.incoming_variables, incoming.as_ref()) {
            (None, None) => {}
            (None, Some(_)) => bail!("Root logical plan cannot accept Incoming"),
            (Some(_), None) => bail!("Nested logical plan requires Incoming"),
            (Some(expected), Some(incoming)) => ensure!(
                incoming.variables() == expected,
                "Incoming pattern variables {:?} do not match planned layout {expected:?}",
                incoming.variables()
            ),
        }

        let mut patterns = HashMap::with_capacity(self.descriptors.len());
        for descriptor in &self.descriptors {
            let pattern = descriptor
                .materialize(Arc::clone(&db))
                .with_context(|| format!("Failed to materialize descriptor {}", descriptor.id))?;
            ensure!(
                patterns.insert(descriptor.id, pattern).is_none(),
                "Duplicate descriptor id {} in one scope",
                descriptor.id
            );
        }

        self.stages
            .iter()
            .enumerate()
            .map(|(stage_index, stage)| {
                stage
                    .materialize(&patterns, incoming.as_ref())
                    .with_context(|| format!("Failed to materialize logical stage {stage_index}"))
            })
            .collect()
    }
}

fn fully_bound(variables: &[Variable], bound: &HashSet<Variable>) -> bool {
    variables.iter().all(|variable| bound.contains(variable))
}

// TODO: Could this be removed enitrely? See #454.
fn add_validation_stage(
    descriptors: &[Descriptor],
    incoming_variables: Option<&[Variable]>,
    bound: &[Variable],
    completed: &mut HashSet<ParticipantRef>,
    stages: &mut Vec<LogicalStage>,
) {
    let bound_set: HashSet<Variable> = bound.iter().cloned().collect();
    let mut participants = Vec::new();

    if let Some(incoming_variables) = incoming_variables {
        if !completed.contains(&ParticipantRef::Incoming)
            && fully_bound(incoming_variables, &bound_set)
        {
            participants.push(ParticipantRef::Incoming);
        }
    }

    participants.extend(descriptors.iter().filter_map(|descriptor| {
        let participant = ParticipantRef::Pattern(descriptor.id);
        (!completed.contains(&participant) && fully_bound(&descriptor.variables, &bound_set))
            .then_some(participant)
    }));

    if !participants.is_empty() {
        completed.extend(participants.iter().copied());
        stages.push(LogicalStage {
            added: Vec::new(),
            proposers: Vec::new(),
            participants,
            target_variables: bound.to_vec(),
        });
    }
}

fn can_validate_grouped_or_addition(groundable: &[Variable], added: &[Variable]) -> bool {
    !groundable.is_empty() && groundable.iter().any(|variable| added.contains(variable))
}

fn relation_can_validate_grouped_or_addition(
    variables: &[Variable],
    bound: &HashSet<Variable>,
    added: &[Variable],
    target: &HashSet<Variable>,
) -> bool {
    can_validate_grouped_or_addition(&relation_groundable(variables, bound), added)
        && relation_bound_prefix_len(variables, target).is_some()
}

fn proposing_stage_for(
    variable: &Variable,
    descriptors: &[Descriptor],
    incoming_variables: Option<&[Variable]>,
    bound: &[Variable],
    completed: &HashSet<ParticipantRef>,
) -> Option<(LogicalStage, Vec<ParticipantRef>)> {
    let bound_set: HashSet<Variable> = bound.iter().cloned().collect();
    let mut proposers = Vec::new();

    if let Some(incoming_variables) = incoming_variables {
        if !completed.contains(&ParticipantRef::Incoming)
            && relation_groundable(incoming_variables, &bound_set).contains(variable)
        {
            proposers.push(ParticipantRef::Incoming);
        }
    }
    proposers.extend(descriptors.iter().filter_map(|descriptor| {
        let participant = ParticipantRef::Pattern(descriptor.id);
        (!completed.contains(&participant)
            && !descriptor.is_or()
            && descriptor.groundable(&bound_set).contains(variable))
        .then_some(participant)
    }));

    if !proposers.is_empty() {
        let added = vec![variable.clone()];
        let target_variables: Vec<Variable> = bound.iter().chain(&added).cloned().collect();
        let target_set: HashSet<Variable> = target_variables.iter().cloned().collect();
        let mut participants = Vec::new();

        // TODO: Could these fully bound checks become partially ground checks?
        if let Some(incoming_variables) = incoming_variables {
            let participant = ParticipantRef::Incoming;
            if !completed.contains(&participant)
                && (proposers.contains(&participant)
                    || fully_bound(incoming_variables, &target_set))
            {
                participants.push(participant);
            }
        }
        participants.extend(descriptors.iter().filter_map(|descriptor| {
            let participant = ParticipantRef::Pattern(descriptor.id);
            (!completed.contains(&participant)
                && (proposers.contains(&participant)
                    || fully_bound(&descriptor.variables, &target_set)))
            .then_some(participant)
        }));
        let newly_completed = participants
            .iter()
            .copied()
            .filter(|participant| match participant {
                ParticipantRef::Incoming => {
                    incoming_variables.is_some_and(|variables| fully_bound(variables, &target_set))
                }
                ParticipantRef::Pattern(id) => descriptors
                    .iter()
                    .find(|descriptor| descriptor.id == *id)
                    .is_some_and(|descriptor| fully_bound(&descriptor.variables, &target_set)),
            })
            .collect();
        Some((
            LogicalStage {
                added,
                proposers,
                participants,
                target_variables,
            },
            newly_completed,
        ))
    } else {
        descriptors.iter().find_map(|descriptor| {
            let participant = ParticipantRef::Pattern(descriptor.id);
            if completed.contains(&participant) || !descriptor.is_or() {
                return None;
            }
            let added = descriptor.groundable(&bound_set);
            if !added.contains(variable) {
                return None;
            }
            let target_variables: Vec<Variable> = bound.iter().chain(&added).cloned().collect();
            let target_set: HashSet<Variable> = target_variables.iter().cloned().collect();
            let mut participants = vec![participant];
            let mut newly_completed = vec![participant];

            if let Some(incoming_variables) = incoming_variables {
                let incoming = ParticipantRef::Incoming;
                if !completed.contains(&incoming)
                    && relation_can_validate_grouped_or_addition(
                        incoming_variables,
                        &bound_set,
                        &added,
                        &target_set,
                    )
                {
                    participants.push(incoming);
                    if fully_bound(incoming_variables, &target_set) {
                        newly_completed.push(incoming);
                    }
                }
            }
            for validator in descriptors {
                let validator_participant = ParticipantRef::Pattern(validator.id);
                if !(completed.contains(&validator_participant) || validator.is_or()) {
                    let can_validate = match &validator.kind {
                        DescriptorKind::Relation { .. } => {
                            relation_can_validate_grouped_or_addition(
                                &validator.variables,
                                &bound_set,
                                &added,
                                &target_set,
                            )
                        }
                        _ => can_validate_grouped_or_addition(
                            &validator.groundable(&bound_set),
                            &added,
                        ),
                    };
                    if can_validate {
                        participants.push(validator_participant);
                        if fully_bound(&validator.variables, &target_set) {
                            newly_completed.push(validator_participant);
                        }
                    }
                }
            }

            Some((
                LogicalStage {
                    target_variables,
                    added,
                    proposers: vec![participant],
                    participants,
                },
                newly_completed,
            ))
        })
    }
}

fn plan_stages(
    descriptors: &[Descriptor],
    variable_order: &[Variable],
    incoming_variables: Option<&[Variable]>,
) -> Result<Vec<LogicalStage>> {
    let relevant: HashSet<&Variable> = descriptors
        .iter()
        .flat_map(|descriptor| &descriptor.variables)
        .collect();
    let order: Vec<Variable> = variable_order
        .iter()
        .filter(|variable| relevant.contains(*variable))
        .cloned()
        .collect();
    ensure!(
        order.len() == relevant.len(),
        "Scope contains variables missing from the global variable order"
    );
    let incoming_variables = incoming_variables.map(|variables| {
        variables
            .iter()
            .filter(|variable| relevant.contains(*variable))
            .cloned()
            .collect::<Vec<_>>()
    });
    let incoming_variables = incoming_variables.as_deref();
    let participant_count = descriptors.len() + usize::from(incoming_variables.is_some());
    let mut bound = Vec::new();
    let mut completed = HashSet::new();
    let mut stages = Vec::new();

    loop {
        add_validation_stage(
            descriptors,
            incoming_variables,
            &bound,
            &mut completed,
            &mut stages,
        );

        let bound_set: HashSet<&Variable> = bound.iter().collect();
        let remaining: Vec<&Variable> = order
            .iter()
            .filter(|variable| !bound_set.contains(*variable))
            .collect();
        if remaining.is_empty() {
            if completed.len() == participant_count {
                return Ok(stages);
            }
            let mut unplaced = Vec::new();
            if incoming_variables.is_some() && !completed.contains(&ParticipantRef::Incoming) {
                unplaced.push(ParticipantRef::Incoming);
            }
            unplaced.extend(descriptors.iter().filter_map(|descriptor| {
                let participant = ParticipantRef::Pattern(descriptor.id);
                (!completed.contains(&participant)).then_some(participant)
            }));
            bail!("Unable to place query participants: {unplaced:?}");
        }

        let mut proposed = None;
        for variable in &remaining {
            if let Some(stage) = proposing_stage_for(
                variable,
                descriptors,
                incoming_variables,
                &bound,
                &completed,
            ) {
                proposed = Some(stage);
                break;
            }
        }
        let Some((stage, newly_completed)) = proposed else {
            bail!("Insufficient binding to ground variable {}", remaining[0]);
        };
        completed.extend(newly_completed);
        bound = stage.target_variables.clone();
        stages.push(stage);
    }
}

fn plan_descriptor(
    descriptor: Descriptor,
    incoming_variables: Option<Vec<Variable>>,
    variable_order: &[Variable],
) -> Result<LogicalDescriptor> {
    let kind = match descriptor.kind {
        DescriptorKind::Triple(pattern) => LogicalDescriptorKind::Triple(pattern),
        DescriptorKind::Relation { rows } => LogicalDescriptorKind::Relation { rows },
        DescriptorKind::Predicate { expression } => LogicalDescriptorKind::Predicate { expression },
        DescriptorKind::Function {
            expression,
            input_variables,
            output,
        } => LogicalDescriptorKind::Function {
            expression,
            input_variables,
            output,
        },
        DescriptorKind::Or { branches } => {
            let incoming_variables = incoming_variables.expect("OR has an incoming layout");
            let branches = branches
                .into_iter()
                .enumerate()
                .map(|(branch_index, branch)| {
                    plan_scope(branch, variable_order, Some(incoming_variables.clone()))
                        .with_context(|| {
                            format!(
                                "Failed to plan OR descriptor {} branch {branch_index}",
                                descriptor.id
                            )
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            LogicalDescriptorKind::Or { branches }
        }
        DescriptorKind::Not { children } => {
            let incoming_variables = incoming_variables.expect("NOT has an incoming layout");
            ensure!(
                incoming_variables.iter().collect::<HashSet<_>>()
                    == descriptor.variables.iter().collect::<HashSet<_>>(),
                "NOT descriptor {} incoming layout does not contain every correlated variable",
                descriptor.id
            );
            LogicalDescriptorKind::Not {
                children: plan_scope(children, variable_order, Some(incoming_variables))
                    .with_context(|| format!("Failed to plan NOT descriptor {}", descriptor.id))?,
            }
        }
    };
    Ok(LogicalDescriptor {
        id: descriptor.id,
        variables: descriptor.variables,
        kind,
    })
}

fn plan_scope(
    descriptors: Vec<Descriptor>,
    variable_order: &[Variable],
    incoming_variables: Option<Vec<Variable>>,
) -> Result<LogicalPlan> {
    let relevant: HashSet<&Variable> = descriptors
        .iter()
        .flat_map(|descriptor| &descriptor.variables)
        .collect();
    // We filter the incoming_variables to the actually relevant variables of the nested scope.
    // The incoming BindingBag then gets projected to these relevant incoming variables in the nested ExecPatterns.
    let incoming_variables = incoming_variables.map(|variables| {
        variables
            .into_iter()
            .filter(|variable| relevant.contains(variable))
            .collect::<Vec<_>>()
    });
    let stages = plan_stages(&descriptors, variable_order, incoming_variables.as_deref())?;
    let mut descriptor_input_layouts = HashMap::new();
    let mut previous_target: &[Variable] = &[];
    // TODO: This descriptor_input_layouts magic looks smelly. This should be one fold/walk over
    // LogicalDescriptor's and LogicalPlan's. The only descriptors that really need the input
    // are NOT and OR and they only participate in one stage anyway.
    for stage in &stages {
        for participant in &stage.participants {
            let ParticipantRef::Pattern(id) = participant else {
                continue;
            };
            let input_layout = if stage.proposers.contains(participant) {
                previous_target
            } else {
                &stage.target_variables
            };
            descriptor_input_layouts.entry(*id).or_insert(input_layout);
        }
        previous_target = &stage.target_variables;
    }
    let descriptors = descriptors
        .into_iter()
        .map(|descriptor| {
            let descriptor_input_layout = match &descriptor.kind {
                DescriptorKind::Or { .. } | DescriptorKind::Not { .. } => {
                    let input_layout =
                        descriptor_input_layouts
                            .get(&descriptor.id)
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "Descriptor {} is not placed in any logical stage",
                                    descriptor.id
                                )
                            })?;
                    Some(
                        input_layout
                            .iter()
                            .filter(|variable| descriptor.variables.contains(*variable))
                            .cloned()
                            .collect(),
                    )
                }
                _ => None,
            };
            plan_descriptor(descriptor, descriptor_input_layout, variable_order)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(LogicalPlan {
        incoming_variables,
        descriptors,
        stages,
    })
}

pub(crate) fn build_logical_plan(
    query: &edn::query::ParsedQuery,
    arguments: &[QueryArg],
) -> Result<LogicalPlan> {
    let variable_order = query_variable_order(&query.in_bindings, &query.where_clauses);
    let mut builder = DescriptorBuilder::new();
    let mut descriptors: Vec<Descriptor> = query
        .in_bindings
        .iter()
        .zip(arguments)
        .map(|(binding, argument)| builder.relation(binding, argument))
        .collect();
    descriptors.extend(builder.where_clauses(&query.where_clauses)?);

    plan_scope(descriptors, &variable_order, None)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use bytes::Bytes;
    use edn::kw;
    use edn::query::ToVariable;

    use super::*;
    use crate::codec::Decode;
    use crate::query::engine::GenericJoinEngine;
    use crate::query_validation::validate_query;
    use crate::slate::in_memory_slate;

    fn var(name: &str) -> Variable {
        name.to_var()
    }

    fn relation(id: PatternId, variables: &[&str]) -> Descriptor {
        Descriptor {
            id,
            variables: variables.iter().map(|name| var(name)).collect(),
            kind: DescriptorKind::Relation { rows: Vec::new() },
        }
    }

    fn predicate(id: PatternId, variables: &[&str]) -> Descriptor {
        Descriptor {
            id,
            variables: variables.iter().map(|name| var(name)).collect(),
            kind: DescriptorKind::Predicate {
                expression: Expr::Literal(DataType::Boolean(true)),
            },
        }
    }

    fn function(id: PatternId, input: &str, output: &str) -> Descriptor {
        let input = var(input);
        let output = var(output);
        Descriptor {
            id,
            variables: vec![input.clone(), output.clone()],
            kind: DescriptorKind::Function {
                expression: Expr::Variable(input.clone()),
                input_variables: vec![input],
                output,
            },
        }
    }

    fn or(id: PatternId, variables: &[&str], branches: Vec<Vec<Descriptor>>) -> Descriptor {
        Descriptor {
            id,
            variables: variables.iter().map(|name| var(name)).collect(),
            kind: DescriptorKind::Or { branches },
        }
    }

    fn refs(ids: &[PatternId]) -> Vec<ParticipantRef> {
        ids.iter().copied().map(ParticipantRef::Pattern).collect()
    }

    // Descriptor tests

    #[test]
    fn lowers_query_data_to_one_recursive_descriptor_tree() {
        let query = edn::parse::parse_query(
            r#"[:find ?e ?next
            :in ?name [?age ...]
            :where
            [?e :person/name ?name]
            [(< ?age 30)]
            [(+ ?age 1) ?next]
            (not [?e :person/blocked true])]"#,
        )
        .unwrap();
        let arguments = vec![
            QueryArg::Scalar(DataType::String("Alice".into())),
            QueryArg::Collection(vec![DataType::Long(20), DataType::Long(40)]),
        ];
        validate_query(&query, &arguments).unwrap();

        let plan = build_logical_plan(&query, &arguments).unwrap();

        assert_eq!(
            plan.output_variables(),
            &[var("?name"), var("?age"), var("?e"), var("?next")]
        );
        assert_eq!(plan.descriptors.len(), 6);
        assert!(matches!(
            plan.descriptors[0].kind,
            LogicalDescriptorKind::Relation { .. }
        ));
        assert!(matches!(
            plan.descriptors[2].kind,
            LogicalDescriptorKind::Triple(_)
        ));
        assert!(matches!(
            plan.descriptors[3].kind,
            LogicalDescriptorKind::Predicate { .. }
        ));
        assert!(matches!(
            plan.descriptors[4].kind,
            LogicalDescriptorKind::Function { .. }
        ));
        let LogicalDescriptorKind::Not { children } = &plan.descriptors[5].kind else {
            panic!("expected NOT descriptor");
        };
        assert_eq!(children.incoming_variables(), Some(&[var("?e")][..]));
        assert_eq!(children.descriptors[0].id, 6);
        assert!(matches!(
            children.descriptors[0].kind,
            LogicalDescriptorKind::Triple(_)
        ));
    }

    #[test]
    fn composite_descriptors_distinct_child_variables_in_occurrence_order() {
        let query = edn::parse::parse_query(
            r#"[:find ?e
            :where
            (or (and [?e :a ?x] [?x :b ?e])
                (and [?e :c ?x] [?x :d ?e]))
            (not [?e :blocked-by ?x] [?x :active ?e])]"#,
        )
        .unwrap();
        let mut builder = DescriptorBuilder::new();

        let descriptors = builder.where_clauses(&query.where_clauses).unwrap();

        assert_eq!(descriptors[0].variables, vec![var("?e"), var("?x")]);
        assert_eq!(descriptors[1].variables, vec![var("?e"), var("?x")]);
    }

    #[test]
    fn descriptors_report_only_current_scope_groundability() {
        let relation_descriptor = relation(0, &["?x", "?y"]);
        let function_descriptor = function(1, "?x", "?y");
        let mut bound = HashSet::new();
        assert_eq!(relation_descriptor.groundable(&bound), vec![var("?x")]);
        assert!(function_descriptor.groundable(&bound).is_empty());

        bound.insert(var("?x"));
        assert_eq!(relation_descriptor.groundable(&bound), vec![var("?y")]);
        assert_eq!(function_descriptor.groundable(&bound), vec![var("?y")]);

        let grouped = or(
            2,
            &["?x", "?y"],
            vec![
                vec![relation(3, &["?x"]), function(4, "?x", "?y")],
                vec![relation(5, &["?x", "?y"])],
            ],
        );
        assert_eq!(
            grouped.groundable(&HashSet::new()),
            vec![var("?x"), var("?y")]
        );
    }

    // Stage planning tests

    #[test]
    fn plans_multiple_proposers_and_ready_validators_together() {
        let descriptors = vec![
            relation(0, &["?x"]),
            relation(1, &["?x"]),
            predicate(2, &["?x"]),
        ];

        let stages = plan_stages(&descriptors, &[var("?x")], None).unwrap();

        assert_eq!(
            stages,
            vec![LogicalStage {
                added: vec![var("?x")],
                proposers: refs(&[0, 1]),
                participants: refs(&[0, 1, 2]),
                target_variables: vec![var("?x")],
            }]
        );
    }

    #[test]
    fn grouped_or_partially_validates_relation_then_relation_proposes() {
        let descriptors = vec![
            or(
                0,
                &["?x", "?y"],
                vec![
                    vec![relation(1, &["?x", "?y"])],
                    vec![relation(2, &["?x", "?y"])],
                ],
            ),
            relation(3, &["?y", "?z"]),
        ];

        let stages = plan_stages(&descriptors, &[var("?x"), var("?y"), var("?z")], None).unwrap();

        assert_eq!(
            stages,
            vec![
                LogicalStage {
                    added: vec![var("?x"), var("?y")],
                    proposers: refs(&[0]),
                    participants: refs(&[0, 3]),
                    target_variables: vec![var("?x"), var("?y")],
                },
                LogicalStage {
                    added: vec![var("?z")],
                    proposers: refs(&[3]),
                    participants: refs(&[3]),
                    target_variables: vec![var("?x"), var("?y"), var("?z")],
                },
            ]
        );
    }

    #[test]
    fn grouped_or_does_not_validate_non_prefix_relation() {
        let descriptors = vec![
            relation(0, &["?a", "?b", "?c"]),
            or(
                1,
                &["?x", "?a", "?c"],
                vec![
                    vec![relation(2, &["?x", "?a", "?c"])],
                    vec![relation(3, &["?x", "?a", "?c"])],
                ],
            ),
            relation(4, &["?b"]),
        ];

        let stages = plan_stages(
            &descriptors,
            &[var("?x"), var("?a"), var("?b"), var("?c")],
            None,
        )
        .unwrap();

        assert_eq!(
            stages,
            vec![
                LogicalStage {
                    added: vec![var("?x"), var("?a"), var("?c")],
                    proposers: refs(&[1]),
                    participants: refs(&[1]),
                    target_variables: vec![var("?x"), var("?a"), var("?c")],
                },
                LogicalStage {
                    added: vec![var("?b")],
                    proposers: refs(&[4]),
                    participants: refs(&[0, 4]),
                    target_variables: vec![var("?x"), var("?a"), var("?c"), var("?b")],
                },
            ]
        );
    }

    #[test]
    fn grouped_or_completes_fully_bound_relation_validator() {
        let descriptors = vec![
            or(
                0,
                &["?x", "?y"],
                vec![
                    vec![relation(1, &["?x", "?y"])],
                    vec![relation(2, &["?x", "?y"])],
                ],
            ),
            relation(3, &["?y"]),
        ];

        let stages = plan_stages(&descriptors, &[var("?x"), var("?y")], None).unwrap();

        assert_eq!(
            stages,
            vec![LogicalStage {
                added: vec![var("?x"), var("?y")],
                proposers: refs(&[0]),
                participants: refs(&[0, 3]),
                target_variables: vec![var("?x"), var("?y")],
            }]
        );
    }

    #[test]
    fn grouped_or_validates_and_completes_overlapping_incoming_relation() {
        let descriptors = vec![or(
            0,
            &["?x", "?y"],
            vec![
                vec![relation(1, &["?x", "?y"])],
                vec![relation(2, &["?x", "?y"])],
            ],
        )];
        let incoming = vec![var("?y")];

        let (stage, completed) = proposing_stage_for(
            &var("?x"),
            &descriptors,
            Some(&incoming),
            &[],
            &HashSet::new(),
        )
        .unwrap();

        assert_eq!(
            stage,
            LogicalStage {
                added: vec![var("?x"), var("?y")],
                proposers: refs(&[0]),
                participants: vec![ParticipantRef::Pattern(0), ParticipantRef::Incoming],
                target_variables: vec![var("?x"), var("?y")],
            }
        );
        assert_eq!(
            completed,
            vec![ParticipantRef::Pattern(0), ParticipantRef::Incoming]
        );
    }

    #[test]
    fn predicate_after_grouped_or_uses_validation_only_stage() {
        let descriptors = vec![
            or(
                0,
                &["?x", "?y"],
                vec![
                    vec![relation(1, &["?x", "?y"])],
                    vec![relation(2, &["?x", "?y"])],
                ],
            ),
            predicate(3, &["?x", "?y"]),
        ];

        let stages = plan_stages(&descriptors, &[var("?x"), var("?y")], None).unwrap();

        assert_eq!(
            stages,
            vec![
                LogicalStage {
                    added: vec![var("?x"), var("?y")],
                    proposers: refs(&[0]),
                    participants: refs(&[0]),
                    target_variables: vec![var("?x"), var("?y")],
                },
                LogicalStage {
                    added: Vec::new(),
                    proposers: Vec::new(),
                    participants: refs(&[3]),
                    target_variables: vec![var("?x"), var("?y")],
                },
            ]
        );
    }

    #[test]
    fn first_variable_uses_grouped_or_before_a_later_variable_proposer() {
        let descriptors = vec![
            relation(3, &["?y"]),
            or(
                0,
                &["?x", "?y"],
                vec![
                    vec![relation(1, &["?x", "?y"])],
                    vec![relation(2, &["?x", "?y"])],
                ],
            ),
        ];

        let stages = plan_stages(&descriptors, &[var("?x"), var("?y")], None).unwrap();

        assert_eq!(
            stages,
            vec![LogicalStage {
                added: vec![var("?x"), var("?y")],
                proposers: refs(&[0]),
                participants: refs(&[0, 3]),
                target_variables: vec![var("?x"), var("?y")],
            }]
        );
    }

    #[test]
    fn projects_incoming_layout_before_selecting_proposers() {
        let descriptors = vec![relation(0, &["?x"])];
        let incoming = vec![var("?outer"), var("?x")];
        let plan = plan_scope(descriptors, &[var("?x")], Some(incoming)).unwrap();

        assert_eq!(plan.incoming_variables(), Some(&[var("?x")][..]));
        assert_eq!(
            plan.stages[0].proposers,
            vec![ParticipantRef::Incoming, ParticipantRef::Pattern(0)]
        );
    }

    // This test output seems a bit awkward. Currently GenericJoinEngine always starts from `BindingBag::unit()`.
    // The first empty validation stage kind of removes or keeps the unit row of initial binding bag depending
    // on if the incoming relation has rows or not (even if non of the columns matter in that scope).
    #[test]
    fn plans_projected_zero_column_incoming_as_validation_only_participant() {
        let descriptors = vec![relation(0, &["?x"])];
        let plan =
            plan_scope(descriptors.clone(), &[var("?x")], Some(vec![var("?outer")])).unwrap();

        assert_eq!(plan.incoming_variables(), Some(&[][..]));
        assert_eq!(
            plan.stages,
            vec![
                LogicalStage {
                    added: Vec::new(),
                    proposers: Vec::new(),
                    participants: vec![ParticipantRef::Incoming],
                    target_variables: Vec::new(),
                },
                LogicalStage {
                    added: vec![var("?x")],
                    proposers: refs(&[0]),
                    participants: refs(&[0]),
                    target_variables: vec![var("?x")],
                },
            ]
        );

        let root = plan_scope(descriptors, &[var("?x")], None).unwrap();
        assert_eq!(root.incoming_variables(), None);
        assert_eq!(root.stages.as_slice(), &plan.stages[1..]);
    }

    #[test]
    fn stage_layout_appends_added_variables() {
        let descriptors = vec![function(0, "?x", "?y"), relation(1, &["?x"])];

        let stages = plan_stages(&descriptors, &[var("?y"), var("?x")], None).unwrap();

        assert_eq!(stages[0].target_variables, vec![var("?x")]);
        assert_eq!(stages[1].target_variables, vec![var("?x"), var("?y")]);
    }

    #[test]
    fn reports_insufficient_binding() {
        let descriptors = vec![predicate(0, &["?x"])];

        let error = plan_stages(&descriptors, &[var("?x")], None).unwrap_err();

        assert!(error.to_string().contains("Insufficient binding"));
    }

    #[test]
    fn rejects_scope_variables_missing_from_global_order() {
        let descriptors = vec![relation(0, &["?x"])];

        let error = plan_stages(&descriptors, &[], None).unwrap_err();

        assert!(error.to_string().contains("global variable order"));
    }

    #[test]
    fn recursively_plans_or_inside_not_with_explicit_incoming_layouts() {
        let query = edn::parse::parse_query(
            "[:find ?x
          :in [?x ...]
          :where
          (not (or [(= ?x 2)] [(= ?x 3)]))]",
        )
        .unwrap();
        let arguments = [QueryArg::Collection(vec![DataType::Long(1)])];

        let plan = build_logical_plan(&query, &arguments).unwrap();
        let LogicalDescriptorKind::Not { children } = &plan.descriptors[1].kind else {
            panic!("expected NOT descriptor");
        };
        assert_eq!(children.incoming_variables(), Some(&[var("?x")][..]));
        let LogicalDescriptorKind::Or { branches } = &children.descriptors[0].kind else {
            panic!("expected nested OR descriptor");
        };
        assert_eq!(branches.len(), 2);
        assert!(branches
            .iter()
            .all(|branch| branch.incoming_variables() == Some(&[var("?x")][..])));
    }

    // materialization

    #[test]
    fn materialization_reuses_patterns_and_preserves_proposer_roles() -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        let components = runtime.block_on(in_memory_slate());
        let query = edn::parse::parse_query("[:find ?e ?v :where [?e :name ?v]]").unwrap();
        let logical = build_logical_plan(&query, &[])?;
        let db = Arc::new(DbValue::new(
            components.db,
            runtime.handle().clone(),
            Arc::new(HashMap::from([(kw!(:name), 42)])),
            0,
            components.range_stats,
        ));

        let stages = logical.materialize(db, None)?;

        assert_eq!(logical.output_variables(), &[var("?e"), var("?v")]);
        assert_eq!(stages.len(), 2);
        assert_eq!(stages[0].participants()[0].id(), logical.descriptors[0].id);
        assert!(Arc::ptr_eq(
            &stages[0].participants()[0],
            &stages[1].participants()[0]
        ));
        assert_eq!(stages[0].proposers().len(), 1);
        assert_eq!(stages[1].proposers().len(), 1);
        Ok(())
    }

    #[test]
    fn materialization_preserves_proposers_separately_from_validators() -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        let components = runtime.block_on(in_memory_slate());
        let query = edn::parse::parse_query("[:find ?x :in [?x ...] :where [(>= ?x 0)]]").unwrap();
        let arguments = [QueryArg::Collection(vec![DataType::Long(1)])];
        let logical = build_logical_plan(&query, &arguments)?;
        let db = Arc::new(DbValue::new(
            components.db,
            runtime.handle().clone(),
            Arc::new(HashMap::new()),
            0,
            components.range_stats,
        ));

        let stages = logical.materialize(db, None)?;
        let stage = &stages[0];

        assert_eq!(stage.participants().len(), 2);
        assert_eq!(stage.proposers().len(), 1);
        assert_eq!(stage.proposers().next().unwrap().id(), 0);
        Ok(())
    }

    #[test]
    fn recursively_materializes_and_executes_relations_functions_or_and_not() -> Result<()> {
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
        let db = Arc::new(DbValue::new(
            components.db,
            runtime.handle().clone(),
            Arc::new(HashMap::new()),
            0,
            components.range_stats,
        ));

        let stages = logical.materialize(db, None)?;
        let result = GenericJoinEngine::execute(&stages, BindingBag::unit())?;

        assert_eq!(result.variables, [var("?x"), var("?y")]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(DataType::decode(&result.rows[0][0])?, DataType::Long(1));
        assert_eq!(DataType::decode(&result.rows[0][1])?, DataType::Long(2));
        Ok(())
    }

    #[test]
    fn grouped_or_projects_incoming_preserves_multiplicity_and_accepts_bound_outputs() -> Result<()>
    {
        let runtime = tokio::runtime::Runtime::new()?;
        let components = runtime.block_on(in_memory_slate());
        let query = edn::parse::parse_query(
            "[:find ?x ?y ?z
          :in [?x ...] [?z ...]
          :where
          (or (and [(+ ?x 1) ?y] [(= ?y 2)])
              (and [(+ ?x 2) ?y] [(= ?y 3)]))]",
        )
        .unwrap();
        let arguments = [
            QueryArg::Collection(vec![
                DataType::Long(1),
                DataType::Long(1),
                DataType::Long(2),
            ]),
            QueryArg::Collection(vec![DataType::Long(10)]),
        ];
        let logical = build_logical_plan(&query, &arguments)?;
        let db = Arc::new(DbValue::new(
            components.db,
            runtime.handle().clone(),
            Arc::new(HashMap::new()),
            0,
            components.range_stats,
        ));

        let or_pattern = logical.descriptors[2].materialize(db)?;
        let encoded = |value| Bytes::from(DataType::Long(value).encode());
        let outer = BindingBag::new(
            vec![var("?x"), var("?z")],
            vec![
                vec![encoded(1), encoded(10)],
                vec![encoded(1), encoded(10)],
                vec![encoded(2), encoded(10)],
            ],
        )?;
        let result = or_pattern.join(&outer, &[var("?y")], &[var("?x"), var("?z"), var("?y")])?;

        assert_eq!(result.variables, [var("?x"), var("?z"), var("?y")]);
        let mut rows = result
            .rows
            .iter()
            .map(|row| {
                Ok((
                    DataType::decode(&row[0])?,
                    DataType::decode(&row[1])?,
                    DataType::decode(&row[2])?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        rows.sort_by(|left, right| format!("{left:?}").cmp(&format!("{right:?}")));
        assert_eq!(
            rows,
            vec![
                (DataType::Long(1), DataType::Long(10), DataType::Long(2)),
                (DataType::Long(1), DataType::Long(10), DataType::Long(2)),
                (DataType::Long(1), DataType::Long(10), DataType::Long(3)),
                (DataType::Long(1), DataType::Long(10), DataType::Long(3)),
            ]
        );

        let bound = BindingBag::new(
            vec![var("?x"), var("?z"), var("?y")],
            vec![
                vec![encoded(1), encoded(10), encoded(2)],
                vec![encoded(1), encoded(10), encoded(4)],
            ],
        )?;
        let validated = or_pattern.join(&bound, &[], &[var("?x"), var("?z"), var("?y")])?;

        assert_eq!(validated.variables, [var("?x"), var("?z"), var("?y")]);
        assert_eq!(validated.rows, vec![bound.rows[0].clone()]);
        Ok(())
    }

    #[test]
    fn recursively_executes_or_inside_not() -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        let components = runtime.block_on(in_memory_slate());
        let query = edn::parse::parse_query(
            "[:find ?x
          :in [?x ...]
          :where
          (not (or [(= ?x 2)] [(= ?x 3)]))]",
        )
        .unwrap();
        let arguments = [QueryArg::Collection(vec![
            DataType::Long(1),
            DataType::Long(2),
            DataType::Long(3),
            DataType::Long(4),
        ])];
        let logical = build_logical_plan(&query, &arguments)?;
        let db = Arc::new(DbValue::new(
            components.db,
            runtime.handle().clone(),
            Arc::new(HashMap::new()),
            0,
            components.range_stats,
        ));

        let stages = logical.materialize(db, None)?;
        let result = GenericJoinEngine::execute(&stages, BindingBag::unit())?;
        let mut values = result
            .rows
            .iter()
            .map(|row| -> Result<DataType> { Ok(DataType::decode(&row[0])?) })
            .collect::<Result<Vec<_>>>()?;
        values.sort_by(|left, right| format!("{left:?}").cmp(&format!("{right:?}")));

        assert_eq!(values, vec![DataType::Long(1), DataType::Long(4)]);
        Ok(())
    }

    #[test]
    fn materialization_reports_unknown_attributes() -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        let components = runtime.block_on(in_memory_slate());
        let query = edn::parse::parse_query("[:find ?e :where [?e :missing \"value\"]]").unwrap();
        let logical = build_logical_plan(&query, &[])?;
        let db = Arc::new(DbValue::new(
            components.db,
            runtime.handle().clone(),
            Arc::new(HashMap::new()),
            0,
            components.range_stats,
        ));

        let error = logical
            .materialize(db, None)
            .err()
            .expect("materialization should fail");

        assert!(format!("{error:#}").contains("Unknown attribute: :missing"));
        Ok(())
    }

    #[test]
    fn materialization_validates_incoming_presence_and_layout() -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        let components = runtime.block_on(in_memory_slate());
        let query =
            edn::parse::parse_query("[:find ?x :in [?x ...] :where (or [(= ?x 1)] [(= ?x 2)])]")
                .unwrap();
        let arguments = [QueryArg::Collection(vec![DataType::Long(1)])];
        let root = build_logical_plan(&query, &arguments)?;
        let LogicalDescriptorKind::Or { branches } = &root.descriptors[1].kind else {
            panic!("expected OR descriptor");
        };
        let branch = &branches[0];
        let db = Arc::new(DbValue::new(
            components.db,
            runtime.handle().clone(),
            Arc::new(HashMap::new()),
            0,
            components.range_stats,
        ));

        assert!(root
            .materialize(
                Arc::clone(&db),
                Some(Arc::new(RelationPattern::new(99, BindingBag::unit())))
            )
            .is_err());
        assert!(branch.materialize(Arc::clone(&db), None).is_err());
        let wrong: Arc<dyn ExecPattern> = Arc::new(RelationPattern::new(99, BindingBag::unit()));
        assert!(branch.materialize(db, Some(wrong)).is_err());
        Ok(())
    }
}
