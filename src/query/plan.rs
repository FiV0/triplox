use std::collections::HashSet;

use anyhow::{bail, ensure, Result};
use edn::query::{Binding, Pattern, UnifyVars, Variable, WhereClause};
use itertools::Itertools;

use crate::expr::{expr_variables, Expr};
use crate::ops::{DataType, QueryArg};
use crate::query::{convert_predicate, convert_where_fn, pattern_variables, query_variable_order};

use super::exec_pattern::PatternIndex;

//////////////////////////////////
// Descriptors
//////////////////////////////////

#[derive(Clone, Debug, PartialEq)]
struct Descriptor {
    id: PatternIndex,
    variables: Vec<Variable>,
    kind: DescriptorKind,
}

fn relation_groundable(variables: &[Variable], bound: &HashSet<Variable>) -> Vec<Variable> {
    let prefix_len = variables
        .iter()
        .take_while(|variable| bound.contains(*variable))
        .count();
    if variables[prefix_len..]
        .iter()
        .any(|variable| bound.contains(variable))
    {
        return Vec::new();
    }
    variables.get(prefix_len).cloned().into_iter().collect()
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

// TODO there is like a 1-1 relationship between Descriptor and LogicalDescriptor, as well as
// DescriptorKind and LogicalDescriptorKind. Then only difference is that one carries pure
// transformed patterns (triple, or, not etc..) and the other holds hole query plans for the
// recrusive structures like or and not. For non-recursive patterns the structural is identical.
// There is likely some refactor one can do here.

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
    next_id: PatternIndex,
}

impl DescriptorBuilder {
    fn new() -> Self {
        Self { next_id: 0 }
    }

    fn allocate_id(&mut self) -> PatternIndex {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn relation(&mut self, binding: &Binding, argument: &QueryArg) -> Result<Descriptor> {
        let (variable, rows) = match (binding, argument) {
            (Binding::BindScalar(variable), QueryArg::Scalar(value)) => {
                (variable.clone(), vec![vec![value.clone()]])
            }
            (Binding::BindColl(variable), QueryArg::Collection(values)) => (
                variable.clone(),
                values.iter().cloned().map(|value| vec![value]).collect(),
            ),
            (Binding::BindScalar(_), _) => bail!("Scalar input binding requires a scalar argument"),
            (Binding::BindColl(_), _) => {
                bail!("Collection input binding requires a collection argument")
            }
            (Binding::BindTuple(_), _) | (Binding::BindRel(_), _) => {
                bail!("Tuple and relation input bindings are not supported")
            }
        };
        Ok(Descriptor {
            id: self.allocate_id(),
            variables: vec![variable],
            kind: DescriptorKind::Relation { rows },
        })
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
                ensure!(
                    !input_variables.contains(&function.output),
                    "Function output {} is also an input",
                    function.output
                );
                let mut variables = input_variables.clone();
                variables.push(function.output.clone());
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
                ensure!(
                    matches!(or.unify_vars, UnifyVars::Implicit),
                    "Explicit or-join is not supported"
                );
                ensure!(
                    !or.clauses.is_empty(),
                    "OR must contain at least one branch"
                );
                let id = self.allocate_id();
                let branches = or
                    .clauses
                    .iter()
                    .map(|branch| self.branch(branch))
                    .collect::<Result<Vec<_>>>()?;
                let branch_sets: Vec<HashSet<Variable>> = branches
                    .iter()
                    .map(|branch| {
                        branch
                            .iter()
                            .flat_map(|descriptor| descriptor.variables.iter().cloned())
                            .collect()
                    })
                    .collect();
                ensure!(
                    branch_sets.windows(2).all(|sets| sets[0] == sets[1]),
                    "OR branches must mention the same variables"
                );
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
                bail!("Unsupported where clause: {clause:?}")
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
    id: PatternIndex,
    variables: Vec<Variable>,
    kind: LogicalDescriptorKind,
}

impl LogicalDescriptor {
    pub(crate) fn id(&self) -> PatternIndex {
        self.id
    }

    pub(crate) fn variables(&self) -> &[Variable] {
        &self.variables
    }

    pub(crate) fn kind(&self) -> &LogicalDescriptorKind {
        &self.kind
    }
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
        branches: Vec<LogicalScope>,
    },
    Not {
        body: Box<LogicalScope>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ParticipantRef {
    Pattern(PatternIndex),
    Incoming,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LogicalStage {
    added: Vec<Variable>,
    proposers: Vec<ParticipantRef>,
    participants: Vec<ParticipantRef>,
    target_variables: Vec<Variable>,
}

impl LogicalStage {
    pub(crate) fn added(&self) -> &[Variable] {
        &self.added
    }

    pub(crate) fn proposers(&self) -> &[ParticipantRef] {
        &self.proposers
    }

    pub(crate) fn participants(&self) -> &[ParticipantRef] {
        &self.participants
    }

    pub(crate) fn target_variables(&self) -> &[Variable] {
        &self.target_variables
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LogicalScope {
    incoming_variables: Option<Vec<Variable>>,
    descriptors: Vec<LogicalDescriptor>,
    stages: Vec<LogicalStage>,
}

impl LogicalScope {
    pub(crate) fn incoming_variables(&self) -> Option<&[Variable]> {
        self.incoming_variables.as_deref()
    }

    pub(crate) fn descriptors(&self) -> &[LogicalDescriptor] {
        &self.descriptors
    }

    pub(crate) fn stages(&self) -> &[LogicalStage] {
        &self.stages
    }

    pub(crate) fn output_variables(&self) -> &[Variable] {
        self.stages
            .last()
            .map(LogicalStage::target_variables)
            .unwrap_or_default()
    }
}

#[derive(Clone, Copy)]
enum PlanningParticipant<'a> {
    Incoming(&'a [Variable]),
    Pattern(&'a Descriptor),
}

impl PlanningParticipant<'_> {
    fn reference(&self) -> ParticipantRef {
        match self {
            Self::Incoming(_) => ParticipantRef::Incoming,
            Self::Pattern(descriptor) => ParticipantRef::Pattern(descriptor.id),
        }
    }

    fn variables(&self) -> &[Variable] {
        match self {
            Self::Incoming(variables) => variables,
            Self::Pattern(descriptor) => &descriptor.variables,
        }
    }

    fn groundable(&self, bound: &HashSet<Variable>) -> Vec<Variable> {
        match self {
            Self::Incoming(variables) => relation_groundable(variables, bound),
            Self::Pattern(descriptor) => descriptor.groundable(bound),
        }
    }

    fn is_or(&self) -> bool {
        matches!(self, Self::Pattern(descriptor) if descriptor.is_or())
    }

    fn fully_bound(&self, bound: &HashSet<Variable>) -> bool {
        self.variables()
            .iter()
            .all(|variable| bound.contains(variable))
    }
}

struct ParticipantState<'a> {
    participant: PlanningParticipant<'a>,
    completed: bool,
}

fn scope_variable_order(
    descriptors: &[Descriptor],
    variable_order: &[Variable],
    incoming_variables: Option<&[Variable]>,
) -> Vec<Variable> {
    let relevant: HashSet<Variable> = descriptors
        .iter()
        .flat_map(|descriptor| descriptor.variables.iter().cloned())
        .collect();
    let mut seen = HashSet::new();
    let mut order = Vec::new();

    if let Some(incoming) = incoming_variables {
        for variable in incoming {
            if relevant.contains(variable) && seen.insert(variable.clone()) {
                order.push(variable.clone());
            }
        }
    }
    for variable in variable_order {
        if relevant.contains(variable) && seen.insert(variable.clone()) {
            order.push(variable.clone());
        }
    }
    for descriptor in descriptors {
        for variable in &descriptor.variables {
            if seen.insert(variable.clone()) {
                order.push(variable.clone());
            }
        }
    }
    order
}

fn target_layout(order: &[Variable], bound: &HashSet<Variable>) -> Vec<Variable> {
    order
        .iter()
        .filter(|variable| bound.contains(*variable))
        .cloned()
        .collect()
}

fn plan_stages(
    descriptors: &[Descriptor],
    variable_order: &[Variable],
    incoming_variables: Option<&[Variable]>,
) -> Result<Vec<LogicalStage>> {
    let order = scope_variable_order(descriptors, variable_order, incoming_variables);
    let mut states = Vec::new();
    if let Some(incoming) = incoming_variables {
        states.push(ParticipantState {
            participant: PlanningParticipant::Incoming(incoming),
            completed: false,
        });
    }
    states.extend(descriptors.iter().map(|descriptor| ParticipantState {
        participant: PlanningParticipant::Pattern(descriptor),
        completed: false,
    }));

    let mut bound = HashSet::new();
    let mut current_layout = Vec::new();
    let mut stages = Vec::new();

    while states.iter().any(|state| !state.completed) {
        let validators: Vec<usize> = states
            .iter()
            .enumerate()
            .filter(|(_, state)| !state.completed && state.participant.fully_bound(&bound))
            .map(|(index, _)| index)
            .collect();
        if !validators.is_empty() {
            let participants = validators
                .iter()
                .map(|index| states[*index].participant.reference())
                .collect();
            for index in validators {
                states[index].completed = true;
            }
            stages.push(LogicalStage {
                added: Vec::new(),
                proposers: Vec::new(),
                participants,
                target_variables: current_layout.clone(),
            });
            continue;
        }

        let selected = order.iter().find(|variable| {
            !bound.contains(*variable)
                && states.iter().any(|state| {
                    !state.completed
                        && !state.participant.is_or()
                        && state.participant.groundable(&bound).contains(*variable)
                })
        });
        if let Some(selected) = selected {
            let proposer_indexes: Vec<usize> = states
                .iter()
                .enumerate()
                .filter(|(_, state)| {
                    !state.completed
                        && !state.participant.is_or()
                        && state.participant.groundable(&bound).contains(selected)
                })
                .map(|(index, _)| index)
                .collect();
            bound.insert(selected.clone());
            let target_variables = target_layout(&order, &bound);
            let participant_indexes: Vec<usize> = states
                .iter()
                .enumerate()
                .filter(|(index, state)| {
                    !state.completed
                        && (proposer_indexes.contains(index)
                            || state.participant.fully_bound(&bound))
                })
                .map(|(index, _)| index)
                .collect();
            let proposers = proposer_indexes
                .iter()
                .map(|index| states[*index].participant.reference())
                .collect();
            let participants = participant_indexes
                .iter()
                .map(|index| states[*index].participant.reference())
                .collect();
            for index in participant_indexes {
                if states[index].participant.fully_bound(&bound) {
                    states[index].completed = true;
                }
            }
            stages.push(LogicalStage {
                added: vec![selected.clone()],
                proposers,
                participants,
                target_variables: target_variables.clone(),
            });
            current_layout = target_variables;
            continue;
        }

        let grouped_or = states.iter().enumerate().find_map(|(index, state)| {
            if state.completed || !state.participant.is_or() {
                return None;
            }
            let added = state.participant.groundable(&bound);
            (!added.is_empty()).then_some((index, added))
        });
        if let Some((index, added)) = grouped_or {
            for variable in &added {
                ensure!(
                    bound.insert(variable.clone()),
                    "OR participant tried to add already-bound variable {variable}"
                );
            }
            let target_variables = target_layout(&order, &bound);
            let participant = states[index].participant.reference();
            states[index].completed = true;
            stages.push(LogicalStage {
                added,
                proposers: vec![participant.clone()],
                participants: vec![participant],
                target_variables: target_variables.clone(),
            });
            current_layout = target_variables;
            continue;
        }

        if let Some(variable) = order.iter().find(|variable| !bound.contains(*variable)) {
            bail!("Insufficient binding to ground variable {variable}");
        }
        let unplaced: Vec<ParticipantRef> = states
            .iter()
            .filter(|state| !state.completed)
            .map(|state| state.participant.reference())
            .collect();
        bail!("Unable to place query participants: {unplaced:?}");
    }

    Ok(stages)
}

fn composite_incoming_layout(
    descriptor_id: PatternIndex,
    descriptor_variables: &[Variable],
    stages: &[LogicalStage],
) -> Result<Vec<Variable>> {
    let reference = ParticipantRef::Pattern(descriptor_id);
    let mut previous_layout = Vec::new();
    for stage in stages {
        if stage.participants.contains(&reference) {
            let layout = if stage.proposers.contains(&reference) {
                &previous_layout
            } else {
                &stage.target_variables
            };
            return Ok(layout
                .iter()
                .filter(|variable| descriptor_variables.contains(*variable))
                .cloned()
                .collect());
        }
        previous_layout = stage.target_variables.clone();
    }
    bail!(
        "Composite descriptor {} was not placed in its owning scope",
        descriptor_id
    )
}

fn plan_scope(
    descriptors: Vec<Descriptor>,
    variable_order: &[Variable],
    incoming_variables: Option<Vec<Variable>>,
) -> Result<LogicalScope> {
    let stages = plan_stages(&descriptors, variable_order, incoming_variables.as_deref())?;
    let mut logical_descriptors = Vec::with_capacity(descriptors.len());
    for descriptor in descriptors {
        let Descriptor {
            id,
            variables,
            kind,
        } = descriptor;
        let kind = match kind {
            DescriptorKind::Triple(pattern) => LogicalDescriptorKind::Triple(pattern),
            DescriptorKind::Relation { rows } => LogicalDescriptorKind::Relation { rows },
            DescriptorKind::Predicate { expression } => {
                LogicalDescriptorKind::Predicate { expression }
            }
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
                let incoming = composite_incoming_layout(id, &variables, &stages)?;
                let mut logical_branches = Vec::with_capacity(branches.len());
                for branch in branches {
                    logical_branches.push(plan_scope(
                        branch,
                        variable_order,
                        Some(incoming.clone()),
                    )?);
                }
                LogicalDescriptorKind::Or {
                    branches: logical_branches,
                }
            }
            DescriptorKind::Not { children } => {
                let incoming = composite_incoming_layout(id, &variables, &stages)?;
                LogicalDescriptorKind::Not {
                    body: Box::new(plan_scope(children, variable_order, Some(incoming))?),
                }
            }
        };
        logical_descriptors.push(LogicalDescriptor {
            id,
            variables,
            kind,
        });
    }

    Ok(LogicalScope {
        incoming_variables,
        descriptors: logical_descriptors,
        stages,
    })
}

pub(crate) fn build_logical_plan(
    query: &edn::query::ParsedQuery,
    arguments: &[QueryArg],
) -> Result<LogicalScope> {
    ensure!(
        query.in_bindings.len() == arguments.len(),
        "Query declares {} input bindings but received {} arguments",
        query.in_bindings.len(),
        arguments.len()
    );
    let variable_order = query_variable_order(&query.in_bindings, &query.where_clauses);
    let mut builder = DescriptorBuilder::new();
    let mut descriptors = query
        .in_bindings
        .iter()
        .zip(arguments)
        .map(|(binding, argument)| builder.relation(binding, argument))
        .collect::<Result<Vec<_>>>()?;
    descriptors.extend(builder.where_clauses(&query.where_clauses)?);

    let root_scope = plan_scope(descriptors, &variable_order, None)?;
    ensure!(
        root_scope.output_variables() == variable_order,
        "Root logical scope produces variables {:?}, expected {variable_order:?}",
        root_scope.output_variables()
    );
    Ok(root_scope)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use edn::query::{PatternNonValuePlace, PatternValuePlace, ToVariable};

    use super::*;
    use crate::expr::{BinaryExpr, BinaryOp};
    use crate::query_validation::validate_query;

    fn var(name: &str) -> Variable {
        name.to_var()
    }

    fn relation(id: PatternIndex, variables: &[&str]) -> Descriptor {
        Descriptor {
            id,
            variables: variables.iter().map(|name| var(name)).collect(),
            kind: DescriptorKind::Relation { rows: Vec::new() },
        }
    }

    fn predicate(id: PatternIndex, variables: &[&str]) -> Descriptor {
        let expression = variables
            .first()
            .map(|name| Expr::Variable(var(name)))
            .unwrap_or(Expr::Literal(DataType::Boolean(true)));
        Descriptor {
            id,
            variables: variables.iter().map(|name| var(name)).collect(),
            kind: DescriptorKind::Predicate { expression },
        }
    }

    fn function(id: PatternIndex, input: &str, output: &str) -> Descriptor {
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

    fn or(id: PatternIndex, variables: &[&str], branches: Vec<Vec<Descriptor>>) -> Descriptor {
        Descriptor {
            id,
            variables: variables.iter().map(|name| var(name)).collect(),
            kind: DescriptorKind::Or { branches },
        }
    }

    fn not(id: PatternIndex, variables: &[&str], children: Vec<Descriptor>) -> Descriptor {
        Descriptor {
            id,
            variables: variables.iter().map(|name| var(name)).collect(),
            kind: DescriptorKind::Not { children },
        }
    }

    fn refs(ids: &[PatternIndex]) -> Vec<ParticipantRef> {
        ids.iter().copied().map(ParticipantRef::Pattern).collect()
    }

    #[test]
    fn lowers_inputs_and_every_supported_clause_to_stable_descriptors() {
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
        assert_eq!(plan.descriptors[0].id, 0);
        assert!(matches!(
            plan.descriptors[0].kind,
            LogicalDescriptorKind::Relation { .. }
        ));
        assert_eq!(plan.descriptors[1].id, 1);
        assert!(matches!(
            plan.descriptors[1].kind,
            LogicalDescriptorKind::Relation { .. }
        ));
        assert_eq!(plan.descriptors[2].id, 2);
        assert!(matches!(
            plan.descriptors[2].kind,
            LogicalDescriptorKind::Triple(_)
        ));
        assert_eq!(plan.descriptors[3].id, 3);
        assert!(matches!(
            plan.descriptors[3].kind,
            LogicalDescriptorKind::Predicate { .. }
        ));
        assert_eq!(plan.descriptors[4].id, 4);
        assert!(matches!(
            plan.descriptors[4].kind,
            LogicalDescriptorKind::Function { .. }
        ));
        assert_eq!(plan.descriptors[5].id, 5);
        let LogicalDescriptorKind::Not { body } = &plan.descriptors[5].kind else {
            panic!("expected NOT descriptor");
        };
        assert_eq!(body.descriptors[0].id, 6);
        assert!(matches!(
            body.descriptors[0].kind,
            LogicalDescriptorKind::Triple(_)
        ));
    }

    #[test]
    fn lowers_or_branches_recursively() {
        let query = edn::parse::parse_query(
            r#"[:find ?e :where (or [?e :person/name "A"] [?e :person/name "B"])]"#,
        )
        .unwrap();
        validate_query(&query, &[]).unwrap();
        let plan = build_logical_plan(&query, &[]).unwrap();
        let LogicalDescriptorKind::Or { branches } = &plan.descriptors[0].kind else {
            panic!("expected OR descriptor");
        };
        assert_eq!(plan.descriptors[0].id, 0);
        assert_eq!(branches[0].descriptors[0].id, 1);
        assert_eq!(branches[1].descriptors[0].id, 2);
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
    fn relation_groundability_requires_an_already_bound_prefix() {
        let descriptor = relation(0, &["?x", "?y", "?z"]);
        let mut bound = HashSet::new();
        assert_eq!(descriptor.groundable(&bound), vec![var("?x")]);

        bound.insert(var("?x"));
        assert_eq!(descriptor.groundable(&bound), vec![var("?y")]);

        bound.clear();
        bound.insert(var("?y"));
        assert!(descriptor.groundable(&bound).is_empty());
    }

    #[test]
    fn function_groundability_waits_for_every_input() {
        let descriptor = function(0, "?x", "?y");
        let mut bound = HashSet::new();
        assert!(descriptor.groundable(&bound).is_empty());
        bound.insert(var("?x"));
        assert_eq!(descriptor.groundable(&bound), vec![var("?y")]);
        bound.insert(var("?y"));
        assert!(descriptor.groundable(&bound).is_empty());
    }

    #[test]
    fn or_groundability_is_fixed_point_and_all_or_nothing() {
        let derivable = or(
            0,
            &["?x", "?y"],
            vec![
                vec![relation(1, &["?x"]), function(2, "?x", "?y")],
                vec![relation(3, &["?x", "?y"])],
            ],
        );
        assert_eq!(
            derivable.groundable(&HashSet::new()),
            vec![var("?x"), var("?y")]
        );

        let partial = or(
            4,
            &["?x", "?y"],
            vec![
                vec![relation(5, &["?x"]), function(6, "?x", "?y")],
                vec![relation(7, &["?x"])],
            ],
        );
        assert!(partial.groundable(&HashSet::new()).is_empty());
    }

    #[test]
    fn plans_multiple_proposers_and_fully_bound_validators_together() {
        let descriptors = vec![
            relation(0, &["?x"]),
            relation(1, &["?x"]),
            predicate(2, &["?x"]),
        ];

        let scope = plan_scope(descriptors, &[var("?x")], None).unwrap();

        assert_eq!(
            scope.stages,
            vec![LogicalStage {
                added: vec![var("?x")],
                proposers: refs(&[0, 1]),
                participants: refs(&[0, 1, 2]),
                target_variables: vec![var("?x")],
            }]
        );
    }

    #[test]
    fn grouped_or_is_the_sole_proposer_then_runnable_constraints_validate() {
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

        let scope = plan_scope(descriptors, &[var("?x"), var("?y")], None).unwrap();

        assert_eq!(scope.stages.len(), 2);
        assert_eq!(scope.stages[0].added, vec![var("?x"), var("?y")]);
        assert_eq!(scope.stages[0].proposers, refs(&[0]));
        assert_eq!(scope.stages[0].participants, refs(&[0]));
        assert!(scope.stages[1].added.is_empty());
        assert!(scope.stages[1].proposers.is_empty());
        assert_eq!(scope.stages[1].participants, refs(&[3]));
    }

    #[test]
    fn or_validates_only_after_all_of_its_variables_are_bound() {
        let descriptors = vec![
            relation(0, &["?x", "?y"]),
            or(
                1,
                &["?x", "?y"],
                vec![
                    vec![relation(2, &["?x", "?y"])],
                    vec![relation(3, &["?x", "?y"])],
                ],
            ),
        ];

        let scope = plan_scope(descriptors, &[var("?x"), var("?y")], None).unwrap();

        assert_eq!(scope.stages.len(), 2);
        assert_eq!(scope.stages[0].participants, refs(&[0]));
        assert_eq!(scope.stages[1].proposers, refs(&[0]));
        assert_eq!(scope.stages[1].participants, refs(&[0, 1]));
    }

    #[test]
    fn nested_logical_scopes_project_relevant_outer_variables_and_plan_incoming_as_relation() {
        let descriptors = vec![
            relation(0, &["?x"]),
            relation(1, &["?z"]),
            or(
                2,
                &["?x", "?y"],
                vec![
                    vec![relation(3, &["?x", "?y"])],
                    vec![relation(4, &["?x", "?y"])],
                ],
            ),
        ];
        let order = vec![var("?x"), var("?z"), var("?y")];
        let root = plan_scope(descriptors, &order, None).unwrap();

        let LogicalDescriptorKind::Or { branches } = &root.descriptors[2].kind else {
            panic!("expected OR scopes");
        };
        assert_eq!(branches[0].incoming_variables, Some(vec![var("?x")]));
        assert_eq!(
            branches[0].stages[0].proposers,
            vec![ParticipantRef::Incoming, ParticipantRef::Pattern(3)]
        );
        assert_eq!(
            branches[0].stages[0].participants,
            vec![ParticipantRef::Incoming, ParticipantRef::Pattern(3)]
        );
        assert_eq!(branches[0].stages[1].proposers, refs(&[3]));
    }

    #[test]
    fn not_receives_an_incoming_participant() {
        let descriptors = vec![
            relation(0, &["?x"]),
            not(1, &["?x"], vec![relation(2, &["?x"])]),
        ];
        let order = vec![var("?x")];
        let root = plan_scope(descriptors, &order, None).unwrap();
        assert_eq!(root.stages[0].participants, refs(&[0, 1]));

        let LogicalDescriptorKind::Not { body } = &root.descriptors[1].kind else {
            panic!("expected NOT scope");
        };
        assert_eq!(body.incoming_variables, Some(vec![var("?x")]));
        assert_eq!(
            body.stages[0].proposers,
            vec![ParticipantRef::Incoming, ParticipantRef::Pattern(2)]
        );
    }

    #[test]
    fn logical_descriptors_own_child_scopes_recursively() {
        let descriptors = vec![
            relation(0, &["?x"]),
            not(
                1,
                &["?x"],
                vec![or(
                    2,
                    &["?x"],
                    vec![vec![relation(3, &["?x"])], vec![relation(4, &["?x"])]],
                )],
            ),
        ];

        let root = plan_scope(descriptors, &[var("?x")], None).unwrap();
        let LogicalDescriptorKind::Not { body } = &root.descriptors[1].kind else {
            panic!("expected NOT descriptor");
        };
        let LogicalDescriptorKind::Or { branches } = &body.descriptors[0].kind else {
            panic!("expected nested OR descriptor");
        };

        assert_eq!(body.incoming_variables, Some(vec![var("?x")]));
        assert_eq!(branches.len(), 2);
        assert!(branches
            .iter()
            .all(|branch| branch.incoming_variables == Some(vec![var("?x")])));
        assert_eq!(branches[0].descriptors[0].id, 3);
        assert_eq!(branches[1].descriptors[0].id, 4);
    }

    #[test]
    fn uses_query_order_as_tie_breaker() {
        let descriptors = vec![relation(0, &["?y"]), relation(1, &["?x"])];
        let scope = plan_scope(descriptors, &[var("?x"), var("?y")], None).unwrap();
        assert_eq!(scope.stages[0].added, vec![var("?x")]);
        assert_eq!(scope.stages[1].added, vec![var("?y")]);
    }

    #[test]
    fn incoming_with_no_columns_remains_an_explicit_validation_participant() {
        let scope = plan_scope(vec![], &[], Some(vec![])).unwrap();
        assert_eq!(
            scope.stages,
            vec![LogicalStage {
                added: vec![],
                proposers: vec![],
                participants: vec![ParticipantRef::Incoming],
                target_variables: vec![],
            }]
        );
    }

    #[test]
    fn triple_descriptors_preserve_runtime_independent_pattern_data() {
        let pattern = Pattern::simple(
            PatternNonValuePlace::Variable(var("?e")),
            PatternNonValuePlace::Entid(42),
            PatternValuePlace::Variable(var("?v")),
        )
        .unwrap();
        let mut builder = DescriptorBuilder::new();

        let descriptor = builder.triple(&pattern);

        assert_eq!(descriptor.variables, vec![var("?e"), var("?v")]);
        assert_eq!(descriptor.kind, DescriptorKind::Triple(pattern));
    }

    #[test]
    fn planner_reorders_bound_layout_back_to_scope_order() {
        let descriptors = vec![function(0, "?x", "?y"), relation(1, &["?x"])];
        let scope = plan_scope(descriptors, &[var("?y"), var("?x")], None).unwrap();

        assert_eq!(scope.stages[0].added, vec![var("?x")]);
        assert_eq!(scope.stages[0].target_variables, vec![var("?x")]);
        assert_eq!(scope.stages[1].added, vec![var("?y")]);
        assert_eq!(scope.stages[1].target_variables, vec![var("?y"), var("?x")]);
    }

    #[test]
    fn planner_places_constant_only_descriptors_in_validation_stage() {
        let descriptors = vec![predicate(0, &[])];
        let scope = plan_scope(descriptors, &[], None).unwrap();

        assert_eq!(
            scope.stages,
            vec![LogicalStage {
                added: vec![],
                proposers: vec![],
                participants: refs(&[0]),
                target_variables: vec![],
            }]
        );
    }

    #[test]
    fn planner_handles_an_or_branch_with_different_internal_dependency_order() {
        let descriptor = or(
            0,
            &["?x", "?y"],
            vec![
                vec![relation(1, &["?x"]), function(2, "?x", "?y")],
                vec![relation(3, &["?y"]), function(4, "?y", "?x")],
            ],
        );
        let descriptors = vec![descriptor];
        let order = vec![var("?x"), var("?y")];
        let root = plan_scope(descriptors, &order, None).unwrap();

        let LogicalDescriptorKind::Or { branches } = &root.descriptors[0].kind else {
            panic!("expected OR scopes");
        };
        assert_eq!(branches[0].stages[1].added, vec![var("?x")]);
        assert_eq!(branches[1].stages[1].added, vec![var("?y")]);
        assert_eq!(
            branches[1].stages.last().unwrap().target_variables,
            vec![var("?x"), var("?y")]
        );
    }

    #[test]
    fn expression_descriptor_data_remains_owned_by_the_logical_scope() {
        let expression = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Variable(var("?x"))),
            op: BinaryOp::Lt,
            right: Box::new(Expr::Literal(DataType::Long(10))),
        });
        let descriptor = Descriptor {
            id: 1,
            variables: vec![var("?x")],
            kind: DescriptorKind::Predicate {
                expression: expression.clone(),
            },
        };
        let scope = plan_scope(vec![relation(0, &["?x"]), descriptor], &[var("?x")], None).unwrap();

        assert_eq!(
            scope.descriptors[1].kind,
            LogicalDescriptorKind::Predicate { expression }
        );
    }
}
