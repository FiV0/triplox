use std::collections::HashSet;

use anyhow::{bail, Result};
use edn::query::{Binding, Pattern, Variable, WhereClause};
use itertools::Itertools;

use crate::expr::{expr_variables, Expr};
use crate::ops::{DataType, QueryArg};
use crate::query::{convert_predicate, convert_where_fn, pattern_variables, query_variable_order};

use super::exec_pattern::PatternId;

//////////////////////////////////
// Descriptors
//////////////////////////////////

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Descriptor {
    id: PatternId,
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
    pub(crate) fn id(&self) -> PatternId {
        self.id
    }

    pub(crate) fn variables(&self) -> &[Variable] {
        &self.variables
    }

    pub(crate) fn kind(&self) -> &DescriptorKind {
        &self.kind
    }

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
pub(crate) enum DescriptorKind {
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
pub(crate) struct LogicalPlan {
    variable_order: Vec<Variable>,
    descriptors: Vec<Descriptor>,
}

impl LogicalPlan {
    pub(crate) fn variable_order(&self) -> &[Variable] {
        &self.variable_order
    }

    pub(crate) fn descriptors(&self) -> &[Descriptor] {
        &self.descriptors
    }
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

fn target_layout(order: &[Variable], bound: &[Variable], added: &[Variable]) -> Vec<Variable> {
    let available: HashSet<&Variable> = bound.iter().chain(added).collect();
    order
        .iter()
        .filter(|variable| available.contains(*variable))
        .cloned()
        .collect()
}

fn fully_bound(variables: &[Variable], bound: &HashSet<Variable>) -> bool {
    variables.iter().all(|variable| bound.contains(variable))
}

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

fn can_validate_grouped_or(groundable: &[Variable], added: &[Variable]) -> bool {
    !groundable.is_empty() && groundable.iter().any(|variable| added.contains(variable))
}

fn proposing_stage_for(
    variable: &Variable,
    descriptors: &[Descriptor],
    incoming_variables: Option<&[Variable]>,
    order: &[Variable],
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
        let target_variables = target_layout(order, bound, &added);
        let target_set: HashSet<Variable> = target_variables.iter().cloned().collect();
        let mut participants = Vec::new();

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
        let completed = participants
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
        return Some((
            LogicalStage {
                added,
                proposers,
                participants,
                target_variables,
            },
            completed,
        ));
    }

    descriptors.iter().find_map(|descriptor| {
        let participant = ParticipantRef::Pattern(descriptor.id);
        if completed.contains(&participant) || !descriptor.is_or() {
            return None;
        }
        let added = descriptor.groundable(&bound_set);
        if !added.contains(variable) {
            return None;
        }
        let target_variables = target_layout(order, bound, &added);
        let target_set: HashSet<Variable> = target_variables.iter().cloned().collect();
        let mut participants = vec![participant];
        let mut newly_completed = vec![participant];

        if let Some(incoming_variables) = incoming_variables {
            let incoming = ParticipantRef::Incoming;
            let groundable = relation_groundable(incoming_variables, &bound_set);
            if !completed.contains(&incoming) && can_validate_grouped_or(&groundable, &added) {
                participants.push(incoming);
                if fully_bound(incoming_variables, &target_set) {
                    newly_completed.push(incoming);
                }
            }
        }
        for validator in descriptors {
            let validator_participant = ParticipantRef::Pattern(validator.id);
            if completed.contains(&validator_participant) || validator.is_or() {
                continue;
            }
            let groundable = validator.groundable(&bound_set);
            if can_validate_grouped_or(&groundable, &added) {
                participants.push(validator_participant);
                if fully_bound(&validator.variables, &target_set) {
                    newly_completed.push(validator_participant);
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

pub(crate) fn plan_scope(
    descriptors: &[Descriptor],
    variable_order: &[Variable],
    incoming_variables: Option<&[Variable]>,
) -> Result<Vec<LogicalStage>> {
    let order = scope_variable_order(descriptors, variable_order, incoming_variables);
    let relevant: HashSet<&Variable> = order.iter().collect();
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
                &order,
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

    Ok(LogicalPlan {
        variable_order,
        descriptors,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use edn::query::{PatternNonValuePlace, PatternValuePlace, ToVariable};

    use super::*;
    use crate::query_validation::validate_query;

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
            plan.variable_order(),
            &[var("?name"), var("?age"), var("?e"), var("?next")]
        );
        assert_eq!(plan.descriptors.len(), 6);
        assert!(matches!(
            plan.descriptors[0].kind,
            DescriptorKind::Relation { .. }
        ));
        assert!(matches!(
            plan.descriptors[2].kind,
            DescriptorKind::Triple(_)
        ));
        assert!(matches!(
            plan.descriptors[3].kind,
            DescriptorKind::Predicate { .. }
        ));
        assert!(matches!(
            plan.descriptors[4].kind,
            DescriptorKind::Function { .. }
        ));
        let DescriptorKind::Not { children } = &plan.descriptors[5].kind else {
            panic!("expected NOT descriptor");
        };
        assert_eq!(children[0].id, 6);
        assert!(matches!(children[0].kind, DescriptorKind::Triple(_)));
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

    #[test]
    fn plans_multiple_proposers_and_ready_validators_together() {
        let descriptors = vec![
            relation(0, &["?x"]),
            relation(1, &["?x"]),
            predicate(2, &["?x"]),
        ];

        let stages = plan_scope(&descriptors, &[var("?x")], None).unwrap();

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

        let stages = plan_scope(&descriptors, &[var("?x"), var("?y"), var("?z")], None).unwrap();

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

        let stages = plan_scope(&descriptors, &[var("?x"), var("?y")], None).unwrap();

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
            &[var("?x"), var("?y")],
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

        let stages = plan_scope(&descriptors, &[var("?x"), var("?y")], None).unwrap();

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

        let stages = plan_scope(&descriptors, &[var("?x"), var("?y")], None).unwrap();

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
    fn incoming_is_projected_but_empty_incoming_remains_explicit() {
        let descriptors = vec![relation(0, &["?x"])];
        let incoming = vec![var("?outer"), var("?x")];
        let stages = plan_scope(&descriptors, &[var("?x")], Some(&incoming)).unwrap();
        assert_eq!(
            stages[0].proposers,
            vec![ParticipantRef::Incoming, ParticipantRef::Pattern(0)]
        );

        let stages = plan_scope(&[], &[], Some(&[])).unwrap();
        assert_eq!(
            stages,
            vec![LogicalStage {
                added: Vec::new(),
                proposers: Vec::new(),
                participants: vec![ParticipantRef::Incoming],
                target_variables: Vec::new(),
            }]
        );
    }

    #[test]
    fn target_layout_follows_scope_order() {
        let descriptors = vec![function(0, "?x", "?y"), relation(1, &["?x"])];

        let stages = plan_scope(&descriptors, &[var("?y"), var("?x")], None).unwrap();

        assert_eq!(stages[0].target_variables, vec![var("?x")]);
        assert_eq!(stages[1].target_variables, vec![var("?y"), var("?x")]);
    }

    #[test]
    fn reports_insufficient_binding() {
        let descriptors = vec![predicate(0, &["?x"])];

        let error = plan_scope(&descriptors, &[var("?x")], None).unwrap_err();

        assert!(error.to_string().contains("Insufficient binding"));
    }

    #[test]
    fn triple_descriptors_keep_runtime_independent_pattern_data() {
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
}
