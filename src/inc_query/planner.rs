use std::cmp::Reverse;
use std::collections::HashSet;

use anyhow::{anyhow, bail, Result};
use edn::query::Variable;

use super::descriptor::{Descriptor, DescriptorKind, PatternDescriptor, ScopeDescriptor};
use super::PatternSlot;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RelPlan {
    pub incoming_vars: Option<Vec<Variable>>,
    pub output_vars: Vec<Variable>,
    pub kind: RelPlanKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RelPlanKind {
    Pattern(PatternPlan),
    Chain(ChainPlan),
    Union(UnionPlan),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PatternPlan {
    pub attribute: i64,
    pub entity: PatternSlot,
    pub value: PatternSlot,
    pub pattern_vars: Vec<Variable>,
    pub join: Option<JoinStep>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ChainPlan {
    pub children: Vec<RelPlan>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UnionPlan {
    pub branches: Vec<RelPlan>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JoinStep {
    pub left_vars: Vec<Variable>,
    pub right_vars: Vec<Variable>,
    pub key_vars: Vec<Variable>,
    pub output_vars: Vec<Variable>,
}

fn append_new_variables(base: &[Variable], additional: &[Variable]) -> Vec<Variable> {
    let mut output = base.to_vec();
    let mut seen = base.iter().cloned().collect::<HashSet<_>>();
    for variable in additional {
        if seen.insert(variable.clone()) {
            output.push(variable.clone());
        }
    }
    output
}

fn plan_join(left_vars: &[Variable], right_vars: &[Variable]) -> JoinStep {
    let right_set = right_vars.iter().collect::<HashSet<_>>();
    let key_vars = left_vars
        .iter()
        .filter(|variable| right_set.contains(variable))
        .cloned()
        .collect();

    JoinStep {
        left_vars: left_vars.to_vec(),
        right_vars: right_vars.to_vec(),
        key_vars,
        output_vars: append_new_variables(left_vars, right_vars),
    }
}

fn required_variables(descriptor: &Descriptor) -> Vec<Variable> {
    let groundable = descriptor.groundable.iter().collect::<HashSet<_>>();
    descriptor
        .variables
        .iter()
        .filter(|variable| !groundable.contains(variable))
        .cloned()
        .collect()
}

struct PendingDescriptor<'a> {
    descriptor: &'a Descriptor,
    required_variables: Vec<Variable>,
}

fn order_descriptors<'a>(
    descriptors: &'a [Descriptor],
    initial_grounded: &[Variable],
) -> Result<Vec<&'a Descriptor>> {
    let mut ordered = Vec::with_capacity(descriptors.len());
    let mut grounded = initial_grounded.iter().cloned().collect::<HashSet<_>>();
    let mut remaining = descriptors
        .iter()
        .map(|descriptor| PendingDescriptor {
            descriptor,
            required_variables: required_variables(descriptor),
        })
        .collect::<Vec<_>>();

    while !remaining.is_empty() {
        let candidate = remaining
            .iter()
            .enumerate()
            .filter(|(_, descriptor)| {
                descriptor
                    .required_variables
                    .iter()
                    .all(|variable| grounded.contains(variable))
            })
            .max_by_key(|(_, descriptor)| {
                let shared = descriptor
                    .descriptor
                    .variables
                    .iter()
                    .filter(|variable| grounded.contains(variable))
                    .count();
                (shared, Reverse(descriptor.descriptor.position))
            })
            .map(|(index, _)| index);

        let Some(candidate) = candidate else {
            let descriptor = remaining
                .iter()
                .min_by_key(|descriptor| descriptor.descriptor.position)
                .expect("non-empty remaining descriptors must have a first position");
            let missing = descriptor
                .required_variables
                .iter()
                .find(|variable| !grounded.contains(variable))
                .expect("a non-introducible descriptor must have a missing variable");
            return Err(anyhow!(
                "Insufficient bindings for incremental query variable {}",
                missing
            ));
        };

        let descriptor = remaining.remove(candidate);
        grounded.extend(descriptor.descriptor.variables.iter().cloned());
        ordered.push(descriptor.descriptor);
    }

    Ok(ordered)
}

fn plan_pattern(
    descriptor: &Descriptor,
    pattern: &PatternDescriptor,
    incoming_vars: Option<Vec<Variable>>,
) -> RelPlan {
    let pattern_vars = descriptor.variables.clone();
    let join = incoming_vars
        .as_ref()
        .map(|incoming| plan_join(incoming, &pattern_vars));
    let output_vars = join
        .as_ref()
        .map(|join| join.output_vars.clone())
        .unwrap_or_else(|| pattern_vars.clone());

    RelPlan {
        incoming_vars,
        output_vars,
        kind: RelPlanKind::Pattern(PatternPlan {
            attribute: pattern.attribute,
            entity: pattern.entity.clone(),
            value: pattern.value.clone(),
            pattern_vars,
            join,
        }),
    }
}

fn plan_union(
    descriptor: &Descriptor,
    branches: &[ScopeDescriptor],
    incoming_vars: Option<Vec<Variable>>,
) -> Result<RelPlan> {
    let output_vars = incoming_vars
        .as_ref()
        .map(|incoming| append_new_variables(incoming, &descriptor.groundable))
        .unwrap_or_else(|| descriptor.variables.clone());
    let branches = branches
        .iter()
        .map(|branch| plan_scope(branch, incoming_vars.clone()))
        .collect::<Result<Vec<_>>>()?;

    Ok(RelPlan {
        incoming_vars,
        output_vars,
        kind: RelPlanKind::Union(UnionPlan { branches }),
    })
}

fn plan_descriptor(
    descriptor: &Descriptor,
    incoming_vars: Option<Vec<Variable>>,
) -> Result<RelPlan> {
    match &descriptor.kind {
        DescriptorKind::Pattern(pattern) => Ok(plan_pattern(descriptor, pattern, incoming_vars)),
        DescriptorKind::Or(or) => plan_union(descriptor, &or.branches, incoming_vars),
    }
}

pub(super) fn plan_scope(
    scope: &ScopeDescriptor,
    incoming_vars: Option<Vec<Variable>>,
) -> Result<RelPlan> {
    let initial_grounded = incoming_vars.as_deref().unwrap_or_default();
    let ordered = order_descriptors(&scope.descriptors, initial_grounded)?;
    if ordered.is_empty() {
        bail!("Incremental queries require at least one triple pattern");
    }

    let mut children = Vec::with_capacity(ordered.len());
    let mut child_incoming = incoming_vars.clone();
    for descriptor in ordered {
        let child = plan_descriptor(descriptor, child_incoming)?;
        child_incoming = Some(child.output_vars.clone());
        children.push(child);
    }

    if children.len() == 1 {
        return Ok(children.remove(0));
    }

    let output_vars = children
        .last()
        .expect("multi-node chain must have a final child")
        .output_vars
        .clone();
    Ok(RelPlan {
        incoming_vars,
        output_vars,
        kind: RelPlanKind::Chain(ChainPlan { children }),
    })
}

pub(super) fn collect_leaf_patterns<'a>(plan: &'a RelPlan, patterns: &mut Vec<&'a PatternPlan>) {
    match &plan.kind {
        RelPlanKind::Pattern(pattern) => patterns.push(pattern),
        RelPlanKind::Chain(chain) => {
            for child in &chain.children {
                collect_leaf_patterns(child, patterns);
            }
        }
        RelPlanKind::Union(union) => {
            for branch in &union.branches {
                collect_leaf_patterns(branch, patterns);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use edn::query::ToVariable;

    use super::*;

    #[test]
    fn ordering_reports_a_missing_required_variable() {
        let missing = "?missing".to_var();
        let descriptor = Descriptor {
            position: 0,
            variables: vec![missing.clone()],
            groundable: vec![],
            kind: DescriptorKind::Pattern(PatternDescriptor {
                attribute: 10,
                entity: PatternSlot::Variable(missing),
                value: PatternSlot::Constant(vec![]),
            }),
        };

        let error = order_descriptors(&[descriptor], &[]).unwrap_err();

        assert!(error.to_string().contains("?missing"));
        assert!(error.to_string().contains("Insufficient bindings"));
    }

    #[test]
    fn preserves_a_zero_column_incoming_relation() {
        let entity = "?e".to_var();
        let descriptor = Descriptor {
            position: 0,
            variables: vec![entity.clone()],
            groundable: vec![entity.clone()],
            kind: DescriptorKind::Pattern(PatternDescriptor {
                attribute: 10,
                entity: PatternSlot::Variable(entity.clone()),
                value: PatternSlot::Constant(vec![]),
            }),
        };
        let scope = ScopeDescriptor {
            descriptors: vec![descriptor],
            variables: vec![entity.clone()],
            groundable: vec![entity],
        };

        let plan = plan_scope(&scope, Some(vec![])).unwrap();

        assert_eq!(plan.incoming_vars, Some(vec![]));
        let RelPlanKind::Pattern(pattern) = plan.kind else {
            panic!("expected pattern plan");
        };
        let join = pattern
            .join
            .expect("zero-column relation is still an incoming relation");
        assert!(join.left_vars.is_empty());
        assert!(join.key_vars.is_empty());
    }
}
