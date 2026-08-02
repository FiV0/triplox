use std::cmp::Reverse;
use std::collections::HashSet;

use anyhow::{anyhow, bail, Result};
use edn::query::Variable;

use super::descriptor::{Descriptor, DescriptorKind, PatternDescriptor, ScopeDescriptor};
use super::PatternSlot;
use crate::expr::Expr;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RelPlan {
    pub incoming_vars: Option<Vec<Variable>>,
    pub output_vars: Vec<Variable>,
    pub kind: RelPlanKind,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RelPlanKind {
    Pattern(PatternPlan),
    Filter {
        expr: Expr,
    },
    Function {
        expr: Expr,
        output_var: Variable,
    },
    Chain {
        children: Vec<RelPlan>,
    },
    Difference {
        key_vars: Vec<Variable>,
        negative: Box<RelPlan>,
    },
    Union {
        branches: Vec<RelPlan>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PatternPlan {
    pub attribute: i64,
    pub entity: PatternSlot,
    pub value: PatternSlot,
    pub pattern_vars: Vec<Variable>,
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
            .max_by_key(|(index, descriptor)| {
                let shared = descriptor
                    .descriptor
                    .variables
                    .iter()
                    .filter(|variable| grounded.contains(variable))
                    .count();
                (shared, Reverse(*index))
            })
            .map(|(index, _)| index);

        let Some(candidate) = candidate else {
            let descriptor = remaining
                .first()
                .expect("non-empty remaining descriptors must have a first descriptor");
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
    let output_vars = incoming_vars
        .as_ref()
        .map(|incoming| append_new_variables(incoming, &pattern_vars))
        .unwrap_or_else(|| pattern_vars.clone());

    RelPlan {
        incoming_vars,
        output_vars,
        kind: RelPlanKind::Pattern(PatternPlan {
            attribute: pattern.attribute,
            entity: pattern.entity.clone(),
            value: pattern.value.clone(),
            pattern_vars,
        }),
    }
}

fn plan_filter(expr: &Expr, incoming_vars: Option<Vec<Variable>>) -> Result<RelPlan> {
    let incoming_vars = incoming_vars
        .ok_or_else(|| anyhow!("Cannot plan predicate without a positive relation"))?;
    Ok(RelPlan {
        incoming_vars: Some(incoming_vars.clone()),
        output_vars: incoming_vars,
        kind: RelPlanKind::Filter { expr: expr.clone() },
    })
}

fn plan_function(
    expr: &Expr,
    output_var: &Variable,
    incoming_vars: Option<Vec<Variable>>,
) -> Result<RelPlan> {
    let incoming_vars =
        incoming_vars.ok_or_else(|| anyhow!("Cannot plan function without a positive relation"))?;
    let output_vars = append_new_variables(&incoming_vars, std::slice::from_ref(output_var));
    Ok(RelPlan {
        incoming_vars: Some(incoming_vars),
        output_vars,
        kind: RelPlanKind::Function {
            expr: expr.clone(),
            output_var: output_var.clone(),
        },
    })
}

fn plan_union(
    descriptor: &Descriptor,
    branches: &[ScopeDescriptor],
    incoming_vars: Option<Vec<Variable>>,
) -> Result<RelPlan> {
    // Ordering guarantees variables the union cannot ground are already incoming.
    let output_vars = incoming_vars
        .as_ref()
        .map(|incoming| append_new_variables(incoming, &descriptor.variables))
        .unwrap_or_else(|| descriptor.variables.clone());
    let branches = branches
        .iter()
        .map(|branch| plan_scope(branch, incoming_vars.clone()))
        .collect::<Result<Vec<_>>>()?;

    Ok(RelPlan {
        incoming_vars,
        output_vars,
        kind: RelPlanKind::Union { branches },
    })
}

fn plan_difference(
    descriptor: &Descriptor,
    scope: &ScopeDescriptor,
    incoming_vars: Option<Vec<Variable>>,
) -> Result<RelPlan> {
    let incoming_vars =
        incoming_vars.ok_or_else(|| anyhow!("Cannot plan `not` without a positive relation"))?;
    let key_vars = incoming_vars
        .iter()
        .filter(|variable| descriptor.variables.contains(variable))
        .cloned()
        .collect::<Vec<_>>();
    let negative = plan_scope(scope, Some(key_vars.clone()))?;
    debug_assert_eq!(negative.output_vars, key_vars);

    Ok(RelPlan {
        incoming_vars: Some(incoming_vars.clone()),
        output_vars: incoming_vars,
        kind: RelPlanKind::Difference {
            key_vars,
            negative: Box::new(negative),
        },
    })
}

fn plan_descriptor(
    descriptor: &Descriptor,
    incoming_vars: Option<Vec<Variable>>,
) -> Result<RelPlan> {
    match &descriptor.kind {
        DescriptorKind::Pattern(pattern) => Ok(plan_pattern(descriptor, pattern, incoming_vars)),
        DescriptorKind::Predicate { expr } => plan_filter(expr, incoming_vars),
        DescriptorKind::Function { expr, output_var } => {
            plan_function(expr, output_var, incoming_vars)
        }
        DescriptorKind::Not { scope } => plan_difference(descriptor, scope, incoming_vars),
        DescriptorKind::Or { branches } => plan_union(descriptor, branches, incoming_vars),
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
        kind: RelPlanKind::Chain { children },
    })
}

pub(super) fn collect_leaf_patterns<'a>(plan: &'a RelPlan, patterns: &mut Vec<&'a PatternPlan>) {
    match &plan.kind {
        RelPlanKind::Pattern(pattern) => patterns.push(pattern),
        RelPlanKind::Filter { .. } => {}
        RelPlanKind::Function { .. } => {}
        RelPlanKind::Chain { children } => {
            for child in children {
                collect_leaf_patterns(child, patterns);
            }
        }
        RelPlanKind::Difference { negative, .. } => {
            collect_leaf_patterns(negative, patterns);
        }
        RelPlanKind::Union { branches } => {
            for branch in branches {
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
        assert_eq!(plan.output_vars, vec!["?e".to_var()]);
        assert!(matches!(plan.kind, RelPlanKind::Pattern(_)));
    }
}
