use std::collections::HashSet;

use anyhow::{bail, Result};
use edn::query::Variable;

use super::descriptor::{Descriptor, DescriptorKind, PatternDescriptor, ScopeDescriptor};
use super::PatternSlot;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RelPlan {
    pub output_vars: Vec<Variable>,
    pub kind: RelPlanKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RelPlanKind {
    Pattern(PatternPlan),
    Join(JoinPlan),
    Union(UnionPlan),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PatternPlan {
    pub attribute: i64,
    pub entity: PatternSlot,
    pub value: PatternSlot,
    pub output_vars: Vec<Variable>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UnionPlan {
    pub branches: Vec<RelPlan>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JoinPlan {
    pub inputs: Vec<RelPlan>,
    pub steps: Vec<JoinStep>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JoinStep {
    pub right_input_index: usize,
    pub left_vars: Vec<Variable>,
    pub right_vars: Vec<Variable>,
    pub key_vars: Vec<Variable>,
    pub output_vars: Vec<Variable>,
}

fn plan_pattern(descriptor: &Descriptor, pattern: &PatternDescriptor) -> PatternPlan {
    PatternPlan {
        attribute: pattern.attribute,
        entity: pattern.entity.clone(),
        value: pattern.value.clone(),
        output_vars: descriptor.variables.clone(),
    }
}

fn plan_descriptor(descriptor: &Descriptor) -> Result<RelPlan> {
    match &descriptor.kind {
        DescriptorKind::Pattern(pattern) => {
            let pattern = plan_pattern(descriptor, pattern);
            Ok(RelPlan {
                output_vars: pattern.output_vars.clone(),
                kind: RelPlanKind::Pattern(pattern),
            })
        }
        DescriptorKind::Or(or) => {
            let branches = or
                .branches
                .iter()
                .map(plan_scope)
                .collect::<Result<Vec<_>>>()?;
            Ok(RelPlan {
                output_vars: descriptor.variables.clone(),
                kind: RelPlanKind::Union(UnionPlan { branches }),
            })
        }
    }
}

pub(super) fn plan_scope(scope: &ScopeDescriptor) -> Result<RelPlan> {
    let inputs = scope
        .descriptors
        .iter()
        .map(plan_descriptor)
        .collect::<Result<Vec<_>>>()?;
    plan_inputs(inputs)
}

fn plan_inputs(mut inputs: Vec<RelPlan>) -> Result<RelPlan> {
    if inputs.is_empty() {
        bail!("Incremental queries require at least one triple pattern");
    }
    if inputs.len() == 1 {
        return Ok(inputs.remove(0));
    }

    let steps = plan_join_steps(&inputs);
    let output_vars = steps
        .last()
        .map(|step| step.output_vars.clone())
        .expect("multi-input join plan must have at least one step");

    Ok(RelPlan {
        output_vars,
        kind: RelPlanKind::Join(JoinPlan { inputs, steps }),
    })
}

pub(super) fn collect_leaf_patterns<'a>(plan: &'a RelPlan, patterns: &mut Vec<&'a PatternPlan>) {
    match &plan.kind {
        RelPlanKind::Pattern(pattern) => patterns.push(pattern),
        RelPlanKind::Join(join) => {
            for input in &join.inputs {
                collect_leaf_patterns(input, patterns);
            }
        }
        RelPlanKind::Union(union) => {
            for branch in &union.branches {
                collect_leaf_patterns(branch, patterns);
            }
        }
    }
}

// This plans a very naive left-deep join plan, always joining on the intersection
// of left (the accumulate so far) and right.
fn plan_join_steps(inputs: &[RelPlan]) -> Vec<JoinStep> {
    let mut steps = Vec::new();
    let mut left_vars = inputs[0].output_vars.clone();
    let mut bound: HashSet<Variable> = left_vars.iter().cloned().collect();

    for (right_input_index, input) in inputs.iter().enumerate().skip(1) {
        let right_vars = input.output_vars.clone();
        let right_set: HashSet<Variable> = right_vars.iter().cloned().collect();
        let key_vars = left_vars
            .iter()
            .filter(|var| right_set.contains(*var))
            .cloned()
            .collect::<Vec<_>>();

        let mut output_vars = left_vars.clone();
        for var in &right_vars {
            if bound.insert(var.clone()) {
                output_vars.push(var.clone());
            }
        }

        steps.push(JoinStep {
            right_input_index,
            left_vars,
            right_vars,
            key_vars,
            output_vars: output_vars.clone(),
        });
        left_vars = output_vars;
    }

    steps
}
