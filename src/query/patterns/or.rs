use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use edn::query::Variable;

use super::relation::RelationPattern;
use crate::query::binding_bag::BindingBag;
use crate::query::engine::GenericJoinEngine;
use crate::query::exec_pattern::{ExecPattern, PatternIndex, Proposal};
use crate::query::stage::StageTemplate;

pub(crate) struct OrPattern {
    index: PatternIndex,
    variables: Vec<Variable>,
    incoming_variables: Vec<Variable>,
    branches: Vec<Vec<StageTemplate>>,
}

impl OrPattern {
    pub(crate) fn new(
        index: PatternIndex,
        variables: Vec<Variable>,
        incoming_variables: Vec<Variable>,
        branches: Vec<Vec<StageTemplate>>,
    ) -> Result<Self> {
        ensure_unique("OR", &variables)?;
        ensure_unique("OR incoming", &incoming_variables)?;
        ensure!(
            incoming_variables
                .iter()
                .all(|variable| variables.contains(variable)),
            "OR incoming variables must be OR variables"
        );
        ensure!(!branches.is_empty(), "OR must contain at least one branch");
        let expected: HashSet<&Variable> = variables.iter().collect();
        for (branch_index, branch) in branches.iter().enumerate() {
            let final_stage = branch
                .last()
                .ok_or_else(|| anyhow::anyhow!("OR branch {branch_index} has no stages"))?;
            ensure!(
                final_stage
                    .target_variables()
                    .iter()
                    .collect::<HashSet<_>>()
                    == expected,
                "OR branch {branch_index} does not produce the OR variable set"
            );
            ensure!(
                branch.iter().any(StageTemplate::contains_incoming),
                "OR branch {branch_index} has no Incoming participant"
            );
        }
        Ok(Self {
            index,
            variables,
            incoming_variables,
            branches,
        })
    }

    fn validate_invocation(&self, input: &BindingBag) -> Result<()> {
        for variable in &self.incoming_variables {
            ensure!(
                input.variables.contains(variable),
                "OR pattern {} requires incoming variable {variable}",
                self.index
            );
        }
        for variable in &self.variables {
            ensure!(
                !input.variables.contains(variable) || self.incoming_variables.contains(variable),
                "OR pattern {} received unplanned bound variable {variable}",
                self.index
            );
        }
        Ok(())
    }

    fn execute_branches(&self, input: &BindingBag) -> Result<BindingBag> {
        self.validate_invocation(input)?;
        let projected = input.project(&self.incoming_variables)?;
        let incoming: Arc<dyn ExecPattern> = Arc::new(RelationPattern::new(self.index, projected));
        let mut result = BindingBag::empty(self.variables.clone())?;

        for (branch_index, templates) in self.branches.iter().enumerate() {
            let stages = templates
                .iter()
                .map(|template| template.materialize(&incoming))
                .collect::<Result<Vec<_>>>()
                .with_context(|| {
                    format!(
                        "OR pattern {} failed to materialize branch {branch_index}",
                        self.index
                    )
                })?;
            let branch = GenericJoinEngine::execute(&stages, BindingBag::unit())
                .with_context(|| {
                    format!("OR pattern {} failed in branch {branch_index}", self.index)
                })?
                .reorder(&self.variables)?;
            result = result.distinct_union(&branch)?;
        }
        Ok(result)
    }
}

fn ensure_unique(label: &str, variables: &[Variable]) -> Result<()> {
    let mut seen = HashSet::new();
    for variable in variables {
        ensure!(seen.insert(variable), "{label} variables repeat {variable}");
    }
    Ok(())
}

impl ExecPattern for OrPattern {
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
            "OR pattern {} received {} proposals for {} input rows",
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
        if added.is_empty() {
            ensure!(
                target_variables == input.variables,
                "OR pattern {} validation must preserve the input layout",
                self.index
            );
            ensure!(
                self.variables
                    .iter()
                    .all(|variable| input.variables.contains(variable)),
                "OR pattern {} validation requires every OR variable to be bound",
                self.index
            );
            return input.semijoin(&self.execute_branches(input)?);
        }

        let expected_added: HashSet<&Variable> = self
            .variables
            .iter()
            .filter(|variable| !input.variables.contains(*variable))
            .collect();
        ensure!(
            added.iter().collect::<HashSet<_>>() == expected_added,
            "OR pattern {} must propose every missing OR variable",
            self.index
        );
        input
            .natural_join(&self.execute_branches(input)?)?
            .reorder(target_variables)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use edn::query::ToVariable;

    use super::OrPattern;
    use crate::codec::Encode;
    use crate::expr::{BinaryExpr, BinaryOp, Expr};
    use crate::ops::DataType;
    use crate::query::binding_bag::BindingBag;
    use crate::query::exec_pattern::ExecPattern;
    use crate::query::patterns::predicate::PredicatePattern;
    use crate::query::patterns::relation::RelationPattern;
    use crate::query::stage::{ParticipantTemplate, StageTemplate};

    fn bytes(value: &str) -> Bytes {
        Bytes::copy_from_slice(value.as_bytes())
    }

    fn encoded(value: DataType) -> Bytes {
        Bytes::from(value.encode())
    }

    fn binding_bag(variables: &[&str], rows: Vec<Vec<Bytes>>) -> BindingBag {
        BindingBag::new(
            variables.iter().map(|variable| variable.to_var()).collect(),
            rows,
        )
        .unwrap()
    }

    fn assert_bag_eq_unordered(actual: BindingBag, expected: BindingBag) {
        assert_eq!(actual.variables, expected.variables);
        let mut actual_rows = actual.rows;
        let mut expected_rows = expected.rows;
        actual_rows.sort();
        expected_rows.sort();
        assert_eq!(actual_rows, expected_rows);
    }

    fn relation(index: usize, variables: &[&str], rows: Vec<Vec<Bytes>>) -> Arc<dyn ExecPattern> {
        Arc::new(RelationPattern::new(index, binding_bag(variables, rows)))
    }

    fn incoming_stage() -> StageTemplate {
        StageTemplate::new(vec![], vec![ParticipantTemplate::Incoming], vec![]).unwrap()
    }

    #[test]
    fn branches_align_different_internal_orders_and_distinct_union_results() {
        let xy = relation(1, &["?x", "?y"], vec![vec![bytes("a"), bytes("1")]]);
        let yx = relation(
            2,
            &["?y", "?x"],
            vec![vec![bytes("1"), bytes("a")], vec![bytes("2"), bytes("b")]],
        );
        let branches = vec![
            vec![
                incoming_stage(),
                StageTemplate::new(
                    vec!["?x".to_var()],
                    vec![ParticipantTemplate::Pattern(Arc::clone(&xy))],
                    vec!["?x".to_var()],
                )
                .unwrap(),
                StageTemplate::new(
                    vec!["?y".to_var()],
                    vec![ParticipantTemplate::Pattern(xy)],
                    vec!["?x".to_var(), "?y".to_var()],
                )
                .unwrap(),
            ],
            vec![
                incoming_stage(),
                StageTemplate::new(
                    vec!["?y".to_var()],
                    vec![ParticipantTemplate::Pattern(Arc::clone(&yx))],
                    vec!["?y".to_var()],
                )
                .unwrap(),
                StageTemplate::new(
                    vec!["?x".to_var()],
                    vec![ParticipantTemplate::Pattern(yx)],
                    vec!["?y".to_var(), "?x".to_var()],
                )
                .unwrap(),
            ],
        ];
        let pattern =
            OrPattern::new(100, vec!["?x".to_var(), "?y".to_var()], vec![], branches).unwrap();

        let result = pattern
            .join(
                &BindingBag::unit(),
                &["?x".to_var(), "?y".to_var()],
                &["?x".to_var(), "?y".to_var()],
            )
            .unwrap();

        assert_eq!(
            result,
            binding_bag(
                &["?x", "?y"],
                vec![vec![bytes("a"), bytes("1")], vec![bytes("b"), bytes("2")],],
            )
        );
    }

    #[test]
    fn proposal_and_validation_preserve_outer_bags_and_unrelated_columns() {
        let values = relation(1, &["?x"], vec![vec![bytes("a")], vec![bytes("b")]]);
        let proposing = OrPattern::new(
            100,
            vec!["?x".to_var()],
            vec![],
            vec![vec![
                incoming_stage(),
                StageTemplate::new(
                    vec!["?x".to_var()],
                    vec![ParticipantTemplate::Pattern(values)],
                    vec!["?x".to_var()],
                )
                .unwrap(),
            ]],
        )
        .unwrap();
        let outer = binding_bag(&["?outer"], vec![vec![bytes("u")], vec![bytes("u")]]);

        assert_bag_eq_unordered(
            proposing
                .join(
                    &outer,
                    &["?x".to_var()],
                    &["?x".to_var(), "?outer".to_var()],
                )
                .unwrap(),
            binding_bag(
                &["?x", "?outer"],
                vec![
                    vec![bytes("a"), bytes("u")],
                    vec![bytes("b"), bytes("u")],
                    vec![bytes("a"), bytes("u")],
                    vec![bytes("b"), bytes("u")],
                ],
            ),
        );

        let allowed = relation(2, &["?x"], vec![vec![bytes("a")]]);
        let validating = OrPattern::new(
            101,
            vec!["?x".to_var()],
            vec!["?x".to_var()],
            vec![vec![StageTemplate::new(
                vec!["?x".to_var()],
                vec![
                    ParticipantTemplate::Incoming,
                    ParticipantTemplate::Pattern(allowed),
                ],
                vec!["?x".to_var()],
            )
            .unwrap()]],
        )
        .unwrap();
        let bound = binding_bag(
            &["?outer", "?x"],
            vec![
                vec![bytes("u"), bytes("a")],
                vec![bytes("u"), bytes("a")],
                vec![bytes("v"), bytes("b")],
            ],
        );
        assert_eq!(
            validating.join(&bound, &[], &bound.variables).unwrap(),
            binding_bag(
                &["?outer", "?x"],
                vec![vec![bytes("u"), bytes("a")], vec![bytes("u"), bytes("a")],],
            )
        );
    }

    #[test]
    fn branch_predicates_cannot_filter_rows_from_other_branches() {
        let values_a = relation(
            1,
            &["?x"],
            vec![
                vec![encoded(DataType::Long(1))],
                vec![encoded(DataType::Long(2))],
            ],
        );
        let values_b = relation(
            2,
            &["?x"],
            vec![
                vec![encoded(DataType::Long(2))],
                vec![encoded(DataType::Long(3))],
            ],
        );
        let equals = |index, value| -> Arc<dyn ExecPattern> {
            Arc::new(PredicatePattern::new(
                index,
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(Expr::Variable("?x".to_var())),
                    op: BinaryOp::Eq,
                    right: Box::new(Expr::Literal(DataType::Long(value))),
                }),
            ))
        };
        let branch = |relation, predicate| {
            vec![
                incoming_stage(),
                StageTemplate::new(
                    vec!["?x".to_var()],
                    vec![
                        ParticipantTemplate::Pattern(relation),
                        ParticipantTemplate::Pattern(predicate),
                    ],
                    vec!["?x".to_var()],
                )
                .unwrap(),
            ]
        };
        let pattern = OrPattern::new(
            100,
            vec!["?x".to_var()],
            vec![],
            vec![
                branch(values_a, equals(3, 1)),
                branch(values_b, equals(4, 3)),
            ],
        )
        .unwrap();

        let result = pattern
            .join(&BindingBag::unit(), &["?x".to_var()], &["?x".to_var()])
            .unwrap();

        assert_eq!(
            result,
            binding_bag(
                &["?x"],
                vec![
                    vec![encoded(DataType::Long(1))],
                    vec![encoded(DataType::Long(3))],
                ],
            )
        );
    }
}
