use std::collections::HashMap;

use anyhow::{ensure, Result};
use edn::query::Variable;

use super::evaluation::{binding_positions, update_bindings};
use crate::expr::{evaluate_as_bool, expr_variables, EvalContext, Expr};
use crate::ops::DataType;
use crate::query::binding_bag::{BindingBag, BindingRow};
use crate::query::exec_pattern::{ExecPattern, PatternIndex};

pub(crate) struct PredicatePattern {
    index: PatternIndex,
    variables: Vec<Variable>,
    expression: Expr,
}

impl PredicatePattern {
    pub(crate) fn new(index: PatternIndex, expression: Expr) -> Self {
        Self {
            index,
            variables: expr_variables(&expression),
            expression,
        }
    }

    fn matches(
        &self,
        row: &BindingRow,
        positions: &[(Variable, usize)],
        bindings: &mut HashMap<Variable, DataType>,
    ) -> Result<bool> {
        update_bindings(row, positions, bindings)?;
        Ok(evaluate_as_bool(
            &self.expression,
            &EvalContext::new(bindings),
        ))
    }
}

impl ExecPattern for PredicatePattern {
    fn index(&self) -> PatternIndex {
        self.index
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
            "Predicate pattern {} cannot propose variables: {added:?}",
            self.index
        );
        ensure!(
            target_variables == input.variables,
            "Predicate pattern {} validation must preserve the input layout",
            self.index
        );
        for variable in &self.variables {
            ensure!(
                input.variables.contains(variable),
                "Predicate pattern {} requires bound variable {variable}",
                self.index
            );
        }

        let mut matches = Vec::new();
        let positions = binding_positions(input, &self.variables)?;
        let mut bindings = HashMap::with_capacity(positions.len());
        for (row_index, row) in input.rows.iter().enumerate() {
            if self.matches(row, &positions, &mut bindings)? {
                matches.push(row_index);
            }
        }
        input.select_rows(&matches)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use edn::query::ToVariable;

    use super::PredicatePattern;
    use crate::codec::Encode;
    use crate::expr::{BinaryExpr, BinaryOp, Expr};
    use crate::ops::DataType;
    use crate::query::binding_bag::BindingBag;
    use crate::query::exec_pattern::{ExecPattern, Proposal};

    fn encoded(value: DataType) -> Bytes {
        Bytes::from(value.encode())
    }

    fn less_than(left: &str, right: &str) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Variable(left.to_var())),
            op: BinaryOp::Lt,
            right: Box::new(Expr::Variable(right.to_var())),
        })
    }

    #[test]
    fn filters_rows_by_named_columns_without_changing_layout_or_multiplicity() {
        let pattern = PredicatePattern::new(4, less_than("?x", "?y"));
        let input = BindingBag::new(
            vec!["?outer".to_var(), "?y".to_var(), "?x".to_var()],
            vec![
                vec![
                    encoded(DataType::Long(1)),
                    encoded(DataType::Long(20)),
                    encoded(DataType::Long(10)),
                ],
                vec![
                    encoded(DataType::Long(1)),
                    encoded(DataType::Long(20)),
                    encoded(DataType::Long(10)),
                ],
                vec![
                    encoded(DataType::Long(2)),
                    encoded(DataType::Long(5)),
                    encoded(DataType::Long(10)),
                ],
                vec![
                    encoded(DataType::Long(3)),
                    encoded(DataType::String("wrong".into())),
                    encoded(DataType::Long(10)),
                ],
            ],
        )
        .unwrap();

        let filtered = pattern.join(&input, &[], &input.variables).unwrap();

        assert_eq!(
            filtered,
            BindingBag::new(
                input.variables.to_vec(),
                vec![input.rows[0].clone(), input.rows[1].clone()]
            )
            .unwrap()
        );
    }

    #[test]
    fn never_proposes_and_reports_contract_or_decode_errors() {
        let pattern = PredicatePattern::new(4, less_than("?x", "?y"));
        let input = BindingBag::new(
            vec!["?x".to_var(), "?y".to_var()],
            vec![vec![
                Bytes::from_static(b"invalid"),
                encoded(DataType::Long(2)),
            ]],
        )
        .unwrap();
        let mut proposals = vec![Proposal::default()];

        pattern
            .count(&input, &["?z".to_var()], &mut proposals)
            .unwrap();
        assert_eq!(proposals, vec![Proposal::default()]);
        assert!(pattern
            .join(&input, &["?z".to_var()], &input.variables)
            .is_err());
        assert!(pattern
            .join(&input, &[], &["?y".to_var(), "?x".to_var()],)
            .is_err());
        assert!(pattern.join(&input, &[], &input.variables).is_err());

        let unbound =
            BindingBag::new(vec!["?x".to_var()], vec![vec![encoded(DataType::Long(1))]]).unwrap();
        assert!(pattern.join(&unbound, &[], &unbound.variables).is_err());
    }
}
