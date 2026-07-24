use anyhow::{ensure, Result};
use edn::query::Variable;

use super::evaluation::{decode_bindings, eval_context};
use crate::expr::{evaluate_as_bool, expr_variables, Expr};
use crate::query::binding_bag::BindingBag;
use crate::query::exec_pattern::{ExecPattern, PatternId, Proposal};

pub(crate) struct PredicatePattern {
    id: PatternId,
    variables: Vec<Variable>,
    expression: Expr,
}

impl PredicatePattern {
    pub(crate) fn new(id: PatternId, expression: Expr) -> Self {
        Self {
            id,
            variables: expr_variables(&expression),
            expression,
        }
    }

    fn matches(&self, input: &BindingBag, row_index: usize) -> Result<bool> {
        let bindings = decode_bindings(input, &input.rows[row_index], self.variables.as_slice())?;
        Ok(evaluate_as_bool(&self.expression, &eval_context(&bindings)))
    }
}

impl ExecPattern for PredicatePattern {
    fn id(&self) -> PatternId {
        self.id
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
            "Predicate pattern {} received {} proposals for {} input rows",
            self.id,
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
        ensure!(
            added.is_empty(),
            "Predicate pattern {} cannot propose variables: {added:?}",
            self.id
        );
        ensure!(
            target_variables == input.variables,
            "Predicate pattern {} validation must preserve the input layout",
            self.id
        );
        for variable in &self.variables {
            ensure!(
                input.variables.contains(variable),
                "Predicate pattern {} requires bound variable {variable}",
                self.id
            );
        }

        let mut matches = Vec::new();
        for row_index in 0..input.rows.len() {
            if self.matches(input, row_index)? {
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

        let filtered = pattern.join(&input, &[], input.variables).unwrap();

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
        assert!(pattern.count(&input, &[], &mut []).is_err());
        assert!(pattern
            .join(&input, &["?z".to_var()], input.variables)
            .is_err());
        assert!(pattern
            .join(&input, &[], &["?y".to_var(), "?x".to_var()],)
            .is_err());
        assert!(pattern.join(&input, &[], input.variables).is_err());

        let unbound =
            BindingBag::new(vec!["?x".to_var()], vec![vec![encoded(DataType::Long(1))]]).unwrap();
        assert!(pattern.join(&unbound, &[], unbound.variables).is_err());
    }
}
