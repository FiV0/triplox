use std::collections::HashMap;

use anyhow::{ensure, Context, Result};
use bytes::Bytes;
use edn::query::Variable;

use super::evaluation::{binding_positions, update_bindings};
use crate::codec::{Decode, Encode};
use crate::expr::{evaluate, expr_variables, EvalContext, Expr};
use crate::ops::DataType;
use crate::query::binding_bag::{BindingBag, BindingRow};
use crate::query::exec_pattern::{ExecPattern, PatternIndex, Proposal};

pub(crate) struct FunctionPattern {
    index: PatternIndex,
    variables: Vec<Variable>,
    input_variables: Vec<Variable>,
    output: Variable,
    expression: Expr,
}

impl FunctionPattern {
    pub(crate) fn new(index: PatternIndex, expression: Expr, output: Variable) -> Result<Self> {
        let input_variables = expr_variables(&expression);
        ensure!(
            !input_variables.contains(&output),
            "Function pattern {index} output {output} is also an input"
        );
        let mut variables = input_variables.clone();
        variables.push(output.clone());
        Ok(Self {
            index,
            variables,
            input_variables,
            output,
            expression,
        })
    }

    fn ensure_inputs_bound(&self, input: &BindingBag) -> Result<()> {
        for variable in &self.input_variables {
            ensure!(
                input.variables.contains(variable),
                "Function pattern {} requires bound input {variable}",
                self.index
            );
        }
        Ok(())
    }

    fn compute(
        &self,
        row: &BindingRow,
        positions: &[(Variable, usize)],
        bindings: &mut HashMap<Variable, DataType>,
    ) -> Result<Option<DataType>> {
        update_bindings(row, positions, bindings)?;
        Ok(evaluate(&self.expression, &EvalContext::new(bindings)).map(|value| value.into_owned()))
    }
}

impl ExecPattern for FunctionPattern {
    fn index(&self) -> PatternIndex {
        self.index
    }

    fn variables(&self) -> &[Variable] {
        &self.variables
    }

    fn count(
        &self,
        input: &BindingBag,
        added: &[Variable],
        proposals: &mut [Proposal],
    ) -> Result<()> {
        ensure!(
            proposals.len() == input.rows.len(),
            "Function pattern {} received {} proposals for {} input rows",
            self.index,
            proposals.len(),
            input.rows.len()
        );
        if added != std::slice::from_ref(&self.output)
            || input.variables.contains(&self.output)
            || self
                .input_variables
                .iter()
                .any(|variable| !input.variables.contains(variable))
        {
            return Ok(());
        }

        let positions = binding_positions(input, &self.input_variables)?;
        let mut bindings = HashMap::with_capacity(positions.len());
        for (row, proposal) in input.rows.iter().zip(proposals) {
            if self.compute(row, &positions, &mut bindings)?.is_some() {
                proposal.consider(self.index, 1);
            }
        }
        Ok(())
    }

    fn join(
        &self,
        input: &BindingBag,
        added: &[Variable],
        target_variables: &[Variable],
    ) -> Result<BindingBag> {
        self.ensure_inputs_bound(input)?;

        if added.is_empty() {
            ensure!(
                target_variables == input.variables,
                "Function pattern {} validation must preserve the input layout",
                self.index
            );
            ensure!(
                input.variables.contains(&self.output),
                "Function pattern {} validation requires bound output {}",
                self.index,
                self.output
            );
            let output_column = input.column_index(&self.output)?;
            let positions = binding_positions(input, &self.input_variables)?;
            let mut bindings = HashMap::with_capacity(positions.len());
            let mut matches = Vec::new();
            for (row_index, row) in input.rows.iter().enumerate() {
                let output = DataType::decode(&row[output_column]).with_context(|| {
                    format!(
                        "Failed to decode function pattern {} output {}",
                        self.index, self.output
                    )
                })?;
                if self.compute(row, &positions, &mut bindings)?.as_ref() == Some(&output) {
                    matches.push(row_index);
                }
            }
            return input.select_rows(&matches);
        }

        ensure!(
            added == std::slice::from_ref(&self.output),
            "Function pattern {} can only propose output {}",
            self.index,
            self.output
        );
        ensure!(
            !input.variables.contains(&self.output),
            "Function pattern {} cannot add already-bound output {}",
            self.index,
            self.output
        );
        let positions = binding_positions(input, &self.input_variables)?;
        let mut bindings = HashMap::with_capacity(positions.len());
        let extensions = input
            .rows
            .iter()
            .map(|row| {
                Ok(self
                    .compute(row, &positions, &mut bindings)?
                    .map(|value| vec![vec![Bytes::from(value.encode())]])
                    .unwrap_or_default())
            })
            .collect::<Result<Vec<_>>>()?;
        input
            .extend_rows(added.to_vec(), extensions)?
            .reorder(target_variables)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use edn::query::ToVariable;

    use super::FunctionPattern;
    use crate::codec::Encode;
    use crate::expr::{BinaryExpr, BinaryOp, Expr};
    use crate::ops::DataType;
    use crate::query::binding_bag::BindingBag;
    use crate::query::exec_pattern::{ExecPattern, Proposal};

    fn encoded(value: DataType) -> Bytes {
        Bytes::from(value.encode())
    }

    fn add(left: &str, amount: i64) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Variable(left.to_var())),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(DataType::Long(amount))),
        })
    }

    #[test]
    fn proposes_one_computed_output_per_valid_input_row_in_target_order() {
        let pattern = FunctionPattern::new(8, add("?x", 1), "?y".to_var()).unwrap();
        let input = BindingBag::new(
            vec!["?outer".to_var(), "?x".to_var()],
            vec![
                vec![encoded(DataType::Long(90)), encoded(DataType::Long(1))],
                vec![encoded(DataType::Long(90)), encoded(DataType::Long(1))],
                vec![
                    encoded(DataType::Long(91)),
                    encoded(DataType::String("wrong".into())),
                ],
            ],
        )
        .unwrap();
        let mut proposals = vec![Proposal::default(); 3];

        pattern
            .count(&input, &["?y".to_var()], &mut proposals)
            .unwrap();
        assert_eq!(proposals[0].proposer(), Some(8));
        assert_eq!(proposals[1].proposer(), Some(8));
        assert_eq!(proposals[2].proposer(), None);

        let joined = pattern
            .join(
                &input,
                &["?y".to_var()],
                &["?y".to_var(), "?outer".to_var(), "?x".to_var()],
            )
            .unwrap();
        assert_eq!(
            joined,
            BindingBag::new(
                vec!["?y".to_var(), "?outer".to_var(), "?x".to_var()],
                vec![
                    vec![
                        encoded(DataType::Long(2)),
                        encoded(DataType::Long(90)),
                        encoded(DataType::Long(1)),
                    ],
                    vec![
                        encoded(DataType::Long(2)),
                        encoded(DataType::Long(90)),
                        encoded(DataType::Long(1)),
                    ],
                ],
            )
            .unwrap()
        );
    }

    #[test]
    fn validates_an_already_bound_output_and_preserves_outer_columns() {
        let pattern = FunctionPattern::new(8, add("?x", 1), "?y".to_var()).unwrap();
        let input = BindingBag::new(
            vec!["?y".to_var(), "?outer".to_var(), "?x".to_var()],
            vec![
                vec![
                    encoded(DataType::Long(2)),
                    encoded(DataType::Long(90)),
                    encoded(DataType::Long(1)),
                ],
                vec![
                    encoded(DataType::Long(3)),
                    encoded(DataType::Long(91)),
                    encoded(DataType::Long(1)),
                ],
            ],
        )
        .unwrap();

        assert_eq!(
            pattern.join(&input, &[], &input.variables).unwrap(),
            BindingBag::new(input.variables.to_vec(), vec![input.rows[0].clone()]).unwrap()
        );
    }

    #[test]
    fn constant_functions_start_from_the_unit_relation() {
        let pattern = FunctionPattern::new(
            8,
            Expr::Literal(DataType::String("value".into())),
            "?out".to_var(),
        )
        .unwrap();

        assert_eq!(
            pattern
                .join(&BindingBag::unit(), &["?out".to_var()], &["?out".to_var()])
                .unwrap(),
            BindingBag::new(
                vec!["?out".to_var()],
                vec![vec![encoded(DataType::String("value".into()))]]
            )
            .unwrap()
        );
    }

    #[test]
    fn reports_invalid_contracts_and_encoded_inputs() {
        assert!(FunctionPattern::new(8, Expr::Variable("?x".to_var()), "?x".to_var()).is_err());
        let pattern = FunctionPattern::new(8, add("?x", 1), "?y".to_var()).unwrap();
        let malformed = BindingBag::new(
            vec!["?x".to_var()],
            vec![vec![Bytes::from_static(b"invalid")]],
        )
        .unwrap();
        let mut proposals = vec![Proposal::default()];

        assert!(pattern
            .count(&malformed, &["?y".to_var()], &mut proposals)
            .is_err());
        assert!(pattern.count(&malformed, &[], &mut []).is_err());

        let unbound = BindingBag::unit();
        assert!(pattern
            .join(&unbound, &["?y".to_var()], &["?y".to_var()])
            .is_err());

        let bound_input =
            BindingBag::new(vec!["?x".to_var()], vec![vec![encoded(DataType::Long(1))]]).unwrap();
        assert!(pattern
            .join(&bound_input, &[], &bound_input.variables)
            .is_err());
        assert!(pattern
            .join(
                &bound_input,
                &["?wrong".to_var()],
                &["?x".to_var(), "?wrong".to_var()],
            )
            .is_err());

        let malformed_output = BindingBag::new(
            vec!["?x".to_var(), "?y".to_var()],
            vec![vec![
                encoded(DataType::Long(1)),
                Bytes::from_static(b"invalid"),
            ]],
        )
        .unwrap();
        assert!(pattern
            .join(&malformed_output, &[], &malformed_output.variables)
            .is_err());
    }
}
