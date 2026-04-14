use std::collections::HashMap;

use bytes::Bytes;

use crate::algo::generic_join::{Extension, Prefix, PrefixExtender};
use crate::codec::{Decode, Encode};
use crate::expr::{evaluate, EvalContext, Expr};
use crate::ops::DataType;
use edn::query::{ToVariable, Variable};

/// A prefix extender that evaluates a function expression and produces a new binding.
///
/// Unlike GenericPredicatePrefixExtender (filter-only), this extender **proposes**
/// exactly one candidate: the computed result of the expression. It participates at
/// the output variable's join level, where all input variables are already bound in
/// the prefix.
///
/// Uses the same expression engine (`Expr` + `evaluate`) as
/// GenericPredicatePrefixExtender.
pub struct GenericFnPrefixExtender {
    /// The expression tree to evaluate (produces the output value).
    expr: Expr,
    /// Input variables already bound in the prefix: (variable_name, prefix_index).
    prefix_vars: Vec<(Variable, usize)>,
    /// The join level this extender participates at (the output variable's level).
    output_level: usize,
}

impl GenericFnPrefixExtender {
    pub fn new(expr: Expr, prefix_vars: Vec<(Variable, usize)>, output_level: usize) -> Self {
        Self { expr, prefix_vars, output_level }
    }

    /// Evaluate the expression against the given prefix, returning the serialized result.
    fn compute(&self, prefix: &Prefix) -> Option<Extension> {
        let prefix_values: Vec<(Variable, DataType)> = self
            .prefix_vars
            .iter()
            .map(|(var, idx)| {
                let dt = DataType::decode(&prefix[*idx]).expect("failed to decode prefix variable");
                (var.clone(), dt)
            })
            .collect();

        let bindings: HashMap<Variable, &DataType> =
            prefix_values.iter().map(|(var, dt)| (var.clone(), dt)).collect();

        let ctx = EvalContext::new(bindings);
        let result = evaluate(&self.expr, &ctx)?;
        Some(Bytes::from(result.as_ref().encode()))
    }
}

impl PrefixExtender for GenericFnPrefixExtender {
    fn count(&self, _prefix: &Prefix) -> usize {
        1
    }

    fn propose(&self, prefix: &Prefix) -> Vec<Extension> {
        match self.compute(prefix) {
            Some(ext) => vec![ext],
            None => vec![],
        }
    }

    fn intersect(&self, prefix: &Prefix, extensions: &[Extension]) -> Vec<Extension> {
        match self.compute(prefix) {
            Some(result) => extensions.iter().filter(|ext| **ext == result).cloned().collect(),
            None => vec![],
        }
    }

    fn participates_in_level(&self, level: usize) -> bool {
        level == self.output_level
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::algo::generic_join::{GenericJoin, SingleLevelExtender};
    use crate::expr::{BinaryExpr, BinaryOp, UnaryExpr, UnaryOp};

    fn serialize(dt: &DataType) -> Bytes {
        Bytes::from(dt.encode())
    }

    #[test]
    fn test_count_returns_one() {
        let ext = GenericFnPrefixExtender::new(Expr::Literal(DataType::Long(42)), vec![], 0);
        assert_eq!(ext.count(&vec![]), 1);
    }

    #[test]
    fn test_participates_in_level() {
        let ext = GenericFnPrefixExtender::new(Expr::Literal(DataType::Long(42)), vec![], 2);
        assert!(!ext.participates_in_level(0));
        assert!(!ext.participates_in_level(1));
        assert!(ext.participates_in_level(2));
        assert!(!ext.participates_in_level(3));
    }

    #[test]
    fn test_propose_constant_expr() {
        // Expression that evaluates to a constant (no variables)
        let ext = GenericFnPrefixExtender::new(Expr::Literal(DataType::Long(42)), vec![], 0);
        let result = ext.propose(&vec![]);
        assert_eq!(result.len(), 1);
        assert_eq!(DataType::decode(&result[0]).unwrap(), DataType::Long(42));
    }

    #[test]
    fn test_propose_unary_abs() {
        // (abs ?x) where ?x = -5 at prefix level 0
        let expr = Expr::UnaryExpr(UnaryExpr {
            op: UnaryOp::Abs,
            operand: Box::new(Expr::Variable("?x".to_var())),
        });
        let ext = GenericFnPrefixExtender::new(
            expr,
            vec![("?x".to_var(), 0)],
            1, // output at level 1
        );

        let prefix = vec![serialize(&DataType::Long(-5))];
        let result = ext.propose(&prefix);
        assert_eq!(result.len(), 1);
        assert_eq!(DataType::decode(&result[0]).unwrap(), DataType::Long(5));
    }

    #[test]
    fn test_propose_binary_add_with_constant() {
        // (+ ?x 10) where ?x = 25 at prefix level 0
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Variable("?x".to_var())),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(DataType::Long(10))),
        });
        let ext = GenericFnPrefixExtender::new(expr, vec![("?x".to_var(), 0)], 1);

        let prefix = vec![serialize(&DataType::Long(25))];
        let result = ext.propose(&prefix);
        assert_eq!(result.len(), 1);
        assert_eq!(DataType::decode(&result[0]).unwrap(), DataType::Long(35));
    }

    #[test]
    fn test_propose_binary_add_two_variables() {
        // (+ ?x ?y) where ?x = 10 at level 0, ?y = 20 at level 1
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Variable("?x".to_var())),
            op: BinaryOp::Add,
            right: Box::new(Expr::Variable("?y".to_var())),
        });
        let ext = GenericFnPrefixExtender::new(
            expr,
            vec![("?x".to_var(), 0), ("?y".to_var(), 1)],
            2, // output at level 2
        );

        let prefix = vec![serialize(&DataType::Long(10)), serialize(&DataType::Long(20))];
        let result = ext.propose(&prefix);
        assert_eq!(result.len(), 1);
        assert_eq!(DataType::decode(&result[0]).unwrap(), DataType::Long(30));
    }

    #[test]
    fn test_propose_returns_empty_on_type_mismatch() {
        // (+ ?x 10) where ?x is a String — type mismatch returns empty
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Variable("?x".to_var())),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(DataType::Long(10))),
        });
        let ext = GenericFnPrefixExtender::new(expr, vec![("?x".to_var(), 0)], 1);

        let prefix = vec![serialize(&DataType::String("hello".to_string()))];
        let result = ext.propose(&prefix);
        assert!(result.is_empty());
    }

    #[test]
    fn test_intersect_matching() {
        // (+ ?x 10) where ?x = 25 → result = 35
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Variable("?x".to_var())),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(DataType::Long(10))),
        });
        let ext = GenericFnPrefixExtender::new(expr, vec![("?x".to_var(), 0)], 1);

        let prefix = vec![serialize(&DataType::Long(25))];
        let extensions = vec![
            serialize(&DataType::Long(30)),
            serialize(&DataType::Long(35)),
            serialize(&DataType::Long(40)),
        ];

        let result = ext.intersect(&prefix, &extensions);
        assert_eq!(result.len(), 1);
        assert_eq!(DataType::decode(&result[0]).unwrap(), DataType::Long(35));
    }

    #[test]
    fn test_intersect_no_match() {
        // (+ ?x 10) where ?x = 25 → result = 35, but extensions don't contain 35
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Variable("?x".to_var())),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(DataType::Long(10))),
        });
        let ext = GenericFnPrefixExtender::new(expr, vec![("?x".to_var(), 0)], 1);

        let prefix = vec![serialize(&DataType::Long(25))];
        let extensions = vec![serialize(&DataType::Long(30)), serialize(&DataType::Long(40))];

        let result = ext.intersect(&prefix, &extensions);
        assert!(result.is_empty());
    }

    #[test]
    fn test_integration_with_generic_join() {
        // Level 0: values 10, 20, 30
        // Level 1 (fn): (+ ?x 1) → should produce 11, 21, 31
        let values = vec![
            serialize(&DataType::Long(10)),
            serialize(&DataType::Long(20)),
            serialize(&DataType::Long(30)),
        ];
        let level0 = SingleLevelExtender::new(values, 0);

        let fn_expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Variable("?x".to_var())),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(DataType::Long(1))),
        });
        let fn_ext = GenericFnPrefixExtender::new(fn_expr, vec![("?x".to_var(), 0)], 1);

        let extenders: Vec<&dyn PrefixExtender> = vec![&level0, &fn_ext];
        let join = GenericJoin::new(extenders, 2);
        let result = join.join();

        assert_eq!(result.len(), 3);
        // Each tuple has [original, original+1]
        for tuple in &result {
            let original = DataType::decode(&tuple[0]).unwrap();
            let computed = DataType::decode(&tuple[1]).unwrap();
            match (original, computed) {
                (DataType::Long(o), DataType::Long(c)) => assert_eq!(c, o + 1),
                _ => panic!("unexpected types"),
            }
        }
    }

    #[test]
    fn test_integration_three_levels() {
        // Level 0: ages 10, 20, 30
        // Level 1: names "alice", "bob"
        // Level 2 (fn): (+ ?age 1) → age + 1
        let ages = vec![
            serialize(&DataType::Long(10)),
            serialize(&DataType::Long(20)),
            serialize(&DataType::Long(30)),
        ];
        let names = vec![
            serialize(&DataType::String("alice".into())),
            serialize(&DataType::String("bob".into())),
        ];
        let level0 = SingleLevelExtender::new(ages, 0);
        let level1 = SingleLevelExtender::new(names, 1);

        let fn_expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Variable("?age".to_var())),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(DataType::Long(1))),
        });
        let fn_ext = GenericFnPrefixExtender::new(fn_expr, vec![("?age".to_var(), 0)], 2);

        let extenders: Vec<&dyn PrefixExtender> = vec![&level0, &level1, &fn_ext];
        let join = GenericJoin::new(extenders, 3);
        let result = join.join();

        // 3 ages × 2 names = 6 tuples, each with a computed age+1
        assert_eq!(result.len(), 6);
    }

    #[test]
    fn test_propose_constant_only_expression() {
        // (+ 1 2) — no input variables, pure constant expression
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Literal(DataType::Long(1))),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(DataType::Long(2))),
        });
        let ext = GenericFnPrefixExtender::new(
            expr,
            vec![], // no prefix vars
            0,
        );

        let result = ext.propose(&vec![]);
        assert_eq!(result.len(), 1);
        assert_eq!(DataType::decode(&result[0]).unwrap(), DataType::Long(3));
    }
}
