use std::collections::HashMap;

use crate::algo::generic_join::{Extension, Prefix, PrefixExtender};
use crate::codec::Decode;
use crate::expr::{evaluate_as_bool, EvalContext, Expr};
use crate::ops::DataType;
use edn::query::{ToVariable, Variable};

/// A prefix extender that filters extensions by evaluating a predicate expression.
///
/// Like GenericNotPrefixExtender, this never proposes candidates — it only
/// filters extensions via `intersect`. It participates at the level of its
/// last (highest join-order) variable argument.
///
/// Ported from hooray2's GenericPredicatePrefixExtender, using a DataFusion-inspired
/// expression tree instead of flat function+args.
pub struct GenericPredicatePrefixExtender {
    /// The expression tree to evaluate (must produce Boolean).
    expr: Expr,
    /// Variables already bound in the prefix: (variable_name, prefix_index).
    prefix_vars: Vec<(Variable, usize)>,
    /// The variable resolved from the extension being tested.
    extension_var: Variable,
    /// The join level this extender participates at (highest variable level).
    level: usize,
}

impl GenericPredicatePrefixExtender {
    pub fn new(
        expr: Expr,
        prefix_vars: Vec<(Variable, usize)>,
        extension_var: Variable,
        level: usize,
    ) -> Self {
        Self {
            expr,
            prefix_vars,
            extension_var,
            level,
        }
    }
}

impl PrefixExtender for GenericPredicatePrefixExtender {
    fn count(&self, _prefix: &Prefix) -> usize {
        usize::MAX
    }

    fn propose(&self, _prefix: &Prefix) -> Vec<Extension> {
        panic!("Propose should not be called on predicate prefix extender")
    }

    fn intersect(&self, prefix: &Prefix, extensions: &[Extension]) -> Vec<Extension> {
        // Pre-decode prefix variables once (same for all extensions).
        let prefix_values: Vec<(Variable, DataType)> = self
            .prefix_vars
            .iter()
            .map(|(var, idx)| {
                let dt = DataType::decode(&prefix[*idx]).expect("failed to decode prefix variable");
                (var.clone(), dt)
            })
            .collect();

        extensions
            .iter()
            .filter(|ext| {
                let ext_val = DataType::decode(ext).expect("failed to decode extension value");

                let mut bindings: HashMap<Variable, &DataType> = prefix_values
                    .iter()
                    .map(|(var, dt)| (var.clone(), dt))
                    .collect();
                bindings.insert(self.extension_var.clone(), &ext_val);

                let ctx = EvalContext::new(bindings);
                evaluate_as_bool(&self.expr, &ctx)
            })
            .cloned()
            .collect()
    }

    fn participates_in_level(&self, level: usize) -> bool {
        level == self.level
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    use crate::algo::generic_join::{GenericJoin, SingleLevelExtender};
    use crate::codec::Encode;
    use crate::expr::{BinaryExpr, BinaryOp};

    fn serialize(dt: &DataType) -> Bytes {
        Bytes::from(dt.encode())
    }

    #[test]
    fn test_count_returns_max() {
        let ext = GenericPredicatePrefixExtender::new(
            Expr::Literal(DataType::Boolean(true)),
            vec![],
            "?x".to_var(),
            0,
        );
        assert_eq!(ext.count(&vec![]), usize::MAX);
    }

    #[test]
    #[should_panic(expected = "Propose should not be called")]
    fn test_propose_panics() {
        let ext = GenericPredicatePrefixExtender::new(
            Expr::Literal(DataType::Boolean(true)),
            vec![],
            "?x".to_var(),
            0,
        );
        ext.propose(&vec![]);
    }

    #[test]
    fn test_participates_in_level() {
        let ext = GenericPredicatePrefixExtender::new(
            Expr::Literal(DataType::Boolean(true)),
            vec![],
            "?x".to_var(),
            2,
        );
        assert!(!ext.participates_in_level(0));
        assert!(!ext.participates_in_level(1));
        assert!(ext.participates_in_level(2));
        assert!(!ext.participates_in_level(3));
    }

    #[test]
    fn test_binary_lt_one_var_one_const() {
        // (< ?age 30)
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Variable("?age".to_var())),
            op: BinaryOp::Lt,
            right: Box::new(Expr::Literal(DataType::Long(30))),
        });
        let ext = GenericPredicatePrefixExtender::new(expr, vec![], "?age".to_var(), 0);

        let extensions = vec![
            serialize(&DataType::Long(25)),
            serialize(&DataType::Long(30)),
            serialize(&DataType::Long(35)),
            serialize(&DataType::Long(10)),
        ];

        let result = ext.intersect(&vec![], &extensions);
        assert_eq!(result.len(), 2);
        assert_eq!(DataType::decode(&result[0]).unwrap(), DataType::Long(25));
        assert_eq!(DataType::decode(&result[1]).unwrap(), DataType::Long(10));
    }

    #[test]
    fn test_binary_lt_two_vars() {
        // (< ?a ?b) where ?a is at level 0 (prefix), ?b is at level 1 (extension)
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Variable("?a".to_var())),
            op: BinaryOp::Lt,
            right: Box::new(Expr::Variable("?b".to_var())),
        });
        let ext =
            GenericPredicatePrefixExtender::new(expr, vec![("?a".to_var(), 0)], "?b".to_var(), 1);

        // prefix has ?a = 10 at level 0
        let prefix = vec![serialize(&DataType::Long(10))];
        let extensions = vec![
            serialize(&DataType::Long(5)),  // 10 < 5 = false
            serialize(&DataType::Long(10)), // 10 < 10 = false
            serialize(&DataType::Long(15)), // 10 < 15 = true
            serialize(&DataType::Long(20)), // 10 < 20 = true
        ];

        let result = ext.intersect(&prefix, &extensions);
        assert_eq!(result.len(), 2);
        assert_eq!(DataType::decode(&result[0]).unwrap(), DataType::Long(15));
        assert_eq!(DataType::decode(&result[1]).unwrap(), DataType::Long(20));
    }

    #[test]
    fn test_binary_two_vars_reversed_arg_order() {
        // (< ?b ?a) where ?a is at level 0 (prefix), ?b is at level 1 (extension)
        // The extension variable (?b) appears first in the expression args.
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Variable("?b".to_var())),
            op: BinaryOp::Lt,
            right: Box::new(Expr::Variable("?a".to_var())),
        });
        let ext =
            GenericPredicatePrefixExtender::new(expr, vec![("?a".to_var(), 0)], "?b".to_var(), 1);

        // prefix has ?a = 10
        let prefix = vec![serialize(&DataType::Long(10))];
        let extensions = vec![
            serialize(&DataType::Long(5)),  // 5 < 10 = true
            serialize(&DataType::Long(10)), // 10 < 10 = false
            serialize(&DataType::Long(15)), // 15 < 10 = false
        ];

        let result = ext.intersect(&prefix, &extensions);
        assert_eq!(result.len(), 1);
        assert_eq!(DataType::decode(&result[0]).unwrap(), DataType::Long(5));
    }

    #[test]
    fn test_integration_with_generic_join() {
        // Level 0: values 10, 20, 30, 40
        // Predicate at level 0: (< ?x 25) — should keep only 10, 20
        let values = vec![
            serialize(&DataType::Long(10)),
            serialize(&DataType::Long(20)),
            serialize(&DataType::Long(30)),
            serialize(&DataType::Long(40)),
        ];
        let level0 = SingleLevelExtender::new(values, 0);

        let pred_expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Variable("?x".to_var())),
            op: BinaryOp::Lt,
            right: Box::new(Expr::Literal(DataType::Long(25))),
        });
        let pred = GenericPredicatePrefixExtender::new(pred_expr, vec![], "?x".to_var(), 0);

        let extenders: Vec<&dyn PrefixExtender> = vec![&level0, &pred];
        let join = GenericJoin::new(extenders, 1);
        let result = join.join();

        assert_eq!(result.len(), 2);
        assert_eq!(DataType::decode(&result[0][0]).unwrap(), DataType::Long(20));
        assert_eq!(DataType::decode(&result[1][0]).unwrap(), DataType::Long(10));
    }

    #[test]
    fn test_integration_two_level_with_predicate() {
        // Level 0: ages 10, 20, 30
        // Level 1: names "alice", "bob"
        // Predicate at level 0: (>= ?age 20) — should filter to ages 20, 30
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

        let pred_expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Variable("?age".to_var())),
            op: BinaryOp::GtEq,
            right: Box::new(Expr::Literal(DataType::Long(20))),
        });
        let pred = GenericPredicatePrefixExtender::new(pred_expr, vec![], "?age".to_var(), 0);

        let extenders: Vec<&dyn PrefixExtender> = vec![&level0, &level1, &pred];
        let join = GenericJoin::new(extenders, 2);
        let result = join.join();

        // 2 ages (20, 30) * 2 names = 4 tuples
        assert_eq!(result.len(), 4);
    }
}
