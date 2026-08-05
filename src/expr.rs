use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use regex::Regex;

use chrono::Datelike;

use crate::ops::DataType;
use edn::query::{ToVariable, Variable};

/// Operators for binary expressions (inspired by DataFusion's Operator enum).
#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOp {
    // Comparison
    Lt,
    LtEq,
    Gt,
    GtEq,
    Eq,
    NotEq,
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    // String
    Concat,
}

/// Operators for unary expressions.
#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Not,
    // Math
    Abs,
    // String
    Upper,
    Lower,
    Strlen,
    Str,
    // Date/Time
    Year,
    Month,
}

/// A binary expression node (inspired by DataFusion's BinaryExpr).
#[derive(Debug, Clone, PartialEq)]
pub struct BinaryExpr {
    pub left: Box<Expr>,
    pub op: BinaryOp,
    pub right: Box<Expr>,
}

/// A unary expression node.
#[derive(Debug, Clone, PartialEq)]
pub struct UnaryExpr {
    pub op: UnaryOp,
    pub operand: Box<Expr>,
}

/// A regexp_like expression node. Compiles the pattern once at plan time.
#[derive(Debug, Clone)]
pub struct RegexpLikeExpr {
    pub pattern_src: String,
    pub pattern: Arc<Regex>,
    pub subject: Box<Expr>,
}

impl PartialEq for RegexpLikeExpr {
    fn eq(&self, other: &Self) -> bool {
        self.pattern_src == other.pattern_src && self.subject == other.subject
    }
}

/// A conditional (if) expression node.
#[derive(Debug, Clone, PartialEq)]
pub struct IfExpr {
    pub condition: Box<Expr>,
    pub then_expr: Box<Expr>,
    pub else_expr: Box<Expr>,
}

/// Expression tree (inspired by DataFusion's Expr enum).
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A query variable reference (DataFusion: Column).
    Variable(Variable),
    /// A constant value (DataFusion: Literal).
    Literal(DataType),
    /// A binary operation.
    BinaryExpr(BinaryExpr),
    /// A unary operation.
    UnaryExpr(UnaryExpr),
    /// regexp_like predicate with a pre-compiled pattern.
    RegexpLike(RegexpLikeExpr),
    /// A conditional (if) expression.
    IfExpr(IfExpr),
}

/// Collect all variables referenced in an expression, deduplicated, in first-appearance order.
pub fn expr_variables(expr: &Expr) -> Vec<Variable> {
    let mut seen = HashSet::new();
    let mut vars = Vec::new();
    collect_vars(expr, &mut seen, &mut vars);
    vars
}

fn collect_vars(expr: &Expr, seen: &mut HashSet<Variable>, vars: &mut Vec<Variable>) {
    match expr {
        Expr::Variable(v) => {
            if seen.insert(v.clone()) {
                vars.push(v.clone());
            }
        }
        Expr::Literal(_) => {}
        Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
            collect_vars(left, seen, vars);
            collect_vars(right, seen, vars);
        }
        Expr::UnaryExpr(UnaryExpr { operand, .. }) => {
            collect_vars(operand, seen, vars);
        }
        Expr::RegexpLike(RegexpLikeExpr { subject, .. }) => {
            collect_vars(subject, seen, vars);
        }
        Expr::IfExpr(IfExpr {
            condition,
            then_expr,
            else_expr,
        }) => {
            collect_vars(condition, seen, vars);
            collect_vars(then_expr, seen, vars);
            collect_vars(else_expr, seen, vars);
        }
    }
}

/// Row-at-a-time evaluation context.
pub struct EvalContext<'a> {
    bindings: &'a HashMap<Variable, DataType>,
}

impl<'a> EvalContext<'a> {
    pub fn new(bindings: &'a HashMap<Variable, DataType>) -> Self {
        Self { bindings }
    }

    pub fn resolve(&self, var: &Variable) -> Option<&DataType> {
        self.bindings.get(var)
    }
}

/// Evaluate an expression tree against a context.
/// Returns Cow::Borrowed for variable/literal access (zero-copy),
/// Cow::Owned only for computed results (binary/unary ops).
/// Returns None if a variable is unbound or types are incompatible.
pub fn evaluate<'a>(expr: &'a Expr, ctx: &'a EvalContext<'a>) -> Option<Cow<'a, DataType>> {
    match expr {
        Expr::Variable(var) => ctx.resolve(var).map(Cow::Borrowed),
        Expr::Literal(dt) => Some(Cow::Borrowed(dt)),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let l = evaluate(left, ctx)?;
            let r = evaluate(right, ctx)?;
            eval_binary_op(&l, op, &r).map(Cow::Owned)
        }
        Expr::UnaryExpr(UnaryExpr { op, operand }) => {
            let v = evaluate(operand, ctx)?;
            eval_unary_op(op, &v).map(Cow::Owned)
        }
        Expr::RegexpLike(RegexpLikeExpr {
            pattern, subject, ..
        }) => {
            let v = evaluate(subject, ctx)?;
            match v.as_ref() {
                DataType::String(s) => Some(Cow::Owned(DataType::Boolean(pattern.is_match(s)))),
                _ => None,
            }
        }
        Expr::IfExpr(IfExpr {
            condition,
            then_expr,
            else_expr,
        }) => {
            if evaluate_as_bool(condition, ctx) {
                evaluate(then_expr, ctx)
            } else {
                evaluate(else_expr, ctx)
            }
        }
    }
}

/// Entry point for predicate evaluation — returns true/false directly.
pub fn evaluate_as_bool(expr: &Expr, ctx: &EvalContext) -> bool {
    matches!(
        evaluate(expr, ctx).as_deref(),
        Some(DataType::Boolean(true))
    )
}

fn eval_binary_op(left: &DataType, op: &BinaryOp, right: &DataType) -> Option<DataType> {
    match op {
        BinaryOp::Lt => Some(DataType::Boolean(
            left.partial_compare(right).ok()? == Ordering::Less,
        )),
        BinaryOp::LtEq => {
            let ord = left.partial_compare(right).ok()?;
            Some(DataType::Boolean(
                ord == Ordering::Less || ord == Ordering::Equal,
            ))
        }
        BinaryOp::Gt => Some(DataType::Boolean(
            left.partial_compare(right).ok()? == Ordering::Greater,
        )),
        BinaryOp::GtEq => {
            let ord = left.partial_compare(right).ok()?;
            Some(DataType::Boolean(
                ord == Ordering::Greater || ord == Ordering::Equal,
            ))
        }
        BinaryOp::Eq => Some(DataType::Boolean(left == right)),
        BinaryOp::NotEq => Some(DataType::Boolean(left != right)),
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
            eval_arithmetic(left, op, right)
        }
        BinaryOp::Concat => match (left, right) {
            (DataType::String(a), DataType::String(b)) => {
                Some(DataType::String(format!("{}{}", a, b)))
            }
            _ => None,
        },
    }
}

fn eval_arithmetic(left: &DataType, op: &BinaryOp, right: &DataType) -> Option<DataType> {
    match (left, right) {
        (DataType::Long(a), DataType::Long(b)) => {
            let result = match op {
                BinaryOp::Add => a.checked_add(*b)?,
                BinaryOp::Sub => a.checked_sub(*b)?,
                BinaryOp::Mul => a.checked_mul(*b)?,
                BinaryOp::Div => a.checked_div(*b)?,
                BinaryOp::Mod => a.checked_rem(*b)?,
                _ => unreachable!(),
            };
            Some(DataType::Long(result))
        }
        (DataType::Double(a), DataType::Double(b)) => {
            let result = match op {
                BinaryOp::Add => a + b,
                BinaryOp::Sub => a - b,
                BinaryOp::Mul => a * b,
                BinaryOp::Div => {
                    if *b == 0.0 {
                        return None;
                    }
                    a / b
                }
                BinaryOp::Mod => {
                    if *b == 0.0 {
                        return None;
                    }
                    a % b
                }
                _ => unreachable!(),
            };
            Some(DataType::Double(result))
        }
        // Cross-numeric: Long × Double → Double
        (DataType::Long(a), DataType::Double(b)) => {
            eval_arithmetic(&DataType::Double(*a as f64), op, &DataType::Double(*b))
        }
        (DataType::Double(a), DataType::Long(b)) => {
            eval_arithmetic(&DataType::Double(*a), op, &DataType::Double(*b as f64))
        }
        _ => None,
    }
}

fn eval_unary_op(op: &UnaryOp, val: &DataType) -> Option<DataType> {
    match op {
        UnaryOp::Not => match val {
            DataType::Boolean(b) => Some(DataType::Boolean(!b)),
            _ => None,
        },
        UnaryOp::Abs => match val {
            DataType::Long(n) => Some(DataType::Long(n.checked_abs()?)),
            DataType::Double(n) => Some(DataType::Double(n.abs())),
            _ => None,
        },
        UnaryOp::Upper => match val {
            DataType::String(s) => Some(DataType::String(s.to_uppercase())),
            _ => None,
        },
        UnaryOp::Lower => match val {
            DataType::String(s) => Some(DataType::String(s.to_lowercase())),
            _ => None,
        },
        UnaryOp::Strlen => match val {
            DataType::String(s) => Some(DataType::Long(s.len() as i64)),
            _ => None,
        },
        UnaryOp::Str => {
            let s = match val {
                DataType::Long(n) => n.to_string(),
                DataType::BigInt(n) => n.to_string(),
                DataType::Double(n) => n.to_string(),
                DataType::Float(n) => n.to_string(),
                DataType::Boolean(b) => b.to_string(),
                DataType::String(s) => s.clone(),
                DataType::Uuid(u) => u.to_string(),
                other => format!("{:?}", other),
            };
            Some(DataType::String(s))
        }
        UnaryOp::Year => match val {
            DataType::Instant(dt) => Some(DataType::Long(dt.year() as i64)),
            _ => None,
        },
        UnaryOp::Month => match val {
            DataType::Instant(dt) => Some(DataType::Long(dt.month() as i64)),
            _ => None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test helper — strips the Cow wrapper so tests compare plain DataType values.
    fn eval(expr: &Expr, ctx: &EvalContext) -> Option<DataType> {
        evaluate(expr, ctx).map(|c| c.into_owned())
    }

    fn var(name: &str) -> Expr {
        Expr::Variable(name.to_var())
    }

    fn lit_long(n: i64) -> Expr {
        Expr::Literal(DataType::Long(n))
    }

    fn lit_bool(b: bool) -> Expr {
        Expr::Literal(DataType::Boolean(b))
    }

    fn binary(left: Expr, op: BinaryOp, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }

    fn unary(op: UnaryOp, operand: Expr) -> Expr {
        Expr::UnaryExpr(UnaryExpr {
            op,
            operand: Box::new(operand),
        })
    }

    #[test]
    fn test_evaluate_literal() {
        let expr = lit_long(42);
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Long(42)));
    }

    #[test]
    fn test_evaluate_variable() {
        let expr = var("?x");
        let x = DataType::Long(10);
        let ctx_bindings = HashMap::from([("?x".to_var(), x.clone())]);
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Long(10)));
    }

    #[test]
    fn test_evaluate_unbound_variable() {
        let expr = var("?missing");
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), None);
    }

    #[test]
    fn test_evaluate_lt() {
        let expr = binary(var("?age"), BinaryOp::Lt, lit_long(30));
        let young = DataType::Long(25);
        let ctx_young_bindings = HashMap::from([("?age".to_var(), young.clone())]);
        let ctx_young = EvalContext::new(&ctx_young_bindings);
        let old = DataType::Long(35);
        let ctx_old_bindings = HashMap::from([("?age".to_var(), old.clone())]);
        let ctx_old = EvalContext::new(&ctx_old_bindings);
        assert_eq!(eval(&expr, &ctx_young), Some(DataType::Boolean(true)));
        assert_eq!(eval(&expr, &ctx_old), Some(DataType::Boolean(false)));
    }

    #[test]
    fn test_evaluate_lteq() {
        let expr = binary(var("?x"), BinaryOp::LtEq, lit_long(10));
        let val_eq = DataType::Long(10);
        let val_less = DataType::Long(5);
        let val_greater = DataType::Long(15);
        let ctx_eq_bindings = HashMap::from([("?x".to_var(), val_eq.clone())]);
        let ctx_eq = EvalContext::new(&ctx_eq_bindings);
        let ctx_less_bindings = HashMap::from([("?x".to_var(), val_less.clone())]);
        let ctx_less = EvalContext::new(&ctx_less_bindings);
        let ctx_greater_bindings = HashMap::from([("?x".to_var(), val_greater.clone())]);
        let ctx_greater = EvalContext::new(&ctx_greater_bindings);
        assert_eq!(eval(&expr, &ctx_eq), Some(DataType::Boolean(true)));
        assert_eq!(eval(&expr, &ctx_less), Some(DataType::Boolean(true)));
        assert_eq!(eval(&expr, &ctx_greater), Some(DataType::Boolean(false)));
    }

    #[test]
    fn test_evaluate_eq_neq() {
        let expr_eq = binary(var("?a"), BinaryOp::Eq, var("?b"));
        let expr_neq = binary(var("?a"), BinaryOp::NotEq, var("?b"));

        let five = DataType::Long(5);
        let ten = DataType::Long(10);
        let ctx_same_bindings =
            HashMap::from([("?a".to_var(), five.clone()), ("?b".to_var(), five.clone())]);
        let ctx_same = EvalContext::new(&ctx_same_bindings);
        let ctx_diff_bindings =
            HashMap::from([("?a".to_var(), five.clone()), ("?b".to_var(), ten)]);
        let ctx_diff = EvalContext::new(&ctx_diff_bindings);
        assert_eq!(eval(&expr_eq, &ctx_same), Some(DataType::Boolean(true)));
        assert_eq!(eval(&expr_eq, &ctx_diff), Some(DataType::Boolean(false)));
        assert_eq!(eval(&expr_neq, &ctx_same), Some(DataType::Boolean(false)));
        assert_eq!(eval(&expr_neq, &ctx_diff), Some(DataType::Boolean(true)));
    }

    #[test]
    fn test_evaluate_gt_gteq() {
        let gt = binary(var("?x"), BinaryOp::Gt, lit_long(10));
        let gteq = binary(var("?x"), BinaryOp::GtEq, lit_long(10));

        let val_eq = DataType::Long(10);
        let val_gt = DataType::Long(15);
        let ctx_eq_bindings = HashMap::from([("?x".to_var(), val_eq.clone())]);
        let ctx_eq = EvalContext::new(&ctx_eq_bindings);
        let ctx_gt_bindings = HashMap::from([("?x".to_var(), val_gt.clone())]);
        let ctx_gt = EvalContext::new(&ctx_gt_bindings);
        assert_eq!(eval(&gt, &ctx_eq), Some(DataType::Boolean(false)));
        assert_eq!(eval(&gt, &ctx_gt), Some(DataType::Boolean(true)));
        assert_eq!(eval(&gteq, &ctx_eq), Some(DataType::Boolean(true)));
    }

    #[test]
    fn test_evaluate_not() {
        let expr = unary(UnaryOp::Not, lit_bool(true));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Boolean(false)));

        let expr2 = unary(UnaryOp::Not, lit_bool(false));
        assert_eq!(eval(&expr2, &ctx), Some(DataType::Boolean(true)));
    }

    #[test]
    fn test_evaluate_not_non_boolean() {
        let expr = unary(UnaryOp::Not, lit_long(42));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), None);
    }

    #[test]
    fn test_evaluate_nested_not_lt() {
        // NOT (< ?x 10)
        let expr = unary(UnaryOp::Not, binary(var("?x"), BinaryOp::Lt, lit_long(10)));
        let low = DataType::Long(5);
        let high = DataType::Long(15);
        let ctx_low_bindings = HashMap::from([("?x".to_var(), low.clone())]);
        let ctx_low = EvalContext::new(&ctx_low_bindings);
        let ctx_high_bindings = HashMap::from([("?x".to_var(), high.clone())]);
        let ctx_high = EvalContext::new(&ctx_high_bindings);
        assert_eq!(eval(&expr, &ctx_low), Some(DataType::Boolean(false)));
        assert_eq!(eval(&expr, &ctx_high), Some(DataType::Boolean(true)));
    }

    #[test]
    fn test_evaluate_incompatible_types() {
        let expr = binary(
            Expr::Literal(DataType::String("hello".into())),
            BinaryOp::Lt,
            lit_long(10),
        );
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), None);
    }

    #[test]
    fn test_evaluate_as_bool() {
        let expr = binary(var("?x"), BinaryOp::Lt, lit_long(10));
        let low = DataType::Long(5);
        let high = DataType::Long(15);
        let ctx_low_bindings = HashMap::from([("?x".to_var(), low.clone())]);
        let ctx_low = EvalContext::new(&ctx_low_bindings);
        let ctx_high_bindings = HashMap::from([("?x".to_var(), high.clone())]);
        let ctx_high = EvalContext::new(&ctx_high_bindings);
        assert!(evaluate_as_bool(&expr, &ctx_low));
        assert!(!evaluate_as_bool(&expr, &ctx_high));
    }

    #[test]
    fn test_expr_variables() {
        let expr = binary(var("?a"), BinaryOp::Lt, var("?b"));
        assert_eq!(expr_variables(&expr), vec!["?a".to_var(), "?b".to_var()]);
    }

    #[test]
    fn test_expr_variables_dedup() {
        // (< ?a ?a) — same variable used twice
        let expr = binary(var("?a"), BinaryOp::Lt, var("?a"));
        assert_eq!(expr_variables(&expr), vec!["?a".to_var()]);
    }

    #[test]
    fn test_expr_variables_nested() {
        // NOT (< ?x 10) — variable inside nested expression
        let expr = unary(UnaryOp::Not, binary(var("?x"), BinaryOp::Lt, lit_long(10)));
        assert_eq!(expr_variables(&expr), vec!["?x".to_var()]);
    }

    #[test]
    fn test_expr_variables_no_vars() {
        let expr = binary(lit_long(1), BinaryOp::Lt, lit_long(2));
        assert_eq!(expr_variables(&expr), Vec::<Variable>::new());
    }

    // --- Arithmetic operator tests ---

    fn lit_double(n: f64) -> Expr {
        Expr::Literal(DataType::Double(n))
    }

    fn lit_string(s: &str) -> Expr {
        Expr::Literal(DataType::String(s.to_string()))
    }

    #[test]
    fn test_add_long() {
        let expr = binary(lit_long(3), BinaryOp::Add, lit_long(4));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Long(7)));
    }

    #[test]
    fn test_sub_long() {
        let expr = binary(lit_long(10), BinaryOp::Sub, lit_long(3));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Long(7)));
    }

    #[test]
    fn test_mul_long() {
        let expr = binary(lit_long(5), BinaryOp::Mul, lit_long(6));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Long(30)));
    }

    #[test]
    fn test_div_long() {
        let expr = binary(lit_long(10), BinaryOp::Div, lit_long(3));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Long(3)));
    }

    #[test]
    fn test_div_by_zero_long() {
        let expr = binary(lit_long(10), BinaryOp::Div, lit_long(0));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), None);
    }

    #[test]
    fn test_mod_long() {
        let expr = binary(lit_long(10), BinaryOp::Mod, lit_long(3));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Long(1)));
    }

    #[test]
    fn test_mod_by_zero() {
        let expr = binary(lit_long(10), BinaryOp::Mod, lit_long(0));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), None);
    }

    #[test]
    fn test_add_double() {
        let expr = binary(lit_double(1.5), BinaryOp::Add, lit_double(2.5));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Double(4.0)));
    }

    #[test]
    fn test_div_by_zero_double() {
        let expr = binary(lit_double(1.0), BinaryOp::Div, lit_double(0.0));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), None);
    }

    #[test]
    fn test_add_mixed_long_double() {
        let expr = binary(lit_long(3), BinaryOp::Add, lit_double(1.5));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Double(4.5)));
    }

    #[test]
    fn test_add_mixed_double_long() {
        let expr = binary(lit_double(1.5), BinaryOp::Add, lit_long(3));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Double(4.5)));
    }

    #[test]
    fn test_arithmetic_incompatible_types() {
        let expr = binary(lit_string("hello"), BinaryOp::Add, lit_long(1));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), None);
    }

    #[test]
    fn test_concat() {
        let expr = binary(lit_string("hello"), BinaryOp::Concat, lit_string(" world"));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(
            eval(&expr, &ctx),
            Some(DataType::String("hello world".to_string()))
        );
    }

    #[test]
    fn test_concat_non_strings() {
        let expr = binary(lit_long(1), BinaryOp::Concat, lit_string("x"));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), None);
    }

    // --- Unary operator tests ---

    #[test]
    fn test_abs_positive_long() {
        let expr = unary(UnaryOp::Abs, lit_long(42));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Long(42)));
    }

    #[test]
    fn test_abs_negative_long() {
        let expr = unary(UnaryOp::Abs, lit_long(-42));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Long(42)));
    }

    #[test]
    fn test_abs_double() {
        let expr = unary(UnaryOp::Abs, lit_double(-3.15));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Double(3.15)));
    }

    #[test]
    fn test_abs_non_numeric() {
        let expr = unary(UnaryOp::Abs, lit_string("hello"));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), None);
    }

    #[test]
    fn test_upper() {
        let expr = unary(UnaryOp::Upper, lit_string("hello"));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(
            eval(&expr, &ctx),
            Some(DataType::String("HELLO".to_string()))
        );
    }

    #[test]
    fn test_lower() {
        let expr = unary(UnaryOp::Lower, lit_string("HELLO"));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(
            eval(&expr, &ctx),
            Some(DataType::String("hello".to_string()))
        );
    }

    #[test]
    fn test_strlen() {
        let expr = unary(UnaryOp::Strlen, lit_string("hello"));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Long(5)));
    }

    #[test]
    fn test_strlen_non_string() {
        let expr = unary(UnaryOp::Strlen, lit_long(42));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), None);
    }

    #[test]
    fn test_str_from_long() {
        let expr = unary(UnaryOp::Str, lit_long(42));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::String("42".to_string())));
    }

    #[test]
    fn test_str_from_double() {
        let expr = unary(UnaryOp::Str, lit_double(3.15));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(
            eval(&expr, &ctx),
            Some(DataType::String("3.15".to_string()))
        );
    }

    #[test]
    fn test_str_from_bool() {
        let expr = unary(UnaryOp::Str, lit_bool(true));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(
            eval(&expr, &ctx),
            Some(DataType::String("true".to_string()))
        );
    }

    #[test]
    fn test_nested_arithmetic() {
        // (+ (* ?x 2) 10)
        let expr = binary(
            binary(var("?x"), BinaryOp::Mul, lit_long(2)),
            BinaryOp::Add,
            lit_long(10),
        );
        let x = DataType::Long(5);
        let ctx_bindings = HashMap::from([("?x".to_var(), x.clone())]);
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Long(20)));
    }

    #[test]
    fn test_arithmetic_with_variable() {
        let expr = binary(var("?age"), BinaryOp::Add, lit_long(1));
        let age = DataType::Long(25);
        let ctx_bindings = HashMap::from([("?age".to_var(), age.clone())]);
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Long(26)));
    }

    // --- Date/Time extraction tests ---

    #[test]
    fn test_year_from_instant() {
        use chrono::TimeZone;
        let dt = chrono::Utc.with_ymd_and_hms(2024, 6, 15, 12, 0, 0).unwrap();
        let expr = unary(UnaryOp::Year, Expr::Literal(DataType::Instant(dt)));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Long(2024)));
    }

    #[test]
    fn test_month_from_instant() {
        use chrono::TimeZone;
        let dt = chrono::Utc.with_ymd_and_hms(2024, 6, 15, 12, 0, 0).unwrap();
        let expr = unary(UnaryOp::Month, Expr::Literal(DataType::Instant(dt)));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Long(6)));
    }

    #[test]
    fn test_year_non_instant() {
        let expr = unary(UnaryOp::Year, lit_long(42));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), None);
    }

    #[test]
    fn test_month_non_instant() {
        let expr = unary(UnaryOp::Month, lit_string("2024-06-15"));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), None);
    }

    // --- regexp_like tests ---

    fn regexp_like(pattern: &str, subject: Expr) -> Expr {
        Expr::RegexpLike(RegexpLikeExpr {
            pattern_src: pattern.to_string(),
            pattern: Arc::new(Regex::new(pattern).expect("valid regex")),
            subject: Box::new(subject),
        })
    }

    // --- IfExpr tests ---

    fn if_expr(condition: Expr, then_expr: Expr, else_expr: Expr) -> Expr {
        Expr::IfExpr(IfExpr {
            condition: Box::new(condition),
            then_expr: Box::new(then_expr),
            else_expr: Box::new(else_expr),
        })
    }

    #[test]
    fn test_regexp_like_match() {
        let expr = regexp_like("^.*BRASS$", var("?ptype"));
        let ptype = DataType::String("STANDARD POLISHED BRASS".to_string());
        let ctx_bindings = HashMap::from([("?ptype".to_var(), ptype.clone())]);
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Boolean(true)));
    }

    #[test]
    fn test_regexp_like_no_match() {
        let expr = regexp_like("^.*BRASS$", var("?ptype"));
        let ptype = DataType::String("STANDARD POLISHED COPPER".to_string());
        let ctx_bindings = HashMap::from([("?ptype".to_var(), ptype.clone())]);
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Boolean(false)));
    }

    #[test]
    fn test_regexp_like_substring() {
        let expr = regexp_like(".*green.*", var("?name"));
        let matches = DataType::String("forest green pine".to_string());
        let misses = DataType::String("aqua blue".to_string());
        let ctx_match_bindings = HashMap::from([("?name".to_var(), matches.clone())]);
        let ctx_match = EvalContext::new(&ctx_match_bindings);
        let ctx_miss_bindings = HashMap::from([("?name".to_var(), misses.clone())]);
        let ctx_miss = EvalContext::new(&ctx_miss_bindings);
        assert_eq!(eval(&expr, &ctx_match), Some(DataType::Boolean(true)));
        assert_eq!(eval(&expr, &ctx_miss), Some(DataType::Boolean(false)));
    }

    #[test]
    fn test_regexp_like_case_insensitive_inline_flag() {
        let expr = regexp_like("(?i)^.*green.*", var("?name"));
        let val = DataType::String("Forest GREEN pine".to_string());
        let ctx_bindings = HashMap::from([("?name".to_var(), val.clone())]);
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Boolean(true)));
    }

    #[test]
    fn test_regexp_like_non_string_subject_drops() {
        let expr = regexp_like(".*", var("?n"));
        let n = DataType::Long(42);
        let ctx_bindings = HashMap::from([("?n".to_var(), n.clone())]);
        let ctx = EvalContext::new(&ctx_bindings);
        // Non-String subject -> None (row dropped). See issue #222.
        assert_eq!(eval(&expr, &ctx), None);
    }

    #[test]
    fn test_regexp_like_unbound_subject() {
        let expr = regexp_like(".*", var("?missing"));
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), None);
    }

    #[test]
    fn test_regexp_like_expr_variables() {
        let expr = regexp_like("^foo", var("?name"));
        assert_eq!(expr_variables(&expr), vec!["?name".to_var()]);
    }

    #[test]
    fn test_regexp_like_evaluate_as_bool() {
        let expr = regexp_like("^foo", var("?name"));
        let hit = DataType::String("foobar".to_string());
        let miss = DataType::String("bar".to_string());
        let ctx_hit_bindings = HashMap::from([("?name".to_var(), hit.clone())]);
        let ctx_hit = EvalContext::new(&ctx_hit_bindings);
        let ctx_miss_bindings = HashMap::from([("?name".to_var(), miss.clone())]);
        let ctx_miss = EvalContext::new(&ctx_miss_bindings);
        assert!(evaluate_as_bool(&expr, &ctx_hit));
        assert!(!evaluate_as_bool(&expr, &ctx_miss));
    }

    #[test]
    fn test_regexp_like_partial_eq_by_pattern_src() {
        let a = regexp_like("^foo", var("?x"));
        let b = regexp_like("^foo", var("?x"));
        let c = regexp_like("^bar", var("?x"));
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_if_true_condition() {
        // (if (= ?x 1) 10 0)
        let expr = if_expr(
            binary(var("?x"), BinaryOp::Eq, lit_long(1)),
            lit_long(10),
            lit_long(0),
        );
        let x = DataType::Long(1);
        let ctx_bindings = HashMap::from([("?x".to_var(), x.clone())]);
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Long(10)));
    }

    #[test]
    fn test_if_false_condition() {
        // (if (= ?x 1) 10 0)
        let expr = if_expr(
            binary(var("?x"), BinaryOp::Eq, lit_long(1)),
            lit_long(10),
            lit_long(0),
        );
        let x = DataType::Long(99);
        let ctx_bindings = HashMap::from([("?x".to_var(), x.clone())]);
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Long(0)));
    }

    #[test]
    fn test_if_with_variable_branches() {
        // (if (> ?age 30) ?age 0)
        let expr = if_expr(
            binary(var("?age"), BinaryOp::Gt, lit_long(30)),
            var("?age"),
            lit_long(0),
        );
        let old = DataType::Long(40);
        let ctx_old_bindings = HashMap::from([("?age".to_var(), old.clone())]);
        let ctx_old = EvalContext::new(&ctx_old_bindings);
        assert_eq!(eval(&expr, &ctx_old), Some(DataType::Long(40)));

        let young = DataType::Long(20);
        let ctx_young_bindings = HashMap::from([("?age".to_var(), young.clone())]);
        let ctx_young = EvalContext::new(&ctx_young_bindings);
        assert_eq!(eval(&expr, &ctx_young), Some(DataType::Long(0)));
    }

    #[test]
    fn test_if_unbound_condition_variable() {
        // Unbound var in condition returns else.
        let expr = if_expr(
            binary(var("?missing"), BinaryOp::Eq, lit_long(1)),
            lit_long(10),
            lit_long(0),
        );
        let ctx_bindings = HashMap::new();
        let ctx = EvalContext::new(&ctx_bindings);
        assert_eq!(eval(&expr, &ctx), Some(DataType::Long(0)));
    }

    #[test]
    fn test_if_expr_variables() {
        // All four variables are collected.
        let expr = if_expr(
            binary(var("?a"), BinaryOp::Eq, var("?b")),
            var("?c"),
            var("?d"),
        );
        assert_eq!(
            expr_variables(&expr),
            vec!["?a".to_var(), "?b".to_var(), "?c".to_var(), "?d".to_var(),]
        );
    }
}
