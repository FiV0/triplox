use anyhow::{anyhow, Result};

use edn::parse::parse_query as edn_parse_query;
use edn::query as eq;
use edn::Keyword;

use crate::datalog::{
    FindElement, FindSpec, FnExpr, OrBranch, PatternElement, Query, TriplePattern, Variable,
    WhereClause,
};
use crate::expr::{BinaryExpr, BinaryOp, Expr};
use crate::ops::DataType;

pub fn parse_query(input: &str) -> Result<Query> {
    let parsed = edn_parse_query(input).map_err(|e| anyhow!("EDN parse error: {}", e))?;
    convert_parsed_query(parsed)
}

fn convert_parsed_query(parsed: eq::ParsedQuery) -> Result<Query> {
    let find = convert_find_spec(parsed.find_spec)?;
    let where_clauses = parsed
        .where_clauses
        .into_iter()
        .map(convert_where_clause)
        .collect::<Result<Vec<_>>>()?;

    Ok(Query {
        find,
        where_clauses,
    })
}

fn convert_find_spec(spec: eq::FindSpec) -> Result<FindSpec> {
    match spec {
        eq::FindSpec::FindRel(elements) => {
            let converted = elements
                .into_iter()
                .map(convert_element)
                .collect::<Result<Vec<_>>>()?;
            Ok(FindSpec::FindRel(converted))
        }
        eq::FindSpec::FindColl(_)
        | eq::FindSpec::FindTuple(_)
        | eq::FindSpec::FindScalar(_) => Err(anyhow!(
            "only :find rel (e.g. [:find ?x ?y]) is currently supported"
        )),
    }
}

fn convert_element(elem: eq::Element) -> Result<FindElement> {
    match elem {
        eq::Element::Variable(v) => Ok(FindElement::Variable(var_to_string(&v))),
        eq::Element::Aggregate(agg) => {
            let func_name = agg.func.0.to_string();
            let arg_str = agg
                .args
                .first()
                .map(|a| format!("{}", a))
                .unwrap_or_default();
            Ok(FindElement::Aggregate(func_name, arg_str))
        }
        eq::Element::Corresponding(_) | eq::Element::Pull(_) => {
            Err(anyhow!("pull and corresponding elements are not yet supported"))
        }
    }
}

fn convert_where_clause(clause: eq::WhereClause) -> Result<WhereClause> {
    match clause {
        eq::WhereClause::Pattern(p) => {
            let entity = convert_non_value_place(p.entity)?;
            let attribute = convert_non_value_place(p.attribute)?;
            let value = convert_value_place(p.value)?;
            Ok(WhereClause::Triple(TriplePattern {
                entity,
                attribute,
                value,
            }))
        }
        eq::WhereClause::NotJoin(nj) => match nj.unify_vars {
            eq::UnifyVars::Implicit => {
                let clauses = nj
                    .clauses
                    .into_iter()
                    .map(convert_where_clause)
                    .collect::<Result<Vec<_>>>()?;
                Ok(WhereClause::Not(clauses))
            }
            eq::UnifyVars::Explicit(vars) => {
                let var_names = vars.into_iter().map(|v| var_to_string(&v)).collect();
                let clauses = nj
                    .clauses
                    .into_iter()
                    .map(convert_where_clause)
                    .collect::<Result<Vec<_>>>()?;
                Ok(WhereClause::NotJoin(var_names, clauses))
            }
        },
        eq::WhereClause::OrJoin(oj) => {
            let (clauses, unify_vars, _mentioned) = oj.dismember();
            match unify_vars {
                eq::UnifyVars::Implicit => {
                    let branches = clauses
                        .into_iter()
                        .map(convert_or_where_clause)
                        .collect::<Result<Vec<_>>>()?;
                    Ok(WhereClause::Or(branches))
                }
                eq::UnifyVars::Explicit(vars) => {
                    let var_names = vars.into_iter().map(|v| var_to_string(&v)).collect();
                    let branches = clauses
                        .into_iter()
                        .map(convert_or_where_clause)
                        .collect::<Result<Vec<_>>>()?;
                    Ok(WhereClause::OrJoin(var_names, branches))
                }
            }
        }
        eq::WhereClause::Pred(pred) => {
            let expr = convert_predicate(&pred)?;
            Ok(WhereClause::Predicate(expr))
        }
        eq::WhereClause::WhereFn(wf) => {
            let fn_expr = convert_where_fn(&wf)?;
            Ok(WhereClause::FnExpr(fn_expr))
        }
        eq::WhereClause::RuleExpr => Err(anyhow!("rule expressions are not yet supported")),
        eq::WhereClause::TypeAnnotation(_) => {
            Err(anyhow!("type annotations are not yet supported"))
        }
    }
}

fn convert_or_where_clause(clause: eq::OrWhereClause) -> Result<OrBranch> {
    match clause {
        eq::OrWhereClause::Clause(c) => Ok(OrBranch::Clause(convert_where_clause(c)?)),
        eq::OrWhereClause::And(cs) => {
            let converted = cs
                .into_iter()
                .map(convert_where_clause)
                .collect::<Result<Vec<_>>>()?;
            Ok(OrBranch::And(converted))
        }
    }
}

fn convert_fn_arg(arg: &eq::FnArg) -> Result<Expr> {
    match arg {
        eq::FnArg::Variable(v) => Ok(Expr::Variable(var_to_string(v))),
        eq::FnArg::EntidOrInteger(i) => Ok(Expr::Literal(DataType::Long(*i))),
        eq::FnArg::Constant(c) => match c {
            eq::NonIntegerConstant::Text(t) => Ok(Expr::Literal(DataType::String((**t).clone()))),
            eq::NonIntegerConstant::Float(f) => {
                Ok(Expr::Literal(DataType::Double(f.into_inner())))
            }
            eq::NonIntegerConstant::Boolean(b) => Ok(Expr::Literal(DataType::Boolean(*b))),
            other => Err(anyhow!("unsupported constant type in predicate/fn: {:?}", other)),
        },
        eq::FnArg::IdentOrKeyword(kw) => {
            Ok(Expr::Literal(DataType::Keyword((*kw).clone())))
        }
        other => Err(anyhow!("unsupported fn arg type: {:?}", other)),
    }
}

fn convert_binary_op(name: &str) -> Result<BinaryOp> {
    match name {
        "<" => Ok(BinaryOp::Lt),
        "<=" => Ok(BinaryOp::LtEq),
        ">" => Ok(BinaryOp::Gt),
        ">=" => Ok(BinaryOp::GtEq),
        "=" => Ok(BinaryOp::Eq),
        "not=" | "!=" => Ok(BinaryOp::NotEq),
        "+" => Ok(BinaryOp::Add),
        "-" => Ok(BinaryOp::Sub),
        "*" => Ok(BinaryOp::Mul),
        "/" | "quot" => Ok(BinaryOp::Div),
        "mod" => Ok(BinaryOp::Mod),
        "concat" => Ok(BinaryOp::Concat),
        _ => Err(anyhow!("unknown operator: {}", name)),
    }
}

fn convert_predicate(pred: &eq::Predicate) -> Result<Expr> {
    let op_name = &pred.operator.0;
    let op = convert_binary_op(op_name)?;
    if pred.args.len() != 2 {
        return Err(anyhow!(
            "predicate '{}' requires exactly 2 arguments, got {}",
            op_name,
            pred.args.len()
        ));
    }
    let left = convert_fn_arg(&pred.args[0])?;
    let right = convert_fn_arg(&pred.args[1])?;
    Ok(Expr::BinaryExpr(BinaryExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }))
}

fn convert_where_fn(wf: &eq::WhereFn) -> Result<FnExpr> {
    let op_name = &wf.operator.0;
    let op = convert_binary_op(op_name)?;
    if wf.args.len() != 2 {
        return Err(anyhow!(
            "function '{}' requires exactly 2 arguments, got {}",
            op_name,
            wf.args.len()
        ));
    }
    let left = convert_fn_arg(&wf.args[0])?;
    let right = convert_fn_arg(&wf.args[1])?;
    let expr = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    });
    let output = match &wf.binding {
        eq::Binding::BindScalar(v) => var_to_string(v),
        other => return Err(anyhow!("only scalar binding is supported for where-fn, got {:?}", other)),
    };
    Ok(FnExpr { expr, output })
}

fn convert_non_value_place(place: eq::PatternNonValuePlace) -> Result<PatternElement> {
    match place {
        eq::PatternNonValuePlace::Placeholder => Ok(PatternElement::Wildcard),
        eq::PatternNonValuePlace::Variable(v) => Ok(PatternElement::Variable(var_to_string(&v))),
        eq::PatternNonValuePlace::Entid(i) => Ok(PatternElement::Constant(DataType::Long(i))),
        eq::PatternNonValuePlace::Ident(kw) => {
            Ok(PatternElement::Constant(DataType::Keyword((*kw).clone())))
        }
    }
}

fn convert_value_place(place: eq::PatternValuePlace) -> Result<PatternElement> {
    match place {
        eq::PatternValuePlace::Placeholder => Ok(PatternElement::Wildcard),
        eq::PatternValuePlace::Variable(v) => Ok(PatternElement::Variable(var_to_string(&v))),
        eq::PatternValuePlace::EntidOrInteger(i) => {
            Ok(PatternElement::Constant(DataType::Long(i)))
        }
        eq::PatternValuePlace::IdentOrKeyword(kw) => {
            Ok(PatternElement::Constant(DataType::Keyword((*kw).clone())))
        }
        eq::PatternValuePlace::Constant(c) => convert_non_integer_constant(c),
    }
}

fn convert_non_integer_constant(c: eq::NonIntegerConstant) -> Result<PatternElement> {
    match c {
        eq::NonIntegerConstant::Boolean(b) => Ok(PatternElement::Constant(DataType::Boolean(b))),
        eq::NonIntegerConstant::Float(f) => {
            Ok(PatternElement::Constant(DataType::Double(f.into_inner())))
        }
        eq::NonIntegerConstant::Text(t) => {
            Ok(PatternElement::Constant(DataType::String((*t).clone())))
        }
        eq::NonIntegerConstant::Instant(dt) => {
            Ok(PatternElement::Constant(DataType::Instant(dt)))
        }
        eq::NonIntegerConstant::Uuid(u) => Ok(PatternElement::Constant(DataType::Uuid(u))),
        eq::NonIntegerConstant::BigInteger(bi) => {
            let val: i128 = bi
                .try_into()
                .map_err(|_| anyhow!("BigInteger too large for i128"))?;
            Ok(PatternElement::Constant(DataType::BigInt(val)))
        }
    }
}

fn var_to_string(v: &eq::Variable) -> Variable {
    v.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse both vector and map syntax, assert they produce identical results.
    fn parse_both(vec_syntax: &str, map_syntax: &str) -> Query {
        let q1 = parse_query(vec_syntax).unwrap();
        let q2 = parse_query(map_syntax).unwrap();
        assert_eq!(q1, q2, "vector and map syntax should produce identical queries");
        q1
    }

    /// Assert both vector and map syntax fail to parse.
    fn assert_both_err(vec_syntax: &str, map_syntax: &str) -> (anyhow::Error, anyhow::Error) {
        let e1 = parse_query(vec_syntax).unwrap_err();
        let e2 = parse_query(map_syntax).unwrap_err();
        (e1, e2)
    }

    #[test]
    fn parse_simple_find_rel() {
        let q = parse_both(
            "[:find ?x ?y :where [?x :foo/bar ?y]]",
            "{:find [?x ?y] :where [[?x :foo/bar ?y]]}",
        );
        assert_eq!(
            q.find,
            FindSpec::FindRel(vec![
                FindElement::Variable("?x".into()),
                FindElement::Variable("?y".into()),
            ])
        );
        assert_eq!(q.where_clauses.len(), 1);
        assert_eq!(
            q.where_clauses[0],
            WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?x".into()),
                attribute: PatternElement::Constant(DataType::Keyword(Keyword::namespaced("foo", "bar"))),
                value: PatternElement::Variable("?y".into()),
            })
        );
    }

    #[test]
    fn parse_wildcard_pattern() {
        let q = parse_both(
            "[:find ?x :where [?x _ 42]]",
            "{:find [?x] :where [[?x _ 42]]}",
        );
        assert_eq!(
            q.where_clauses[0],
            WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?x".into()),
                attribute: PatternElement::Wildcard,
                value: PatternElement::Constant(DataType::Long(42)),
            })
        );
    }

    #[test]
    fn parse_boolean_constant() {
        let q = parse_both(
            "[:find ?x :where [?x :active true]]",
            "{:find [?x] :where [[?x :active true]]}",
        );
        assert_eq!(
            q.where_clauses[0],
            WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?x".into()),
                attribute: PatternElement::Constant(DataType::Keyword(Keyword::plain("active"))),
                value: PatternElement::Constant(DataType::Boolean(true)),
            })
        );
    }

    #[test]
    fn parse_string_constant() {
        let q = parse_both(
            r#"[:find ?x :where [?x :name "Alice"]]"#,
            r#"{:find [?x] :where [[?x :name "Alice"]]}"#,
        );
        assert_eq!(
            q.where_clauses[0],
            WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?x".into()),
                attribute: PatternElement::Constant(DataType::Keyword(Keyword::plain("name"))),
                value: PatternElement::Constant(DataType::String("Alice".into())),
            })
        );
    }

    #[test]
    fn parse_uuid_constant() {
        let q = parse_both(
            r#"[:find ?x :where [?x :id #uuid "4cb3f828-752d-497a-90c9-b1fd516d5644"]]"#,
            r#"{:find [?x] :where [[?x :id #uuid "4cb3f828-752d-497a-90c9-b1fd516d5644"]]}"#,
        );
        let expected_uuid =
            uuid::Uuid::parse_str("4cb3f828-752d-497a-90c9-b1fd516d5644").unwrap();
        assert_eq!(
            q.where_clauses[0],
            WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?x".into()),
                attribute: PatternElement::Constant(DataType::Keyword(Keyword::plain("id"))),
                value: PatternElement::Constant(DataType::Uuid(expected_uuid)),
            })
        );
    }

    #[test]
    fn parse_multiple_where_clauses() {
        let q = parse_both(
            "[:find ?x ?y :where [?x :foo/bar ?y] [?y :baz/qux ?x]]",
            "{:find [?x ?y] :where [[?x :foo/bar ?y] [?y :baz/qux ?x]]}",
        );
        assert_eq!(q.where_clauses.len(), 2);
    }

    #[test]
    fn parse_or_clause() {
        let q = parse_both(
            "[:find ?x :where (or [?x _ 10] [?x _ 15])]",
            "{:find [?x] :where [(or [?x _ 10] [?x _ 15])]}",
        );
        assert_eq!(q.where_clauses.len(), 1);
        match &q.where_clauses[0] {
            WhereClause::Or(branches) => {
                assert_eq!(branches.len(), 2);
            }
            other => panic!("expected Or, got {:?}", other),
        }
    }

    #[test]
    fn parse_or_join_clause() {
        let q = parse_both(
            "[:find ?x :where (or-join [?x] [?x _ 10] [?x _ 15])]",
            "{:find [?x] :where [(or-join [?x] [?x _ 10] [?x _ 15])]}",
        );
        match &q.where_clauses[0] {
            WhereClause::OrJoin(vars, branches) => {
                assert_eq!(vars, &vec!["?x".to_string()]);
                assert_eq!(branches.len(), 2);
            }
            other => panic!("expected OrJoin, got {:?}", other),
        }
    }

    #[test]
    fn parse_not_clause() {
        let q = parse_both(
            "[:find ?x :where [?x :foo/bar _] (not [?x :foo/baz _])]",
            "{:find [?x] :where [[?x :foo/bar _] (not [?x :foo/baz _])]}",
        );
        assert_eq!(q.where_clauses.len(), 2);
        match &q.where_clauses[1] {
            WhereClause::Not(clauses) => {
                assert_eq!(clauses.len(), 1);
            }
            other => panic!("expected Not, got {:?}", other),
        }
    }

    #[test]
    fn parse_find_coll_unsupported() {
        let (e1, e2) = assert_both_err(
            "[:find [?x ...] :where [?x _ 10]]",
            "{:find [[?x ...]] :where [[?x _ 10]]}",
        );
        assert!(e1.to_string().contains("only :find rel"));
        assert!(e2.to_string().contains("only :find rel"));
    }

    #[test]
    fn parse_find_scalar_unsupported() {
        assert_both_err(
            "[:find ?x . :where [?x _ 10]]",
            "{:find [?x .] :where [[?x _ 10]]}",
        );
    }

    #[test]
    fn parse_predicate() {
        let q = parse_both(
            "[:find ?x :where [?x _ ?y] [(< ?y 10)]]",
            "{:find [?x] :where [[?x _ ?y] [(< ?y 10)]]}",
        );
        assert_eq!(q.where_clauses.len(), 2);
        assert_eq!(
            q.where_clauses[1],
            WhereClause::Predicate(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Variable("?y".into())),
                op: BinaryOp::Lt,
                right: Box::new(Expr::Literal(DataType::Long(10))),
            }))
        );
    }

    #[test]
    fn parse_fn() {
        let q = parse_both(
            "[:find ?x ?result :where [?x _ ?y] [(quot ?y 2) ?result]]",
            "{:find [?x ?result] :where [[?x _ ?y] [(quot ?y 2) ?result]]}",
        );
        assert_eq!(q.where_clauses.len(), 2);
        assert_eq!(
            q.where_clauses[1],
            WhereClause::FnExpr(FnExpr {
                expr: Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(Expr::Variable("?y".into())),
                    op: BinaryOp::Div,
                    right: Box::new(Expr::Literal(DataType::Long(2))),
                }),
                output: "?result".into(),
            })
        );
    }

    #[test]
    fn parse_invalid_edn() {
        let result = parse_query("not valid edn");
        assert!(result.is_err());
    }

    #[test]
    fn parse_entid_in_entity_position() {
        let q = parse_both(
            "[:find ?v :where [42 :foo/bar ?v]]",
            "{:find [?v] :where [[42 :foo/bar ?v]]}",
        );
        assert_eq!(
            q.where_clauses[0],
            WhereClause::Triple(TriplePattern {
                entity: PatternElement::Constant(DataType::Long(42)),
                attribute: PatternElement::Constant(DataType::Keyword(Keyword::namespaced("foo", "bar"))),
                value: PatternElement::Variable("?v".into()),
            })
        );
    }

    #[test]
    fn parse_negative_integer_in_value() {
        let q = parse_both(
            "[:find ?x :where [?x :foo/bar -5]]",
            "{:find [?x] :where [[?x :foo/bar -5]]}",
        );
        assert_eq!(
            q.where_clauses[0],
            WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?x".into()),
                attribute: PatternElement::Constant(DataType::Keyword(Keyword::namespaced("foo", "bar"))),
                value: PatternElement::Constant(DataType::Long(-5)),
            })
        );
    }
}
