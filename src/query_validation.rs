use std::collections::{HashMap, HashSet};

use anyhow::Error;
use edn::query::{
    Binding, Element, FindSpec, Limit, OrJoin, OrWhereClause, ParsedQuery, Pattern,
    PatternNonValuePlace, PatternValuePlace, Variable, WhereClause,
};

use crate::expr::expr_variables;
use crate::ops::{DataType, QueryArg};
use crate::query::{
    build_var_index, collect_variables_from_or_branch, convert_predicate, convert_where_fn,
    pattern_variables, query_variable_order, resolve_order_columns,
};

/// Validate that all variables in NOT clauses are bound by positive clauses.
fn validate_not_clauses(
    where_clauses: &[WhereClause],
    var_index: &HashMap<&Variable, usize>,
) -> Result<(), Error> {
    for clause in where_clauses {
        if let WhereClause::NotJoin(nj) = clause {
            for var in not_clause_variables(&nj.clauses) {
                if !var_index.contains_key(&var) {
                    return Err(anyhow::anyhow!(
                        "Variable {} in NOT clause is not bound by positive clauses",
                        var
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Collect all variables referenced by inner clauses of a NOT.
fn not_clause_variables(inner_clauses: &[WhereClause]) -> Vec<Variable> {
    let mut vars = Vec::new();
    let mut seen = HashSet::new();
    for clause in inner_clauses {
        if let WhereClause::Pattern(pattern) = clause {
            for var in pattern_variables(pattern) {
                if seen.insert(var.clone()) {
                    vars.push(var);
                }
            }
        }
    }
    vars
}

fn validate_pattern_variables(where_clauses: &[WhereClause]) -> Result<(), Error> {
    for clause in where_clauses {
        validate_pattern_variables_in_clause(clause)?;
    }
    Ok(())
}

fn validate_pattern_variables_in_or_branch(branch: &OrWhereClause) -> Result<(), Error> {
    match branch {
        OrWhereClause::Clause(clause) => validate_pattern_variables_in_clause(clause),
        OrWhereClause::And(children) => validate_pattern_variables(children),
    }
}

fn validate_pattern_variables_in_clause(clause: &WhereClause) -> Result<(), Error> {
    match clause {
        WhereClause::Pattern(pattern) => validate_single_pattern_variables(pattern),
        WhereClause::OrJoin(oj) => {
            for branch in &oj.clauses {
                validate_pattern_variables_in_or_branch(branch)?;
            }
            Ok(())
        }
        WhereClause::NotJoin(nj) => validate_pattern_variables(&nj.clauses),
        _ => Ok(()),
    }
}

fn validate_single_pattern_variables(pattern: &Pattern) -> Result<(), Error> {
    if matches!(&pattern.entity, PatternNonValuePlace::Placeholder) {
        return Err(anyhow::anyhow!(
            "Placeholders in entity position are not supported"
        ));
    }
    if matches!(&pattern.value, PatternValuePlace::Placeholder) {
        return Err(anyhow::anyhow!(
            "Placeholders in value position are not supported"
        ));
    }

    let mut seen = HashSet::new();
    for var in pattern_variables(pattern) {
        if !seen.insert(var.clone()) {
            return Err(anyhow::anyhow!(
                "Repeated variable {} in a single pattern is not supported",
                var
            ));
        }
    }
    Ok(())
}

/// Validate that all variables in Predicate clauses are bound by positive clauses,
/// and that each predicate has at least one variable.
fn validate_predicate_clauses(
    where_clauses: &[WhereClause],
    var_index: &HashMap<&Variable, usize>,
) -> Result<(), Error> {
    for clause in where_clauses {
        if let WhereClause::Pred(pred) = clause {
            let expr = convert_predicate(pred)?;
            let vars = expr_variables(&expr);
            if vars.is_empty() {
                return Err(anyhow::anyhow!(
                    "Predicate expression must reference at least one variable"
                ));
            }
            for var in &vars {
                if !var_index.contains_key(var) {
                    return Err(anyhow::anyhow!(
                        "Predicate variable {} is not bound by positive clauses",
                        var
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Validate that all WhereFn input variables precede the output variable in the join order.
fn validate_fn_clauses(
    where_clauses: &[WhereClause],
    var_index: &HashMap<&Variable, usize>,
) -> Result<(), Error> {
    for clause in where_clauses {
        if let WhereClause::WhereFn(wf) = clause {
            let fn_expr = convert_where_fn(wf)?;
            let output_pos = var_index.get(&fn_expr.output).ok_or_else(|| {
                anyhow::anyhow!(
                    "Function output variable {} not in join order",
                    fn_expr.output
                )
            })?;
            for var in fn_expr.input_variables() {
                let input_pos = var_index.get(&var).ok_or_else(|| {
                    anyhow::anyhow!("Function input variable {} not in join order", var)
                })?;
                if input_pos >= output_pos {
                    return Err(anyhow::anyhow!(
                        "Function input variable {} must precede output variable {} in join order",
                        var,
                        fn_expr.output
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Validate that all aggregate variables are bound by where clauses.
fn validate_aggregate_clauses(
    find: &FindSpec,
    var_index: &HashMap<&Variable, usize>,
) -> Result<(), Error> {
    let elements = match find {
        FindSpec::FindRel(elements) => elements,
        _ => return Ok(()),
    };
    for elem in elements {
        if let Element::Aggregate(agg) = elem {
            let func_name = agg.func.0.to_string();
            for arg in &agg.args {
                if matches!(arg, edn::query::FnArg::SExpr(..)) {
                    return Err(anyhow::anyhow!(
                        "Nested expressions are not supported as arguments to aggregate '{}'",
                        func_name
                    ));
                }
            }
            if agg.args.len() == 1 {
                if let edn::query::FnArg::Variable(ref var) = agg.args[0] {
                    if !var_index.contains_key(var) {
                        return Err(anyhow::anyhow!(
                            "Aggregate variable {} in ({} {}) is not bound by where clauses",
                            var,
                            func_name,
                            var
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

fn validate_or_clauses(clauses: &[WhereClause]) -> Result<(), Error> {
    for clause in clauses {
        validate_or_clause(clause)?;
    }
    Ok(())
}

fn validate_or_clause(clause: &WhereClause) -> Result<(), Error> {
    match clause {
        WhereClause::OrJoin(oj) => validate_or_join(oj),
        WhereClause::NotJoin(nj) => validate_or_clauses(&nj.clauses),
        _ => Ok(()),
    }
}

fn validate_or_join(oj: &OrJoin) -> Result<(), Error> {
    validate_or_branch_variables(&oj.clauses)?;
    for branch in &oj.clauses {
        validate_or_branch(branch)?;
    }
    Ok(())
}

fn validate_or_branch(branch: &OrWhereClause) -> Result<(), Error> {
    match branch {
        OrWhereClause::Clause(clause) => validate_or_clause(clause),
        OrWhereClause::And(children) => validate_or_clauses(children),
    }
}

/// Validate that all OR branches have the same free variables.
fn validate_or_branch_variables(branches: &[OrWhereClause]) -> Result<(), Error> {
    if branches.is_empty() {
        return Err(anyhow::anyhow!("OR clause must have at least one branch"));
    }

    let first_vars: HashSet<Variable> = collect_variables_from_or_branch(&branches[0])
        .into_iter()
        .collect();

    for (i, branch) in branches.iter().enumerate().skip(1) {
        let branch_vars: HashSet<Variable> = collect_variables_from_or_branch(branch)
            .into_iter()
            .collect();
        if branch_vars != first_vars {
            return Err(anyhow::anyhow!(
                "OR branch {} has different free variables {:?} than branch 0 {:?}",
                i,
                branch_vars,
                first_vars
            ));
        }
    }
    Ok(())
}

/// Validate that :in bindings match the provided arguments in count and type.
fn validate_in_bindings(in_bindings: &[Binding], args: &[QueryArg]) -> Result<(), Error> {
    if in_bindings.len() != args.len() {
        return Err(anyhow::anyhow!(
            ":in clause declares {} binding(s) but {} argument(s) provided",
            in_bindings.len(),
            args.len()
        ));
    }

    for (i, (binding, arg)) in in_bindings.iter().zip(args.iter()).enumerate() {
        match (binding, arg) {
            (Binding::BindScalar(_), QueryArg::Scalar(_)) => {}
            (Binding::BindColl(_), QueryArg::Collection(_)) => {}
            (Binding::BindScalar(_), _) => {
                return Err(anyhow::anyhow!(
                    "Scalar binding {} expects a Scalar argument, but argument {} is {:?}",
                    binding,
                    i,
                    arg
                ));
            }
            (Binding::BindColl(_), _) => {
                return Err(anyhow::anyhow!(
                    "Collection binding {} expects a Collection argument, but argument {} is {:?}",
                    binding,
                    i,
                    arg
                ));
            }
            (Binding::BindTuple(_), _) | (Binding::BindRel(_), _) => {
                return Err(anyhow::anyhow!(
                    "Tuple and relation bindings are not yet supported (binding {})",
                    binding
                ));
            }
        }
    }

    Ok(())
}

/// Validate a query before execution.
// TODO: Move query validation into the edn parsing crate so that invalid
// queries are rejected at parse time rather than at execution time.
pub(crate) fn validate_query(query: &ParsedQuery, args: &[QueryArg]) -> Result<(), Error> {
    validate_in_bindings(&query.in_bindings, args)?;

    let join_order = query_variable_order(&query.in_bindings, &query.where_clauses);
    if join_order.is_empty() {
        return Err(anyhow::anyhow!("Query has no variables"));
    }
    let var_index = build_var_index(&join_order);
    validate_or_clauses(&query.where_clauses)?;
    validate_pattern_variables(&query.where_clauses)?;
    validate_not_clauses(&query.where_clauses, &var_index)?;
    validate_predicate_clauses(&query.where_clauses, &var_index)?;
    validate_fn_clauses(&query.where_clauses, &var_index)?;
    validate_aggregate_clauses(&query.find_spec, &var_index)?;

    // Validate ORDER BY variables are in the find spec.
    if let Some(orders) = &query.order {
        resolve_order_columns(orders, &query.find_spec)?;
    }

    // Variable limits must be bound in :in as a scalar non-negative Long.
    if let Limit::Variable(v) = &query.limit {
        let idx = query
            .in_bindings
            .iter()
            .position(|b| matches!(b, Binding::BindScalar(bv) if bv == v))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Variable limit {} is not bound as a scalar in :in clause",
                    v
                )
            })?;
        match &args[idx] {
            QueryArg::Scalar(DataType::Long(n)) => {
                if *n < 0 {
                    return Err(anyhow::anyhow!("Limit must be non-negative, got {}", n));
                }
            }
            other => {
                return Err(anyhow::anyhow!(
                    "Variable limit {} must be bound to a Long, got {:?}",
                    v,
                    other
                ));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse an EDN query string into a ParsedQuery.
    fn parse_query(input: &str) -> ParsedQuery {
        edn::parse::parse_query(input).expect("failed to parse query")
    }

    #[test]
    fn test_validate_predicate_unbound_variable() {
        let parsed = parse_query(r#"[:find ?e :where [?e :name "Alice"] [(< ?unbound 30)]]"#);
        let result = validate_query(&parsed, &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("?unbound"));
    }

    #[test]
    fn test_validate_predicate_no_variables() {
        let parsed = parse_query(r#"[:find ?e :where [?e :name "Alice"] [(< 1 2)]]"#);
        let result = validate_query(&parsed, &[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("at least one variable"));
    }

    #[test]
    fn test_validate_rejects_repeated_variable_in_single_pattern() {
        let parsed = parse_query("{:find [?x] :where [[?x :g/to ?x]]}");
        let err = validate_query(&parsed, &[]).unwrap_err();
        assert!(
            err.to_string()
                .contains("Repeated variable ?x in a single pattern is not supported"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_validate_rejects_nested_or_mismatched_branch_variables() {
        let parsed = parse_query(
            r#"[:find ?e :where (or [?e :name "A"] (or [?e :name "B"] [?v :name "C"]))]"#,
        );
        let err = validate_query(&parsed, &[]).unwrap_err();
        assert!(
            err.to_string()
                .contains("OR branch 1 has different free variables"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_validate_rejects_or_mismatched_branch_variables_inside_not() {
        let parsed = parse_query(
            r#"[:find ?e :where [?e :name "A"] (not (or [?e :name "B"] [?v :name "C"]))]"#,
        );
        let err = validate_query(&parsed, &[]).unwrap_err();
        assert!(
            err.to_string()
                .contains("OR branch 1 has different free variables"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_validate_fn_unbound_input() {
        let parsed =
            parse_query(r#"[:find ?e :where [?e :name "Alice"] [(+ ?unbound 1) ?result]]"#);
        let result = validate_query(&parsed, &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("?unbound"));
    }

    #[test]
    fn test_validate_fn_input_must_precede_output() {
        // Output ?e is at level 0 (from Triple), input ?age is at level 1 — input doesn't precede output
        let parsed = parse_query("[:find ?e :where [?e :age ?age] [(+ ?age 1) ?e]]");
        let result = validate_query(&parsed, &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must precede"));
    }

    #[test]
    fn test_fn_expr_before_input_triple_is_valid() {
        // FnExpr appears before the Triple that binds its input — should be valid
        // because query_variable_order reorders FnExpr clauses after Triples
        let parsed =
            parse_query("[:find ?e ?next_age :where [(+ ?age 1) ?next_age] [?e :age ?age]]");
        let result = validate_query(&parsed, &[]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_order_var_not_in_find() {
        let parsed = parse_query("[:find ?e :where [?e :name ?name] :order [?name :asc]]");
        let err = validate_query(&parsed, &[]).unwrap_err();
        assert!(
            err.to_string().contains("ORDER BY variable"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_validate_limit_variable_requires_in_binding() {
        // Variable limit without :in binding should fail
        let parsed = parse_query("[:find ?e :where [?e :name ?name] :limit ?limit]");
        let err = validate_query(&parsed, &[]).unwrap_err();
        assert!(
            err.to_string()
                .contains("not bound as a scalar in :in clause"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_validate_limit_variable_with_in_binding() {
        let parsed = parse_query("[:find ?e :in ?limit :where [?e :name ?name] :limit ?limit]");
        // Providing a scalar Long arg should pass validation
        assert!(validate_query(&parsed, &[QueryArg::Scalar(DataType::Long(10))]).is_ok());
    }

    #[test]
    fn test_validate_limit_variable_rejects_collection_binding() {
        let parsed =
            parse_query("[:find ?e :in [?limit ...] :where [?e :name ?name] :limit ?limit]");
        let err =
            validate_query(&parsed, &[QueryArg::Collection(vec![DataType::Long(10)])]).unwrap_err();
        assert!(
            err.to_string()
                .contains("not bound as a scalar in :in clause"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_validate_rejects_sexpr_in_aggregate() {
        let parsed = parse_query("[:find ?e (count (+ ?age 1)) :where [?e :age ?age]]");
        let err = validate_query(&parsed, &[]).unwrap_err();
        assert!(
            err.to_string()
                .contains("Nested expressions are not supported"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_validate_in_arg_count_mismatch() {
        let parsed = parse_query("[:find ?e :in ?x :where [?e :name ?x]]");
        let err = validate_query(&parsed, &[]).unwrap_err();
        assert!(
            err.to_string().contains("1 binding(s) but 0 argument(s)"),
            "unexpected error: {}",
            err
        );
    }
}
