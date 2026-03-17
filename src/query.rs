#![allow(unused)]

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Error;
use tokio::runtime::Handle;

use crate::algo::generic_join::{GenericJoin, PrefixExtender, ResultTuple};
use crate::clock::Instant;
use crate::aggregate::{make_accumulator, Accumulator};
use crate::datalog::{
    AggregateFunc, FindElement, FindSpec, FnExpr, OrBranch, PatternClause, PatternElement, Query,
    TriplePattern, Variable, WhereClause,
};
use crate::expr::{expr_variables, Expr};
use crate::index::IndexType;
use crate::iterator::generic_and_prefix_extender::GenericAndPrefixExtender;
use crate::iterator::generic_fn_prefix_extender::GenericFnPrefixExtender;
use crate::iterator::generic_not_prefix_extender::GenericNotPrefixExtender;
use crate::iterator::generic_or_prefix_extender::GenericOrPrefixExtender;
use crate::iterator::generic_predicate_prefix_extender::GenericPredicatePrefixExtender;
use crate::iterator::generic_prefix_extender::GenericPrefixExtender;
use crate::codec::{Encode, Decode};
use crate::ops::DataType;

/// Each inner Vec is a projected row of decoded DataType values.
pub type QueryResult = Vec<Vec<DataType>>;

/// Extract variables from an OrBranch.
fn collect_variables_from_or_branch(branch: &OrBranch) -> Vec<Variable> {
    match branch {
        OrBranch::Clause(clause) => collect_variables_from_clause(clause),
        OrBranch::And(children) => children
            .iter()
            .flat_map(collect_variables_from_clause)
            .collect(),
    }
}

/// Recursively extract variables from a single WhereClause.
/// Only positive (Triple/Or/FnExpr) clauses contribute variables. NOT and Predicate
/// clauses do not introduce new variables.
fn collect_variables_from_clause(clause: &WhereClause) -> Vec<Variable> {
    match clause {
        WhereClause::Triple(triple) => PatternClause::from(triple.clone()).variables(),
        WhereClause::Or(branches) => {
            // All branches must have the same free variables (validated in execute_query).
            // Extract from the first branch only.
            branches
                .first()
                .map(collect_variables_from_or_branch)
                .unwrap_or_default()
        }
        WhereClause::FnExpr(fn_expr) => vec![fn_expr.output.clone()],
        WhereClause::Not(_) | WhereClause::Predicate(_) => vec![],
        _ => vec![],
    }
}

/// Extract variables from where clauses in first-appearance order (E-A-V within each pattern).
/// Only positive (Triple/Or/FnExpr) clauses contribute to the variable order. NOT and Predicate
/// clauses do not introduce new variables.
///
/// FnExpr clauses are moved to the back so their output variables appear after all
/// Triple/Or variables in the join order.
/// TODO: refine to only require the clauses that bind a FnExpr's input variables
/// to appear before that FnExpr (topological sort).
pub fn query_variable_order(where_clauses: &[WhereClause]) -> Vec<Variable> {
    let mut seen = HashSet::new();
    let mut order = Vec::new();

    // Non-FnExpr clauses first, then FnExpr clauses, preserving relative order within each group.
    let reordered: Vec<&WhereClause> = where_clauses
        .iter()
        .filter(|c| !matches!(c, WhereClause::FnExpr(_)))
        .chain(where_clauses.iter().filter(|c| matches!(c, WhereClause::FnExpr(_))))
        .collect();

    for clause in reordered {
        for var in collect_variables_from_clause(clause) {
            if seen.insert(var.clone()) {
                order.push(var);
            }
        }
    }
    order
}

/// Build a variable → position index from the join order for O(1) lookups.
fn build_var_index(join_order: &[Variable]) -> HashMap<&Variable, usize> {
    join_order.iter().enumerate().map(|(i, v)| (v, i)).collect()
}

/// Collect all variables referenced by inner clauses of a NOT.
fn not_clause_variables(inner_clauses: &[WhereClause]) -> Vec<Variable> {
    let mut vars = Vec::new();
    let mut seen = HashSet::new();
    for clause in inner_clauses {
        if let WhereClause::Triple(triple) = clause {
            let pattern = PatternClause::from(triple.clone());
            for var in pattern.variables() {
                if seen.insert(var.clone()) {
                    vars.push(var);
                }
            }
        }
    }
    vars
}

/// Validate that all variables in NOT clauses are bound by positive clauses.
fn validate_not_clauses(
    where_clauses: &[WhereClause],
    var_index: &HashMap<&Variable, usize>,
) -> Result<(), Error> {
    for clause in where_clauses {
        if let WhereClause::Not(inner) = clause {
            for var in not_clause_variables(inner) {
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

/// Validate that all variables in Predicate clauses are bound by positive clauses,
/// and that each predicate has at least one variable.
fn validate_predicate_clauses(
    where_clauses: &[WhereClause],
    var_index: &HashMap<&Variable, usize>,
) -> Result<(), Error> {
    for clause in where_clauses {
        if let WhereClause::Predicate(expr) = clause {
            let vars = expr_variables(expr);
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

/// Validate that all FnExpr input variables precede the output variable in the join order.
fn validate_fn_clauses(
    where_clauses: &[WhereClause],
    var_index: &HashMap<&Variable, usize>,
) -> Result<(), Error> {
    for clause in where_clauses {
        if let WhereClause::FnExpr(fn_expr) = clause {
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

/// Compile a predicate expression into a GenericPredicatePrefixExtender.
///
/// Determines which variable has the highest join level (the extension variable)
/// and which are already bound in the prefix.
fn compile_predicate(
    expr: &Expr,
    var_index: &HashMap<&Variable, usize>,
) -> Result<Box<dyn PrefixExtender>, Error> {
    let vars = expr_variables(expr);
    if vars.is_empty() {
        return Err(anyhow::anyhow!(
            "Predicate expression must reference at least one variable"
        ));
    }

    // Find each variable's join level
    let mut var_levels: Vec<(Variable, usize)> = vars
        .iter()
        .map(|v| {
            let level = var_index.get(v).copied().ok_or_else(|| {
                anyhow::anyhow!("Predicate variable {} not found in join order", v)
            })?;
            Ok((v.clone(), level))
        })
        .collect::<Result<_, Error>>()?;

    // The variable with the highest join level is the extension variable
    let (ext_idx, _) = var_levels
        .iter()
        .enumerate()
        .max_by_key(|(_, (_, level))| *level)
        .unwrap();

    let (extension_var, level) = var_levels.remove(ext_idx);
    let prefix_vars = var_levels;

    Ok(Box::new(GenericPredicatePrefixExtender::new(
        expr.clone(),
        prefix_vars,
        extension_var,
        level,
    )))
}

/// Compile a FnExpr into a GenericFnPrefixExtender.
fn compile_fn_expr(
    fn_expr: &FnExpr,
    var_index: &HashMap<&Variable, usize>,
) -> Result<Box<dyn PrefixExtender>, Error> {
    let input_vars = fn_expr.input_variables();

    let prefix_vars: Vec<(Variable, usize)> = input_vars
        .iter()
        .map(|v| {
            let level = var_index.get(v).copied().ok_or_else(|| {
                anyhow::anyhow!("FnExpr input variable {} not in join order", v)
            })?;
            Ok((v.clone(), level))
        })
        .collect::<Result<_, Error>>()?;

    let output_level = var_index.get(&fn_expr.output).copied().ok_or_else(|| {
        anyhow::anyhow!(
            "FnExpr output variable {} not in join order",
            fn_expr.output
        )
    })?;

    Ok(Box::new(GenericFnPrefixExtender::new(
        fn_expr.expr.clone(),
        prefix_vars,
        output_level,
    )))
}

/// Determine index types for each participating level.
///
/// For single-variable patterns: use the appropriate index based on what's constant.
/// For two-variable patterns:
///   - Level 0: use AE or AV (just attribute + one component) to propose first variable
///   - Level 1: use AEV or AVE (attribute + both components) to verify/propose second variable
fn determine_index_types(
    pattern: &PatternClause,
    join_order: &[Variable],
    var_index: &HashMap<&Variable, usize>,
    participating_levels: &[usize],
) -> Result<Vec<IndexType>, Error> {
    let pattern_vars = pattern.variables();

    if pattern_vars.len() == 1 {
        // Single variable pattern - use the standard index_type
        let index_type = pattern.index_type(join_order.to_vec())?;
        Ok(vec![index_type])
    } else if pattern_vars.len() == 2 {
        // Two variable pattern - need different indices for each level
        // Determine which variable comes first in join order
        let first_var_pos = var_index.get(&pattern_vars[0]).copied();
        let second_var_pos = var_index.get(&pattern_vars[1]).copied();

        match (first_var_pos, second_var_pos) {
            (Some(e_pos), Some(v_pos)) => {
                // Both vars are entity and value (attribute is always constant for now)
                if e_pos < v_pos {
                    // Entity comes first: AE for level 0, AEV for level 1
                    Ok(vec![IndexType::AE, IndexType::AEV])
                } else {
                    // Value comes first: AV for level 0, AVE for level 1
                    Ok(vec![IndexType::AV, IndexType::AVE])
                }
            }
            _ => Err(anyhow::anyhow!("Variables not found in join order")),
        }
    } else if pattern_vars.is_empty() {
        // No variables - pattern is fully constant (existence check)
        Ok(vec![])
    } else {
        Err(anyhow::anyhow!("Patterns with more than 2 variables not supported"))
    }
}

/// Compute the constant prefix bytes for a pattern based on index type.
/// For AVE index with constant value: include serialized value.
/// For AEV index with constant entity: include serialized entity.
fn compute_constant_prefix(pattern: &PatternClause, index_type: IndexType) -> Result<Vec<u8>, Error> {
    match index_type {
        IndexType::AVE => {
            // AVE key order: [attr, value, entity]
            // If value is constant, include it in prefix
            if let PatternElement::Constant(value) = &pattern.value {
                Ok(value.encode())
            } else {
                Ok(vec![])
            }
        }
        IndexType::AEV => {
            // AEV key order: [attr, entity, value]
            // If entity is constant, include it in prefix
            if let PatternElement::Constant(entity) = &pattern.entity {
                Ok(entity.encode())
            } else {
                Ok(vec![])
            }
        }
        IndexType::AE | IndexType::AV => {
            // These are used for two-variable patterns at level 0
            // No constants to add (we just scan all entities or values for an attribute)
            Ok(vec![])
        }
        _ => Ok(vec![]),
    }
}

/// Compile a TriplePattern into a PrefixExtender.
pub fn compile_pattern(
    pattern: &TriplePattern,
    join_order: &[Variable],
    var_index: &HashMap<&Variable, usize>,
    attribute_id: i64,
    slate: Arc<slatedb::DbSnapshot>,
    handle: Handle,
    as_of: Instant,
) -> Result<GenericPrefixExtender, Error> {
    let pattern_clause = PatternClause::from(pattern.clone());
    let pattern_vars = pattern_clause.variables();

    // Determine participating levels
    let participating_levels: Vec<usize> = pattern_vars
        .iter()
        .filter_map(|v| var_index.get(v).copied())
        .collect();

    // Determine index types for each level
    let index_types =
        determine_index_types(&pattern_clause, join_order, var_index, &participating_levels)?;

    // Compute constant prefix (for patterns with constant entity or value)
    let constant_prefix = if !index_types.is_empty() {
        compute_constant_prefix(&pattern_clause, index_types[0])?
    } else {
        vec![]
    };

    Ok(GenericPrefixExtender::new(
        slate,
        handle,
        index_types,
        attribute_id,
        constant_prefix,
        participating_levels,
        as_of,
    ))
}

/// How to produce one column of the output row.
///
/// Two index spaces are in play:
/// - **join-order index**: position in the raw result tuple from the join
/// - **group position**: position in `FindPlan::group_key_indices`
///
/// `GroupVar` uses a group position (indirect) so that `group_key_indices` can
/// be iterated directly in the hot loop without filtering. `Aggregate` uses a
/// join-order index (direct) since aggregates are only read once per row.
pub(crate) enum Projection {
    /// Index into `group_key_indices`, which itself holds a join-order index.
    GroupVar(usize),
    /// Aggregate function + join-order index of the input variable.
    Aggregate(AggregateFunc, usize),
}

/// Compiled find plan that describes how to project + aggregate results.
pub(crate) struct FindPlan {
    /// Join-order indices of the group-by variables, iterated directly to build group keys.
    pub group_key_indices: Vec<usize>,
    /// One projection per find element, in find-clause order.
    pub projections: Vec<Projection>,
    /// Whether any aggregation is needed.
    pub has_aggregates: bool,
}

/// Compile a find spec into a FindPlan.
fn compile_find_plan(
    find: &FindSpec,
    var_index: &HashMap<&Variable, usize>,
) -> Result<FindPlan, Error> {
    let elements = match find {
        FindSpec::FindRel(elements) => elements,
    };

    let mut group_key_indices = Vec::new();
    let mut projections = Vec::new();
    let mut has_aggregates = false;

    for elem in elements {
        match elem {
            FindElement::Variable(var) => {
                let idx = *var_index
                    .get(var)
                    .ok_or_else(|| anyhow::anyhow!("Find variable {} not in where clauses", var))?;
                let group_pos = group_key_indices.len();
                group_key_indices.push(idx);
                projections.push(Projection::GroupVar(group_pos));
            }
            FindElement::Aggregate(func, var) => {
                has_aggregates = true;
                let idx = *var_index.get(var).ok_or_else(|| {
                    anyhow::anyhow!("Aggregate variable {} not in where clauses", var)
                })?;
                projections.push(Projection::Aggregate(func.clone(), idx));
            }
            FindElement::PullExpr(_) => {
                return Err(anyhow::anyhow!("PullExpr not supported"));
            }
        }
    }

    Ok(FindPlan {
        group_key_indices,
        projections,
        has_aggregates,
    })
}

/// Project join results without aggregation.
fn project_results(
    results: Vec<ResultTuple>,
    plan: &FindPlan,
) -> Result<QueryResult, Error> {
    results
        .into_iter()
        .map(|tuple| {
            plan.projections
                .iter()
                .map(|proj| match proj {
                    Projection::GroupVar(group_pos) => {
                        let join_idx = plan.group_key_indices[*group_pos];
                        DataType::decode(&tuple[join_idx])
                            .map_err(|e| anyhow::anyhow!("deserialization error: {}", e))
                    }
                    Projection::Aggregate(_, _) => unreachable!(),
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()
}

/// Execute aggregation over join results according to the find plan.
fn execute_aggregation(
    results: Vec<ResultTuple>,
    plan: &FindPlan,
) -> Result<QueryResult, Error> {
    // Aggregation path: group by non-aggregate columns, accumulate aggregates.
    // Key: serialized group columns; Value: (group_values, accumulators)
    let mut groups: HashMap<Vec<u8>, (Vec<DataType>, Vec<Box<dyn Accumulator>>)> = HashMap::new();

    // Count how many aggregate projections we have (for initializing accumulators)
    let agg_funcs: Vec<&AggregateFunc> = plan
        .projections
        .iter()
        .filter_map(|p| match p {
            Projection::Aggregate(func, _) => Some(func),
            _ => None,
        })
        .collect();

    // When there are no group-by variables (all-aggregate query), all rows
    // collapse into a single group. Seed it so that empty input still
    // produces one output row.
    if plan.group_key_indices.is_empty() {
        let accs: Vec<Box<dyn Accumulator>> =
            agg_funcs.iter().map(|f| make_accumulator(f)).collect();
        groups.insert(Vec::new(), (Vec::new(), accs));
    }

    for tuple in &results {
        // Build group key by concatenating raw serialized bytes of group
        // columns.
        let mut group_key = Vec::new();
        for &idx in &plan.group_key_indices {
            group_key.extend_from_slice(&tuple[idx]);
        }

        let (_, accumulators) = match groups.entry(group_key) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let group_values: Vec<DataType> = plan
                    .group_key_indices
                    .iter()
                    .map(|&idx| DataType::decode(&tuple[idx]).map_err(|e| anyhow::anyhow!("{}", e)))
                    .collect::<Result<Vec<_>, _>>()?;
                let accs: Vec<Box<dyn Accumulator>> =
                    agg_funcs.iter().map(|f| make_accumulator(f)).collect();
                e.insert((group_values, accs))
            }
        };

        // TODO: skip deserialization for Count (value is ignored)
        let mut agg_idx = 0;
        for proj in &plan.projections {
            if let Projection::Aggregate(_, join_idx) = proj {
                let value = DataType::decode(&tuple[*join_idx]).map_err(|e| anyhow::anyhow!("{}", e))?;
                accumulators[agg_idx].accumulate(&value)?;
                agg_idx += 1;
            }
        }
    }

    // Finalize: assemble output rows
    let mut output = Vec::with_capacity(groups.len());
    for (_, (group_values, accumulators)) in groups {
        let mut row = Vec::with_capacity(plan.projections.len());
        let mut agg_idx = 0;
        for proj in &plan.projections {
            match proj {
                Projection::GroupVar(group_pos) => {
                    row.push(group_values[*group_pos].clone());
                }
                Projection::Aggregate(_, _) => {
                    row.push(accumulators[agg_idx].finalize()?);
                    agg_idx += 1;
                }
            }
        }
        output.push(row);
    }

    Ok(output)
}

/// Validate that all aggregate variables are bound by where clauses.
fn validate_aggregate_clauses(
    find: &FindSpec,
    var_index: &HashMap<&Variable, usize>,
) -> Result<(), Error> {
    let elements = match find {
        FindSpec::FindRel(elements) => elements,
    };
    for elem in elements {
        if let FindElement::Aggregate(func, var) = elem {
            if !var_index.contains_key(var) {
                return Err(anyhow::anyhow!(
                    "Aggregate variable {} in ({} {}) is not bound by where clauses",
                    var,
                    func,
                    var
                ));
            }
        }
    }
    Ok(())
}

/// Extract attribute name from a DataType constant.
fn extract_attribute_name(data: &DataType) -> Result<String, Error> {
    match data {
        DataType::Keyword(kw) => match kw.namespace() {
            Some(ns) => Ok(format!("{}/{}", ns, kw.name())),
            None => Ok(kw.name().to_string()),
        },
        _ => Err(anyhow::anyhow!("Attribute must be a keyword, got {:?}", data)),
    }
}

/// Resolve attribute PatternElement to attribute ID.
fn resolve_attribute(
    elem: &PatternElement,
    attribute_map: &HashMap<String, i64>,
) -> Result<i64, Error> {
    match elem {
        PatternElement::Constant(data) => {
            let name = extract_attribute_name(data)?;
            attribute_map
                .get(&name)
                .copied()
                .ok_or_else(|| anyhow::anyhow!("Unknown attribute: {}", name))
        }
        _ => Err(anyhow::anyhow!("Attribute must be a constant")),
    }
}

/// Validate that all OR branches have the same free variables.
fn validate_or_branches(branches: &[OrBranch]) -> Result<(), Error> {
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

/// Validate a query before execution. Checks:
/// - The query has at least one variable
/// - All OR branches have the same free variables
/// - All variables in NOT clauses are bound by positive clauses
pub fn validate_query(query: &Query) -> Result<(), Error> {
    let join_order = query_variable_order(&query.where_clauses);
    if join_order.is_empty() {
        return Err(anyhow::anyhow!("Query has no variables"));
    }
    let var_index = build_var_index(&join_order);
    for clause in &query.where_clauses {
        if let WhereClause::Or(branches) = clause {
            validate_or_branches(branches)?;
        }
    }
    validate_not_clauses(&query.where_clauses, &var_index)?;
    validate_predicate_clauses(&query.where_clauses, &var_index)?;
    validate_fn_clauses(&query.where_clauses, &var_index)?;
    validate_aggregate_clauses(&query.find, &var_index)?;
    Ok(())
}

/// Compile an OrBranch into a PrefixExtender.
fn compile_or_branch(
    branch: &OrBranch,
    join_order: &[Variable],
    var_index: &HashMap<&Variable, usize>,
    slate: &Arc<slatedb::DbSnapshot>,
    handle: &Handle,
    attribute_map: &HashMap<String, i64>,
    as_of: Instant,
) -> Result<Box<dyn PrefixExtender>, Error> {
    match branch {
        OrBranch::Clause(clause) => {
            compile_where_clause(clause, join_order, var_index, slate, handle, attribute_map, as_of)
        }
        OrBranch::And(children) => {
            let extenders: Vec<Box<dyn PrefixExtender>> = children
                .iter()
                .map(|c| compile_where_clause(c, join_order, var_index, slate, handle, attribute_map, as_of))
                .collect::<Result<_, _>>()?;
            Ok(Box::new(GenericAndPrefixExtender::new(extenders)))
        }
    }
}

/// Compile a single WhereClause into a PrefixExtender, recursively handling nested clauses.
fn compile_where_clause(
    clause: &WhereClause,
    join_order: &[Variable],
    var_index: &HashMap<&Variable, usize>,
    slate: &Arc<slatedb::DbSnapshot>,
    handle: &Handle,
    attribute_map: &HashMap<String, i64>,
    as_of: Instant,
) -> Result<Box<dyn PrefixExtender>, Error> {
    match clause {
        WhereClause::Triple(pattern) => {
            let attr_id = resolve_attribute(&pattern.attribute, attribute_map)?;
            let extender = compile_pattern(
                pattern,
                join_order,
                var_index,
                attr_id,
                slate.clone(),
                handle.clone(),
                as_of,
            )?;
            Ok(Box::new(extender))
        }
        WhereClause::Or(branches) => {
            let children: Vec<Box<dyn PrefixExtender>> = branches
                .iter()
                .map(|b| compile_or_branch(b, join_order, var_index, slate, handle, attribute_map, as_of))
                .collect::<Result<_, _>>()?;
            Ok(Box::new(GenericOrPrefixExtender::new(children)))
        }
        WhereClause::Not(inner_clauses) => {
            let children: Vec<Box<dyn PrefixExtender>> = inner_clauses
                .iter()
                .map(|c| {
                    compile_where_clause(c, join_order, var_index, slate, handle, attribute_map, as_of)
                })
                .collect::<Result<_, _>>()?;
            let not_level = var_index.len() - 1;
            Ok(Box::new(GenericNotPrefixExtender::new(children, not_level)))
        }
        WhereClause::Predicate(expr) => compile_predicate(expr, var_index),
        WhereClause::FnExpr(fn_expr) => compile_fn_expr(fn_expr, var_index),
        _ => Err(anyhow::anyhow!(
            "Unsupported where clause type: {:?}",
            clause
        )),
    }
}

/// Execute a query against the database.
pub fn execute_query(
    query: &Query,
    slate: Arc<slatedb::DbSnapshot>,
    handle: Handle,
    attribute_map: &HashMap<String, i64>,
    as_of: Instant,
) -> Result<QueryResult, Error> {
    // 1. Extract variable order
    let join_order = query_variable_order(&query.where_clauses);
    let num_levels = join_order.len();
    let var_index = build_var_index(&join_order);

    // 2. Compile patterns into extenders
    let mut extenders: Vec<Box<dyn PrefixExtender>> = Vec::new();

    for clause in &query.where_clauses {
        extenders.push(compile_where_clause(
            clause,
            &join_order,
            &var_index,
            &slate,
            &handle,
            attribute_map,
            as_of,
        )?);
    }

    // 3. Run GenericJoin
    let extender_refs: Vec<&dyn PrefixExtender> = extenders.iter().map(|e| e.as_ref()).collect();
    let join = GenericJoin::new(extender_refs, num_levels);
    let results = join.join();

    // 4. Project and aggregate results based on find clause
    let plan = compile_find_plan(&query.find, &var_index)?;
    if plan.has_aggregates {
        execute_aggregation(results, &plan)
    } else {
        project_results(results, &plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edn::Keyword;

    fn kw(name: &str) -> DataType {
        DataType::Keyword(Keyword::plain(name))
    }

    #[test]
    fn test_query_variable_order_single_pattern() {
        let where_clauses = vec![WhereClause::Triple(TriplePattern {
            entity: PatternElement::Variable("?e".to_string()),
            attribute: PatternElement::Constant(kw("name")),
            value: PatternElement::Variable("?name".to_string()),
        })];

        let order = query_variable_order(&where_clauses);
        assert_eq!(order, vec!["?e", "?name"]);
    }

    #[test]
    fn test_query_variable_order_multiple_patterns() {
        let where_clauses = vec![
            WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?e".to_string()),
                attribute: PatternElement::Constant(kw("name")),
                value: PatternElement::Variable("?name".to_string()),
            }),
            WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?e".to_string()),
                attribute: PatternElement::Constant(kw("age")),
                value: PatternElement::Variable("?age".to_string()),
            }),
        ];

        let order = query_variable_order(&where_clauses);
        // ?e appears first in pattern 1, ?name second in pattern 1, ?age in pattern 2
        assert_eq!(order, vec!["?e", "?name", "?age"]);
    }

    #[test]
    fn test_query_variable_order_with_constants() {
        let where_clauses = vec![WhereClause::Triple(TriplePattern {
            entity: PatternElement::Variable("?e".to_string()),
            attribute: PatternElement::Constant(kw("name")),
            value: PatternElement::Constant(DataType::String("Alice".to_string())),
        })];

        let order = query_variable_order(&where_clauses);
        assert_eq!(order, vec!["?e"]);
    }



    #[test]
    fn test_query_variable_order_with_or_clause() {
        let where_clauses = vec![WhereClause::Or(vec![
            OrBranch::Clause(WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?e".to_string()),
                attribute: PatternElement::Constant(kw("name")),
                value: PatternElement::Constant(DataType::String("alice".to_string())),
            })),
            OrBranch::Clause(WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?e".to_string()),
                attribute: PatternElement::Constant(kw("name")),
                value: PatternElement::Constant(DataType::String("bob".to_string())),
            })),
        ])];

        let order = query_variable_order(&where_clauses);
        assert_eq!(order, vec!["?e"]);
    }

    #[test]
    fn test_query_variable_order_or_and_triple() {
        let where_clauses = vec![
            WhereClause::Or(vec![
                OrBranch::Clause(WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(kw("name")),
                    value: PatternElement::Constant(DataType::String("alice".to_string())),
                })),
                OrBranch::Clause(WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(kw("name")),
                    value: PatternElement::Constant(DataType::String("bob".to_string())),
                })),
            ]),
            WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?e".to_string()),
                attribute: PatternElement::Constant(kw("age")),
                value: PatternElement::Variable("?age".to_string()),
            }),
        ];

        let order = query_variable_order(&where_clauses);
        assert_eq!(order, vec!["?e", "?age"]);
    }

    #[test]
    fn test_query_variable_order_ignores_predicates() {
        // Predicate clauses should NOT contribute variables to join order
        let where_clauses = vec![
            WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?e".to_string()),
                attribute: PatternElement::Constant(kw("age")),
                value: PatternElement::Variable("?age".to_string()),
            }),
            WhereClause::Predicate(crate::expr::Expr::BinaryExpr(crate::expr::BinaryExpr {
                left: Box::new(crate::expr::Expr::Variable("?age".to_string())),
                op: crate::expr::BinaryOp::Lt,
                right: Box::new(crate::expr::Expr::Literal(DataType::Long(30))),
            })),
        ];

        let order = query_variable_order(&where_clauses);
        assert_eq!(order, vec!["?e", "?age"]);
    }

    #[test]
    fn test_validate_predicate_unbound_variable() {
        let query = Query {
            find: FindSpec::FindRel(vec![FindElement::Variable("?e".to_string())]),
            where_clauses: vec![
                WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(kw("name")),
                    value: PatternElement::Constant(DataType::String("Alice".to_string())),
                }),
                WhereClause::Predicate(crate::expr::Expr::BinaryExpr(crate::expr::BinaryExpr {
                    left: Box::new(crate::expr::Expr::Variable("?unbound".to_string())),
                    op: crate::expr::BinaryOp::Lt,
                    right: Box::new(crate::expr::Expr::Literal(DataType::Long(30))),
                })),
            ],
        };
        let result = validate_query(&query);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("?unbound"));
    }

    #[test]
    fn test_validate_predicate_no_variables() {
        let query = Query {
            find: FindSpec::FindRel(vec![FindElement::Variable("?e".to_string())]),
            where_clauses: vec![
                WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(kw("name")),
                    value: PatternElement::Constant(DataType::String("Alice".to_string())),
                }),
                WhereClause::Predicate(crate::expr::Expr::BinaryExpr(crate::expr::BinaryExpr {
                    left: Box::new(crate::expr::Expr::Literal(DataType::Long(1))),
                    op: crate::expr::BinaryOp::Lt,
                    right: Box::new(crate::expr::Expr::Literal(DataType::Long(2))),
                })),
            ],
        };
        let result = validate_query(&query);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("at least one variable"));
    }

    #[test]
    fn test_query_variable_order_with_fn_expr() {
        // FnExpr should contribute the output variable to the join order
        let where_clauses = vec![
            WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?e".to_string()),
                attribute: PatternElement::Constant(DataType::String("age".to_string())),
                value: PatternElement::Variable("?age".to_string()),
            }),
            WhereClause::FnExpr(FnExpr {
                expr: crate::expr::Expr::BinaryExpr(crate::expr::BinaryExpr {
                    left: Box::new(crate::expr::Expr::Variable("?age".to_string())),
                    op: crate::expr::BinaryOp::Add,
                    right: Box::new(crate::expr::Expr::Literal(DataType::Long(1))),
                }),
                output: "?next_age".to_string(),
            }),
        ];

        let order = query_variable_order(&where_clauses);
        // ?e, ?age from Triple; ?next_age from FnExpr (input ?age already seen)
        assert_eq!(order, vec!["?e", "?age", "?next_age"]);
    }

    #[test]
    fn test_validate_fn_unbound_input() {
        let query = Query {
            find: FindSpec::FindRel(vec![FindElement::Variable("?e".to_string())]),
            where_clauses: vec![
                WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(DataType::String("name".to_string())),
                    value: PatternElement::Constant(DataType::String("Alice".to_string())),
                }),
                WhereClause::FnExpr(FnExpr {
                    expr: crate::expr::Expr::BinaryExpr(crate::expr::BinaryExpr {
                        left: Box::new(crate::expr::Expr::Variable("?unbound".to_string())),
                        op: crate::expr::BinaryOp::Add,
                        right: Box::new(crate::expr::Expr::Literal(DataType::Long(1))),
                    }),
                    output: "?result".to_string(),
                }),
            ],
        };
        let result = validate_query(&query);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("?unbound"));
    }

    #[test]
    fn test_validate_fn_input_must_precede_output() {
        // Output ?e is at level 0 (from Triple), input ?age is at level 1 — input doesn't precede output
        let query = Query {
            find: FindSpec::FindRel(vec![FindElement::Variable("?e".to_string())]),
            where_clauses: vec![
                WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(DataType::String("age".to_string())),
                    value: PatternElement::Variable("?age".to_string()),
                }),
                WhereClause::FnExpr(FnExpr {
                    expr: crate::expr::Expr::BinaryExpr(crate::expr::BinaryExpr {
                        left: Box::new(crate::expr::Expr::Variable("?age".to_string())),
                        op: crate::expr::BinaryOp::Add,
                        right: Box::new(crate::expr::Expr::Literal(DataType::Long(1))),
                    }),
                    output: "?e".to_string(),
                }),
            ],
        };
        let result = validate_query(&query);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must precede"));
    }

    #[test]
    fn test_fn_expr_before_input_triple_is_valid() {
        // FnExpr appears before the Triple that binds its input — should be valid
        let query = Query {
            find: FindSpec::FindRel(vec![
                FindElement::Variable("?e".to_string()),
                FindElement::Variable("?next_age".to_string()),
            ]),
            where_clauses: vec![
                WhereClause::FnExpr(FnExpr {
                    expr: crate::expr::Expr::BinaryExpr(crate::expr::BinaryExpr {
                        left: Box::new(crate::expr::Expr::Variable("?age".to_string())),
                        op: crate::expr::BinaryOp::Add,
                        right: Box::new(crate::expr::Expr::Literal(DataType::Long(1))),
                    }),
                    output: "?next_age".to_string(),
                }),
                WhereClause::Triple(TriplePattern {
                    entity: PatternElement::Variable("?e".to_string()),
                    attribute: PatternElement::Constant(DataType::String("age".to_string())),
                    value: PatternElement::Variable("?age".to_string()),
                }),
            ],
        };
        let result = validate_query(&query);
        assert!(result.is_ok());
    }

    #[test]
    fn test_query_variable_order_fn_expr_before_triple() {
        // FnExpr appears first, but its output should still come after Triple vars
        let where_clauses = vec![
            WhereClause::FnExpr(FnExpr {
                expr: crate::expr::Expr::BinaryExpr(crate::expr::BinaryExpr {
                    left: Box::new(crate::expr::Expr::Variable("?age".to_string())),
                    op: crate::expr::BinaryOp::Add,
                    right: Box::new(crate::expr::Expr::Literal(DataType::Long(1))),
                }),
                output: "?next_age".to_string(),
            }),
            WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?e".to_string()),
                attribute: PatternElement::Constant(DataType::String("age".to_string())),
                value: PatternElement::Variable("?age".to_string()),
            }),
        ];

        let order = query_variable_order(&where_clauses);
        // Triple vars first, then FnExpr output
        assert_eq!(order, vec!["?e", "?age", "?next_age"]);
    }

    #[test]
    fn test_query_variable_order_with_and_inside_or() {
        // And inside Or should collect variables from all And children
        let where_clauses = vec![WhereClause::Or(vec![OrBranch::And(vec![
            WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?e".to_string()),
                attribute: PatternElement::Constant(kw("name")),
                value: PatternElement::Variable("?name".to_string()),
            }),
            WhereClause::Triple(TriplePattern {
                entity: PatternElement::Variable("?e".to_string()),
                attribute: PatternElement::Constant(kw("age")),
                value: PatternElement::Variable("?age".to_string()),
            }),
        ])])];

        let order = query_variable_order(&where_clauses);
        assert_eq!(order, vec!["?e", "?name", "?age"]);
    }

    #[test]
    fn test_execute_aggregation_all_aggs_empty_input() {
        // [:find (count ?e)] with no results → single row [[0]]
        use crate::datalog::AggregateFunc;
        let plan = FindPlan {
            group_key_indices: vec![],
            projections: vec![Projection::Aggregate(AggregateFunc::Count, 0)],
            has_aggregates: true,
        };
        let results: Vec<ResultTuple> = vec![];
        let output = execute_aggregation(results, &plan).unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0], vec![DataType::Long(0)]);
    }

    #[test]
    fn test_execute_aggregation_with_group_keys_empty_input() {
        // [:find ?dept (count ?e)] with no results → empty (no groups formed)
        use crate::datalog::AggregateFunc;
        let plan = FindPlan {
            group_key_indices: vec![0],
            projections: vec![
                Projection::GroupVar(0),
                Projection::Aggregate(AggregateFunc::Count, 1),
            ],
            has_aggregates: true,
        };
        let results: Vec<ResultTuple> = vec![];
        let output = execute_aggregation(results, &plan).unwrap();
        assert!(output.is_empty());
    }
}
