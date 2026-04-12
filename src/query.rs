#![allow(unused)]

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Error;
use tokio::runtime::Handle;

use crate::schema::IdentMap;

use edn::query::{
    Direction, Element, FindSpec, Limit, NonIntegerConstant, NotJoin, OrJoin, OrWhereClause, Order,
    ParsedQuery, Pattern, PatternNonValuePlace, PatternValuePlace, Predicate as EdnPredicate,
    ToVariable, UnifyVars, Variable, WhereClause, WhereFn as EdnWhereFn,
};

use crate::aggregate::{make_accumulator, Accumulator};
use crate::algo::generic_join::{GenericJoin, PrefixExtender, ResultTuple, SingleLevelExtender};
use crate::codec::{Decode, Encode};
use crate::expr::{expr_variables, BinaryExpr, BinaryOp, Expr};
use crate::index::IndexType;
use crate::iterator::generic_and_prefix_extender::GenericAndPrefixExtender;
use crate::iterator::generic_fn_prefix_extender::GenericFnPrefixExtender;
use crate::iterator::generic_not_prefix_extender::GenericNotPrefixExtender;
use crate::iterator::generic_or_prefix_extender::GenericOrPrefixExtender;
use crate::iterator::generic_predicate_prefix_extender::GenericPredicatePrefixExtender;
use crate::iterator::generic_prefix_extender::GenericPrefixExtender;
use crate::ops::{DataType, QueryArg};

/// Each inner Vec is a projected row of decoded DataType values.
pub type QueryResult = Vec<Vec<DataType>>;

// ---------------------------------------------------------------------------
// AggregateFunc (moved from datalog.rs)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AggregateFunc {
    Count,
    CountDistinct,
    Sum,
    Avg,
    Min,
    Max,
}

impl std::fmt::Display for AggregateFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Count => write!(f, "count"),
            Self::CountDistinct => write!(f, "count-distinct"),
            Self::Sum => write!(f, "sum"),
            Self::Avg => write!(f, "avg"),
            Self::Min => write!(f, "min"),
            Self::Max => write!(f, "max"),
        }
    }
}

fn parse_aggregate_func(name: &str) -> Result<AggregateFunc, Error> {
    match name {
        "count" => Ok(AggregateFunc::Count),
        "count-distinct" => Ok(AggregateFunc::CountDistinct),
        "sum" => Ok(AggregateFunc::Sum),
        "avg" => Ok(AggregateFunc::Avg),
        "min" => Ok(AggregateFunc::Min),
        "max" => Ok(AggregateFunc::Max),
        _ => Err(anyhow::anyhow!("Unknown aggregate function: {}", name)),
    }
}

// ---------------------------------------------------------------------------
// FnExpr (moved from datalog.rs)
// ---------------------------------------------------------------------------

/// A function expression that computes a value and binds it to an output variable.
/// Example: `[(+ ?age 1) ?next_age]`
#[derive(Debug, Clone, PartialEq)]
pub struct FnExpr {
    pub expr: Expr,
    pub output: Variable,
}

impl FnExpr {
    /// Variables referenced in the expression (inputs).
    pub fn input_variables(&self) -> Vec<Variable> {
        expr_variables(&self.expr)
    }
}

// ---------------------------------------------------------------------------
// Pattern helpers (replacing PatternClause methods)
// ---------------------------------------------------------------------------

/// Extract variables from a Pattern's e/a/v positions.
fn pattern_variables(pattern: &Pattern) -> Vec<Variable> {
    let mut vars = vec![];
    if let PatternNonValuePlace::Variable(ref v) = pattern.entity {
        vars.push(v.clone());
    }
    if let PatternNonValuePlace::Variable(ref v) = pattern.attribute {
        vars.push(v.clone());
    }
    if let PatternValuePlace::Variable(ref v) = pattern.value {
        vars.push(v.clone());
    }
    vars
}

/// Resolve attribute from a PatternNonValuePlace to an attribute ID.
fn resolve_attribute_from_pattern(
    attr: &PatternNonValuePlace,
    ident_map: &IdentMap,
) -> Result<i64, Error> {
    match attr {
        PatternNonValuePlace::Ident(ref kw) => ident_map
            .get(kw.as_ref())
            .copied()
            .ok_or_else(|| anyhow::anyhow!("Unknown attribute: {}", kw)),
        PatternNonValuePlace::Entid(id) => Ok(*id),
        _ => Err(anyhow::anyhow!("Attribute must be a keyword or entid")),
    }
}

/// Convert a PatternNonValuePlace to a DataType (for constant positions).
fn non_value_place_to_datatype(place: &PatternNonValuePlace) -> Option<DataType> {
    match place {
        PatternNonValuePlace::Entid(id) => Some(DataType::Long(*id)),
        PatternNonValuePlace::Ident(ref kw) => Some(DataType::Keyword((**kw).clone())),
        _ => None,
    }
}

/// Convert a PatternValuePlace to a DataType (for constant positions).
fn value_place_to_datatype(place: &PatternValuePlace) -> Option<DataType> {
    match place {
        PatternValuePlace::EntidOrInteger(i) => Some(DataType::Long(*i)),
        PatternValuePlace::IdentOrKeyword(ref kw) => Some(DataType::Keyword((**kw).clone())),
        PatternValuePlace::Constant(ref c) => non_integer_constant_to_datatype(c),
        _ => None,
    }
}

/// Convert a NonIntegerConstant to a DataType.
fn non_integer_constant_to_datatype(c: &NonIntegerConstant) -> Option<DataType> {
    match c {
        NonIntegerConstant::Boolean(b) => Some(DataType::Boolean(*b)),
        NonIntegerConstant::BigInteger(ref bi) => {
            // Try to convert to i128
            let val: i128 = bi.clone().try_into().ok()?;
            Some(DataType::BigInt(val))
        }
        NonIntegerConstant::Float(f) => Some(DataType::Double(f.into_inner())),
        NonIntegerConstant::Text(ref s) => Some(DataType::String((**s).clone())),
        NonIntegerConstant::Instant(dt) => Some(DataType::Instant(*dt)),
        NonIntegerConstant::Uuid(u) => Some(DataType::Uuid(*u)),
    }
}

/// Check if a PatternNonValuePlace is a variable.
fn is_non_value_variable(place: &PatternNonValuePlace) -> bool {
    matches!(place, PatternNonValuePlace::Variable(_))
}

/// Check if a PatternValuePlace is a variable.
fn is_value_variable(place: &PatternValuePlace) -> bool {
    matches!(place, PatternValuePlace::Variable(_))
}

/// Check if a PatternNonValuePlace is a constant (Entid or Ident).
fn is_non_value_constant(place: &PatternNonValuePlace) -> bool {
    matches!(
        place,
        PatternNonValuePlace::Entid(_) | PatternNonValuePlace::Ident(_)
    )
}

/// Check if a PatternValuePlace is a constant.
fn is_value_constant(place: &PatternValuePlace) -> bool {
    matches!(
        place,
        PatternValuePlace::EntidOrInteger(_)
            | PatternValuePlace::IdentOrKeyword(_)
            | PatternValuePlace::Constant(_)
    )
}

/// Check if a PatternNonValuePlace is a placeholder.
fn is_non_value_placeholder(place: &PatternNonValuePlace) -> bool {
    matches!(place, PatternNonValuePlace::Placeholder)
}

/// Check if a PatternValuePlace is a placeholder.
fn is_value_placeholder(place: &PatternValuePlace) -> bool {
    matches!(place, PatternValuePlace::Placeholder)
}

// ---------------------------------------------------------------------------
// Predicate/WhereFn conversion (edn types -> Expr/FnExpr)
// ---------------------------------------------------------------------------

fn convert_binary_op(name: &str) -> Result<BinaryOp, Error> {
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
        _ => Err(anyhow::anyhow!("Unsupported operator: {}", name)),
    }
}

fn convert_fn_arg(arg: &edn::query::FnArg) -> Result<Expr, Error> {
    match arg {
        edn::query::FnArg::Variable(v) => Ok(Expr::Variable(v.clone())),
        edn::query::FnArg::EntidOrInteger(i) => Ok(Expr::Literal(DataType::Long(*i))),
        edn::query::FnArg::IdentOrKeyword(ref kw) => {
            Ok(Expr::Literal(DataType::Keyword((*kw).clone())))
        }
        edn::query::FnArg::Constant(ref c) => non_integer_constant_to_datatype(c)
            .map(Expr::Literal)
            .ok_or_else(|| anyhow::anyhow!("Cannot convert constant to DataType")),
        _ => Err(anyhow::anyhow!("Unsupported fn arg type")),
    }
}

fn convert_predicate(pred: &EdnPredicate) -> Result<Expr, Error> {
    let op_name = pred.operator.0.as_str();
    let op = convert_binary_op(op_name)?;
    if pred.args.len() != 2 {
        return Err(anyhow::anyhow!(
            "Predicate '{}' expects 2 args, got {}",
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

fn convert_where_fn(wf: &EdnWhereFn) -> Result<FnExpr, Error> {
    let op_name = wf.operator.0.as_str();
    let op = convert_binary_op(op_name)?;
    if wf.args.len() != 2 {
        return Err(anyhow::anyhow!(
            "WhereFn '{}' expects 2 args, got {}",
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
        edn::query::Binding::BindScalar(v) => v.clone(),
        _ => return Err(anyhow::anyhow!("Only scalar binding supported")),
    };
    Ok(FnExpr { expr, output })
}

// ---------------------------------------------------------------------------
// Variable collection from clauses
// ---------------------------------------------------------------------------

/// Extract variables from an OrWhereClause.
fn collect_variables_from_or_branch(branch: &OrWhereClause) -> Vec<Variable> {
    match branch {
        OrWhereClause::Clause(clause) => collect_variables_from_clause(clause),
        OrWhereClause::And(children) => children
            .iter()
            .flat_map(collect_variables_from_clause)
            .collect(),
    }
}

/// Recursively extract variables from a single WhereClause.
/// Only positive (Pattern/OrJoin/WhereFn) clauses contribute variables. NotJoin and Pred
/// clauses do not introduce new variables.
fn collect_variables_from_clause(clause: &WhereClause) -> Vec<Variable> {
    match clause {
        WhereClause::Pattern(pattern) => pattern_variables(pattern),
        WhereClause::OrJoin(oj) => {
            // All branches must have the same free variables (validated in execute_query).
            // Extract from the first branch only.
            oj.clauses
                .first()
                .map(collect_variables_from_or_branch)
                .unwrap_or_default()
        }
        WhereClause::WhereFn(wf) => {
            // The output variable is the one contributed.
            match &wf.binding {
                edn::query::Binding::BindScalar(v) => vec![v.clone()],
                _ => vec![],
            }
        }
        WhereClause::NotJoin(_) | WhereClause::Pred(_) => vec![],
        _ => vec![],
    }
}

/// Extract variables from where clauses in first-appearance order (E-A-V within each pattern).
/// Only positive (Pattern/OrJoin/WhereFn) clauses contribute to the variable order. NotJoin and Pred
/// clauses do not introduce new variables.
///
/// WhereFn clauses are moved to the back so their output variables appear after all
/// Pattern/OrJoin variables in the join order.
/// TODO: refine to only require the clauses that bind a WhereFn's input variables
/// to appear before that WhereFn (topological sort).
pub fn query_variable_order(in_vars: &[Variable], where_clauses: &[WhereClause]) -> Vec<Variable> {
    let mut seen = HashSet::new();
    let mut order = Vec::new();

    // In-binding variables come first in the join order.
    for var in in_vars {
        if seen.insert(var.clone()) {
            order.push(var.clone());
        }
    }

    // Non-WhereFn clauses first, then WhereFn clauses, preserving relative order within each group.
    let reordered: Vec<&WhereClause> = where_clauses
        .iter()
        .filter(|c| !matches!(c, WhereClause::WhereFn(_)))
        .chain(
            where_clauses
                .iter()
                .filter(|c| matches!(c, WhereClause::WhereFn(_))),
        )
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

/// Build a variable -> position index from the join order for O(1) lookups.
fn build_var_index(join_order: &[Variable]) -> HashMap<&Variable, usize> {
    join_order.iter().enumerate().map(|(i, v)| (v, i)).collect()
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
            let level = var_index
                .get(v)
                .copied()
                .ok_or_else(|| anyhow::anyhow!("FnExpr input variable {} not in join order", v))?;
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

/// Determine the index type for a single-variable pattern using Pattern directly.
fn pattern_index_type(pattern: &Pattern, join_order: &[Variable]) -> Result<IndexType, Error> {
    let entity = &pattern.entity;
    let attribute = &pattern.attribute;
    let value = &pattern.value;

    // Attribute must be constant (Ident or Entid)
    if matches!(
        attribute,
        PatternNonValuePlace::Placeholder | PatternNonValuePlace::Variable(_)
    ) {
        return Err(anyhow::anyhow!(
            "Attribute position must be a keyword or entid"
        ));
    }

    match (entity, value) {
        (PatternNonValuePlace::Entid(_) | PatternNonValuePlace::Ident(_), _) => Ok(IndexType::AEV),
        (PatternNonValuePlace::Placeholder, _) => Ok(IndexType::AV),
        (PatternNonValuePlace::Variable(_), PatternValuePlace::Placeholder) => Ok(IndexType::AE),
        (PatternNonValuePlace::Variable(ref v1), PatternValuePlace::Variable(ref v2)) => {
            let entity_pos = join_order.iter().position(|v| v == v1);
            let value_pos = join_order.iter().position(|v| v == v2);
            match (entity_pos, value_pos) {
                (Some(e_pos), Some(v_pos)) => {
                    if e_pos < v_pos {
                        Ok(IndexType::AEV)
                    } else {
                        Ok(IndexType::AVE)
                    }
                }
                _ => Err(anyhow::anyhow!("Variables not found in join order")),
            }
        }
        // Variable entity + constant value (EntidOrInteger, IdentOrKeyword, Constant)
        (PatternNonValuePlace::Variable(_), _) => Ok(IndexType::AVE),
    }
}

/// Determine index types for each participating level.
///
/// For single-variable patterns: use the appropriate index based on what's constant.
/// For two-variable patterns:
///   - Level 0: use AE or AV (just attribute + one component) to propose first variable
///   - Level 1: use AEV or AVE (attribute + both components) to verify/propose second variable
fn determine_index_types(
    pattern: &Pattern,
    join_order: &[Variable],
    var_index: &HashMap<&Variable, usize>,
) -> Result<Vec<IndexType>, Error> {
    let pat_vars = pattern_variables(pattern);

    if pat_vars.len() == 1 {
        let index_type = pattern_index_type(pattern, join_order)?;
        Ok(vec![index_type])
    } else if pat_vars.len() == 2 {
        let first_var_pos = var_index.get(&pat_vars[0]).copied();
        let second_var_pos = var_index.get(&pat_vars[1]).copied();

        match (first_var_pos, second_var_pos) {
            (Some(e_pos), Some(v_pos)) => {
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
    } else if pat_vars.is_empty() {
        Ok(vec![])
    } else {
        Err(anyhow::anyhow!(
            "Patterns with more than 2 variables not supported"
        ))
    }
}

/// Compute the constant prefix bytes for a pattern based on index type.
fn compute_constant_prefix(pattern: &Pattern, index_type: IndexType) -> Result<Vec<u8>, Error> {
    match index_type {
        IndexType::AVE => {
            // AVE key order: [attr, value, entity]
            // If value is constant, include it in prefix
            if let Some(dt) = value_place_to_datatype(&pattern.value) {
                Ok(dt.encode())
            } else {
                Ok(vec![])
            }
        }
        IndexType::AEV => {
            // AEV key order: [attr, entity, value]
            // If entity is constant, include it in prefix
            if let Some(dt) = non_value_place_to_datatype(&pattern.entity) {
                Ok(dt.encode())
            } else {
                Ok(vec![])
            }
        }
        IndexType::AE | IndexType::AV => {
            // These are used for two-variable patterns at level 0
            Ok(vec![])
        }
        _ => Ok(vec![]),
    }
}

/// Compile a Pattern into a PrefixExtender.
pub fn compile_pattern(
    pattern: &Pattern,
    join_order: &[Variable],
    var_index: &HashMap<&Variable, usize>,
    attribute_id: i64,
    slate: Arc<slatedb::DbSnapshot>,
    handle: Handle,
    as_of: i64,
) -> Result<GenericPrefixExtender, Error> {
    let pat_vars = pattern_variables(pattern);

    // Determine participating levels (sorted ascending — build_slate_prefix relies on
    // ordered iteration to append already-bound components in index-key layout order).
    let mut participating_levels: Vec<usize> = pat_vars
        .iter()
        .filter_map(|v| var_index.get(v).copied())
        .collect();
    participating_levels.sort_unstable();

    // Determine index types for each level
    let index_types = determine_index_types(pattern, join_order, var_index)?;

    // Compute constant prefix (for patterns with constant entity or value)
    let constant_prefix = if !index_types.is_empty() {
        compute_constant_prefix(pattern, index_types[0])?
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

// ---------------------------------------------------------------------------
// Find plan (projection + aggregation)
// ---------------------------------------------------------------------------

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
        _ => return Err(anyhow::anyhow!("Only FindRel is currently supported")),
    };

    let mut group_key_indices = Vec::new();
    let mut projections = Vec::new();
    let mut has_aggregates = false;

    for elem in elements {
        match elem {
            Element::Variable(var) => {
                let idx = *var_index
                    .get(var)
                    .ok_or_else(|| anyhow::anyhow!("Find variable {} not in where clauses", var))?;
                let group_pos = group_key_indices.len();
                group_key_indices.push(idx);
                projections.push(Projection::GroupVar(group_pos));
            }
            Element::Aggregate(agg) => {
                has_aggregates = true;
                let func_name = agg.func.0.to_string();
                let func = parse_aggregate_func(&func_name)?;
                if agg.args.len() != 1 {
                    return Err(anyhow::anyhow!(
                        "Aggregate function '{}' requires exactly 1 argument, got {}",
                        func_name,
                        agg.args.len()
                    ));
                }
                let var = match &agg.args[0] {
                    edn::query::FnArg::Variable(v) => v,
                    other => {
                        return Err(anyhow::anyhow!(
                            "Aggregate argument must be a variable, got {:?}",
                            other
                        ))
                    }
                };
                let idx = *var_index.get(var).ok_or_else(|| {
                    anyhow::anyhow!("Aggregate variable {} not in where clauses", var)
                })?;
                projections.push(Projection::Aggregate(func, idx));
            }
            Element::Corresponding(_) | Element::Pull(_) => {
                return Err(anyhow::anyhow!(
                    "Pull and corresponding elements are not yet supported"
                ));
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
fn project_results(results: Vec<ResultTuple>, plan: &FindPlan) -> Result<QueryResult, Error> {
    results
        .into_iter()
        .map(|tuple| {
            plan.projections
                .iter()
                .map(|proj| match proj {
                    Projection::GroupVar(group_pos) => {
                        let join_idx = plan.group_key_indices[*group_pos];
                        Ok::<_, Error>(DataType::decode(&tuple[join_idx])?)
                    }
                    Projection::Aggregate(_, _) => unreachable!(),
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()
}

/// Execute aggregation over join results according to the find plan.
fn execute_aggregation(results: Vec<ResultTuple>, plan: &FindPlan) -> Result<QueryResult, Error> {
    #[allow(clippy::type_complexity)]
    let mut groups: HashMap<Vec<u8>, (Vec<DataType>, Vec<Box<dyn Accumulator>>)> = HashMap::new();

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
                    .map(|&idx| Ok::<_, Error>(DataType::decode(&tuple[idx])?))
                    .collect::<Result<Vec<_>, _>>()?;
                let accs: Vec<Box<dyn Accumulator>> =
                    agg_funcs.iter().map(|f| make_accumulator(f)).collect();
                e.insert((group_values, accs))
            }
        };

        let mut agg_idx = 0;
        for proj in &plan.projections {
            if let Projection::Aggregate(_, join_idx) = proj {
                let value = DataType::decode(&tuple[*join_idx])?;
                accumulators[agg_idx].accumulate(&value)?;
                agg_idx += 1;
            }
        }
    }

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
        _ => return Ok(()),
    };
    for elem in elements {
        if let Element::Aggregate(agg) = elem {
            let func_name = agg.func.0.to_string();
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

/// Validate that all OR branches have the same free variables.
fn validate_or_branches(branches: &[OrWhereClause]) -> Result<(), Error> {
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

/// Map find-spec variables to their output column index in the projected result.
fn find_var_positions(find_spec: &FindSpec) -> Result<HashMap<&Variable, usize>, Error> {
    let elements = match find_spec {
        FindSpec::FindRel(elements) => elements,
        _ => {
            return Err(anyhow::anyhow!(
                "ORDER BY is only supported with relational :find (FindRel)"
            ))
        }
    };

    Ok(elements
        .iter()
        .enumerate()
        .filter_map(|(i, elem)| match elem {
            Element::Variable(v) => Some((v, i)),
            _ => None,
        })
        .collect())
}

/// Resolve ORDER BY variables to output column indices in the find spec.
fn resolve_order_columns(
    orders: &[Order],
    find_spec: &FindSpec,
) -> Result<Vec<(usize, Direction)>, Error> {
    let var_positions = find_var_positions(find_spec)?;

    orders
        .iter()
        .map(|Order(dir, var)| {
            var_positions
                .get(var)
                .map(|&idx| (idx, dir.clone()))
                .ok_or_else(|| {
                    anyhow::anyhow!("ORDER BY variable {} is not in the :find clause", var)
                })
        })
        .collect()
}

/// Apply ORDER BY sorting and LIMIT truncation to projected results.
fn apply_order_and_limit(
    mut results: QueryResult,
    order: &Option<Vec<Order>>,
    limit: &Limit,
    find_spec: &FindSpec,
) -> Result<QueryResult, Error> {
    if let Some(orders) = order {
        // Also resolved in validate_query(); duplicated here to keep the API simple.
        let sort_keys = resolve_order_columns(orders, find_spec)?;
        // TODO: replace with Vec::try_sort_by once stabilized (rust-lang/rust#130044)
        // TODO: For large result sets, consider on-disk sorting to avoid OOM.
        let mut sort_err: Option<anyhow::Error> = None;
        results.sort_by(|a, b| {
            if sort_err.is_some() {
                return std::cmp::Ordering::Equal;
            }
            for (col, dir) in &sort_keys {
                match a[*col].partial_compare(&b[*col]) {
                    Ok(std::cmp::Ordering::Equal) => continue,
                    Ok(o) => {
                        return match dir {
                            Direction::Ascending => o,
                            Direction::Descending => o.reverse(),
                        };
                    }
                    Err(e) => {
                        sort_err = Some(e);
                        return std::cmp::Ordering::Equal;
                    }
                }
            }
            std::cmp::Ordering::Equal
        });
        if let Some(e) = sort_err {
            return Err(e);
        }
    }

    match limit {
        Limit::None => {}
        Limit::Fixed(n) => {
            results.truncate(*n as usize);
        }
        // Defense-in-depth: execute_query resolves variable limits before calling this.
        Limit::Variable(v) => {
            return Err(anyhow::anyhow!(
                "Variable limit {} should have been resolved from :in bindings",
                v
            ));
        }
    }

    Ok(results)
}

/// Validate a query before execution.
// TODO: Move query validation into the edn parsing crate so that invalid
// queries are rejected at parse time rather than at execution time.
pub fn validate_query(query: &ParsedQuery, args: &[QueryArg]) -> Result<(), Error> {
    // Validate :in binding count matches args
    if query.in_vars.len() != args.len() {
        return Err(anyhow::anyhow!(
            ":in clause declares {} variable(s) but {} argument(s) provided",
            query.in_vars.len(),
            args.len()
        ));
    }

    // Only scalar bindings are supported for now
    for (i, arg) in args.iter().enumerate() {
        if !matches!(arg, QueryArg::Scalar(_)) {
            return Err(anyhow::anyhow!(
                "Only scalar bindings are currently supported, but argument {} for {} is {:?}",
                i,
                query.in_vars[i],
                arg
            ));
        }
    }

    let join_order = query_variable_order(&query.in_vars, &query.where_clauses);
    if join_order.is_empty() {
        return Err(anyhow::anyhow!("Query has no variables"));
    }
    let var_index = build_var_index(&join_order);
    for clause in &query.where_clauses {
        if let WhereClause::OrJoin(oj) = clause {
            validate_or_branches(&oj.clauses)?;
        }
    }
    validate_not_clauses(&query.where_clauses, &var_index)?;
    validate_predicate_clauses(&query.where_clauses, &var_index)?;
    validate_fn_clauses(&query.where_clauses, &var_index)?;
    validate_aggregate_clauses(&query.find_spec, &var_index)?;

    // Validate ORDER BY variables are in the find spec.
    if let Some(orders) = &query.order {
        resolve_order_columns(orders, &query.find_spec)?;
    }

    // Variable limits must be bound in :in to a non-negative Long.
    if let Limit::Variable(v) = &query.limit {
        let idx =
            query.in_vars.iter().position(|iv| iv == v).ok_or_else(|| {
                anyhow::anyhow!("Variable limit {} is not bound in :in clause", v)
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

/// Compile an OrWhereClause into a PrefixExtender.
fn compile_or_branch(
    branch: &OrWhereClause,
    join_order: &[Variable],
    var_index: &HashMap<&Variable, usize>,
    slate: &Arc<slatedb::DbSnapshot>,
    handle: &Handle,
    ident_map: &IdentMap,
    as_of: i64,
) -> Result<Box<dyn PrefixExtender>, Error> {
    match branch {
        OrWhereClause::Clause(clause) => compile_where_clause(
            clause, join_order, var_index, slate, handle, ident_map, as_of,
        ),
        OrWhereClause::And(children) => {
            let extenders: Vec<Box<dyn PrefixExtender>> = children
                .iter()
                .map(|c| {
                    compile_where_clause(c, join_order, var_index, slate, handle, ident_map, as_of)
                })
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
    ident_map: &IdentMap,
    as_of: i64,
) -> Result<Box<dyn PrefixExtender>, Error> {
    match clause {
        WhereClause::Pattern(pattern) => {
            let attr_id = resolve_attribute_from_pattern(&pattern.attribute, ident_map)?;
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
        WhereClause::OrJoin(oj) => {
            let children: Vec<Box<dyn PrefixExtender>> = oj
                .clauses
                .iter()
                .map(|b| {
                    compile_or_branch(b, join_order, var_index, slate, handle, ident_map, as_of)
                })
                .collect::<Result<_, _>>()?;
            Ok(Box::new(GenericOrPrefixExtender::new(children)))
        }
        WhereClause::NotJoin(nj) => {
            let children: Vec<Box<dyn PrefixExtender>> = nj
                .clauses
                .iter()
                .map(|c| {
                    compile_where_clause(c, join_order, var_index, slate, handle, ident_map, as_of)
                })
                .collect::<Result<_, _>>()?;
            let not_level = var_index.len() - 1;
            Ok(Box::new(GenericNotPrefixExtender::new(children, not_level)))
        }
        WhereClause::Pred(pred) => {
            let expr = convert_predicate(pred)?;
            compile_predicate(&expr, var_index)
        }
        WhereClause::WhereFn(wf) => {
            let fn_expr = convert_where_fn(wf)?;
            compile_fn_expr(&fn_expr, var_index)
        }
        _ => Err(anyhow::anyhow!(
            "Unsupported where clause type: {:?}",
            clause
        )),
    }
}

/// Resolve a variable limit from :in bindings.
///
/// `validate_query` guarantees the variable is present in `in_vars` and bound
/// to a non-negative `Long`, so this is infallible for validated queries.
fn resolve_limit(limit: &Limit, in_vars: &[Variable], args: &[QueryArg]) -> Limit {
    match limit {
        Limit::Variable(v) => {
            let idx = in_vars
                .iter()
                .position(|iv| iv == v)
                .expect("validate_query ensures variable limit is in :in");
            match &args[idx] {
                QueryArg::Scalar(DataType::Long(n)) => Limit::Fixed(*n as u64),
                _ => unreachable!("validate_query ensures variable limit binds to a Long"),
            }
        }
        other => other.clone(),
    }
}

/// Execute a query against the database.
pub fn execute_query(
    query: &ParsedQuery,
    args: &[QueryArg],
    slate: Arc<slatedb::DbSnapshot>,
    handle: Handle,
    ident_map: &IdentMap,
    as_of: i64,
) -> Result<QueryResult, Error> {
    // 1. Extract variable order (in_vars prepended)
    let join_order = query_variable_order(&query.in_vars, &query.where_clauses);
    let num_levels = join_order.len();
    let var_index = build_var_index(&join_order);

    // 2. Compile in-binding arguments into extenders
    let mut extenders: Vec<Box<dyn PrefixExtender>> = Vec::new();

    for (in_var, arg) in query.in_vars.iter().zip(args.iter()) {
        let level = *var_index.get(in_var).expect("in_var must be in join order");
        // validate_query ensures every arg is a Scalar; non-scalars are rejected there.
        let QueryArg::Scalar(dt) = arg else {
            unreachable!("validate_query ensures only scalar bindings reach execute_query");
        };
        let encoded = dt.encode();
        extenders.push(Box::new(SingleLevelExtender::new(
            vec![bytes::Bytes::from(encoded)],
            level,
        )));
    }

    // 3. Compile WHERE patterns into extenders
    for clause in &query.where_clauses {
        extenders.push(compile_where_clause(
            clause,
            &join_order,
            &var_index,
            &slate,
            &handle,
            ident_map,
            as_of,
        )?);
    }

    // 4. Run GenericJoin
    let extender_refs: Vec<&dyn PrefixExtender> = extenders.iter().map(|e| e.as_ref()).collect();
    let join = GenericJoin::new(extender_refs, num_levels);
    let results = join.join();

    // 5. Project and aggregate results based on find clause
    let plan = compile_find_plan(&query.find_spec, &var_index)?;
    let projected = if plan.has_aggregates {
        execute_aggregation(results, &plan)?
    } else {
        project_results(results, &plan)?
    };

    // 6. Resolve variable limit from :in bindings and apply ORDER BY + LIMIT
    let resolved_limit = resolve_limit(&query.limit, &query.in_vars, args);
    apply_order_and_limit(projected, &query.order, &resolved_limit, &query.find_spec)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse an EDN query string into a ParsedQuery.
    fn parse_query(input: &str) -> ParsedQuery {
        edn::parse::parse_query(input).expect("failed to parse query")
    }

    #[test]
    fn test_query_variable_order_single_pattern() {
        let parsed = parse_query("[:find ?e ?name :where [?e :name ?name]]");
        let order = query_variable_order(&parsed.in_vars, &parsed.where_clauses);
        assert_eq!(order, vec!["?e".to_var(), "?name".to_var()]);
    }

    #[test]
    fn test_query_variable_order_multiple_patterns() {
        let parsed = parse_query("[:find ?e ?name ?age :where [?e :name ?name] [?e :age ?age]]");
        let order = query_variable_order(&parsed.in_vars, &parsed.where_clauses);
        assert_eq!(
            order,
            vec!["?e".to_var(), "?name".to_var(), "?age".to_var(),]
        );
    }

    #[test]
    fn test_query_variable_order_with_constants() {
        let parsed = parse_query(r#"[:find ?e :where [?e :name "Alice"]]"#);
        let order = query_variable_order(&parsed.in_vars, &parsed.where_clauses);
        assert_eq!(order, vec!["?e".to_var()]);
    }

    #[test]
    fn test_query_variable_order_with_or_clause() {
        let parsed = parse_query("[:find ?e :where (or [?e _ 10] [?e _ 15])]");
        let order = query_variable_order(&parsed.in_vars, &parsed.where_clauses);
        assert_eq!(order, vec!["?e".to_var()]);
    }

    #[test]
    fn test_query_variable_order_or_and_triple() {
        let parsed = parse_query(
            r#"[:find ?e ?age :where (or [?e :name "alice"] [?e :name "bob"]) [?e :age ?age]]"#,
        );
        let order = query_variable_order(&parsed.in_vars, &parsed.where_clauses);
        assert_eq!(order, vec!["?e".to_var(), "?age".to_var(),]);
    }

    #[test]
    fn test_query_variable_order_ignores_predicates() {
        let parsed = parse_query("[:find ?e ?age :where [?e :age ?age] [(< ?age 30)]]");
        let order = query_variable_order(&parsed.in_vars, &parsed.where_clauses);
        assert_eq!(order, vec!["?e".to_var(), "?age".to_var(),]);
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
    fn test_query_variable_order_with_fn_expr() {
        let parsed =
            parse_query("[:find ?e ?next_age :where [?e :age ?age] [(+ ?age 1) ?next_age]]");
        let order = query_variable_order(&parsed.in_vars, &parsed.where_clauses);
        assert_eq!(
            order,
            vec!["?e".to_var(), "?age".to_var(), "?next_age".to_var(),]
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
    fn test_query_variable_order_fn_expr_before_triple() {
        // FnExpr appears first, but its output should still come after Triple vars
        let parsed =
            parse_query("[:find ?e ?next_age :where [(+ ?age 1) ?next_age] [?e :age ?age]]");
        let order = query_variable_order(&parsed.in_vars, &parsed.where_clauses);
        assert_eq!(
            order,
            vec!["?e".to_var(), "?age".to_var(), "?next_age".to_var(),]
        );
    }

    #[test]
    fn test_query_variable_order_with_and_inside_or() {
        let parsed =
            parse_query("[:find ?e ?name ?age :where (or (and [?e :name ?name] [?e :age ?age]))]");
        let order = query_variable_order(&parsed.in_vars, &parsed.where_clauses);
        assert_eq!(
            order,
            vec!["?e".to_var(), "?name".to_var(), "?age".to_var(),]
        );
    }

    #[test]
    fn test_execute_aggregation_all_aggs_empty_input() {
        // [:find (count ?e)] with no results -> single row [[0]]
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
        // [:find ?dept (count ?e)] with no results -> empty (no groups formed)
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

    // -- ORDER BY and LIMIT tests --

    fn make_find_rel(vars: &[&str]) -> FindSpec {
        FindSpec::FindRel(
            vars.iter()
                .map(|name| Element::Variable(Variable::from_valid_name(name)))
                .collect(),
        )
    }

    fn rows(data: Vec<Vec<i64>>) -> QueryResult {
        data.into_iter()
            .map(|row| row.into_iter().map(DataType::from).collect())
            .collect()
    }

    #[test]
    fn test_apply_order_ascending() {
        let find = make_find_rel(&["?a"]);
        let order = Some(vec![Order(
            Direction::Ascending,
            Variable::from_valid_name("?a"),
        )]);
        let result = apply_order_and_limit(
            rows(vec![vec![3], vec![1], vec![2]]),
            &order,
            &Limit::None,
            &find,
        )
        .unwrap();
        assert_eq!(result, rows(vec![vec![1], vec![2], vec![3]]));
    }

    #[test]
    fn test_apply_order_descending() {
        let find = make_find_rel(&["?a"]);
        let order = Some(vec![Order(
            Direction::Descending,
            Variable::from_valid_name("?a"),
        )]);
        let result = apply_order_and_limit(
            rows(vec![vec![1], vec![3], vec![2]]),
            &order,
            &Limit::None,
            &find,
        )
        .unwrap();
        assert_eq!(result, rows(vec![vec![3], vec![2], vec![1]]));
    }

    #[test]
    fn test_apply_order_multi_key() {
        let find = make_find_rel(&["?a", "?b"]);
        let order = Some(vec![
            Order(Direction::Ascending, Variable::from_valid_name("?a")),
            Order(Direction::Descending, Variable::from_valid_name("?b")),
        ]);
        let input = rows(vec![vec![1, 10], vec![2, 20], vec![1, 30], vec![2, 10]]);
        let result = apply_order_and_limit(input, &order, &Limit::None, &find).unwrap();
        assert_eq!(
            result,
            rows(vec![vec![1, 30], vec![1, 10], vec![2, 20], vec![2, 10]])
        );
    }

    #[test]
    fn test_apply_limit_fixed() {
        let find = make_find_rel(&["?a"]);
        let result = apply_order_and_limit(
            rows(vec![vec![1], vec![2], vec![3], vec![4], vec![5]]),
            &None,
            &Limit::Fixed(3),
            &find,
        )
        .unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_apply_limit_zero() {
        let find = make_find_rel(&["?a"]);
        let result =
            apply_order_and_limit(rows(vec![vec![1], vec![2]]), &None, &Limit::Fixed(0), &find)
                .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_apply_limit_exceeds_rows() {
        let find = make_find_rel(&["?a"]);
        let input = rows(vec![vec![1], vec![2]]);
        let result = apply_order_and_limit(input, &None, &Limit::Fixed(100), &find).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_apply_order_then_limit() {
        let find = make_find_rel(&["?a"]);
        let order = Some(vec![Order(
            Direction::Ascending,
            Variable::from_valid_name("?a"),
        )]);
        let result = apply_order_and_limit(
            rows(vec![vec![5], vec![3], vec![1], vec![4], vec![2]]),
            &order,
            &Limit::Fixed(3),
            &find,
        )
        .unwrap();
        assert_eq!(result, rows(vec![vec![1], vec![2], vec![3]]));
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
            err.to_string().contains("not bound in :in clause"),
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
    fn test_validate_in_arg_count_mismatch() {
        let parsed = parse_query("[:find ?e :in ?x :where [?e :name ?x]]");
        let err = validate_query(&parsed, &[]).unwrap_err();
        assert!(
            err.to_string().contains("1 variable(s) but 0 argument(s)"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_query_variable_order_with_in_vars() {
        let parsed = parse_query("[:find ?e ?name :in ?name :where [?e :person/name ?name]]");
        let order = query_variable_order(&parsed.in_vars, &parsed.where_clauses);
        // ?name from :in should come first, then ?e from WHERE
        assert_eq!(order, vec!["?name".to_var(), "?e".to_var(),]);
    }
}
