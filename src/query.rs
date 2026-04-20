#![allow(unused)]

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Error;
use slatedb::{DbMetadataOps, DbReadOps};
use tokio::runtime::Handle;

use crate::schema::IdentMap;

use edn::query::{
    Binding, Direction, Element, FindSpec, Limit, NonIntegerConstant, NotJoin, OrJoin,
    OrWhereClause, Order, ParsedQuery, Pattern, PatternNonValuePlace, PatternValuePlace, Predicate,
    ToVariable, UnifyVars, Variable, WhereClause, WhereFn,
};

use crate::aggregate::{make_accumulator, Accumulator};
use crate::algo::generic_join::{GenericJoin, PrefixExtender, ResultTuple, SingleLevelExtender};
use crate::codec::{Decode, Encode};
use crate::expr::{
    expr_variables, BinaryExpr, BinaryOp, Expr, IfExpr, RegexpLikeExpr, UnaryExpr, UnaryOp,
};
use crate::index::IndexType;
use crate::iterator::generic_and_prefix_extender::GenericAndPrefixExtender;
use crate::iterator::generic_fn_prefix_extender::GenericFnPrefixExtender;
use crate::iterator::generic_not_prefix_extender::GenericNotPrefixExtender;
use crate::iterator::generic_or_prefix_extender::GenericOrPrefixExtender;
use crate::iterator::generic_predicate_prefix_extender::GenericPredicatePrefixExtender;
use crate::iterator::generic_prefix_extender::GenericPrefixExtender;
use crate::ops::{DataType, QueryArg};
use regex::Regex;

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
pub(crate) fn pattern_variables(pattern: &Pattern) -> Vec<Variable> {
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
pub(crate) fn non_integer_constant_to_datatype(c: &NonIntegerConstant) -> Option<DataType> {
    match c {
        NonIntegerConstant::Boolean(b) => Some(DataType::Boolean(*b)),
        NonIntegerConstant::BigInteger(ref bi) => {
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

fn convert_unary_op(name: &str) -> Result<UnaryOp, Error> {
    match name {
        "not" => Ok(UnaryOp::Not),
        "abs" => Ok(UnaryOp::Abs),
        "upper" => Ok(UnaryOp::Upper),
        "lower" => Ok(UnaryOp::Lower),
        "strlen" => Ok(UnaryOp::Strlen),
        "str" => Ok(UnaryOp::Str),
        "year" => Ok(UnaryOp::Year),
        "month" => Ok(UnaryOp::Month),
        _ => Err(anyhow::anyhow!("Unsupported unary operator: {}", name)),
    }
}

fn convert_sexpr(op_name: &str, args: &[edn::query::FnArg]) -> Result<Expr, Error> {
    if op_name == "regexp_like" {
        return build_regexp_like(args);
    }

    if op_name == "if" {
        if args.len() != 3 {
            return Err(anyhow::anyhow!(
                "'if' expects 3 args (condition, then, else), got {}",
                args.len()
            ));
        }
        let condition = convert_fn_arg(&args[0])?;
        let then_expr = convert_fn_arg(&args[1])?;
        let else_expr = convert_fn_arg(&args[2])?;
        return Ok(Expr::IfExpr(IfExpr {
            condition: Box::new(condition),
            then_expr: Box::new(then_expr),
            else_expr: Box::new(else_expr),
        }));
    }

    if args.len() == 2 {
        if let Ok(op) = convert_binary_op(op_name) {
            let left = convert_fn_arg(&args[0])?;
            let right = convert_fn_arg(&args[1])?;
            return Ok(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }));
        }
    }

    if args.len() == 1 {
        if let Ok(op) = convert_unary_op(op_name) {
            let operand = convert_fn_arg(&args[0])?;
            return Ok(Expr::UnaryExpr(UnaryExpr {
                op,
                operand: Box::new(operand),
            }));
        }
    }

    Err(anyhow::anyhow!(
        "Unsupported function '{}' with {} args",
        op_name,
        args.len()
    ))
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
        edn::query::FnArg::SExpr(ref func, ref args) => convert_sexpr(func.0.as_str(), args),
        _ => Err(anyhow::anyhow!("Unsupported fn arg type")),
    }
}

/// Extract a literal string from a FnArg, for operators that require a literal pattern.
fn fn_arg_as_text_literal(arg: &edn::query::FnArg) -> Option<&str> {
    if let edn::query::FnArg::Constant(NonIntegerConstant::Text(s)) = arg {
        Some(s.as_str())
    } else {
        None
    }
}

/// Build a RegexpLike expression from the 2-arg `(regexp_like ?subject "pattern")` form.
/// Pattern must be a string literal; compiled once at plan time.
fn build_regexp_like(args: &[edn::query::FnArg]) -> Result<Expr, Error> {
    if args.len() != 2 {
        return Err(anyhow::anyhow!(
            "regexp_like expects 2 args (subject, pattern), got {}",
            args.len()
        ));
    }
    let subject = convert_fn_arg(&args[0])?;
    let pattern_src = fn_arg_as_text_literal(&args[1]).ok_or_else(|| {
        anyhow::anyhow!("regexp_like requires a string literal as second argument (pattern)")
    })?;
    let pattern = Regex::new(pattern_src).map_err(|e| {
        anyhow::anyhow!(
            "invalid regex pattern for regexp_like `{}`: {}",
            pattern_src,
            e
        )
    })?;
    Ok(Expr::RegexpLike(RegexpLikeExpr {
        pattern_src: pattern_src.to_string(),
        pattern: Arc::new(pattern),
        subject: Box::new(subject),
    }))
}

pub(crate) fn convert_predicate(pred: &Predicate) -> Result<Expr, Error> {
    convert_fn_arg(&pred.expr)
}

fn is_direct_regexp_like(arg: &edn::query::FnArg) -> bool {
    matches!(
        arg,
        edn::query::FnArg::SExpr(func, _) if func.0.as_str() == "regexp_like"
    )
}

pub(crate) fn convert_where_fn(wf: &WhereFn) -> Result<FnExpr, Error> {
    if is_direct_regexp_like(&wf.expr) {
        return Err(anyhow::anyhow!(
            "regexp_like is a predicate, not a binding function"
        ));
    }
    let expr = convert_fn_arg(&wf.expr)?;
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
pub(crate) fn collect_variables_from_or_branch(branch: &OrWhereClause) -> Vec<Variable> {
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
pub fn query_variable_order(
    in_bindings: &[Binding],
    where_clauses: &[WhereClause],
) -> Vec<Variable> {
    let mut seen = HashSet::new();
    let mut order = Vec::new();

    // In-binding variables come first in the join order.
    for binding in in_bindings {
        for var in binding.variables().into_iter().flatten() {
            if seen.insert(var.clone()) {
                order.push(var);
            }
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
pub(crate) fn build_var_index(join_order: &[Variable]) -> HashMap<&Variable, usize> {
    join_order.iter().enumerate().map(|(i, v)| (v, i)).collect()
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
#[allow(clippy::too_many_arguments)]
pub fn compile_pattern<D, M>(
    pattern: &Pattern,
    join_order: &[Variable],
    var_index: &HashMap<&Variable, usize>,
    attribute_id: i64,
    slate: Arc<D>,
    handle: Handle,
    as_of: i64,
    range_stats: Arc<slatedb_estimates::RangeStats<M>>,
) -> Result<GenericPrefixExtender<D, M>, Error>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
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
        range_stats,
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
pub(crate) fn resolve_order_columns(
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

/// Compile :in bindings and their arguments into `SingleLevelExtender`s.
///
/// `validate_in_bindings` must be called first to ensure binding/arg types match.
fn compile_in_bindings(
    in_bindings: &[Binding],
    args: &[QueryArg],
    var_index: &HashMap<&Variable, usize>,
) -> Vec<Box<dyn PrefixExtender>> {
    in_bindings
        .iter()
        .zip(args.iter())
        .map(|(binding, arg)| -> Box<dyn PrefixExtender> {
            match (binding, arg) {
                (Binding::BindScalar(var), QueryArg::Scalar(dt)) => {
                    let level = *var_index.get(var).expect("in_var must be in join order");
                    let encoded = dt.encode();
                    Box::new(SingleLevelExtender::new(
                        vec![bytes::Bytes::from(encoded)],
                        level,
                    ))
                }
                (Binding::BindColl(var), QueryArg::Collection(items)) => {
                    let level = *var_index.get(var).expect("in_var must be in join order");
                    let encoded_values: Vec<bytes::Bytes> = items
                        .iter()
                        .map(|dt| bytes::Bytes::from(dt.encode()))
                        .collect();
                    Box::new(SingleLevelExtender::new(encoded_values, level))
                }
                _ => unreachable!("validate_in_bindings ensures binding/arg types match"),
            }
        })
        .collect()
}

/// Compile an OrWhereClause into a PrefixExtender.
#[allow(clippy::too_many_arguments)]
fn compile_or_branch<D, M>(
    branch: &OrWhereClause,
    join_order: &[Variable],
    var_index: &HashMap<&Variable, usize>,
    slate: &Arc<D>,
    handle: &Handle,
    ident_map: &IdentMap,
    as_of: i64,
    range_stats: &Arc<slatedb_estimates::RangeStats<M>>,
) -> Result<Box<dyn PrefixExtender>, Error>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
    match branch {
        OrWhereClause::Clause(clause) => compile_where_clause(
            clause,
            join_order,
            var_index,
            slate,
            handle,
            ident_map,
            as_of,
            range_stats,
        ),
        OrWhereClause::And(children) => {
            let extenders: Vec<Box<dyn PrefixExtender>> = children
                .iter()
                .map(|c| {
                    compile_where_clause(
                        c,
                        join_order,
                        var_index,
                        slate,
                        handle,
                        ident_map,
                        as_of,
                        range_stats,
                    )
                })
                .collect::<Result<_, _>>()?;
            Ok(Box::new(GenericAndPrefixExtender::new(extenders)))
        }
    }
}

/// Compile a single WhereClause into a PrefixExtender, recursively handling nested clauses.
#[allow(clippy::too_many_arguments)]
fn compile_where_clause<D, M>(
    clause: &WhereClause,
    join_order: &[Variable],
    var_index: &HashMap<&Variable, usize>,
    slate: &Arc<D>,
    handle: &Handle,
    ident_map: &IdentMap,
    as_of: i64,
    range_stats: &Arc<slatedb_estimates::RangeStats<M>>,
) -> Result<Box<dyn PrefixExtender>, Error>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
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
                range_stats.clone(),
            )?;
            Ok(Box::new(extender))
        }
        WhereClause::OrJoin(oj) => {
            let children: Vec<Box<dyn PrefixExtender>> = oj
                .clauses
                .iter()
                .map(|b| {
                    compile_or_branch(
                        b,
                        join_order,
                        var_index,
                        slate,
                        handle,
                        ident_map,
                        as_of,
                        range_stats,
                    )
                })
                .collect::<Result<_, _>>()?;
            Ok(Box::new(GenericOrPrefixExtender::new(children)))
        }
        WhereClause::NotJoin(nj) => {
            let children: Vec<Box<dyn PrefixExtender>> = nj
                .clauses
                .iter()
                .map(|c| {
                    compile_where_clause(
                        c,
                        join_order,
                        var_index,
                        slate,
                        handle,
                        ident_map,
                        as_of,
                        range_stats,
                    )
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
/// `validate_query` guarantees the variable is present as a scalar binding in
/// `in_bindings` and bound to a non-negative `Long`, so this is infallible for
/// validated queries.
fn resolve_limit(limit: &Limit, in_bindings: &[Binding], args: &[QueryArg]) -> Limit {
    match limit {
        Limit::Variable(v) => {
            let idx = in_bindings
                .iter()
                .position(|b| matches!(b, Binding::BindScalar(bv) if bv == v))
                .expect("validate_query ensures variable limit is a scalar in :in");
            match &args[idx] {
                QueryArg::Scalar(DataType::Long(n)) => Limit::Fixed(*n as u64),
                _ => unreachable!("validate_query ensures variable limit binds to a Long"),
            }
        }
        other => other.clone(),
    }
}

/// Execute a query against the database.
pub fn execute_query<D, M>(
    query: &ParsedQuery,
    args: &[QueryArg],
    slate: Arc<D>,
    handle: Handle,
    ident_map: &IdentMap,
    as_of: i64,
    range_stats: Arc<slatedb_estimates::RangeStats<M>>,
) -> Result<QueryResult, Error>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
    // 1. Extract variable order (in_bindings prepended)
    let join_order = query_variable_order(&query.in_bindings, &query.where_clauses);
    let num_levels = join_order.len();
    let var_index = build_var_index(&join_order);

    // 2. Compile in-binding arguments into extenders
    let mut extenders = compile_in_bindings(&query.in_bindings, args, &var_index);

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
            &range_stats,
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
    let resolved_limit = resolve_limit(&query.limit, &query.in_bindings, args);
    apply_order_and_limit(projected, &query.order, &resolved_limit, &query.find_spec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_validation::validate_query;

    /// Parse an EDN query string into a ParsedQuery.
    fn parse_query(input: &str) -> ParsedQuery {
        edn::parse::parse_query(input).expect("failed to parse query")
    }

    #[test]
    fn test_query_variable_order_single_pattern() {
        let parsed = parse_query("[:find ?e ?name :where [?e :name ?name]]");
        let order = query_variable_order(&parsed.in_bindings, &parsed.where_clauses);
        assert_eq!(order, vec!["?e".to_var(), "?name".to_var()]);
    }

    #[test]
    fn test_query_variable_order_multiple_patterns() {
        let parsed = parse_query("[:find ?e ?name ?age :where [?e :name ?name] [?e :age ?age]]");
        let order = query_variable_order(&parsed.in_bindings, &parsed.where_clauses);
        assert_eq!(
            order,
            vec!["?e".to_var(), "?name".to_var(), "?age".to_var(),]
        );
    }

    #[test]
    fn test_query_variable_order_with_constants() {
        let parsed = parse_query(r#"[:find ?e :where [?e :name "Alice"]]"#);
        let order = query_variable_order(&parsed.in_bindings, &parsed.where_clauses);
        assert_eq!(order, vec!["?e".to_var()]);
    }

    #[test]
    fn test_query_variable_order_with_or_clause() {
        let parsed = parse_query("[:find ?e :where (or [?e _ 10] [?e _ 15])]");
        let order = query_variable_order(&parsed.in_bindings, &parsed.where_clauses);
        assert_eq!(order, vec!["?e".to_var()]);
    }

    #[test]
    fn test_query_variable_order_or_and_triple() {
        let parsed = parse_query(
            r#"[:find ?e ?age :where (or [?e :name "alice"] [?e :name "bob"]) [?e :age ?age]]"#,
        );
        let order = query_variable_order(&parsed.in_bindings, &parsed.where_clauses);
        assert_eq!(order, vec!["?e".to_var(), "?age".to_var(),]);
    }

    #[test]
    fn test_query_variable_order_ignores_predicates() {
        let parsed = parse_query("[:find ?e ?age :where [?e :age ?age] [(< ?age 30)]]");
        let order = query_variable_order(&parsed.in_bindings, &parsed.where_clauses);
        assert_eq!(order, vec!["?e".to_var(), "?age".to_var(),]);
    }

    #[test]
    fn test_query_variable_order_with_fn_expr() {
        let parsed =
            parse_query("[:find ?e ?next_age :where [?e :age ?age] [(+ ?age 1) ?next_age]]");
        let order = query_variable_order(&parsed.in_bindings, &parsed.where_clauses);
        assert_eq!(
            order,
            vec!["?e".to_var(), "?age".to_var(), "?next_age".to_var(),]
        );
    }

    #[test]
    fn test_query_variable_order_fn_expr_before_triple() {
        // FnExpr appears first, but its output should still come after Triple vars
        let parsed =
            parse_query("[:find ?e ?next_age :where [(+ ?age 1) ?next_age] [?e :age ?age]]");
        let order = query_variable_order(&parsed.in_bindings, &parsed.where_clauses);
        assert_eq!(
            order,
            vec!["?e".to_var(), "?age".to_var(), "?next_age".to_var(),]
        );
    }

    #[test]
    fn test_query_variable_order_with_and_inside_or() {
        let parsed =
            parse_query("[:find ?e ?name ?age :where (or (and [?e :name ?name] [?e :age ?age]))]");
        let order = query_variable_order(&parsed.in_bindings, &parsed.where_clauses);
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
    fn test_validate_in_arg_count_mismatch() {
        let parsed = parse_query("[:find ?e :in ?x :where [?e :name ?x]]");
        let err = validate_query(&parsed, &[]).unwrap_err();
        assert!(
            err.to_string().contains("1 binding(s) but 0 argument(s)"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_query_variable_order_with_in_vars() {
        let parsed = parse_query("[:find ?e ?name :in ?name :where [?e :person/name ?name]]");
        let order = query_variable_order(&parsed.in_bindings, &parsed.where_clauses);
        // ?name from :in should come first, then ?e from WHERE
        assert_eq!(order, vec!["?name".to_var(), "?e".to_var(),]);
    }

    // --- regexp_like query conversion tests ---

    #[test]
    fn test_regexp_like_valid_query() {
        let parsed =
            parse_query(r#"[:find ?name :where [?e :name ?name] [(regexp_like ?name "^B")]]"#);
        let result = validate_query(&parsed, &[]);
        assert!(result.is_ok(), "expected ok, got {:?}", result);
    }

    #[test]
    fn test_regexp_like_invalid_pattern_rejected_at_plan_time() {
        let parsed =
            parse_query(r#"[:find ?name :where [?e :name ?name] [(regexp_like ?name "[")]]"#);
        let result = validate_query(&parsed, &[]);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("invalid regex pattern"),
            "unexpected error: {}",
            msg
        );
    }

    #[test]
    fn test_regexp_like_non_literal_pattern_rejected() {
        // Pattern arg is a variable ?pat, not a string literal.
        let parsed = parse_query(
            r#"[:find ?name :where [?e :name ?name] [?e :pat ?pat] [(regexp_like ?name ?pat)]]"#,
        );
        let result = validate_query(&parsed, &[]);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("string literal"), "unexpected error: {}", msg);
    }

    #[test]
    fn test_regexp_like_wrong_arity_rejected() {
        let parsed = parse_query(r#"[:find ?name :where [?e :name ?name] [(regexp_like ?name)]]"#);
        let result = validate_query(&parsed, &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("expects 2 args"));
    }

    #[test]
    fn test_regexp_like_binding_form_rejected() {
        // regexp_like is predicate-only; binding form should be rejected.
        let parsed = parse_query(
            r#"[:find ?name ?hit :where [?e :name ?name] [(regexp_like ?name "^B") ?hit]]"#,
        );
        let result = validate_query(&parsed, &[]);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("predicate, not a binding function"),
            "unexpected error: {}",
            msg
        );
    }

    #[test]
    fn test_query_variable_order_with_if_expr() {
        let parsed = parse_query(
            r#"[:find ?name ?flag :where [?e :name ?name] [?e :age ?age] [(if (> ?age 30) 1 0) ?flag]]"#,
        );
        let order = query_variable_order(&parsed.in_bindings, &parsed.where_clauses);
        assert_eq!(
            order,
            vec![
                "?e".to_var(),
                "?name".to_var(),
                "?age".to_var(),
                "?flag".to_var(),
            ]
        );
    }

    #[test]
    fn test_validate_if_expr_valid() {
        let parsed = parse_query(
            r#"[:find ?name ?flag :where [?e :name ?name] [?e :age ?age] [(if (> ?age 30) ?age 0) ?flag]]"#,
        );
        let result = validate_query(&parsed, &[]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_if_expr_unbound_condition_variable() {
        let parsed = parse_query(
            r#"[:find ?e ?flag :where [?e :name "Alice"] [(if (> ?unbound 30) 1 0) ?flag]]"#,
        );
        let result = validate_query(&parsed, &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("?unbound"));
    }
}
