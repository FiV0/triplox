// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

//! This module defines some core types that support find expressions: sources,
//! variables, expressions, etc.
//! These are produced as 'fuel' by the query parser, consumed by the query
//! translator and executor.
//!
//! Many of these types are defined as simple structs that are little more than
//! a richer type alias: a variable, for example, is really just a fancy kind
//! of string.
//!
//! At some point in the future, we might consider reducing copying and memory
//! usage by recasting all of these string-holding structs and enums in terms
//! of string references, with those references being slices of some parsed
//! input query string, and valid for the lifetime of that string.
//!
//! For now, for the sake of simplicity, all of these strings are heap-allocated.
//!
//! Furthermore, we might cut out some of the chaff here: each time a 'tagged'
//! type is used within an enum, we have an opportunity to simplify and use the
//! inner type directly in conjunction with matching on the enum. Before diving
//! deeply into this it's worth recognizing that this loss of 'sovereignty' is
//! a tradeoff against well-typed function signatures and other such boundaries.
use std::collections::{BTreeSet, HashSet};

use std;
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

use crate::{BigInt, DateTime, OrderedFloat, Utc, Uuid};

use crate::value_rc::{FromRc, ValueRc};

pub use crate::{Keyword, PlainSymbol};

pub type SrcVarName = String; // Do not include the required syntactic '$'.

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Variable(pub Arc<PlainSymbol>);

impl Variable {
    pub fn as_str(&self) -> &str {
        self.0.as_ref().0.as_str()
    }

    pub fn name(&self) -> PlainSymbol {
        self.0.as_ref().clone()
    }

    /// Return a new `Variable`, assuming that the provided string is a valid name.
    pub fn from_valid_name(name: &str) -> Variable {
        let s = PlainSymbol::plain(name);
        assert!(s.is_var_symbol());
        Variable(Arc::new(s))
    }
}

pub trait ToVariable {
    fn to_var(&self) -> Variable;
}

impl ToVariable for str {
    fn to_var(&self) -> Variable {
        Variable::from_valid_name(self)
    }
}

pub trait FromValue<T> {
    fn from_value(v: &crate::ValueAndSpan) -> Option<T>;
}

/// If the provided EDN value is a PlainSymbol beginning with '?', return
/// it wrapped in a Variable. If not, return None.
/// TODO: intern strings. #398.
impl FromValue<Variable> for Variable {
    fn from_value(v: &crate::ValueAndSpan) -> Option<Variable> {
        if let crate::SpannedValue::PlainSymbol(ref s) = v.inner {
            Variable::from_symbol(s)
        } else {
            None
        }
    }
}

impl Variable {
    pub fn from_arc(sym: Arc<PlainSymbol>) -> Option<Variable> {
        if sym.is_var_symbol() {
            Some(Variable(sym.clone()))
        } else {
            None
        }
    }

    /// TODO: intern strings. #398.
    pub fn from_symbol(sym: &PlainSymbol) -> Option<Variable> {
        if sym.is_var_symbol() {
            Some(Variable(Arc::new(sym.clone())))
        } else {
            None
        }
    }
}

impl fmt::Debug for Variable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "var({})", self.0)
    }
}

impl std::fmt::Display for Variable {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct QueryFunction(pub PlainSymbol);

impl FromValue<QueryFunction> for QueryFunction {
    fn from_value(v: &crate::ValueAndSpan) -> Option<QueryFunction> {
        if let crate::SpannedValue::PlainSymbol(ref s) = v.inner {
            QueryFunction::from_symbol(s)
        } else {
            None
        }
    }
}

impl QueryFunction {
    pub fn from_symbol(sym: &PlainSymbol) -> Option<QueryFunction> {
        // TODO: validate the acceptable set of function names.
        Some(QueryFunction(sym.clone()))
    }
}

impl std::fmt::Display for QueryFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Direction {
    Ascending,
    Descending,
}

/// An abstract declaration of ordering: direction and variable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Order(pub Direction, pub Variable); // Future: Element instead of Variable?

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SrcVar {
    DefaultSrc,
    NamedSrc(SrcVarName),
}

impl FromValue<SrcVar> for SrcVar {
    fn from_value(v: &crate::ValueAndSpan) -> Option<SrcVar> {
        if let crate::SpannedValue::PlainSymbol(ref s) = v.inner {
            SrcVar::from_symbol(s)
        } else {
            None
        }
    }
}

impl SrcVar {
    pub fn from_symbol(sym: &PlainSymbol) -> Option<SrcVar> {
        if sym.is_src_symbol() {
            if sym.0 == "$" {
                Some(SrcVar::DefaultSrc)
            } else {
                Some(SrcVar::NamedSrc(sym.name().to_string()))
            }
        } else {
            None
        }
    }
}

/// These are the scalar values representable in EDN.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NonIntegerConstant {
    Boolean(bool),
    BigInteger(BigInt),
    Float(OrderedFloat<f64>),
    Text(ValueRc<String>),
    Instant(DateTime<Utc>),
    Uuid(Uuid),
}

impl<'a> From<&'a str> for NonIntegerConstant {
    fn from(val: &'a str) -> NonIntegerConstant {
        NonIntegerConstant::Text(ValueRc::new(val.to_string()))
    }
}

impl From<String> for NonIntegerConstant {
    fn from(val: String) -> NonIntegerConstant {
        NonIntegerConstant::Text(ValueRc::new(val))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FnArg {
    Variable(Variable),
    SrcVar(SrcVar),
    EntidOrInteger(i64),
    IdentOrKeyword(Keyword),
    Constant(NonIntegerConstant),
    /// A nested s-expression function call, e.g. `(= ?x 10)` or `(+ (* ?x 2) 1)`.
    SExpr(PlainSymbol, Vec<FnArg>),
}

impl FromValue<FnArg> for FnArg {
    fn from_value(v: &crate::ValueAndSpan) -> Option<FnArg> {
        use crate::SpannedValue::*;
        match v.inner {
            Integer(x) => Some(FnArg::EntidOrInteger(x)),
            PlainSymbol(ref x) if x.is_src_symbol() => SrcVar::from_symbol(x).map(FnArg::SrcVar),
            PlainSymbol(ref x) if x.is_var_symbol() => {
                Variable::from_symbol(x).map(FnArg::Variable)
            }
            PlainSymbol(_) => None,
            Keyword(ref x) => Some(FnArg::IdentOrKeyword(x.clone())),
            Instant(x) => Some(FnArg::Constant(NonIntegerConstant::Instant(x))),
            Uuid(x) => Some(FnArg::Constant(NonIntegerConstant::Uuid(x))),
            Boolean(x) => Some(FnArg::Constant(NonIntegerConstant::Boolean(x))),
            Float(x) => Some(FnArg::Constant(NonIntegerConstant::Float(x))),
            BigInteger(ref x) => Some(FnArg::Constant(NonIntegerConstant::BigInteger(x.clone()))),
            Text(ref x) =>
            // TODO: intern strings. #398.
            {
                Some(FnArg::Constant(x.clone().into()))
            }
            Nil | NamespacedSymbol(_) | Vector(_) | List(_) | Set(_) | Map(_) => None,
        }
    }
}

// For display in column headings in the repl.
impl std::fmt::Display for FnArg {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            FnArg::Variable(var) => write!(f, "{}", var),
            FnArg::SrcVar(var) => {
                if var == &SrcVar::DefaultSrc {
                    write!(f, "$")
                } else {
                    write!(
                        f,
                        "${}",
                        match var {
                            SrcVar::NamedSrc(ref n) => n,
                            _ => "",
                        }
                    )
                }
            }
            &FnArg::EntidOrInteger(entid) => write!(f, "{}", entid),
            FnArg::IdentOrKeyword(kw) => write!(f, "{}", kw),
            FnArg::Constant(constant) => write!(f, "{}", constant),
            FnArg::SExpr(ref func, ref args) => {
                write!(f, "({}", func)?;
                for arg in args {
                    write!(f, " {}", arg)?;
                }
                write!(f, ")")
            }
        }
    }
}

impl FnArg {
    pub fn as_variable(&self) -> Option<&Variable> {
        match self {
            FnArg::Variable(v) => Some(v),
            _ => None,
        }
    }
}

/// e, a, tx can't be values -- no strings, no floats -- and so
/// they can only be variables, entity IDs, ident keywords, or
/// placeholders.
/// This encoding allows us to represent integers that aren't
/// entity IDs. That'll get filtered out in the context of the
/// database.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PatternNonValuePlace {
    Placeholder,
    Variable(Variable),
    Entid(i64), // Will always be +ve. See #190.
    Ident(ValueRc<Keyword>),
}

impl From<Rc<Keyword>> for PatternNonValuePlace {
    fn from(value: Rc<Keyword>) -> Self {
        PatternNonValuePlace::Ident(ValueRc::from_rc(value))
    }
}

impl From<Keyword> for PatternNonValuePlace {
    fn from(value: Keyword) -> Self {
        PatternNonValuePlace::Ident(ValueRc::new(value))
    }
}

impl PatternNonValuePlace {
    // I think we'll want move variants, so let's leave these here for now.
    #[allow(dead_code)]
    fn into_pattern_value_place(self) -> PatternValuePlace {
        match self {
            PatternNonValuePlace::Placeholder => PatternValuePlace::Placeholder,
            PatternNonValuePlace::Variable(x) => PatternValuePlace::Variable(x),
            PatternNonValuePlace::Entid(x) => PatternValuePlace::EntidOrInteger(x),
            PatternNonValuePlace::Ident(x) => PatternValuePlace::IdentOrKeyword(x),
        }
    }

    fn to_pattern_value_place(&self) -> PatternValuePlace {
        match *self {
            PatternNonValuePlace::Placeholder => PatternValuePlace::Placeholder,
            PatternNonValuePlace::Variable(ref x) => PatternValuePlace::Variable(x.clone()),
            PatternNonValuePlace::Entid(x) => PatternValuePlace::EntidOrInteger(x),
            PatternNonValuePlace::Ident(ref x) => PatternValuePlace::IdentOrKeyword(x.clone()),
        }
    }
}

impl FromValue<PatternNonValuePlace> for PatternNonValuePlace {
    fn from_value(v: &crate::ValueAndSpan) -> Option<PatternNonValuePlace> {
        match v.inner {
            crate::SpannedValue::Integer(x) => {
                if x >= 0 {
                    Some(PatternNonValuePlace::Entid(x))
                } else {
                    None
                }
            }
            crate::SpannedValue::PlainSymbol(ref x) => {
                if x.0.as_str() == "_" {
                    Some(PatternNonValuePlace::Placeholder)
                } else {
                    Variable::from_symbol(x).map(PatternNonValuePlace::Variable)
                }
            }
            crate::SpannedValue::Keyword(ref x) => Some(x.clone().into()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IdentOrEntid {
    Ident(Keyword),
    Entid(i64),
}

/// The `v` part of a pattern can be much broader: it can represent
/// integers that aren't entity IDs (particularly negative integers),
/// strings, and all the rest. We group those under `Constant`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PatternValuePlace {
    Placeholder,
    Variable(Variable),
    EntidOrInteger(i64),
    IdentOrKeyword(ValueRc<Keyword>),
    Constant(NonIntegerConstant),
}

impl From<Rc<Keyword>> for PatternValuePlace {
    fn from(value: Rc<Keyword>) -> Self {
        PatternValuePlace::IdentOrKeyword(ValueRc::from_rc(value))
    }
}

impl From<Keyword> for PatternValuePlace {
    fn from(value: Keyword) -> Self {
        PatternValuePlace::IdentOrKeyword(ValueRc::new(value))
    }
}

impl FromValue<PatternValuePlace> for PatternValuePlace {
    fn from_value(v: &crate::ValueAndSpan) -> Option<PatternValuePlace> {
        match v.inner {
            crate::SpannedValue::Integer(x) => Some(PatternValuePlace::EntidOrInteger(x)),
            crate::SpannedValue::PlainSymbol(ref x) if x.0.as_str() == "_" => {
                Some(PatternValuePlace::Placeholder)
            }
            crate::SpannedValue::PlainSymbol(ref x) => {
                Variable::from_symbol(x).map(PatternValuePlace::Variable)
            }
            crate::SpannedValue::Keyword(ref x) if x.is_namespaced() => Some(x.clone().into()),
            crate::SpannedValue::Boolean(x) => {
                Some(PatternValuePlace::Constant(NonIntegerConstant::Boolean(x)))
            }
            crate::SpannedValue::Float(x) => {
                Some(PatternValuePlace::Constant(NonIntegerConstant::Float(x)))
            }
            crate::SpannedValue::BigInteger(ref x) => Some(PatternValuePlace::Constant(
                NonIntegerConstant::BigInteger(x.clone()),
            )),
            crate::SpannedValue::Instant(x) => {
                Some(PatternValuePlace::Constant(NonIntegerConstant::Instant(x)))
            }
            crate::SpannedValue::Text(ref x) =>
            // TODO: intern strings. #398.
            {
                Some(PatternValuePlace::Constant(x.clone().into()))
            }
            crate::SpannedValue::Uuid(ref u) => {
                Some(PatternValuePlace::Constant(NonIntegerConstant::Uuid(*u)))
            }

            // TODO(#105): review which of these types should be supported
            // in the value position of query patterns.
            crate::SpannedValue::Nil => None,
            crate::SpannedValue::NamespacedSymbol(_) => None,
            crate::SpannedValue::Keyword(ref x) => Some(x.clone().into()),
            crate::SpannedValue::Map(_) => None,
            crate::SpannedValue::List(_) => None,
            crate::SpannedValue::Set(_) => None,
            crate::SpannedValue::Vector(_) => None,
        }
    }
}

impl PatternValuePlace {
    // I think we'll want move variants, so let's leave these here for now.
    #[allow(dead_code)]
    fn into_pattern_non_value_place(self) -> Option<PatternNonValuePlace> {
        match self {
            PatternValuePlace::Placeholder => Some(PatternNonValuePlace::Placeholder),
            PatternValuePlace::Variable(x) => Some(PatternNonValuePlace::Variable(x)),
            PatternValuePlace::EntidOrInteger(x) => {
                if x >= 0 {
                    Some(PatternNonValuePlace::Entid(x))
                } else {
                    None
                }
            }
            PatternValuePlace::IdentOrKeyword(x) => Some(PatternNonValuePlace::Ident(x)),
            PatternValuePlace::Constant(_) => None,
        }
    }

    fn to_pattern_non_value_place(&self) -> Option<PatternNonValuePlace> {
        match *self {
            PatternValuePlace::Placeholder => Some(PatternNonValuePlace::Placeholder),
            PatternValuePlace::Variable(ref x) => Some(PatternNonValuePlace::Variable(x.clone())),
            PatternValuePlace::EntidOrInteger(x) => {
                if x >= 0 {
                    Some(PatternNonValuePlace::Entid(x))
                } else {
                    None
                }
            }
            PatternValuePlace::IdentOrKeyword(ref x) => {
                Some(PatternNonValuePlace::Ident(x.clone()))
            }
            PatternValuePlace::Constant(_) => None,
        }
    }
}

// Not yet used.
// pub enum PullDefaultValue {
//     EntidOrInteger(i64),
//     IdentOrKeyword(Rc<Keyword>),
//     Constant(NonIntegerConstant),
// }

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PullConcreteAttribute {
    Ident(Arc<Keyword>),
    Entid(i64),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedPullAttribute {
    pub attribute: PullConcreteAttribute,
    pub alias: Option<Arc<Keyword>>,
}

impl From<PullConcreteAttribute> for NamedPullAttribute {
    fn from(a: PullConcreteAttribute) -> Self {
        NamedPullAttribute {
            attribute: a,
            alias: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PullAttributeSpec {
    Wildcard,
    Attribute(NamedPullAttribute),
    // PullMapSpec(Vec<…>),
    // LimitedAttribute(NamedPullAttribute, u64),  // Limit nil => Attribute instead.
    // DefaultedAttribute(NamedPullAttribute, PullDefaultValue),
}

impl std::fmt::Display for PullConcreteAttribute {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PullConcreteAttribute::Ident(k) => {
                write!(f, "{}", k)
            }
            &PullConcreteAttribute::Entid(i) => {
                write!(f, "{}", i)
            }
        }
    }
}

impl std::fmt::Display for NamedPullAttribute {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(alias) = &self.alias {
            write!(f, "{} :as {}", self.attribute, alias)
        } else {
            write!(f, "{}", self.attribute)
        }
    }
}

impl std::fmt::Display for PullAttributeSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            &PullAttributeSpec::Wildcard => {
                write!(f, "*")
            }
            PullAttributeSpec::Attribute(attr) => {
                write!(f, "{}", attr)
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Pull {
    pub var: Variable,
    pub patterns: Vec<PullAttributeSpec>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Aggregate {
    pub func: QueryFunction,
    pub args: Vec<FnArg>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Element {
    Variable(Variable),
    Aggregate(Aggregate),
    Pull(Pull),
}

impl Element {
    /// Returns true if the element must yield only one value.
    pub fn is_unit(&self) -> bool {
        match *self {
            Element::Variable(_) => false,
            Element::Pull(_) => false,
            Element::Aggregate(_) => true,
        }
    }
}

impl From<Variable> for Element {
    fn from(x: Variable) -> Element {
        Element::Variable(x)
    }
}

impl std::fmt::Display for Element {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Element::Variable(var) => {
                write!(f, "{}", var)
            }
            &Element::Pull(Pull {
                ref var,
                ref patterns,
            }) => {
                write!(f, "(pull {} [ ", var)?;
                for p in patterns.iter() {
                    write!(f, "{} ", p)?;
                }
                write!(f, "])")
            }
            Element::Aggregate(agg) => match agg.args.len() {
                0 => write!(f, "({})", agg.func),
                1 => write!(f, "({} {})", agg.func, agg.args[0]),
                _ => write!(f, "({} {:?})", agg.func, agg.args),
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Limit {
    None,
    Fixed(u64),
    Variable(Variable),
}

/// A definition of the first part of a find query: the
/// `[:find ?foo ?bar…]` bit.
///
/// There are four different kinds of find specs, allowing you to query for
/// a single value, a collection of values from different entities, a single
/// tuple (relation), or a collection of tuples.
///
/// Examples:
///
/// ```ignore
/// # use edn::query::{Element, FindSpec, Variable};
///
/// # fn main() {
///
///   let elements = vec![
///     Element::Variable("?foo".to_var()),
///     Element::Variable("?bar".to_var()),
///   ];
///   let rel = FindSpec::FindRel(elements);
///
///   if let FindSpec::FindRel(elements) = rel {
///     assert_eq!(2, elements.len());
///   }
///
/// # }
/// ```
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FindSpec {
    /// Returns an array of arrays, represented as a single array with length a multiple of width.
    FindRel(Vec<Element>),

    /// Returns an array of scalars, usually homogeneous.
    /// This is equivalent to mapping over the results of a `FindRel`,
    /// returning the first value of each.
    FindColl(Element),

    /// Returns a single tuple: a heterogeneous array of scalars. Equivalent to
    /// taking the first result from a `FindRel`.
    FindTuple(Vec<Element>),

    /// Returns a single scalar value. Equivalent to taking the first result
    /// from a `FindColl`.
    FindScalar(Element),
}

/// Returns true if the provided `FindSpec` returns at most one result.
impl FindSpec {
    pub fn is_unit_limited(&self) -> bool {
        use self::FindSpec::*;
        match *self {
            FindScalar(..) => true,
            FindTuple(..) => true,
            FindRel(..) => false,
            FindColl(..) => false,
        }
    }

    pub fn expected_column_count(&self) -> usize {
        use self::FindSpec::*;
        match self {
            &FindScalar(..) => 1,
            &FindColl(..) => 1,
            &FindTuple(ref elems) | &FindRel(ref elems) => elems.len(),
        }
    }

    /// Returns true if the provided `FindSpec` cares about distinct results.
    ///
    /// I use the words "cares about" because find is generally defined in terms of producing distinct
    /// results at the Datalog level.
    ///
    /// Two of the find specs (scalar and tuple) produce only a single result. Those don't need to be
    /// run with `SELECT DISTINCT`, because we're only consuming a single result. Those queries will be
    /// run with `LIMIT 1`.
    ///
    /// Additionally, some projections cannot produce duplicate results: `[:find (max ?x) …]`, for
    /// example.
    ///
    /// This function gives us the hook to add that logic when we're ready.
    ///
    /// Beyond this, `DISTINCT` is not always needed. For example, in some kinds of accumulation or
    /// sampling projections we might not need to do it at the SQL level because we're consuming into
    /// a dupe-eliminating data structure like a Set, or we know that a particular query cannot produce
    /// duplicate results.
    pub fn requires_distinct(&self) -> bool {
        !self.is_unit_limited()
    }

    pub fn columns<'s>(&'s self) -> Box<dyn Iterator<Item = &'s Element> + 's> {
        use self::FindSpec::*;
        match self {
            FindScalar(e) => Box::new(std::iter::once(e)),
            FindColl(e) => Box::new(std::iter::once(e)),
            FindTuple(v) => Box::new(v.iter()),
            FindRel(v) => Box::new(v.iter()),
        }
    }
}

// Datomic accepts variable or placeholder.  DataScript accepts recursive bindings.  Mentat sticks
// to the non-recursive form Datomic accepts, which is much simpler to process.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum VariableOrPlaceholder {
    Placeholder,
    Variable(Variable),
}

impl VariableOrPlaceholder {
    pub fn into_var(self) -> Option<Variable> {
        match self {
            VariableOrPlaceholder::Placeholder => None,
            VariableOrPlaceholder::Variable(var) => Some(var),
        }
    }

    pub fn var(&self) -> Option<&Variable> {
        match self {
            &VariableOrPlaceholder::Placeholder => None,
            VariableOrPlaceholder::Variable(var) => Some(var),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Binding {
    BindScalar(Variable),
    BindColl(Variable),
    BindRel(Vec<VariableOrPlaceholder>),
    BindTuple(Vec<VariableOrPlaceholder>),
}

impl Binding {
    /// Return each variable or `None`, in order.
    pub fn variables(&self) -> Vec<Option<Variable>> {
        match self {
            &Binding::BindScalar(ref var) | &Binding::BindColl(ref var) => vec![Some(var.clone())],
            &Binding::BindRel(ref vars) | &Binding::BindTuple(ref vars) => {
                vars.iter().map(|x| x.var().cloned()).collect()
            }
        }
    }

    /// Return `true` if no variables are bound, i.e., all binding entries are placeholders.
    pub fn is_empty(&self) -> bool {
        match self {
            &Binding::BindScalar(_) | &Binding::BindColl(_) => false,
            &Binding::BindRel(ref vars) | &Binding::BindTuple(ref vars) => {
                vars.iter().all(|x| x.var().is_none())
            }
        }
    }

    /// Return `true` if no variable is bound twice, i.e., each binding entry is either a
    /// placeholder or unique.
    ///
    /// ```ignore
    /// use edn::query::{Binding,Variable,VariableOrPlaceholder};
    /// use std::rc::Rc;
    ///
    /// let v = "?foo".to_var();
    /// let vv = VariableOrPlaceholder::Variable(v);
    /// let p = VariableOrPlaceholder::Placeholder;
    ///
    /// let e = Binding::BindTuple(vec![p.clone()]);
    /// let b = Binding::BindTuple(vec![p.clone(), vv.clone()]);
    /// let d = Binding::BindTuple(vec![vv.clone(), p, vv]);
    /// assert!(b.is_valid());          // One var, one placeholder: OK.
    /// assert!(!e.is_valid());         // Empty: not OK.
    /// assert!(!d.is_valid());         // Duplicate var: not OK.
    /// ```
    pub fn is_valid(&self) -> bool {
        match self {
            &Binding::BindScalar(_) | &Binding::BindColl(_) => true,
            &Binding::BindRel(ref vars) | &Binding::BindTuple(ref vars) => {
                let mut acc = HashSet::<Variable>::new();
                for var in vars {
                    if let VariableOrPlaceholder::Variable(var) = var {
                        if !acc.insert(var.clone()) {
                            // It's invalid if there was an equal var already present in the set --
                            // i.e., we have a duplicate var.
                            return false;
                        }
                    }
                }
                // We're not valid if every place is a placeholder!
                !acc.is_empty()
            }
        }
    }
}

// Note that the "implicit blank" rule applies.
// A pattern with a reversed attribute — :foo/_bar — is reversed
// at the point of parsing. These `Pattern` instances only represent
// one direction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Pattern {
    pub source: Option<SrcVar>,
    pub entity: PatternNonValuePlace,
    pub attribute: PatternNonValuePlace,
    pub value: PatternValuePlace,
    pub tx: PatternNonValuePlace,
}

impl Pattern {
    pub fn simple(
        e: PatternNonValuePlace,
        a: PatternNonValuePlace,
        v: PatternValuePlace,
    ) -> Option<Pattern> {
        Pattern::new(None, e, a, v, PatternNonValuePlace::Placeholder)
    }

    pub fn new(
        src: Option<SrcVar>,
        e: PatternNonValuePlace,
        a: PatternNonValuePlace,
        v: PatternValuePlace,
        tx: PatternNonValuePlace,
    ) -> Option<Pattern> {
        let aa = a.clone(); // Too tired of fighting borrow scope for now.
        if let PatternNonValuePlace::Ident(ref k) = aa {
            if k.is_backward() {
                // e and v have different types; we must convert them.
                // Not every parseable value is suitable for the entity field!
                // As such, this is a failable constructor.
                let e_v = e.to_pattern_value_place();
                if let Some(v_e) = v.to_pattern_non_value_place() {
                    return Some(Pattern {
                        source: src,
                        entity: v_e,
                        attribute: k.to_reversed().into(),
                        value: e_v,
                        tx,
                    });
                } else {
                    return None;
                }
            }
        }
        Some(Pattern {
            source: src,
            entity: e,
            attribute: a,
            value: v,
            tx,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Predicate {
    pub expr: FnArg, // always SExpr at top level
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WhereFn {
    pub expr: FnArg, // always SExpr at top level
    pub binding: Binding,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UnifyVars {
    /// `Implicit` means the variables in an `or` or `not` are derived from the enclosed pattern.
    /// DataScript regards these vars as 'free': these variables don't need to be bound by the
    /// enclosing environment.
    ///
    /// Datomic's documentation implies that all implicit variables are required:
    ///
    /// > Datomic will attempt to push the or clause down until all necessary variables are bound,
    /// > and will throw an exception if that is not possible.
    ///
    /// but that would render top-level `or` expressions (as used in Datomic's own examples!)
    /// impossible, so we assume that this is an error in the documentation.
    ///
    /// All contained 'arms' in an `or` with implicit variables must bind the same vars.
    Implicit,

    /// `Explicit` means the variables in an `or-join` or `not-join` are explicitly listed,
    /// specified with `required-vars` syntax.
    ///
    /// DataScript parses these as free, but allows (incorrectly) the use of more complicated
    /// `rule-vars` syntax.
    ///
    /// Only the named variables will be unified with the enclosing query.
    ///
    /// Every 'arm' in an `or-join` must mention the entire set of explicit vars.
    Explicit(BTreeSet<Variable>),
}

impl WhereClause {
    pub fn is_pattern(&self) -> bool {
        matches!(self, WhereClause::Pattern(_))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OrWhereClause {
    Clause(WhereClause),
    And(Vec<WhereClause>),
}

impl OrWhereClause {
    pub fn is_pattern_or_patterns(&self) -> bool {
        match self {
            &OrWhereClause::Clause(WhereClause::Pattern(_)) => true,
            OrWhereClause::And(clauses) => clauses.iter().all(|clause| clause.is_pattern()),
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrJoin {
    pub unify_vars: UnifyVars,
    pub clauses: Vec<OrWhereClause>,

    /// Caches the result of `collect_mentioned_variables`.
    mentioned_vars: Option<BTreeSet<Variable>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NotJoin {
    pub unify_vars: UnifyVars,
    pub clauses: Vec<WhereClause>,
}

impl NotJoin {
    pub fn new(unify_vars: UnifyVars, clauses: Vec<WhereClause>) -> NotJoin {
        NotJoin {
            unify_vars,
            clauses,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypeAnnotation {
    pub value_type: Keyword,
    pub variable: Variable,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WhereClause {
    NotJoin(NotJoin),
    OrJoin(OrJoin),
    Pred(Predicate),
    WhereFn(WhereFn),
    RuleExpr,
    Pattern(Pattern),
    TypeAnnotation(TypeAnnotation),
}

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedQuery {
    pub find_spec: FindSpec,
    pub default_source: SrcVar,
    pub with: Vec<Variable>,
    pub in_bindings: Vec<Binding>,
    pub in_sources: BTreeSet<SrcVar>,
    pub limit: Limit,
    pub where_clauses: Vec<WhereClause>,
    pub order: Option<Vec<Order>>,
}

pub(crate) enum QueryPart {
    FindSpec(FindSpec),
    WithVars(Vec<Variable>),
    InBindings(Vec<Binding>),
    Limit(Limit),
    WhereClauses(Vec<WhereClause>),
    Order(Vec<Order>),
}

/// A `ParsedQuery` represents a parsed but potentially invalid query to the query algebrizer.
/// Such a query is syntactically valid but might be semantically invalid, for example because
/// constraints on the set of variables are not respected.
///
/// We split `ParsedQuery` from `FindQuery` because it's not easy to generalize over containers
/// (here, `Vec` and `BTreeSet`) in Rust.
impl ParsedQuery {
    /// Extract a flat list of variables from in_bindings (each binding contributes its variable(s)).
    pub fn in_vars(&self) -> Vec<Variable> {
        self.in_bindings
            .iter()
            .flat_map(|b| b.variables().into_iter().flatten())
            .collect()
    }

    pub(crate) fn from_parts(
        parts: Vec<QueryPart>,
    ) -> std::result::Result<ParsedQuery, &'static str> {
        let mut find_spec: Option<FindSpec> = None;
        let mut with: Option<Vec<Variable>> = None;
        let mut in_bindings: Option<Vec<Binding>> = None;
        let mut limit: Option<Limit> = None;
        let mut where_clauses: Option<Vec<WhereClause>> = None;
        let mut order: Option<Vec<Order>> = None;

        for part in parts.into_iter() {
            match part {
                QueryPart::FindSpec(x) => {
                    if find_spec.is_some() {
                        return Err("find query has repeated :find");
                    }
                    find_spec = Some(x)
                }
                QueryPart::WithVars(x) => {
                    if with.is_some() {
                        return Err("find query has repeated :with");
                    }
                    with = Some(x)
                }
                QueryPart::InBindings(x) => {
                    if in_bindings.is_some() {
                        return Err("find query has repeated :in");
                    }
                    in_bindings = Some(x)
                }
                QueryPart::Limit(x) => {
                    if limit.is_some() {
                        return Err("find query has repeated :limit");
                    }
                    limit = Some(x)
                }
                QueryPart::WhereClauses(x) => {
                    if where_clauses.is_some() {
                        return Err("find query has repeated :where");
                    }
                    where_clauses = Some(x)
                }
                QueryPart::Order(x) => {
                    if order.is_some() {
                        return Err("find query has repeated :order");
                    }
                    order = Some(x)
                }
            }
        }

        Ok(ParsedQuery {
            find_spec: find_spec.ok_or("expected :find")?,
            default_source: SrcVar::DefaultSrc,
            with: with.unwrap_or(vec![]),
            in_bindings: in_bindings.unwrap_or(vec![]),
            in_sources: BTreeSet::default(),
            limit: limit.unwrap_or(Limit::None),
            where_clauses: where_clauses.ok_or("expected :where")?,
            order,
        })
    }
}

impl OrJoin {
    pub fn new(unify_vars: UnifyVars, clauses: Vec<OrWhereClause>) -> OrJoin {
        OrJoin {
            unify_vars,
            clauses,
            mentioned_vars: None,
        }
    }

    /// Return true if either the `OrJoin` is `UnifyVars::Implicit`, or if
    /// every variable mentioned inside the join is also mentioned in the `UnifyVars` list.
    pub fn is_fully_unified(&self) -> bool {
        match &self.unify_vars {
            &UnifyVars::Implicit => true,
            UnifyVars::Explicit(vars) => {
                // We know that the join list must be a subset of the vars in the pattern, or
                // it would have failed validation. That allows us to simply compare counts here.
                // TODO: in debug mode, do a full intersection, and verify that our count check
                // returns the same results.
                // Use the cached list if we have one.
                if let Some(ref mentioned) = self.mentioned_vars {
                    vars.len() == mentioned.len()
                } else {
                    vars.len() == self.collect_mentioned_variables().len()
                }
            }
        }
    }
}

pub trait ContainsVariables {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>);
    fn collect_mentioned_variables(&self) -> BTreeSet<Variable> {
        let mut out = BTreeSet::new();
        self.accumulate_mentioned_variables(&mut out);
        out
    }
}

impl ContainsVariables for WhereClause {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        use self::WhereClause::*;
        match self {
            OrJoin(o) => o.accumulate_mentioned_variables(acc),
            Pred(p) => p.accumulate_mentioned_variables(acc),
            Pattern(p) => p.accumulate_mentioned_variables(acc),
            NotJoin(n) => n.accumulate_mentioned_variables(acc),
            WhereFn(f) => f.accumulate_mentioned_variables(acc),
            TypeAnnotation(a) => a.accumulate_mentioned_variables(acc),
            &RuleExpr => (),
        }
    }
}

impl ContainsVariables for OrWhereClause {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        use self::OrWhereClause::*;
        match self {
            And(clauses) => {
                for clause in clauses {
                    clause.accumulate_mentioned_variables(acc)
                }
            }
            Clause(clause) => clause.accumulate_mentioned_variables(acc),
        }
    }
}

impl ContainsVariables for OrJoin {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        for clause in &self.clauses {
            clause.accumulate_mentioned_variables(acc);
        }
    }
}

impl OrJoin {
    pub fn dismember(self) -> (Vec<OrWhereClause>, UnifyVars, BTreeSet<Variable>) {
        let vars = match self.mentioned_vars {
            Some(m) => m,
            None => self.collect_mentioned_variables(),
        };
        (self.clauses, self.unify_vars, vars)
    }

    pub fn mentioned_variables(&mut self) -> &BTreeSet<Variable> {
        if self.mentioned_vars.is_none() {
            let m = self.collect_mentioned_variables();
            self.mentioned_vars = Some(m);
        }

        if let Some(ref mentioned) = self.mentioned_vars {
            mentioned
        } else {
            unreachable!()
        }
    }
}

impl ContainsVariables for NotJoin {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        for clause in &self.clauses {
            clause.accumulate_mentioned_variables(acc);
        }
    }
}

fn accumulate_fn_arg_variables(arg: &FnArg, acc: &mut BTreeSet<Variable>) {
    match arg {
        FnArg::Variable(v) => acc_ref(acc, v),
        FnArg::SExpr(_, args) => {
            for a in args {
                accumulate_fn_arg_variables(a, acc);
            }
        }
        _ => {}
    }
}

impl ContainsVariables for Predicate {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        accumulate_fn_arg_variables(&self.expr, acc);
    }
}

impl ContainsVariables for TypeAnnotation {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        acc_ref(acc, &self.variable);
    }
}

impl ContainsVariables for Binding {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        match self {
            &Binding::BindScalar(ref v) | &Binding::BindColl(ref v) => acc_ref(acc, v),
            &Binding::BindRel(ref vs) | &Binding::BindTuple(ref vs) => {
                for v in vs {
                    if let VariableOrPlaceholder::Variable(v) = v {
                        acc_ref(acc, v);
                    }
                }
            }
        }
    }
}

impl ContainsVariables for WhereFn {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        accumulate_fn_arg_variables(&self.expr, acc);
        self.binding.accumulate_mentioned_variables(acc);
    }
}

fn acc_ref<T: Clone + Ord>(acc: &mut BTreeSet<T>, v: &T) {
    // Roll on, reference entries!
    if !acc.contains(v) {
        acc.insert(v.clone());
    }
}

impl ContainsVariables for Pattern {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        if let PatternNonValuePlace::Variable(ref v) = self.entity {
            acc_ref(acc, v)
        }
        if let PatternNonValuePlace::Variable(ref v) = self.attribute {
            acc_ref(acc, v)
        }
        if let PatternValuePlace::Variable(ref v) = self.value {
            acc_ref(acc, v)
        }
        if let PatternNonValuePlace::Variable(ref v) = self.tx {
            acc_ref(acc, v)
        }
    }
}

// ---------------------------------------------------------------------------
// Display implementations for EDN round-tripping
// ---------------------------------------------------------------------------

impl std::fmt::Display for NonIntegerConstant {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use chrono::SecondsFormat;
        match self {
            NonIntegerConstant::Boolean(v) => write!(f, "{}", v),
            NonIntegerConstant::BigInteger(ref v) => write!(f, "{}N", v),
            NonIntegerConstant::Float(ref v) => {
                if *v == OrderedFloat(f64::INFINITY) {
                    write!(f, "#f +Infinity")
                } else if *v == OrderedFloat(f64::NEG_INFINITY) {
                    write!(f, "#f -Infinity")
                } else if v.is_nan() {
                    write!(f, "#f NaN")
                } else if v.fract() == 0.0 {
                    write!(f, "{:.1}", v)
                } else {
                    write!(f, "{}", v)
                }
            }
            NonIntegerConstant::Text(ref v) => {
                write!(f, "\"")?;
                for c in v.chars() {
                    match c {
                        '"' => write!(f, "\\\"")?,
                        '\\' => write!(f, "\\\\")?,
                        '\n' => write!(f, "\\n")?,
                        '\r' => write!(f, "\\r")?,
                        '\t' => write!(f, "\\t")?,
                        c => write!(f, "{}", c)?,
                    }
                }
                write!(f, "\"")
            }
            NonIntegerConstant::Instant(v) => write!(
                f,
                "#inst \"{}\"",
                v.to_rfc3339_opts(SecondsFormat::AutoSi, true)
            ),
            NonIntegerConstant::Uuid(ref u) => write!(f, "#uuid \"{}\"", u.hyphenated()),
        }
    }
}

impl std::fmt::Display for PatternNonValuePlace {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PatternNonValuePlace::Placeholder => write!(f, "_"),
            PatternNonValuePlace::Variable(ref v) => write!(f, "{}", v),
            PatternNonValuePlace::Entid(e) => write!(f, "{}", e),
            PatternNonValuePlace::Ident(ref k) => write!(f, "{}", k),
        }
    }
}

impl std::fmt::Display for PatternValuePlace {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PatternValuePlace::Placeholder => write!(f, "_"),
            PatternValuePlace::Variable(ref v) => write!(f, "{}", v),
            PatternValuePlace::EntidOrInteger(i) => write!(f, "{}", i),
            PatternValuePlace::IdentOrKeyword(ref k) => write!(f, "{}", k),
            PatternValuePlace::Constant(ref c) => write!(f, "{}", c),
        }
    }
}

impl std::fmt::Display for Pattern {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{} {} {}", self.entity, self.attribute, self.value)?;
        if !matches!(self.tx, PatternNonValuePlace::Placeholder) {
            write!(f, " {}", self.tx)?;
        }
        write!(f, "]")
    }
}

impl std::fmt::Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.expr)
    }
}

impl std::fmt::Display for Binding {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Binding::BindScalar(ref v) => write!(f, "{}", v),
            Binding::BindColl(ref v) => write!(f, "[{} ...]", v),
            Binding::BindTuple(ref vs) => {
                write!(f, "[")?;
                for (i, v) in vs.iter().enumerate() {
                    if i > 0 {
                        write!(f, " ")?;
                    }
                    match v {
                        VariableOrPlaceholder::Variable(ref var) => write!(f, "{}", var)?,
                        VariableOrPlaceholder::Placeholder => write!(f, "_")?,
                    }
                }
                write!(f, "]")
            }
            Binding::BindRel(ref vs) => {
                write!(f, "[[")?;
                for (i, v) in vs.iter().enumerate() {
                    if i > 0 {
                        write!(f, " ")?;
                    }
                    match v {
                        VariableOrPlaceholder::Variable(ref var) => write!(f, "{}", var)?,
                        VariableOrPlaceholder::Placeholder => write!(f, "_")?,
                    }
                }
                write!(f, "]]")
            }
        }
    }
}

impl std::fmt::Display for WhereFn {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{} {}]", self.expr, self.binding)
    }
}

impl std::fmt::Display for WhereClause {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            WhereClause::Pattern(ref p) => write!(f, "{}", p),
            WhereClause::Pred(ref p) => write!(f, "[{}]", p),
            WhereClause::WhereFn(ref wf) => write!(f, "{}", wf),
            WhereClause::NotJoin(ref nj) => write!(f, "{}", nj),
            WhereClause::OrJoin(ref oj) => write!(f, "{}", oj),
            WhereClause::TypeAnnotation(ref ta) => {
                write!(f, "[(type {} {})]", ta.variable, ta.value_type)
            }
            WhereClause::RuleExpr => write!(f, "(rule-expr)"),
        }
    }
}

impl std::fmt::Display for NotJoin {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self.unify_vars {
            UnifyVars::Implicit => write!(f, "(not")?,
            UnifyVars::Explicit(ref vars) => {
                write!(f, "(not-join [")?;
                for (i, v) in vars.iter().enumerate() {
                    if i > 0 {
                        write!(f, " ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, "]")?;
            }
        }
        for clause in &self.clauses {
            write!(f, " {}", clause)?;
        }
        write!(f, ")")
    }
}

impl std::fmt::Display for OrWhereClause {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            OrWhereClause::Clause(ref c) => write!(f, "{}", c),
            OrWhereClause::And(ref clauses) => {
                write!(f, "(and")?;
                for c in clauses {
                    write!(f, " {}", c)?;
                }
                write!(f, ")")
            }
        }
    }
}

impl std::fmt::Display for OrJoin {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self.unify_vars {
            UnifyVars::Implicit => write!(f, "(or")?,
            UnifyVars::Explicit(ref vars) => {
                write!(f, "(or-join [")?;
                for (i, v) in vars.iter().enumerate() {
                    if i > 0 {
                        write!(f, " ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, "]")?;
            }
        }
        for clause in &self.clauses {
            write!(f, " {}", clause)?;
        }
        write!(f, ")")
    }
}

impl std::fmt::Display for FindSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            FindSpec::FindRel(ref elems) => {
                for (i, e) in elems.iter().enumerate() {
                    if i > 0 {
                        write!(f, " ")?;
                    }
                    write!(f, "{}", e)?;
                }
                Ok(())
            }
            FindSpec::FindColl(ref e) => write!(f, "[{} ...]", e),
            FindSpec::FindTuple(ref elems) => {
                write!(f, "[")?;
                for (i, e) in elems.iter().enumerate() {
                    if i > 0 {
                        write!(f, " ")?;
                    }
                    write!(f, "{}", e)?;
                }
                write!(f, "]")
            }
            FindSpec::FindScalar(ref e) => write!(f, "{} .", e),
        }
    }
}

impl std::fmt::Display for Order {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self.0 {
            Direction::Ascending => write!(f, "[{} :asc]", self.1),
            Direction::Descending => write!(f, "[{} :desc]", self.1),
        }
    }
}

impl std::fmt::Display for Limit {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Limit::None => Ok(()),
            Limit::Fixed(n) => write!(f, "{}", n),
            Limit::Variable(ref v) => write!(f, "{}", v),
        }
    }
}

impl std::str::FromStr for ParsedQuery {
    type Err = peg::error::ParseError<peg::str::LineCol>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        crate::parse::parse_query(s)
    }
}

impl std::fmt::Display for ParsedQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{{:find [{}]", self.find_spec)?;
        if !self.with.is_empty() {
            write!(f, " :with [")?;
            for (i, v) in self.with.iter().enumerate() {
                if i > 0 {
                    write!(f, " ")?;
                }
                write!(f, "{}", v)?;
            }
            write!(f, "]")?;
        }
        if !self.in_bindings.is_empty() {
            write!(f, " :in [")?;
            for (i, b) in self.in_bindings.iter().enumerate() {
                if i > 0 {
                    write!(f, " ")?;
                }
                write!(f, "{}", b)?;
            }
            write!(f, "]")?;
        }
        write!(f, " :where [")?;
        for (i, c) in self.where_clauses.iter().enumerate() {
            if i > 0 {
                write!(f, " ")?;
            }
            write!(f, "{}", c)?;
        }
        write!(f, "]")?;
        if let Some(ref orders) = self.order {
            write!(f, " :order [")?;
            for (i, o) in orders.iter().enumerate() {
                if i > 0 {
                    write!(f, " ")?;
                }
                write!(f, "{}", o)?;
            }
            write!(f, "]")?;
        }
        if let Limit::Fixed(_) | Limit::Variable(_) = self.limit {
            write!(f, " :limit {}", self.limit)?;
        }
        write!(f, "}}")
    }
}
