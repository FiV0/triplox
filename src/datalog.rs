
// TODO
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PullExpr{}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Variable(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FindElement {
    Variable(Variable),
    PullExpr(PullExpr),
    Aggregate(String, String)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FindSpec {
    FineRel(Vec<FindElement>),
    // Todo rest
}

// TODO this needs to move to an edn namespace
// how do deal with floats?
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Constant {
    String(String),
    Int(i64),
    Boolean(bool),
    Keyword(String),
    Symbol(String),
    Nil
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataPattern {
    Variable(Variable),
    Constant(Constant),
    Wildcard
}

// TODO And doesn't make sense at the toplevel
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WhereClause {
    Pattern(DataPattern),
    Not(Vec<WhereClause>),
    And(Vec<WhereClause>),
    Or(Vec<WhereClause>),
    NotJoin(Vec<Variable>, Vec<WhereClause>),
    OrJoin(Vec<Variable>, Vec<WhereClause>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Query {
    Find(FindSpec),
    Where(Vec<WhereClause>)
    // todo :in :order-by :limit :offset
    // :rules think about where they should go
}