
// TODO
pub enum PullExpr{}
pub struct Variable(String);
pub enum FindElement {
    Variable(Variable),
    PullExpr(PullExpr),
    Aggregate(String, String)
}
pub enum FindSpec {
    FineRel(Vec<FindElement>),
    // Todo rest
}

// TODO this needs to move to an edn namespace
pub enum Constant {
    String(String),
    Int(i64),
    Float(f64),
    Boolean(bool),
    Keyword(String),
    Symbol(String),
    Nil
}

pub enum DataPattern {
    Variable(Variable),
    Constant(Constant),
    Wildcard
}

// TODO And doesn't make sense at the toplevel
pub enum WhereClause {
    Pattern(DataPattern),
    Not(Vec<WhereClause>),
    And(Vec<WhereClause>),
    Or(Vec<WhereClause>),
    NotJoin(Vec<Variable>, Vec<WhereClause>),
    OrJoin(Vec<Variable>, Vec<WhereClause>),
}
pub enum Query {
    Find(FindSpec),
    Where(Vec<WhereClause>)
    // todo :in :order-by :limit :offset
    // :rules think about where they should go
}