#![allow(unused)]

use anyhow::Error;

use crate::ops::DataType;

// TODO
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PullExpr{}


pub type Variable = String;

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

#[derive(Debug, PartialEq, Clone)]
pub enum DataPattern {
    Variable(Variable),
    Constant(DataType),
    Wildcard
}

// TODO And doesn't make sense at the toplevel
#[derive(Debug, Clone, PartialEq)]
pub enum WhereClause {
    Pattern {
        entity: DataPattern,
        attribute: DataPattern,
        value: DataPattern
    },
    Not(Vec<WhereClause>),
    And(Vec<WhereClause>),
    Or(Vec<WhereClause>),
    NotJoin(Vec<Variable>, Vec<WhereClause>),
    OrJoin(Vec<Variable>, Vec<WhereClause>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Query {
    Find(FindSpec),
    Where(Vec<WhereClause>)
    // todo :in :order-by :limit :offset
    // :rules think about where they should go
}

#[derive(Debug, Clone, PartialEq)]
pub struct PatternClause {
    pub entity: DataPattern,
    pub attribute: DataPattern,
    pub value: DataPattern
}

impl From<PatternClause> for WhereClause {
    fn from(pattern: PatternClause) -> Self {
        WhereClause::Pattern {
            entity: pattern.entity,
            attribute: pattern.attribute,
            value: pattern.value
        }
    }
}

impl TryFrom<WhereClause> for PatternClause {
    type Error = anyhow::Error;
    
    fn try_from(clause: WhereClause) -> Result<Self, Self::Error> {
        match clause {
            WhereClause::Pattern { entity, attribute, value } => 
                Ok(PatternClause { entity, attribute, value }),
            _ => Err(anyhow::anyhow!("Not a Pattern clause"))
        }
    }
}

impl PatternClause {
    pub fn index_type(&self, join_order: Vec<Variable>) -> Result<IndexType, Error> { 
        match self {
            PatternClause { entity, attribute, value } => 
                match (entity, attribute, value) {
                    (_, DataPattern::Wildcard,_) =>  Err(anyhow::anyhow!("Wildcard not supported in attribute position!")),
                    (_, DataPattern::Variable(_),_) =>  Err(anyhow::anyhow!("Variable not supported in attribute position!")),
                    (DataPattern::Constant(_), DataPattern::Constant(_), _) => Ok(IndexType::EAV),
                    (DataPattern::Wildcard, DataPattern::Constant(_), _) => Ok(IndexType::AVE),
                    (DataPattern::Variable(_), DataPattern::Constant(_), DataPattern::Constant(_)) => Ok(IndexType::AVE),
                    (DataPattern::Variable(v1), DataPattern::Constant(_), DataPattern::Variable(v2)) => {
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
                            _ => Err(anyhow::anyhow!("Variables not found in join order"))
                        }
                    }
                }
        }
    }

    pub fn variables(&self) -> Vec<Variable> {
        let mut vars = vec![];
        match self {
            PatternClause { entity, attribute, value } => 
                match (entity, attribute, value) {
                    (DataPattern::Variable(v), _, _) => vars.push(v.clone()),
                    (_, DataPattern::Variable(v), _) => vars.push(v.clone()),
                    (_, _, DataPattern::Variable(v)) => vars.push(v.clone()),
                    _ => {}
                }
        }
        vars
    }
}