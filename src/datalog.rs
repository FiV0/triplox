#![allow(unused)]

use anyhow::Error;
use bytes::Bytes;

use crate::codec::index_type_to_prefix;
use crate::util::concat_bytes;
use crate::index::IndexType;
use crate::ops::DataType;

// TODO
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PullExpr {}

pub type Variable = String;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FindElement {
    Variable(Variable),
    PullExpr(PullExpr),
    Aggregate(String, String),
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
    Nil,
}

#[derive(Debug, PartialEq, Clone)]
pub enum DataPattern {
    Variable(Variable),
    Constant(DataType),
    Wildcard,
}

// TODO And doesn't make sense at the toplevel
#[derive(Debug, Clone, PartialEq)]
pub enum WhereClause {
    Pattern {
        entity: DataPattern,
        attribute: DataPattern,
        value: DataPattern,
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
    Where(Vec<WhereClause>), // todo :in :order-by :limit :offset
                             // :rules think about where they should go
}

#[derive(Debug, Clone, PartialEq)]
pub struct PatternClause {
    pub entity: DataPattern,
    pub attribute: DataPattern,
    pub value: DataPattern,
}

impl From<PatternClause> for WhereClause {
    fn from(pattern: PatternClause) -> Self {
        WhereClause::Pattern {
            entity: pattern.entity,
            attribute: pattern.attribute,
            value: pattern.value,
        }
    }
}

impl TryFrom<WhereClause> for PatternClause {
    type Error = anyhow::Error;

    fn try_from(clause: WhereClause) -> Result<Self, Self::Error> {
        match clause {
            WhereClause::Pattern {
                entity,
                attribute,
                value,
            } => Ok(PatternClause {
                entity,
                attribute,
                value,
            }),
            _ => Err(anyhow::anyhow!("Not a Pattern clause")),
        }
    }
}

impl PatternClause {
    pub fn index_type(&self, join_order: Vec<Variable>) -> Result<IndexType, Error> {
        match self {
            PatternClause {
                entity,
                attribute,
                value,
            } => match (entity, attribute, value) {
                (_, DataPattern::Wildcard, _) => Err(anyhow::anyhow!(
                    "Wildcard not supported in attribute position!"
                )),
                (_, DataPattern::Variable(_), _) => Err(anyhow::anyhow!(
                    "Variable not supported in attribute position!"
                )),
                (DataPattern::Constant(_), DataPattern::Constant(_), _) => Ok(IndexType::EAV),
                (DataPattern::Wildcard, DataPattern::Constant(_), _) => Ok(IndexType::AV),
                (DataPattern::Variable(_), DataPattern::Constant(_), DataPattern::Constant(_)) => Ok(IndexType::AVE),
                (DataPattern::Variable(_), DataPattern::Constant(_), DataPattern::Wildcard) => Ok(IndexType::AE),
                (
                    DataPattern::Variable(v1),
                    DataPattern::Constant(_),
                    DataPattern::Variable(v2),
                ) => {
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
            },
        }
    }

    fn collect_vars(pattern: &DataPattern, vars: &mut Vec<Variable>) {
        if let DataPattern::Variable(v) = pattern {
            vars.push(v.clone());
        }
    }

    pub fn variables(&self) -> Vec<Variable> {
        let mut vars = vec![];
        match self {
            PatternClause {
                entity,
                attribute,
                value,
            } => {
                Self::collect_vars(entity, &mut vars);
                Self::collect_vars(attribute, &mut vars);
                Self::collect_vars(value, &mut vars);
            }
        }
        vars
    }

    pub fn align_pattern_clause(&self, index_type: IndexType) -> Result<Vec<DataPattern>, Error> {
        match index_type {
            IndexType::EAV => Ok(vec![self.entity.clone(), self.attribute.clone(), self.value.clone()]),
            IndexType::AVE => Ok(vec![self.attribute.clone(), self.value.clone(), self.entity.clone()]),
            IndexType::AEV => Ok(vec![self.attribute.clone(), self.entity.clone(), self.value.clone()]),
            _ => Err(anyhow::anyhow!("VAE index not (yet) supported")),
        }
    }

    pub fn pattern_prefix(&self, index_type: IndexType) -> Result<Vec<u8>, Error> {
        let index_prefix = index_type_to_prefix(index_type)?;
        let mut prefix = vec![];

        let pattern = self.align_pattern_clause(index_type)?;
        let [v1, v2, v3] = &pattern[..] else {
            return Err(anyhow::anyhow!("Should not happen!"));
        };
        if let DataPattern::Variable(v1) = v1 {
            prefix.push(v1.as_bytes());
        }
        if let DataPattern::Variable(v2) = v2 {
            prefix.push(v2.as_bytes());
        }
        if let DataPattern::Variable(v3) = v3 {
            prefix.push(v3.as_bytes());
        }
        Ok(concat_bytes(&prefix))
    }
}
