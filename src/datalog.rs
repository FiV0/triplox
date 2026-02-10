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
    FindRel(Vec<FindElement>),
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
pub enum PatternElement {
    Variable(Variable),
    Constant(DataType),
    Wildcard,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TriplePattern {
    pub entity: PatternElement,
    pub attribute: PatternElement,
    pub value: PatternElement,
}

/// A branch inside an Or/OrJoin clause.
/// And only makes sense inside Or (top-level patterns are implicitly AND'd by the join).
#[derive(Debug, Clone, PartialEq)]
pub enum OrBranch {
    Clause(WhereClause),
    And(Vec<WhereClause>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum WhereClause {
    Triple(TriplePattern),
    Not(Vec<WhereClause>),
    Or(Vec<OrBranch>),
    NotJoin(Vec<Variable>, Vec<WhereClause>),
    OrJoin(Vec<Variable>, Vec<OrBranch>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub find: FindSpec,
    pub where_clauses: Vec<WhereClause>,
    // TODO: :in :order-by :limit :offset
    // :rules think about where they should go
}

#[derive(Debug, Clone, PartialEq)]
pub struct PatternClause {
    pub entity: PatternElement,
    pub attribute: PatternElement,
    pub value: PatternElement,
}

impl From<PatternClause> for WhereClause {
    fn from(pattern: PatternClause) -> Self {
        WhereClause::Triple(TriplePattern {
            entity: pattern.entity,
            attribute: pattern.attribute,
            value: pattern.value,
        })
    }
}

impl TryFrom<WhereClause> for PatternClause {
    type Error = anyhow::Error;

    fn try_from(clause: WhereClause) -> Result<Self, Self::Error> {
        match clause {
            WhereClause::Triple(triple) => Ok(PatternClause {
                entity: triple.entity,
                attribute: triple.attribute,
                value: triple.value,
            }),
            _ => Err(anyhow::anyhow!("Not a Triple clause")),
        }
    }
}

impl From<TriplePattern> for PatternClause {
    fn from(triple: TriplePattern) -> Self {
        PatternClause {
            entity: triple.entity,
            attribute: triple.attribute,
            value: triple.value,
        }
    }
}

impl From<PatternClause> for TriplePattern {
    fn from(pattern: PatternClause) -> Self {
        TriplePattern {
            entity: pattern.entity,
            attribute: pattern.attribute,
            value: pattern.value,
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
                (_, PatternElement::Wildcard, _) => Err(anyhow::anyhow!(
                    "Wildcard not supported in attribute position!"
                )),
                (_, PatternElement::Variable(_), _) => Err(anyhow::anyhow!(
                    "Variable not supported in attribute position!"
                )),
                (PatternElement::Constant(_), PatternElement::Constant(_), _) => Ok(IndexType::EAV),
                (PatternElement::Wildcard, PatternElement::Constant(_), _) => Ok(IndexType::AV),
                (PatternElement::Variable(_), PatternElement::Constant(_), PatternElement::Constant(_)) => Ok(IndexType::AVE),
                (PatternElement::Variable(_), PatternElement::Constant(_), PatternElement::Wildcard) => Ok(IndexType::AE),
                (
                    PatternElement::Variable(v1),
                    PatternElement::Constant(_),
                    PatternElement::Variable(v2),
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

    fn collect_vars(pattern: &PatternElement, vars: &mut Vec<Variable>) {
        if let PatternElement::Variable(v) = pattern {
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

    pub fn align_pattern_clause(&self, index_type: IndexType) -> Result<Vec<PatternElement>, Error> {
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
        if let PatternElement::Variable(v1) = v1 {
            prefix.push(v1.as_bytes());
        }
        if let PatternElement::Variable(v2) = v2 {
            prefix.push(v2.as_bytes());
        }
        if let PatternElement::Variable(v3) = v3 {
            prefix.push(v3.as_bytes());
        }
        Ok(concat_bytes(&prefix))
    }
}
