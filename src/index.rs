use bytes::Bytes;
use anyhow::Error;

use crate::datalog::{DataPattern, PatternClause, Variable};

pub enum IndexType { EAV, AVE, AEV, VAE }

pub (crate) fn pattern_clause_to_index_type(clause: &PatternClause, join_order: Vec<Variable>) -> Result<IndexType, Error> {
    match clause {
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

pub (crate) trait Index {
    fn seek_values(&self, key: Bytes) -> Option<Bytes>;
    fn next_values(&self) -> Option<Bytes>;
}

pub (crate) trait LayeredIndex {
    fn open_level(&self) -> Result<(), Error>;
    fn close_level(&self) -> Result<(), Error>;
    fn max_level(&self) -> u32;
}
