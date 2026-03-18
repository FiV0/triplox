use anyhow::Error;

use crate::datalog::{PatternElement, PatternClause, Variable};

#[derive(Debug, Clone, Copy)]
pub enum IndexType { EAV, AVE, AEV, AE, AV}

pub (crate) fn remove_index_type(bytes: bytes::Bytes) -> bytes::Bytes {
    let mut bytes = bytes.to_vec();
    let _ = bytes.split_off(1);
    bytes::Bytes::from(bytes)
}

pub (crate) fn add_index_type(bytes: bytes::Bytes, index_type: IndexType) -> bytes::Bytes {
    let mut bytes = bytes.to_vec();
    bytes.insert(0, index_type as u8);
    bytes::Bytes::from(bytes)
}

pub (crate) fn pattern_clause_to_index_type(clause: &PatternClause, join_order: Vec<Variable>) -> Result<IndexType, Error> {
    match clause {
        PatternClause { entity, attribute, value } =>
            // TODO: deal with two wildcards
            match (entity, attribute, value) {
                (_, PatternElement::Wildcard,_) =>  Err(anyhow::anyhow!("Wildcard not supported in attribute position!")),
                (_, PatternElement::Variable(_),_) =>  Err(anyhow::anyhow!("Variable not supported in attribute position!")),
                (PatternElement::Constant(_), PatternElement::Constant(_), _) => Ok(IndexType::EAV),
                (PatternElement::Wildcard, PatternElement::Constant(_), _) => Ok(IndexType::AV),
                (PatternElement::Variable(_), PatternElement::Constant(_), PatternElement::Constant(_)) => Ok(IndexType::AVE),
                (PatternElement::Variable(_), PatternElement::Constant(_), PatternElement::Wildcard) => Ok(IndexType::AE),
                (PatternElement::Variable(v1), PatternElement::Constant(_), PatternElement::Variable(v2)) => {
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
