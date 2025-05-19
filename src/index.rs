use std::sync::Arc;

use bytes::Bytes;
use anyhow::Error;

use crate::{datalog::{DataPattern, PatternClause, Variable}, slate::{DEFAULT_READ_OPTIONS, DEFAULT_SCAN_OPTIONS}, util::{create_prefix_range, prefix_successor}};

#[derive(Debug, Clone, Copy)]
pub enum IndexType { EAV, AVE, AEV, VAE , AE, AV}

pub (crate) fn pattern_clause_to_index_type(clause: &PatternClause, join_order: Vec<Variable>) -> Result<IndexType, Error> {
    match clause {
        PatternClause { entity, attribute, value } => 
            match (entity, attribute, value) {
                (_, DataPattern::Wildcard,_) =>  Err(anyhow::anyhow!("Wildcard not supported in attribute position!")),
                (_, DataPattern::Variable(_),_) =>  Err(anyhow::anyhow!("Variable not supported in attribute position!")),
                (DataPattern::Constant(_), DataPattern::Constant(_), _) => Ok(IndexType::EAV),
                (DataPattern::Wildcard, DataPattern::Constant(_), _) => Ok(IndexType::AVE),
                (DataPattern::Variable(_), DataPattern::Constant(_), DataPattern::Constant(_)) => Ok(IndexType::AVE),
                (DataPattern::Variable(_), DataPattern::Constant(_), DataPattern::Wildcard) => Ok(IndexType::AVE),
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

pub (crate) trait IndexIterator {
    fn count(&self) -> Result<u64, Error>;
    fn seek(&self, key: Bytes) -> Result<(), Error>;
    fn next(&self) -> Result<Option<Bytes>, Error>;
}

pub (crate) struct SlateIterator<'a> {
    count: u64,
    index_type: IndexType,
    slate_iterator: slatedb::DbIterator<'a>,
}

impl<'a> SlateIterator<'a> {

    async fn calculate_count(slate: &'a slatedb::Db, prefix: &[u8]) -> Result<u64, Error> {
        let range = create_prefix_range(prefix);
        let mut iterator = slate.scan_with_options(range, &DEFAULT_SCAN_OPTIONS).await?;
        let mut count = 0 as u64;
        while let Some(_) = iterator.next().await? {
            count += 1;
        }
        Ok(count)
    }

    pub async fn new(prefix: &[u8], index_type: IndexType, slate: &'a slatedb::Db) -> Result<Self, Error> {
        let prefix_successor = prefix_successor(prefix);
        let count = Self::calculate_count(slate, prefix).await?;
        let range = create_prefix_range(prefix);
        let mut iterator = slate.scan_with_options(range, &DEFAULT_SCAN_OPTIONS).await?;
        Ok(Self { count, index_type, slate_iterator: iterator })
    }

    pub fn count(&self) -> Result<u64, Error> {
        Ok(self.count)
    }

    pub async fn seek(&mut self, key: Bytes) -> Result<(), Error> {
        Ok(self.slate_iterator.seek(key).await?)
    }

    pub async fn next(&mut self) -> Result<Option<Bytes>, Error> {
        let result = self.slate_iterator.next().await?;
        match result {
            Some(key) => Ok(Some(key.key)),
            None => Ok(None)
        }
    }
}