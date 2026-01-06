use std::sync::Arc;

use bytes::Bytes;
use anyhow::Error;

use crate::{datalog::{DataPattern, PatternClause, Variable}, slate::{DEFAULT_READ_OPTIONS, DEFAULT_SCAN_OPTIONS}, util::{create_prefix_range}};

#[derive(Debug, Clone, Copy)]
pub enum IndexType { EAV, AVE, AEV, VAE , AE, AV}

pub (crate) fn remove_index_type(bytes: Bytes) -> Bytes {
    let mut bytes = bytes.to_vec();
    let _ = bytes.split_off(1);      
    Bytes::from(bytes)
}

pub (crate) fn add_index_type(bytes: Bytes, index_type: IndexType) -> Bytes {
    let mut bytes = bytes.to_vec();
    bytes.insert(0, index_type as u8);
    Bytes::from(bytes)
}

pub (crate) fn pattern_clause_to_index_type(clause: &PatternClause, join_order: Vec<Variable>) -> Result<IndexType, Error> {
    match clause {
        PatternClause { entity, attribute, value } => 
            // TODO: deal with two wildcards
            match (entity, attribute, value) {
                (_, DataPattern::Wildcard,_) =>  Err(anyhow::anyhow!("Wildcard not supported in attribute position!")),
                (_, DataPattern::Variable(_),_) =>  Err(anyhow::anyhow!("Variable not supported in attribute position!")),
                (DataPattern::Constant(_), DataPattern::Constant(_), _) => Ok(IndexType::EAV),
                (DataPattern::Wildcard, DataPattern::Constant(_), _) => Ok(IndexType::AV),
                (DataPattern::Variable(_), DataPattern::Constant(_), DataPattern::Constant(_)) => Ok(IndexType::AVE),
                (DataPattern::Variable(_), DataPattern::Constant(_), DataPattern::Wildcard) => Ok(IndexType::AE),
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

pub trait IndexIterator<T: Ord> {
    fn count(&self) -> Result<u64, Error>;
    fn seek(&mut self, key: T) -> Result<(), Error>;
    fn next(&mut self) -> Result<Option<T>, Error>;
}

pub (crate) struct SlateIterator {
    count: u64,
    slate_iterator: slatedb::DbIterator,
}

impl SlateIterator {

    async fn calculate_count(slate: &slatedb::Db, prefix: &[u8]) -> Result<u64, Error> {
        let range = create_prefix_range(prefix);
        let mut iterator = slate.scan_with_options(range, &DEFAULT_SCAN_OPTIONS).await?;
        let mut count = 0 as u64;
        while let Some(_) = iterator.next().await? {
            count += 1;
        }
        Ok(count)
    }

    pub async fn new(prefix: &[u8], slate: &slatedb::Db) -> Result<Self, Error> {
        let count = Self::calculate_count(slate, prefix).await?;
        let range = create_prefix_range(prefix);
        let mut iterator = slate.scan_with_options(range, &DEFAULT_SCAN_OPTIONS).await?;
        Ok(Self { count, slate_iterator: iterator })
    }

}

impl IndexIterator<Bytes> for SlateIterator {
    fn count(&self) -> Result<u64, Error> {
        Ok(self.count)
    }

    fn seek(&mut self, key: Bytes) -> Result<(), Error> {
        let result = tokio::runtime::Handle::current().block_on(self.slate_iterator.seek(key))?;
        Ok(result)
    }

    fn next(&mut self) -> Result<Option<Bytes>, Error> {
        let result = tokio::runtime::Handle::current().block_on(self.slate_iterator.next())?;
        match result {
            Some(key) => Ok(Some(key.key)),
            None => Ok(None)
        }
    }
}

// vec is assumed to be sorted
pub (crate) struct VecIterator<T: Ord> {
    vec: Vec<T>,
    index: usize,
}

impl<T: Ord> VecIterator<T> {
    pub fn new(vec: Vec<T>) -> Self {
        Self { vec, index: 0 }
    }
}

impl<T: Ord + Clone> IndexIterator<T> for VecIterator<T> {
    fn count(&self) -> Result<u64, Error> {
        Ok(self.vec.len() as u64)
    }

    fn seek(&mut self, key: T) -> Result<(), Error> {
        // Binary search to find the first element >= key
        match self.vec.binary_search(&key) {
            Ok(pos) => {
                self.index = pos;
                Ok(())
            }
            Err(pos) => {
                self.index = pos;
                Ok(())
            }
        }
    }

    fn next(&mut self) -> Result<Option<T>, Error> {
        if self.index < self.vec.len() {
            let result = Some(self.vec[self.index].clone());
            self.index += 1;
            Ok(result)
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vec_iterator_basic_operations() {
        let vec = vec![1, 2, 3, 4, 5];
        let mut iter = VecIterator { vec, index: 0 };

        // Test count
        assert_eq!(iter.count().unwrap(), 5);

        // Test next
        assert_eq!(iter.next().unwrap(), Some(1));
        assert_eq!(iter.next().unwrap(), Some(2));
        assert_eq!(iter.next().unwrap(), Some(3));
        assert_eq!(iter.next().unwrap(), Some(4));
        assert_eq!(iter.next().unwrap(), Some(5));
        assert_eq!(iter.next().unwrap(), None);
    }

    #[test]
    fn test_vec_iterator_seek() {
        let vec = vec![1, 3, 5, 7, 9];
        let mut iter = VecIterator { vec, index: 0 };

        // Test seeking to existing value
        iter.seek(3).unwrap();
        assert_eq!(iter.next().unwrap(), Some(3));
        assert_eq!(iter.next().unwrap(), Some(5));

        // Test seeking to non-existing value
        iter.seek(6).unwrap();
        assert_eq!(iter.next().unwrap(), Some(7));

        // Test seeking to value beyond end
        iter.seek(10).unwrap();
        assert_eq!(iter.next().unwrap(), None);
    }

    #[test]
    fn test_vec_iterator_empty() {
        let vec: Vec<i32> = vec![];
        let mut iter = VecIterator { vec, index: 0 };

        assert_eq!(iter.count().unwrap(), 0);
        assert_eq!(iter.next().unwrap(), None);
        iter.seek(1).unwrap();
        assert_eq!(iter.next().unwrap(), None);
    }
}
