use bytes::Bytes;
use std::sync::Arc;

use anyhow::Error;
use slatedb::DbIterator;

use crate::datalog::{Variable, PatternClause};
use crate::algo::slate_iterator::{Index, SlateIterator};
use crate::index::{IndexType, add_index_type, remove_index_type};

// Leapfrog Triejoin
// https://arxiv.org/pdf/1210.0481.pdf

pub (crate) trait LayeredIndex {
    async fn open_level(&mut self) -> Result<(), Error>;
    fn close_level(&mut self) -> Result<(), Error>;
    fn max_level(&self) -> usize;
}
struct JoinIterator<'a> {
    vars: Vec<Variable>,
    pattern: PatternClause,
    fixed_prefix: Bytes,
    current_prefix: Bytes,
    current_level: usize,
    max_level: usize,
    index_types: Vec<IndexType>,
    slate_iterators: Vec<SlateIterator<'a>>,
    slate: &'a slatedb::Db,
}

impl<'a> JoinIterator<'a> {
    pub fn new(join_order: Vec<Variable>, pattern: PatternClause, slate: Arc<slatedb::Db>) -> Self {
        todo!()
    }
}

impl<'a> Index for JoinIterator<'a> {
    async fn seek(&mut self, key: Bytes) -> Result<(), Error> {
        self.slate_iterators[self.current_level].seek(key).await?;
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<Bytes>, Error> {
        Ok(self.slate_iterators[self.current_level].next().await?)
    }

    fn get_value(&self) -> Result<Option<Bytes>, Error> {
        Ok(self.slate_iterators[self.current_level].get_value()?)
    }

    fn has_next(&self) -> bool {
        self.slate_iterators[self.current_level].has_next()
    }
}

// The top most level does not need to be opened
impl<'a> LayeredIndex for JoinIterator<'a> {
    async fn open_level(&mut self) -> Result<(), Error> {
        if self.current_level == self.max_level {
            return Err(anyhow::anyhow!("Max level reached"));
        }
        let index_type = self.index_types[self.current_level];
        let mut current_value;

        match self.slate_iterators[self.current_level].get_value()? {
            Some(value) => {
                current_value = remove_index_type(value);
            }
            None => {
                return Err(anyhow::anyhow!("Cannot open level, no more value at level {}", self.current_level));
            }
        }
        
        let new_index_value = add_index_type(current_value, self.index_types[self.current_level + 1]);
        self.slate_iterators.push(SlateIterator::new(&new_index_value, &self.slate).await?);
        self.current_level += 1;
        Ok(())
    }

    fn close_level(&mut self) -> Result<(), Error> {
        if self.current_level == 0 {
            return Err(anyhow::anyhow!("Min level reached"));
        }
        self.slate_iterators.pop();
        self.current_level -= 1;
        Ok(())
    }

    fn max_level(&self) -> usize {
        self.max_level
    }
}

// pub struct LeapfrogJoin {
//     pub join_order: Vec<Variable>,
//     pub iterators: Vec<JoinIterator>,
//     pub slate: Arc<slatedb::Db>,
// }

// impl LeapfrogJoin {
//     pub fn new(join_order: Vec<Variable>, iterators: Vec<PatternClause>, slate: Arc<slatedb::Db>) -> Self {
//         todo!()
//     }

//     pub fn join(&self) -> Result<Vec<Tuple>, Error> {
//         todo!()
//     }
// }