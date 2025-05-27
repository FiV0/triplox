use bytes::Bytes;
use std::cmp::Ordering;
use std::sync::Arc;

use anyhow::Error;
use slatedb::DbIterator;

use crate::algo::join::Tuple;
use crate::algo::slate_iterator::{Index, SlateIterator};
use crate::datalog::{PatternClause, Variable};
use crate::index::{add_index_type, remove_index_type, IndexType};
use crate::util::{extract_prefix, strip_prefix};

// Leapfrog Triejoin
// https://arxiv.org/pdf/1210.0481.pdf

pub(crate) trait LayeredIndex {
    async fn open_level(&mut self) -> Result<(), Error>;
    fn close_level(&mut self) -> Result<(), Error>;
    fn max_level(&self) -> usize;
}

// TODO: fully undestand lifetime passing for structs and traits
// especially why slate need the lifetime 'a here
struct JoinIterator<'a> {
    vars: Vec<Variable>,
    pattern: PatternClause,
    current_level: usize,
    max_level: usize,
    index_types: Vec<IndexType>,
    value_extractors: Vec<fn(Bytes) -> Bytes>,
    slate_iterators: Vec<SlateIterator<'a>>,
    slate: &'a slatedb::Db,
}

impl<'a> JoinIterator<'a> {
    pub fn new(join_order: Vec<Variable>, pattern: PatternClause, slate: Arc<slatedb::Db>) -> Self {
        todo!()

    }

    pub fn participates(&self, variable: &Variable) -> bool {
        self.vars.contains(variable)
    }
}

// seek and next errors returned from slatedb should not happen 
// we should just get None
impl<'a> Index for JoinIterator<'a> {
    async fn seek(&mut self, key: Bytes) -> Result<(), Error> {
        self.slate_iterators[self.current_level].seek(key).await?;
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<Bytes>, Error> {
        match self.slate_iterators[self.current_level].next().await? {
            Some(value) => {
                Ok(Some(self.value_extractors[self.current_level](value)))
            }
            None => Ok(None),
        }
    }

    fn get_value(&self) -> Result<Option<Bytes>, Error> {
        match self.slate_iterators[self.current_level].get_value()? {
            Some(value) => {
                Ok(Some(self.value_extractors[self.current_level](value)))
            }
            None => Ok(None),
        }
    }

    fn has_next(&self) -> bool {
        self.slate_iterators[self.current_level].has_next()
    }
}

//  TODO figure out how to deal with the first level
impl<'a> LayeredIndex for JoinIterator<'a> {
    async fn open_level(&mut self) -> Result<(), Error> {
        if self.current_level == self.max_level {
            return Err(anyhow::anyhow!("Max level reached"));
        }
        let index_type = self.index_types[self.current_level];
        let mut current_value;

        match self.slate_iterators[self.current_level].get_value()? {
            Some(value) => {
                // this is the index value without index type prefix
                current_value = remove_index_type(extract_prefix(value, self.current_level+1, index_type));
            }
            None => {
                return Err(anyhow::anyhow!(
                    "Cannot open level, no more value at level {}",
                    self.current_level
                ));
            }
        }

        let new_index_value =
            add_index_type(current_value, self.index_types[self.current_level + 1]);
        self.slate_iterators
            .push(SlateIterator::new(&new_index_value, &self.slate).await?);
        self.current_level += 1;
        Ok(())
    }

    fn close_level(&mut self) -> Result<(), Error> {
        if self.current_level == 0 {
            return Err(anyhow::anyhow!("Min level reached"));
        }
        self.slate_iterators.pop();
        self.current_level = self.current_level.checked_sub(1).expect("Current level must be at least 1");
        Ok(())
    }

    fn max_level(&self) -> usize {
        self.max_level
    }
}

pub struct LeapfrogJoin<'a> {
    join_order: Vec<Variable>,
    iterators: Vec<JoinIterator<'a>>,
    slate: Arc<slatedb::Db>,
}

impl<'a> LeapfrogJoin<'a> {
    pub fn new(
        join_order: Vec<Variable>,
        patterns: Vec<PatternClause>,
        slate: Arc<slatedb::Db>,
    ) -> Self {
        assert!(!join_order.is_empty(), "Join expects at least one variable!");
        todo!()
    }

    // Do we need to maintan the i here for the next intertion
    async fn next_candidate(
        &mut self,
        participants: &Vec<usize>,
    ) -> Result<Option<Bytes>, Error> {
        let mut initial_index = 0;

        let mut first_value = self.iterators[participants[initial_index]].get_value()?;
        if first_value.is_none() {
            return Ok(None);
        }
        let mut current_value = first_value.unwrap();

        let mut i = initial_index + 1;
        loop {
            if i == initial_index {
                return Ok(Some(current_value));
            }

            self.iterators[participants[i]].seek(current_value.clone()).await?;
            if let Some(value) = self.iterators[participants[i]].get_value()? {
                match value.cmp(&current_value) {
                    Ordering::Less => {
                        panic!("next_value < current_value, this should not happen!!!")
                    }
                    Ordering::Greater => {
                        current_value = value;
                        initial_index = i;
                    }
                    Ordering::Equal => (),
                }
            } else {
                return Ok(None);
            }

            i += 1;
        }
    }

    pub async fn join(&mut self) -> Result<Vec<Tuple>, Error> {
        let mut result = Vec::new();
        let mut participants = Vec::new();

        for variable in &self.join_order {
            let mut variable_particpants = Vec::new();
            for (i, iterator) in self.iterators.iter().enumerate() {
                if iterator.participates(variable) {
                    variable_particpants.push(i);
                }
            }
            participants.push(variable_particpants);
        }

        let mut variable_level = 0;
        let mut candidate = Vec::new();
        for &i in &participants[0] {
            self.iterators[i].open_level().await?;
        }

        // this is the hot loop
        loop {
            match self.next_candidate(&participants[variable_level]).await? {
                Some(value) => {
                    candidate.push(value);
                    if variable_level == self.join_order.len() - 1 {
                        result.push(candidate.clone());
                        candidate.pop();
                    } else {
                        variable_level += 1;
                        for &i in &participants[variable_level] {
                            self.iterators[i].open_level().await?;
                        }
                    }
                }
                None => {
                    match variable_level.checked_sub(1) {
                        Some(new_level) => variable_level = new_level,
                        None => break,
                    }
                    let old_level = variable_level + 1;
                    for &i in &participants[old_level] {
                        self.iterators[i].close_level()?;
                    }
                    if candidate.len() == old_level {
                        candidate.pop();
                    }
                }
            }
        }

        Ok(result)
    }
}


#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use slatedb::{Db, config::ScanOptions, config::ReadLevel, SlateDBError};


    use crate::clock::st_from_unix_epoch;
    use crate::util::create_prefix_range;
    use super::*;

    #[tokio::test]
    async fn test_indexer() -> Result<(), Error> {
        todo!()
    }
}


