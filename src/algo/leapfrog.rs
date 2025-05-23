use bytes::Bytes;
use std::sync::Arc;
use std::cmp::Ordering;

use anyhow::Error;
use slatedb::DbIterator;

use crate::datalog::{Variable, PatternClause};
use crate::index::{IndexType, add_index_type, remove_index_type};
use crate::algo::join::Tuple;
use crate::algo::slate_iterator::{Index, SlateIterator};

// Leapfrog Triejoin
// https://arxiv.org/pdf/1210.0481.pdf

pub (crate) trait LayeredIndex {
    async fn open_level(&mut self) -> Result<(), Error>;
    fn close_level(&mut self) -> Result<(), Error>;
    fn max_level(&self) -> usize;
}

// TODO: fully undestand lifetime passing for structs and traits 
// especially why slate need the lifetime 'a here
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

    pub fn participates(&self, variable: &Variable) -> bool {
        self.vars.contains(variable)
    }
}

// seek and next errors returned from slatedb should only happen if 
// we have setup something incorrectly, if the iterator goes out of bounds 
// we should just get None
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

pub struct LeapfrogJoin<'a> {
    join_order: Vec<Variable>,
    iterators: Vec<JoinIterator<'a>>,
    slate: Arc<slatedb::Db>,
}

impl<'a> LeapfrogJoin<'a> {
    pub fn new(join_order: Vec<Variable>, patterns: Vec<PatternClause>, slate: Arc<slatedb::Db>) -> Self {
        assert!(join_order.len() > 0);
        todo!()
    }

    // Do we need to maintan the i here for the next intertion
    async fn next_candidate(&self, mut iterators: Vec<&mut JoinIterator<'a>>) -> Result<Option<Bytes>, Error> {
        let mut initial_index = 0;

        let mut first_value= iterators[initial_index].get_value()?;
        if first_value.is_none() {
            return Ok(None);
        }
        let mut current_value = first_value.unwrap();
        
        let mut i = initial_index + 1;
        loop {
            if i == initial_index {
                return Ok(Some(current_value));
            }

            iterators[i].seek(current_value.clone()).await?;
            if let Some(value) = iterators[i].get_value()? {
                match value.cmp(&current_value) {
                    Ordering::Less => panic!("next_value < current_value, this should not happen!!!"),
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
        let mut variable_level = 0;
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

        let mut candidate = Vec::new();
        while variable_level >= 0 {
            let mut variable_participants = Vec::new();
            for &i in &participants[variable_level] {
                variable_participants.push(&mut self.iterators[i]);
            }

            match self.next_candidate(variable_participants).await? {
                Some(value) => {
                    candidate.push(value);
                }
                None => {
                    variable_participants.iter_mut().try_for_each(|i| i.close_level())?;
                    variable_level -= 1;
                }
            }

        }

        Ok(result)
    }
}