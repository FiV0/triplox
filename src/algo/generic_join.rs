use anyhow::Error;
use bytes::Bytes;
use slatedb::Db;
use std::collections::HashMap;
use std::sync::Arc;

use crate::codec::index_type_to_prefix;
use crate::datalog::DataPattern;
use crate::datalog::{PatternClause, Variable};
use crate::index::{IndexIterator, IndexType, SlateIterator, VecIterator};
use crate::util::{concat_bytes, create_prefix_range, Range};

type Prefix = Vec<Bytes>;
type Extension = Bytes;

pub struct PatternPrefixExtender {
    pub join_order: Vec<Variable>,
    pub pattern: PatternClause,
    pub slate: Arc<slatedb::Db>,
    pub vars: Vec<Variable>,
    pub var_to_index: HashMap<Variable, usize>,
}

pub struct PatternPrefixExtenderIterator<'a> {
    iterator: SlateIterator<'a>,
}

impl PatternPrefixExtender {
    pub fn new(
        join_order: Vec<Variable>,
        pattern: PatternClause,
        slate: Arc<slatedb::Db>,
    ) -> Result<Self, Error> {
        let mut var_to_index = HashMap::new();
        let mut vars = pattern.variables();
        for (i, var) in join_order.iter().enumerate() {
            if vars.contains(var) {
                var_to_index.insert(var.clone(), i);
            }
        }
        vars.sort_by_key(|v: &Variable| var_to_index.get(v).unwrap());
        Ok(Self {
            join_order,
            pattern,
            slate,
            vars,
            var_to_index,
        })
    }

    pub fn participates(&self, var: Variable) -> bool {
        self.vars.contains(&var)
    }

    pub fn create_iterator(&self, prefix: &Prefix) -> Result<PatternPrefixExtenderIterator, Error> {
        todo!()
    }
}

pub trait PrefixExtender<Prefix, Extension> {
    fn count(&self, prefix: &Prefix) -> Result<u64, Error>;
    fn propose(&mut self, prefix: &Prefix) -> Result<Vec<Extension>, Error>;
    fn intersect(&mut self, prefix: &Prefix, extensions: &Vec<Extension>) -> Result<Vec<Extension>, Error>;
}

impl PrefixExtender<Prefix, Extension> for PatternPrefixExtenderIterator<'_> {
    //  all methods assume they participate in the join of the current variable
    fn count(&self, prefix: &Prefix) -> Result<u64, Error> {
        self.iterator.count()
    }

    fn propose(&mut self, prefix: &Prefix) -> Result<Vec<Extension>, Error> {
        let mut extensions = Vec::new();
        while let Some(extension) = self.iterator.next()? {
            extensions.push(extension);
        }
        Ok(extensions)
    }

    fn intersect(&mut self, prefix: &Prefix, extensions: &Vec<Extension>) -> Result<Vec<Extension>, Error> {

        let mut new_extensions = Vec::new();
        let mut ext_iter = VecIterator::new(extensions.clone());
        
        while let (Some(key1), Some(key2)) = (self.iterator.next()?, ext_iter.next()?) {
            if key1 == key2 {
                new_extensions.push(key1);
            } else if key1 < key2 {
                self.iterator.seek(key2);
            } else {
                ext_iter.seek(key1);
            }
        }
        
        Ok(new_extensions)
    }
}