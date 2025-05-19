use bytes::Bytes;
use std::sync::Arc;
use slatedb::Db;
use std::collections::HashMap;
use anyhow::Error;

use crate::datalog::{Variable, PatternClause};
use crate::util::{create_prefix_range, concat_bytes, Range};
use crate::codec::index_type_to_prefix;
use crate::index::IndexType;
use crate::datalog::DataPattern;

type Prefix = Vec<Bytes>;
type Extension = Bytes;

pub trait PrefixExtender<Prefix, Extension> {
    fn count(&self, prefix:&Prefix) -> u64;
    fn propose(&self, prefix:&Prefix) -> Vec<Extension>;
    fn intersect(&self, prefix:&Prefix, extensions:&mut Vec<Extension>);
}


pub struct PatternPrefixExtender {
    pub join_order: Vec<Variable>,
    pub pattern: PatternClause,
    pub slate: Arc<slatedb::Db>,
    pub vars: Vec<Variable>,
    pub index_type: IndexType,
    pub var_to_index: HashMap<Variable, usize>
}

impl PatternPrefixExtender {

    pub fn new(join_order: Vec<Variable>, pattern: PatternClause, slate: Arc<slatedb::Db>) -> Result<Self, Error> {
        let index_type = pattern.index_type(join_order.clone())?;
        let mut var_to_index = HashMap::new();
        let mut vars = pattern.variables();
        for (i, var) in join_order.iter().enumerate() {
            if vars.contains(var) {
                var_to_index.insert(var.clone(), i);
            }
        }
        vars.sort_by_key(|v : &Variable|  var_to_index.get(v).unwrap());
        Ok(Self { join_order, pattern, slate, vars, index_type, var_to_index })
    }

    pub fn participates(&self, var: Variable) -> bool {
        self.vars.contains(&var)
    }


    fn create_range(&self, prefix: &Prefix) -> Result<Range, Error> {
        panic!("Not implemented");

        // let index_prefix = index_type_to_prefix(self.index_type)?;
        // let patterns = self.pattern.align_pattern_clause(self.index_type)?;
        // let mut prefix = vec![&[index_prefix]];

        // for var in self.vars.iter() {
        //     if self.var_to_index[var] < prefix.len() {
        //         prefix.push(patterns[self.var_to_index[var]].pattern_prefix(self.index_type)?);
        //     }
        // }





        // let mut range = create_prefix_range(prefix);
    }

    pub async fn count(&self, prefix: &Prefix) -> u64 {
        panic!("Not implemented");
        // let mut count = 0;
        // let mut iter = self.slate.scan_with_options(prefix, &slatedb::config::ScanOptions::default()).await.unwrap();
        // while let Some(kv) = iter.next().await.unwrap() {
        //     count += 1;
        // }
        // count
    }
}


