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

fn align_pattern_clause(pattern: &PatternClause, index_type: IndexType) -> Result<Vec<DataPattern>, Error> {
    match index_type {
        IndexType::EAV => Ok(vec![pattern.entity, pattern.attribute, pattern.value]),
        IndexType::AVE => Ok(vec![pattern.attribute, pattern.value, pattern.entity]),
        IndexType::AEV => Ok(vec![pattern.attribute, pattern.entity, pattern.value]),
        _ => Err(anyhow::anyhow!("VAE index not (yet) supported"))
    }
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

    pub fn new(join_order: Vec<Variable>, pattern: PatternClause, slate: Arc<slatedb::Db>) -> Self {
        let vars = join_order.iter().map(|v| v.clone()).collect();
        let index_type = index_type_to_prefix(self::index_type);
        let var_to_index = vars.iter().enumerate().map(|(i, v)| (v.clone(), i)).collect();
        Self { join_order, pattern, slate, vars, index_type, var_to_index }
    }

    pub fn participates(&self, var: Variable) -> bool {
        self.vars.contains(&var)
    }


    fn create_range(&self, prefix: &Prefix) -> Range {
        let index_prefix = index_type_to_prefix(self.index_type);
        let patterns = align_pattern_clause(&self.pattern, self.index_type);
        let mut prefix = vec![&[index_prefix]];
        for pattern in patterns {
            prefix.push(pattern);
        }





        let mut range = create_prefix_range(prefix);
    }

    pub async fn count(&self, prefix: &Prefix) -> u64 {
        0
        // let mut count = 0;
        // let mut iter = self.slate.scan_with_options(prefix, &slatedb::config::ScanOptions::default()).await.unwrap();
        // while let Some(kv) = iter.next().await.unwrap() {
        //     count += 1;
        // }
        // count
    }
}


