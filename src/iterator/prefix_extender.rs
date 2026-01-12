use anyhow::Error;
use std::collections::HashMap;
use std::sync::Arc;

use crate::algo::generic_join::{Prefix, PrefixExtender};
use crate::datalog::{PatternClause, Variable};
use crate::index::SlateIterator;

pub struct PatternPrefixExtender {
    pub join_order: Vec<Variable>,
    pub pattern: PatternClause,
    pub slate: Arc<slatedb::Db>,
    pub vars: Vec<Variable>,
    pub var_to_index: HashMap<Variable, usize>,
}

pub struct PatternPrefixExtenderIterator {
    iterator: SlateIterator,
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

    pub fn create_iterator(
        &self,
        prefix: &Prefix,
    ) -> Result<PatternPrefixExtenderIterator, Error> {
        todo!()
    }
}

// TODO: Implement PrefixExtender for PatternPrefixExtenderIterator
// This requires adapting the slatedb-based iterator to the simplified trait
