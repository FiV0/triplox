use std::collections::HashSet;

use anyhow::{ensure, Result};
use edn::query::Variable;

mod evaluation;

pub(crate) mod function;
pub(crate) mod not;
pub(crate) mod or;
pub(crate) mod predicate;
pub(crate) mod relation;
pub(crate) mod triple;

fn ensure_unique(label: &str, variables: &[Variable]) -> Result<()> {
    let mut seen = HashSet::new();
    for variable in variables {
        ensure!(seen.insert(variable), "{label} variables repeat {variable}");
    }
    Ok(())
}
