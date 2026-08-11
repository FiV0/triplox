use std::collections::HashMap;

use anyhow::{Context, Result};
use edn::query::Variable;

use crate::codec::Decode;
use crate::ops::DataType;
use crate::query::binding_bag::{BindingBag, BindingRow};

pub(super) fn binding_positions(
    input: &BindingBag,
    variables: &[Variable],
) -> Result<Vec<(Variable, usize)>> {
    variables
        .iter()
        .map(|variable| Ok((variable.clone(), input.column_index(variable)?)))
        .collect()
}

pub(super) fn update_bindings(
    row: &BindingRow,
    binding_positions: &[(Variable, usize)],
    bindings: &mut HashMap<Variable, DataType>,
) -> Result<()> {
    for (variable, column) in binding_positions {
        let value = DataType::decode(&row[*column])
            .with_context(|| format!("Failed to decode expression variable {variable}"))?;
        // Clone keys only for the first row; later rows overwrite decoded values.
        if let Some(binding) = bindings.get_mut(variable) {
            *binding = value;
        } else {
            bindings.insert(variable.clone(), value);
        }
    }
    Ok(())
}
