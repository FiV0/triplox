use std::collections::HashMap;

use anyhow::{Context, Result};
use edn::query::Variable;

use crate::codec::Decode;
use crate::expr::EvalContext;
use crate::ops::DataType;
use crate::query::binding_bag::{BindingBag, BindingRow};

pub(super) fn decode_bindings(
    input: &BindingBag,
    row: &BindingRow,
    variables: &[Variable],
) -> Result<HashMap<Variable, DataType>> {
    variables
        .iter()
        .map(|variable| {
            let column = input.column_index(variable)?;
            let value = DataType::decode(&row[column])
                .with_context(|| format!("Failed to decode expression variable {variable}"))?;
            Ok((variable.clone(), value))
        })
        .collect()
}

pub(super) fn eval_context(bindings: &HashMap<Variable, DataType>) -> EvalContext<'_> {
    EvalContext::new(bindings)
}
