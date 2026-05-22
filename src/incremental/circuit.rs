use dbsp::{DynZWeight, OrdWSet, OrdZSet, RootCircuit, Stream, ZWeight};

use crate::inc_query::{PatternPlan, PatternSlot};
use crate::incremental::{EncodedRow, EncodedTriple};

pub(crate) type RowZSet = OrdWSet<EncodedRow, ZWeight, DynZWeight>;

pub(crate) fn pattern_stream(
    input: &Stream<RootCircuit, OrdZSet<EncodedTriple>>,
    pattern: PatternPlan,
) -> Stream<RootCircuit, RowZSet> {
    input.flat_map(move |triple| pattern_row(&pattern, triple))
}

fn pattern_row(pattern: &PatternPlan, triple: &EncodedTriple) -> Option<EncodedRow> {
    if triple.attribute != pattern.attribute {
        return None;
    }
    if !slot_matches(&pattern.entity, &triple.entity) {
        return None;
    }
    if !slot_matches(&pattern.value, &triple.value) {
        return None;
    }

    Some(
        pattern
            .output_vars
            .iter()
            .filter_map(|var| {
                if matches!(&pattern.entity, PatternSlot::Variable(entity_var) if entity_var == var)
                {
                    Some(triple.entity.clone())
                } else if matches!(&pattern.value, PatternSlot::Variable(value_var) if value_var == var)
                {
                    Some(triple.value.clone())
                } else {
                    None
                }
            })
            .collect(),
    )
}

fn slot_matches(slot: &PatternSlot, value: &[u8]) -> bool {
    match slot {
        PatternSlot::Variable(_) | PatternSlot::Placeholder => true,
        PatternSlot::Constant(constant) => constant.as_slice() == value,
    }
}

#[cfg(test)]
mod tests {
    use dbsp::{
        typed_batch::IndexedZSetReader, utils::Tup2, OrdZSet, OutputHandle, RootCircuit,
        ZSetHandle, ZWeight,
    };
    use edn::query::ToVariable;

    use super::*;
    use crate::codec::Encode;
    use crate::inc_query::PatternSlot;
    use crate::ops::DataType;

    fn build_pattern_circuit(
        circuit: &mut RootCircuit,
        pattern: PatternPlan,
    ) -> anyhow::Result<(ZSetHandle<EncodedTriple>, OutputHandle<RowZSet>)> {
        let (input, handle) = circuit.add_input_zset::<EncodedTriple>();
        let rows = pattern_stream(&input, pattern);
        Ok((handle, rows.output()))
    }

    fn collect_rows(output: &OutputHandle<RowZSet>) -> Vec<(EncodedRow, ZWeight)> {
        output
            .consolidate()
            .iter()
            .map(|(row, (), weight)| (row, weight))
            .collect()
    }

    fn append(
        handle: &ZSetHandle<EncodedTriple>,
        triples: impl IntoIterator<Item = (EncodedTriple, ZWeight)>,
    ) {
        let mut batch = triples
            .into_iter()
            .map(|(triple, weight)| Tup2(triple, weight))
            .collect::<Vec<_>>();
        handle.append(&mut batch);
    }

    fn triple(entity: i64, attribute: i64, value: DataType) -> EncodedTriple {
        EncodedTriple {
            entity: DataType::Long(entity).encode(),
            attribute,
            value: value.encode(),
        }
    }

    fn single_var_pattern() -> PatternPlan {
        PatternPlan {
            attribute: 10,
            entity: PatternSlot::Variable("?e".to_var()),
            value: PatternSlot::Variable("?name".to_var()),
            output_vars: vec!["?e".to_var(), "?name".to_var()],
        }
    }

    #[test]
    fn single_pattern_emits_matching_add() {
        let (circuit, (handle, output)) =
            RootCircuit::build(|circuit| build_pattern_circuit(circuit, single_var_pattern()))
                .unwrap();

        append(
            &handle,
            [(triple(42, 10, DataType::String("Alice".to_string())), 1)],
        );
        circuit.transaction().unwrap();

        assert_eq!(
            collect_rows(&output),
            vec![(
                vec![
                    DataType::Long(42).encode(),
                    DataType::String("Alice".to_string()).encode()
                ],
                1,
            )]
        );
    }

    #[test]
    fn single_pattern_emits_matching_retract() {
        let (circuit, (handle, output)) =
            RootCircuit::build(|circuit| build_pattern_circuit(circuit, single_var_pattern()))
                .unwrap();

        append(
            &handle,
            [(triple(42, 10, DataType::String("Alice".to_string())), -1)],
        );
        circuit.transaction().unwrap();

        assert_eq!(
            collect_rows(&output),
            vec![(
                vec![
                    DataType::Long(42).encode(),
                    DataType::String("Alice".to_string()).encode()
                ],
                -1,
            )]
        );
    }

    #[test]
    fn pattern_filters_constants() {
        let pattern = PatternPlan {
            attribute: 10,
            entity: PatternSlot::Variable("?e".to_var()),
            value: PatternSlot::Constant(DataType::String("Alice".to_string()).encode()),
            output_vars: vec!["?e".to_var()],
        };
        let (circuit, (handle, output)) =
            RootCircuit::build(|circuit| build_pattern_circuit(circuit, pattern)).unwrap();

        append(
            &handle,
            [
                (triple(42, 10, DataType::String("Alice".to_string())), 1),
                (triple(43, 10, DataType::String("Bob".to_string())), 1),
                (triple(44, 11, DataType::String("Alice".to_string())), 1),
            ],
        );
        circuit.transaction().unwrap();

        assert_eq!(
            collect_rows(&output),
            vec![(vec![DataType::Long(42).encode()], 1)]
        );
    }

    #[test]
    fn pattern_omits_placeholders() {
        let pattern = PatternPlan {
            attribute: 10,
            entity: PatternSlot::Placeholder,
            value: PatternSlot::Variable("?name".to_var()),
            output_vars: vec!["?name".to_var()],
        };
        let (circuit, (handle, output)) =
            RootCircuit::build(|circuit| build_pattern_circuit(circuit, pattern)).unwrap();

        append(
            &handle,
            [(triple(42, 10, DataType::String("Alice".to_string())), 1)],
        );
        circuit.transaction().unwrap();

        assert_eq!(
            collect_rows(&output),
            vec![(vec![DataType::String("Alice".to_string()).encode()], 1,)]
        );
    }
}
