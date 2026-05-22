use dbsp::{DynZWeight, OrdWSet, OrdZSet, RootCircuit, Stream, ZWeight};

use crate::inc_query::{IncrementalQueryPlan, PatternPlan, PatternSlot};
use crate::incremental::{EncodedRow, EncodedTriple};

pub(crate) type RowZSet = OrdWSet<EncodedRow, ZWeight, DynZWeight>;

pub(crate) fn pattern_stream(
    input: &Stream<RootCircuit, OrdZSet<EncodedTriple>>,
    pattern: PatternPlan,
) -> Stream<RootCircuit, RowZSet> {
    input.flat_map(move |triple| pattern_row(&pattern, triple))
}

pub(crate) fn query_row_stream(
    input: &Stream<RootCircuit, OrdZSet<EncodedTriple>>,
    plan: IncrementalQueryPlan,
) -> Stream<RootCircuit, RowZSet> {
    let mut stream = pattern_stream(input, plan.patterns[0].clone());
    let mut current_vars = plan.patterns[0].output_vars.clone();

    for pattern in plan.patterns.iter().skip(1) {
        let right = pattern_stream(input, pattern.clone());
        stream = join_rows(stream, right, &current_vars, &pattern.output_vars);
        current_vars = merge_vars(&current_vars, &pattern.output_vars);
    }

    stream
}

fn join_rows(
    left: Stream<RootCircuit, RowZSet>,
    right: Stream<RootCircuit, RowZSet>,
    left_vars: &[edn::query::Variable],
    right_vars: &[edn::query::Variable],
) -> Stream<RootCircuit, RowZSet> {
    let output_vars = merge_vars(left_vars, right_vars);
    let key_vars = left_vars
        .iter()
        .filter(|var| right_vars.contains(*var))
        .cloned()
        .collect::<Vec<_>>();
    let left_key_positions = positions(left_vars, &key_vars);
    let right_key_positions = positions(right_vars, &key_vars);
    let output_sources = output_vars
        .iter()
        .map(|var| {
            left_vars
                .iter()
                .position(|left_var| left_var == var)
                .map(RowSource::Left)
                .unwrap_or_else(|| {
                    RowSource::Right(
                        right_vars
                            .iter()
                            .position(|right_var| right_var == var)
                            .expect("output var must come from one join side"),
                    )
                })
        })
        .collect::<Vec<_>>();

    let left_indexed = left.map_index(move |row| (row_key(row, &left_key_positions), row.clone()));
    let right_indexed =
        right.map_index(move |row| (row_key(row, &right_key_positions), row.clone()));

    left_indexed
        .stream_join(&right_indexed, move |_key, left_row, right_row| {
            merge_rows(left_row, right_row, &output_sources)
        })
        .map(|row| row.clone())
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

#[derive(Clone)]
enum RowSource {
    Left(usize),
    Right(usize),
}

fn merge_vars(
    left_vars: &[edn::query::Variable],
    right_vars: &[edn::query::Variable],
) -> Vec<edn::query::Variable> {
    let mut output_vars = left_vars.to_vec();
    for var in right_vars {
        if !output_vars.contains(var) {
            output_vars.push(var.clone());
        }
    }
    output_vars
}

fn positions(vars: &[edn::query::Variable], selected: &[edn::query::Variable]) -> Vec<usize> {
    selected
        .iter()
        .map(|selected_var| {
            vars.iter()
                .position(|var| var == selected_var)
                .expect("selected var must be present")
        })
        .collect()
}

fn row_key(row: &EncodedRow, positions: &[usize]) -> EncodedRow {
    positions
        .iter()
        .map(|position| row[*position].clone())
        .collect()
}

fn merge_rows(left: &EncodedRow, right: &EncodedRow, sources: &[RowSource]) -> EncodedRow {
    sources
        .iter()
        .map(|source| match source {
            RowSource::Left(position) => left[*position].clone(),
            RowSource::Right(position) => right[*position].clone(),
        })
        .collect()
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
    use crate::inc_query::{IncrementalQueryPlan, JoinKind, JoinPlan, PatternSlot};
    use crate::ops::DataType;

    fn build_pattern_circuit(
        circuit: &mut RootCircuit,
        pattern: PatternPlan,
    ) -> anyhow::Result<(ZSetHandle<EncodedTriple>, OutputHandle<RowZSet>)> {
        let (input, handle) = circuit.add_input_zset::<EncodedTriple>();
        let rows = pattern_stream(&input, pattern);
        Ok((handle, rows.output()))
    }

    fn build_query_circuit(
        circuit: &mut RootCircuit,
        plan: IncrementalQueryPlan,
    ) -> anyhow::Result<(ZSetHandle<EncodedTriple>, OutputHandle<RowZSet>)> {
        let (input, handle) = circuit.add_input_zset::<EncodedTriple>();
        let rows = query_row_stream(&input, plan);
        Ok((handle, rows.output()))
    }

    fn collect_rows(output: &OutputHandle<RowZSet>) -> Vec<(EncodedRow, ZWeight)> {
        let mut rows = output
            .consolidate()
            .iter()
            .map(|(row, (), weight)| (row, weight))
            .collect::<Vec<_>>();
        rows.sort();
        rows
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

    fn entity_join_plan() -> IncrementalQueryPlan {
        let patterns = vec![
            PatternPlan {
                attribute: 10,
                entity: PatternSlot::Variable("?e".to_var()),
                value: PatternSlot::Variable("?name".to_var()),
                output_vars: vec!["?e".to_var(), "?name".to_var()],
            },
            PatternPlan {
                attribute: 11,
                entity: PatternSlot::Variable("?e".to_var()),
                value: PatternSlot::Variable("?age".to_var()),
                output_vars: vec!["?e".to_var(), "?age".to_var()],
            },
        ];
        IncrementalQueryPlan {
            find_vars: vec!["?name".to_var(), "?age".to_var()],
            variables: vec!["?e".to_var(), "?name".to_var(), "?age".to_var()],
            joins: vec![JoinPlan {
                kind: JoinKind::Keyed,
                left_vars: patterns[0].output_vars.clone(),
                right_vars: patterns[1].output_vars.clone(),
                key_vars: vec!["?e".to_var()],
                output_vars: vec!["?e".to_var(), "?name".to_var(), "?age".to_var()],
            }],
            patterns,
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

    #[test]
    fn joins_two_patterns_on_entity() {
        let (circuit, (handle, output)) =
            RootCircuit::build(|circuit| build_query_circuit(circuit, entity_join_plan())).unwrap();

        append(
            &handle,
            [
                (triple(42, 10, DataType::String("Alice".to_string())), 1),
                (triple(42, 11, DataType::Long(30)), 1),
                (triple(43, 10, DataType::String("Bob".to_string())), 1),
            ],
        );
        circuit.transaction().unwrap();

        assert_eq!(
            collect_rows(&output),
            vec![(
                vec![
                    DataType::Long(42).encode(),
                    DataType::String("Alice".to_string()).encode(),
                    DataType::Long(30).encode(),
                ],
                1,
            )]
        );
    }

    #[test]
    fn joins_through_ref_value() {
        let patterns = vec![
            PatternPlan {
                attribute: 12,
                entity: PatternSlot::Variable("?e".to_var()),
                value: PatternSlot::Variable("?friend".to_var()),
                output_vars: vec!["?e".to_var(), "?friend".to_var()],
            },
            PatternPlan {
                attribute: 10,
                entity: PatternSlot::Variable("?friend".to_var()),
                value: PatternSlot::Variable("?friend-name".to_var()),
                output_vars: vec!["?friend".to_var(), "?friend-name".to_var()],
            },
        ];
        let plan = IncrementalQueryPlan {
            find_vars: vec!["?friend-name".to_var()],
            variables: vec!["?e".to_var(), "?friend".to_var(), "?friend-name".to_var()],
            joins: vec![JoinPlan {
                kind: JoinKind::Keyed,
                left_vars: patterns[0].output_vars.clone(),
                right_vars: patterns[1].output_vars.clone(),
                key_vars: vec!["?friend".to_var()],
                output_vars: vec!["?e".to_var(), "?friend".to_var(), "?friend-name".to_var()],
            }],
            patterns,
        };
        let (circuit, (handle, output)) =
            RootCircuit::build(|circuit| build_query_circuit(circuit, plan)).unwrap();

        append(
            &handle,
            [
                (triple(1, 12, DataType::Long(2)), 1),
                (triple(2, 10, DataType::String("Bob".to_string())), 1),
            ],
        );
        circuit.transaction().unwrap();

        assert_eq!(
            collect_rows(&output),
            vec![(
                vec![
                    DataType::Long(1).encode(),
                    DataType::Long(2).encode(),
                    DataType::String("Bob".to_string()).encode(),
                ],
                1,
            )]
        );
    }

    #[test]
    fn joins_three_pattern_chain() {
        let mut plan = entity_join_plan();
        plan.patterns.push(PatternPlan {
            attribute: 12,
            entity: PatternSlot::Variable("?e".to_var()),
            value: PatternSlot::Variable("?friend".to_var()),
            output_vars: vec!["?e".to_var(), "?friend".to_var()],
        });
        plan.joins.push(JoinPlan {
            kind: JoinKind::Keyed,
            left_vars: vec!["?e".to_var(), "?name".to_var(), "?age".to_var()],
            right_vars: vec!["?e".to_var(), "?friend".to_var()],
            key_vars: vec!["?e".to_var()],
            output_vars: vec![
                "?e".to_var(),
                "?name".to_var(),
                "?age".to_var(),
                "?friend".to_var(),
            ],
        });
        plan.variables.push("?friend".to_var());
        let (circuit, (handle, output)) =
            RootCircuit::build(|circuit| build_query_circuit(circuit, plan)).unwrap();

        append(
            &handle,
            [
                (triple(42, 10, DataType::String("Alice".to_string())), 1),
                (triple(42, 11, DataType::Long(30)), 1),
                (triple(42, 12, DataType::Long(43)), 1),
            ],
        );
        circuit.transaction().unwrap();

        assert_eq!(
            collect_rows(&output),
            vec![(
                vec![
                    DataType::Long(42).encode(),
                    DataType::String("Alice".to_string()).encode(),
                    DataType::Long(30).encode(),
                    DataType::Long(43).encode(),
                ],
                1,
            )]
        );
    }

    #[test]
    fn cartesian_product_when_patterns_share_no_variables() {
        let patterns = vec![
            PatternPlan {
                attribute: 10,
                entity: PatternSlot::Variable("?e".to_var()),
                value: PatternSlot::Variable("?name".to_var()),
                output_vars: vec!["?e".to_var(), "?name".to_var()],
            },
            PatternPlan {
                attribute: 11,
                entity: PatternSlot::Variable("?other".to_var()),
                value: PatternSlot::Variable("?age".to_var()),
                output_vars: vec!["?other".to_var(), "?age".to_var()],
            },
        ];
        let plan = IncrementalQueryPlan {
            find_vars: vec!["?name".to_var(), "?age".to_var()],
            variables: vec![
                "?e".to_var(),
                "?name".to_var(),
                "?other".to_var(),
                "?age".to_var(),
            ],
            joins: vec![JoinPlan {
                kind: JoinKind::Cartesian,
                left_vars: patterns[0].output_vars.clone(),
                right_vars: patterns[1].output_vars.clone(),
                key_vars: vec![],
                output_vars: vec![
                    "?e".to_var(),
                    "?name".to_var(),
                    "?other".to_var(),
                    "?age".to_var(),
                ],
            }],
            patterns,
        };
        let (circuit, (handle, output)) =
            RootCircuit::build(|circuit| build_query_circuit(circuit, plan)).unwrap();

        append(
            &handle,
            [
                (triple(1, 10, DataType::String("Alice".to_string())), 1),
                (triple(2, 10, DataType::String("Bob".to_string())), 1),
                (triple(3, 11, DataType::Long(30)), 1),
            ],
        );
        circuit.transaction().unwrap();

        assert_eq!(
            collect_rows(&output),
            vec![
                (
                    vec![
                        DataType::Long(2).encode(),
                        DataType::String("Bob".to_string()).encode(),
                        DataType::Long(3).encode(),
                        DataType::Long(30).encode(),
                    ],
                    1,
                ),
                (
                    vec![
                        DataType::Long(1).encode(),
                        DataType::String("Alice".to_string()).encode(),
                        DataType::Long(3).encode(),
                        DataType::Long(30).encode(),
                    ],
                    1,
                )
            ]
        );
    }
}
