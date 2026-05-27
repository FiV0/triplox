use std::path::Path;

use anyhow::{anyhow, Result};
use dbsp::circuit::{
    CircuitConfig, CircuitStorageConfig, StorageCacheConfig, StorageConfig, StorageOptions,
};
use dbsp::{
    typed_batch::IndexedZSetReader, utils::Tup2, DBSPHandle, DynZWeight, OrdWSet, OrdZSet,
    OutputHandle, RootCircuit, Runtime, Stream, ZSetHandle, ZWeight,
};
use edn::query::Variable;

use crate::codec::Decode;
use crate::inc_query::{IncrementalQueryPlan, JoinPlan, PatternPlan, PatternSlot};
use crate::incremental::{EncodedRow, EncodedTriple};
use crate::ops::DataType;

pub(crate) type RowZSet = OrdWSet<EncodedRow, ZWeight, DynZWeight>;

// Builds the file-backed DBSP runtime configuration for a single query circuit.
fn storage_circuit_config(storage_path: &Path) -> Result<CircuitConfig> {
    if storage_path.exists() {
        std::fs::remove_dir_all(storage_path)?;
    }
    std::fs::create_dir_all(storage_path)?;
    let storage = CircuitStorageConfig::for_config(
        StorageConfig {
            path: storage_path.to_string_lossy().into_owned(),
            cache: StorageCacheConfig::default(),
        },
        StorageOptions {
            min_storage_bytes: Some(0),
            ..StorageOptions::default()
        },
    )
    .map_err(anyhow::Error::from)?;

    Ok(CircuitConfig::with_workers(1).with_storage(Some(storage)))
}

// Checks whether a pattern slot accepts the encoded triple value.
fn slot_matches(slot: &PatternSlot, value: &[u8]) -> bool {
    match slot {
        PatternSlot::Variable(_) => true,
        PatternSlot::Constant(constant) => constant.as_slice() == value,
    }
}

// Converts one matching encoded triple into the row shape requested by a pattern.
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

// Finds the positions of selected variables in a row variable order.
fn positions(vars: &[Variable], selected: &[Variable]) -> Vec<usize> {
    selected
        .iter()
        .map(|selected_var| {
            vars.iter()
                .position(|var| var == selected_var)
                .expect("selected var must be present")
        })
        .collect()
}

// Extracts an encoded join key from a row.
fn row_key(row: &EncodedRow, positions: &[usize]) -> EncodedRow {
    positions
        .iter()
        .map(|position| row[*position].clone())
        .collect()
}

// Projects a row to the requested output positions.
fn project_row(row: &EncodedRow, positions: &[usize]) -> EncodedRow {
    positions
        .iter()
        .map(|position| row[*position].clone())
        .collect()
}

#[derive(Clone)]
enum RowSource {
    Left(usize),
    Right(usize),
}

// Merges joined left and right rows into the planned output row order.
fn merge_rows(left: &EncodedRow, right: &EncodedRow, sources: &[RowSource]) -> EncodedRow {
    sources
        .iter()
        .map(|source| match source {
            RowSource::Left(position) => left[*position].clone(),
            RowSource::Right(position) => right[*position].clone(),
        })
        .collect()
}

// TODO: This filtering should happen at storage level. See #329
// Creates the DBSP stream of rows matching one planned triple pattern.
pub(crate) fn pattern_stream(
    input: &Stream<RootCircuit, OrdZSet<EncodedTriple>>,
    pattern: PatternPlan,
) -> Stream<RootCircuit, RowZSet> {
    input.flat_map(move |triple| pattern_row(&pattern, triple))
}

// Joins two row streams according to one planned join step.
fn join_rows(
    left: Stream<RootCircuit, RowZSet>,
    right: Stream<RootCircuit, RowZSet>,
    join: &JoinPlan,
) -> Stream<RootCircuit, RowZSet> {
    let left_key_positions = positions(&join.left_vars, &join.key_vars);
    let right_key_positions = positions(&join.right_vars, &join.key_vars);
    let output_sources = join
        .output_vars
        .iter()
        .map(|var| {
            join.left_vars
                .iter()
                .position(|left_var| left_var == var)
                .map(RowSource::Left)
                .unwrap_or_else(|| {
                    RowSource::Right(
                        join.right_vars
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

    left_indexed.join(&right_indexed, move |_key, left_row, right_row| {
        merge_rows(left_row, right_row, &output_sources)
    })
}

// Creates the DBSP stream of joined rows for the whole incremental query plan.
pub(crate) fn query_row_stream(
    input: &Stream<RootCircuit, OrdZSet<EncodedTriple>>,
    plan: IncrementalQueryPlan,
) -> Stream<RootCircuit, RowZSet> {
    let patterns = plan.patterns;
    let joins = plan.joins;
    let mut stream = pattern_stream(input, patterns[0].clone());

    for join in joins {
        let right = pattern_stream(input, patterns[join.right_pattern_index].clone());
        stream = join_rows(stream, right, &join);
    }

    stream
}

// Creates the DBSP stream of rows projected to the query find variables.
pub(crate) fn query_find_stream(
    input: &Stream<RootCircuit, OrdZSet<EncodedTriple>>,
    plan: IncrementalQueryPlan,
) -> Stream<RootCircuit, RowZSet> {
    let output_vars = plan
        .joins
        .last()
        .map(|join| &join.output_vars)
        .unwrap_or(&plan.patterns[0].output_vars);
    let find_positions = positions(output_vars, &plan.find_vars);
    query_row_stream(input, plan).map(move |row| project_row(row, &find_positions))
}

// Decodes a DBSP output batch into user-facing values and signed weights.
pub(crate) fn decode_output_rows(batch: &RowZSet) -> Result<Vec<(Vec<DataType>, isize)>> {
    batch
        .iter()
        .map(|(row, (), weight)| {
            let decoded = row
                .iter()
                .map(|value| DataType::decode(value).map_err(anyhow::Error::from))
                .collect::<Result<Vec<_>>>()?;
            let weight = isize::try_from(weight)
                .map_err(|_| anyhow!("DBSP weight {} does not fit in isize", weight))?;
            Ok((decoded, weight))
        })
        .collect()
}

pub(super) struct QueryCircuit {
    _circuit: DBSPHandle,
    _input: ZSetHandle<EncodedTriple>,
    _output: OutputHandle<RowZSet>,
}

impl QueryCircuit {
    // Builds a DBSP circuit for one incremental query plan.
    pub(super) fn build(plan: IncrementalQueryPlan, storage_path: &Path) -> Result<Self> {
        let config = storage_circuit_config(storage_path)?;
        let (circuit, (input, output)) = Runtime::init_circuit(config, move |circuit| {
            let (input, handle) = circuit.add_input_zset::<EncodedTriple>();
            let rows = query_find_stream(&input, plan);
            Ok((handle, rows.output()))
        })
        .map_err(anyhow::Error::from)?;

        Ok(Self {
            _circuit: circuit,
            _input: input,
            _output: output,
        })
    }

    // Loads the initial snapshot into the circuit and discards the bootstrap output.
    pub(super) fn prime(
        &mut self,
        mut initial_triples: Vec<Tup2<EncodedTriple, ZWeight>>,
    ) -> Result<()> {
        self._input.append(&mut initial_triples);
        self._circuit.transaction().map_err(anyhow::Error::from)?;
        let _ = self._output.consolidate();
        Ok(())
    }

    // Applies one weighted triple batch and returns the decoded query result delta.
    pub(super) fn apply(
        &mut self,
        mut triples: Vec<Tup2<EncodedTriple, ZWeight>>,
    ) -> Result<Vec<(Vec<DataType>, isize)>> {
        self._input.append(&mut triples);
        self._circuit.transaction().map_err(anyhow::Error::from)?;
        decode_output_rows(&self._output.consolidate())
    }
}

#[cfg(test)]
mod tests {
    use dbsp::{
        utils::Tup2, DBSPHandle, OrdZSet, OutputHandle, RootCircuit, Runtime, ZSetHandle, ZWeight,
    };
    use edn::query::ToVariable;
    use tempfile::TempDir;

    use super::*;
    use crate::codec::Encode;
    use crate::inc_query::{IncrementalQueryPlan, JoinPlan, PatternSlot};
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

    fn build_find_circuit(
        circuit: &mut RootCircuit,
        plan: IncrementalQueryPlan,
    ) -> anyhow::Result<(ZSetHandle<EncodedTriple>, OutputHandle<RowZSet>)> {
        let (input, handle) = circuit.add_input_zset::<EncodedTriple>();
        let rows = query_find_stream(&input, plan);
        Ok((handle, rows.output()))
    }

    fn build_test_circuit<T, F>(constructor: F) -> (DBSPHandle, T, TempDir)
    where
        T: Send + 'static,
        F: FnOnce(&mut RootCircuit) -> anyhow::Result<T> + Clone + Send + 'static,
    {
        let storage = tempfile::tempdir().unwrap();
        let (circuit, handles) =
            Runtime::init_circuit(storage_circuit_config(storage.path()).unwrap(), constructor)
                .unwrap();
        (circuit, handles, storage)
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

    fn path_has_entries(path: &Path) -> bool {
        std::fs::read_dir(path)
            .unwrap()
            .next()
            .transpose()
            .unwrap()
            .is_some()
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
                right_pattern_index: 1,
                left_vars: patterns[0].output_vars.clone(),
                right_vars: patterns[1].output_vars.clone(),
                key_vars: vec!["?e".to_var()],
                output_vars: vec!["?e".to_var(), "?name".to_var(), "?age".to_var()],
            }],
            patterns,
        }
    }

    fn reordered_join_plan() -> IncrementalQueryPlan {
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
            PatternPlan {
                attribute: 12,
                entity: PatternSlot::Variable("?e".to_var()),
                value: PatternSlot::Variable("?friend".to_var()),
                output_vars: vec!["?e".to_var(), "?friend".to_var()],
            },
        ];
        IncrementalQueryPlan {
            find_vars: vec!["?friend".to_var(), "?age".to_var()],
            variables: vec![
                "?e".to_var(),
                "?name".to_var(),
                "?age".to_var(),
                "?friend".to_var(),
            ],
            joins: vec![
                JoinPlan {
                    right_pattern_index: 2,
                    left_vars: patterns[0].output_vars.clone(),
                    right_vars: patterns[2].output_vars.clone(),
                    key_vars: vec!["?e".to_var()],
                    output_vars: vec!["?e".to_var(), "?name".to_var(), "?friend".to_var()],
                },
                JoinPlan {
                    right_pattern_index: 1,
                    left_vars: vec!["?e".to_var(), "?name".to_var(), "?friend".to_var()],
                    right_vars: patterns[1].output_vars.clone(),
                    key_vars: vec!["?e".to_var()],
                    output_vars: vec![
                        "?e".to_var(),
                        "?name".to_var(),
                        "?friend".to_var(),
                        "?age".to_var(),
                    ],
                },
            ],
            patterns,
        }
    }

    #[test]
    fn query_circuit_uses_file_backed_storage_root() {
        let dir = tempfile::tempdir().unwrap();
        let storage_path = dir.path().join("query-1");
        std::fs::create_dir_all(&storage_path).unwrap();
        std::fs::write(storage_path.join("stale"), b"stale").unwrap();

        let mut circuit = QueryCircuit::build(entity_join_plan(), &storage_path).unwrap();
        circuit
            .prime(vec![
                Tup2(triple(42, 10, DataType::String("Alice".to_string())), 1),
                Tup2(triple(42, 11, DataType::Long(30)), 1),
            ])
            .unwrap();

        assert!(storage_path.exists());
        assert!(!storage_path.join("stale").exists());
        assert!(path_has_entries(&storage_path));
    }

    #[test]
    fn single_pattern_emits_matching_add() {
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(|circuit| build_pattern_circuit(circuit, single_var_pattern()));

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
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(|circuit| build_pattern_circuit(circuit, single_var_pattern()));

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
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_pattern_circuit(circuit, pattern.clone()));

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
    fn joins_two_patterns_on_entity() {
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(|circuit| build_query_circuit(circuit, entity_join_plan()));

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
    fn query_rows_follow_join_plan_order() {
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(|circuit| build_query_circuit(circuit, reordered_join_plan()));

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
                    DataType::Long(43).encode(),
                    DataType::Long(30).encode(),
                ],
                1,
            )]
        );
    }

    #[test]
    fn find_projection_uses_planned_output_order() {
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(|circuit| build_find_circuit(circuit, reordered_join_plan()));

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
            decode_output_rows(&output.consolidate()).unwrap(),
            vec![(vec![DataType::Long(43), DataType::Long(30)], 1)]
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
                right_pattern_index: 1,
                left_vars: patterns[0].output_vars.clone(),
                right_vars: patterns[1].output_vars.clone(),
                key_vars: vec!["?friend".to_var()],
                output_vars: vec!["?e".to_var(), "?friend".to_var(), "?friend-name".to_var()],
            }],
            patterns,
        };
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_query_circuit(circuit, plan.clone()));

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
            right_pattern_index: 2,
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
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_query_circuit(circuit, plan.clone()));

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
                right_pattern_index: 1,
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
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_query_circuit(circuit, plan.clone()));

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

    #[test]
    fn projects_rows_to_find_order_and_decodes() {
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(|circuit| build_find_circuit(circuit, entity_join_plan()));

        append(
            &handle,
            [
                (triple(42, 10, DataType::String("Alice".to_string())), 1),
                (triple(42, 11, DataType::Long(30)), 1),
            ],
        );
        circuit.transaction().unwrap();

        assert_eq!(
            decode_output_rows(&output.consolidate()).unwrap(),
            vec![(
                vec![DataType::String("Alice".to_string()), DataType::Long(30)],
                1
            )]
        );
    }

    #[test]
    fn preserves_negative_delta_weights() {
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(|circuit| build_find_circuit(circuit, entity_join_plan()));

        append(
            &handle,
            [
                (triple(42, 10, DataType::String("Alice".to_string())), -1),
                (triple(42, 11, DataType::Long(30)), 1),
            ],
        );
        circuit.transaction().unwrap();

        assert_eq!(
            decode_output_rows(&output.consolidate()).unwrap(),
            vec![(
                vec![DataType::String("Alice".to_string()), DataType::Long(30)],
                -1
            )]
        );
    }

    #[test]
    fn empty_steps_decode_to_no_rows() {
        let (mut circuit, (_handle, output), _storage) =
            build_test_circuit(|circuit| build_find_circuit(circuit, entity_join_plan()));

        circuit.transaction().unwrap();

        assert!(decode_output_rows(&output.consolidate())
            .unwrap()
            .is_empty());
    }

    #[test]
    fn decode_errors_surface() {
        let batch = RowZSet::from_keys((), vec![Tup2(vec![vec![0xff]], 1)]);
        let err = decode_output_rows(&batch).unwrap_err();

        assert!(err.to_string().contains("DecodeError"));
    }
}
