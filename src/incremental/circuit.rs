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
use crate::inc_query::{IncrementalQueryPlan, PatternPlan, PatternSlot, RelPlan, RelPlanKind};
use crate::incremental::{EncodedRow, EncodedTriple};
use crate::ops::DataType;

pub(crate) type RowZSet = OrdWSet<EncodedRow, ZWeight, DynZWeight>;

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
            .pattern_vars
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

// Selects row values at the requested positions.
fn select_row_positions(row: &EncodedRow, positions: &[usize]) -> EncodedRow {
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

// Joins incoming and pattern streams using their planned row layouts.
fn join_pattern_streams(
    incoming_stream: Stream<RootCircuit, RowZSet>,
    incoming_vars: &[Variable],
    pattern_stream: Stream<RootCircuit, RowZSet>,
    pattern_vars: &[Variable],
    output_vars: &[Variable],
) -> Stream<RootCircuit, RowZSet> {
    let key_vars = incoming_vars
        .iter()
        .filter(|variable| pattern_vars.contains(variable))
        .cloned()
        .collect::<Vec<_>>();
    let incoming_key_positions = positions(incoming_vars, &key_vars);
    let pattern_key_positions = positions(pattern_vars, &key_vars);
    let output_sources = output_vars
        .iter()
        .map(|var| {
            incoming_vars
                .iter()
                .position(|incoming_var| incoming_var == var)
                .map(RowSource::Left)
                .unwrap_or_else(|| {
                    RowSource::Right(
                        pattern_vars
                            .iter()
                            .position(|pattern_var| pattern_var == var)
                            .expect("output var must come from incoming or pattern rows"),
                    )
                })
        })
        .collect::<Vec<_>>();

    let incoming_indexed = incoming_stream.map_index(move |row| {
        (
            select_row_positions(row, &incoming_key_positions),
            row.clone(),
        )
    });
    let pattern_indexed = pattern_stream.map_index(move |row| {
        (
            select_row_positions(row, &pattern_key_positions),
            row.clone(),
        )
    });

    incoming_indexed.join(&pattern_indexed, move |_key, incoming_row, pattern_row| {
        merge_rows(incoming_row, pattern_row, &output_sources)
    })
}

// TODO: This filtering should happen at storage level. See #329
// Creates the DBSP stream of rows matching one planned triple pattern.
pub(crate) fn pattern_stream(
    fact_input: &Stream<RootCircuit, OrdZSet<EncodedTriple>>,
    pattern: PatternPlan,
) -> Stream<RootCircuit, RowZSet> {
    fact_input.flat_map(move |triple| pattern_row(&pattern, triple))
}

#[derive(Clone)]
pub(crate) struct PlannedWhereStream {
    stream: Stream<RootCircuit, RowZSet>,
    vars: Vec<Variable>,
}

fn project_stream(
    stream: Stream<RootCircuit, RowZSet>,
    source_vars: &[Variable],
    target_vars: &[Variable],
) -> Stream<RootCircuit, RowZSet> {
    if source_vars == target_vars {
        return stream;
    }

    let selected_positions = positions(source_vars, target_vars);
    stream.map(move |row| select_row_positions(row, &selected_positions))
}

fn assert_incoming_layout(plan: &RelPlan, incoming: &Option<PlannedWhereStream>) {
    let actual = incoming.as_ref().map(|relation| relation.vars.as_slice());
    assert_eq!(
        actual,
        plan.incoming_vars.as_deref(),
        "running relation layout does not match planned incoming layout"
    );
}

fn difference_stream(
    positive: PlannedWhereStream,
    negative: PlannedWhereStream,
    key_vars: &[Variable],
) -> Stream<RootCircuit, RowZSet> {
    let positive_key_positions = positions(&positive.vars, key_vars);
    let negative_key_positions = positions(&negative.vars, key_vars);
    let positive_indexed = positive.stream.map_index(move |row| {
        (
            select_row_positions(row, &positive_key_positions),
            row.clone(),
        )
    });
    let negative_indexed = negative.stream.map_index(move |row| {
        (
            select_row_positions(row, &negative_key_positions),
            row.clone(),
        )
    });

    positive_indexed
        .antijoin(&negative_indexed)
        .map(|(_key, row)| row.clone())
}

fn rel_stream(
    fact_input: &Stream<RootCircuit, OrdZSet<EncodedTriple>>,
    plan: &RelPlan,
    incoming: Option<PlannedWhereStream>,
) -> PlannedWhereStream {
    assert_incoming_layout(plan, &incoming);

    let stream = match &plan.kind {
        RelPlanKind::Pattern(pattern) => {
            let pattern_stream = pattern_stream(fact_input, pattern.clone());
            match incoming {
                Some(incoming) => join_pattern_streams(
                    incoming.stream,
                    &incoming.vars,
                    pattern_stream,
                    &pattern.pattern_vars,
                    &plan.output_vars,
                ),
                None => pattern_stream,
            }
        }
        RelPlanKind::Chain(chain) => {
            let relation = chain
                .children
                .iter()
                .fold(incoming, |running, child| {
                    Some(rel_stream(fact_input, child, running))
                })
                .expect("chain plan must contain at least one child");
            project_stream(relation.stream, &relation.vars, &plan.output_vars)
        }
        RelPlanKind::Difference(difference) => {
            let positive = incoming.expect("difference plan requires an incoming relation");
            let negative_seed_stream = project_stream(
                positive.stream.clone(),
                &positive.vars,
                &difference.key_vars,
            );
            let negative_seed = PlannedWhereStream {
                stream: negative_seed_stream,
                vars: difference.key_vars.clone(),
            };
            let negative = rel_stream(fact_input, &difference.negative, Some(negative_seed));
            difference_stream(positive, negative, &difference.key_vars)
        }
        RelPlanKind::Union(union) => {
            let mut branches = union
                .branches
                .iter()
                .map(|branch| {
                    let branch = rel_stream(fact_input, branch, incoming.clone());
                    project_stream(branch.stream, &branch.vars, &plan.output_vars)
                })
                .collect::<Vec<_>>();
            let first = branches.remove(0);
            first.sum(branches.iter()).distinct()
        }
    };

    PlannedWhereStream {
        stream,
        vars: plan.output_vars.clone(),
    }
}

// Creates the DBSP stream of joined where rows for the whole incremental query plan.
pub(crate) fn query_where_stream(
    fact_input: &Stream<RootCircuit, OrdZSet<EncodedTriple>>,
    plan: &IncrementalQueryPlan,
) -> PlannedWhereStream {
    rel_stream(fact_input, &plan.where_plan, None)
}

// Creates the DBSP stream of rows projected to the query find variables.
pub(crate) fn query_find_stream(
    where_stream: PlannedWhereStream,
    find_vars: &[Variable],
) -> Stream<RootCircuit, RowZSet> {
    let find_positions = positions(&where_stream.vars, find_vars);
    where_stream
        .stream
        .map(move |row| select_row_positions(row, &find_positions))
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
            let where_stream = query_where_stream(&input, &plan);
            let stream = query_find_stream(where_stream, &plan.find_vars);
            Ok((handle, stream.output()))
        })
        .map_err(anyhow::Error::from)?;

        Ok(Self {
            _circuit: circuit,
            _input: input,
            _output: output,
        })
    }

    // Applies one weighted triple batch and returns the decoded query result delta.
    // The first batch is the initial snapshot, so its delta is the whole query result.
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
    use edn::kw;
    use edn::query::ToVariable;
    use tempfile::TempDir;

    use super::*;
    use crate::codec::Encode;
    use crate::inc_query::test_support::{
        parse_query, test_schema, AGE_ATTR_ID as AGE, FOLLOWS_ATTR_ID as FOLLOWS,
        NAME_ATTR_ID as NAME, TYPE_ATTR_ID as TYPE,
    };
    use crate::inc_query::{plan_query, IncrementalQueryPlan, PatternSlot};
    use crate::ops::{DataType, Entid};

    fn build_pattern_circuit(
        circuit: &mut RootCircuit,
        pattern: PatternPlan,
    ) -> anyhow::Result<(ZSetHandle<EncodedTriple>, OutputHandle<RowZSet>)> {
        let (input, handle) = circuit.add_input_zset::<EncodedTriple>();
        let stream = pattern_stream(&input, pattern);
        Ok((handle, stream.output()))
    }

    fn build_where_circuit(
        circuit: &mut RootCircuit,
        plan: IncrementalQueryPlan,
    ) -> anyhow::Result<(ZSetHandle<EncodedTriple>, OutputHandle<RowZSet>)> {
        let (input, handle) = circuit.add_input_zset::<EncodedTriple>();
        let stream = query_where_stream(&input, &plan).stream;
        Ok((handle, stream.output()))
    }

    fn build_find_circuit(
        circuit: &mut RootCircuit,
        plan: IncrementalQueryPlan,
    ) -> anyhow::Result<(ZSetHandle<EncodedTriple>, OutputHandle<RowZSet>)> {
        let (input, handle) = circuit.add_input_zset::<EncodedTriple>();
        let where_stream = query_where_stream(&input, &plan);
        let stream = query_find_stream(where_stream, &plan.find_vars);
        Ok((handle, stream.output()))
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

    fn triple(entity: Entid, attribute_id: Entid, value: DataType) -> EncodedTriple {
        EncodedTriple {
            entity: DataType::Long(entity).encode(),
            attribute: attribute_id,
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
            attribute: NAME,
            entity: PatternSlot::Variable("?e".to_var()),
            value: PatternSlot::Variable("?name".to_var()),
            pattern_vars: vec!["?e".to_var(), "?name".to_var()],
        }
    }

    fn query_plan(query: &str) -> IncrementalQueryPlan {
        let query = parse_query(query);
        plan_query(&query, &test_schema()).expect("query should plan")
    }

    #[test]
    fn query_circuit_uses_file_backed_storage_root() {
        let dir = tempfile::tempdir().unwrap();
        let storage_path = dir.path().join("query-1");
        std::fs::create_dir_all(&storage_path).unwrap();
        std::fs::write(storage_path.join("stale"), b"stale").unwrap();
        let plan = query_plan("[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]");

        let mut circuit = QueryCircuit::build(plan, &storage_path).unwrap();
        let priming_rows = circuit
            .apply(vec![
                Tup2(triple(42, NAME, DataType::String("Alice".to_string())), 1),
                Tup2(triple(42, AGE, DataType::Long(30)), 1),
            ])
            .unwrap();

        assert_eq!(
            priming_rows,
            vec![(
                vec![DataType::String("Alice".to_string()), DataType::Long(30)],
                1,
            )]
        );

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
            [(triple(42, NAME, DataType::String("Alice".to_string())), 1)],
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
            [(triple(42, NAME, DataType::String("Alice".to_string())), -1)],
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
            attribute: NAME,
            entity: PatternSlot::Variable("?e".to_var()),
            value: PatternSlot::Constant(DataType::String("Alice".to_string()).encode()),
            pattern_vars: vec!["?e".to_var()],
        };
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_pattern_circuit(circuit, pattern.clone()));

        append(
            &handle,
            [
                (triple(42, NAME, DataType::String("Alice".to_string())), 1),
                (triple(43, NAME, DataType::String("Bob".to_string())), 1),
                (triple(44, AGE, DataType::String("Alice".to_string())), 1),
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
        let plan = query_plan("[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]");
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_where_circuit(circuit, plan.clone()));

        append(
            &handle,
            [
                (triple(42, NAME, DataType::String("Alice".to_string())), 1),
                (triple(42, AGE, DataType::Long(30)), 1),
                (triple(43, NAME, DataType::String("Bob".to_string())), 1),
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
    fn query_rows_follow_chain_plan_order() {
        let plan = query_plan(
            "[:find ?friend ?age
              :where
              [?e :name ?name]
              [?e :follows ?friend]
              [?e :age ?age]]",
        );
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_where_circuit(circuit, plan.clone()));

        append(
            &handle,
            [
                (triple(42, NAME, DataType::String("Alice".to_string())), 1),
                (triple(42, AGE, DataType::Long(30)), 1),
                (triple(42, FOLLOWS, DataType::Long(43)), 1),
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
        let plan = query_plan(
            "[:find ?friend ?age
              :where
              [?e :name ?name]
              [?e :follows ?friend]
              [?e :age ?age]]",
        );
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));

        append(
            &handle,
            [
                (triple(42, NAME, DataType::String("Alice".to_string())), 1),
                (triple(42, AGE, DataType::Long(30)), 1),
                (triple(42, FOLLOWS, DataType::Long(43)), 1),
            ],
        );
        circuit.transaction().unwrap();

        assert_eq!(
            decode_output_rows(&output.consolidate()).unwrap(),
            vec![(vec![DataType::Long(43), DataType::Long(30)], 1)]
        );
    }

    #[test]
    fn or_stream_emits_disjoint_union() {
        let plan = query_plan(
            r#"[:find ?e
                :where
                (or [?e :name "Alice"]
                    [?e :name "Bob"])]"#,
        );
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));

        append(
            &handle,
            [
                (triple(1, NAME, DataType::String("Alice".to_string())), 1),
                (triple(2, NAME, DataType::String("Bob".to_string())), 1),
                (triple(3, NAME, DataType::String("Charlie".to_string())), 1),
            ],
        );
        circuit.transaction().unwrap();

        let mut rows = decode_output_rows(&output.consolidate()).unwrap();
        rows.sort_by_key(|row| format!("{:?}", row));
        assert_eq!(
            rows,
            vec![(vec![DataType::Long(1)], 1), (vec![DataType::Long(2)], 1),]
        );
    }

    #[test]
    fn or_stream_collapses_overlapping_branches() {
        let plan = query_plan(
            r#"[:find ?e
                :where
                (or [?e :name "Alice"]
                    [?e :type :person])]"#,
        );
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));

        append(
            &handle,
            [
                (triple(1, NAME, DataType::String("Alice".to_string())), 1),
                (triple(1, TYPE, DataType::Keyword(kw!(:person))), 1),
            ],
        );
        circuit.transaction().unwrap();
        assert_eq!(
            decode_output_rows(&output.consolidate()).unwrap(),
            vec![(vec![DataType::Long(1)], 1)]
        );

        append(
            &handle,
            [(triple(1, NAME, DataType::String("Alice".to_string())), -1)],
        );
        circuit.transaction().unwrap();
        assert!(decode_output_rows(&output.consolidate())
            .unwrap()
            .is_empty());

        append(
            &handle,
            [(triple(1, TYPE, DataType::Keyword(kw!(:person))), -1)],
        );
        circuit.transaction().unwrap();
        assert_eq!(
            decode_output_rows(&output.consolidate()).unwrap(),
            vec![(vec![DataType::Long(1)], -1)]
        );
    }

    #[test]
    fn or_stream_normalizes_branch_row_order() {
        let plan = query_plan("[:find ?e ?v :where (or [?e :name ?v] [?v :follows ?e])]");
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));

        append(
            &handle,
            [
                (triple(1, NAME, DataType::String("Alice".to_string())), 1),
                (triple(2, FOLLOWS, DataType::Long(1)), 1),
            ],
        );
        circuit.transaction().unwrap();

        let mut rows = decode_output_rows(&output.consolidate()).unwrap();
        rows.sort_by_key(|row| format!("{:?}", row));
        assert_eq!(
            rows,
            vec![
                (vec![DataType::Long(1), DataType::Long(2)], 1),
                (
                    vec![DataType::Long(1), DataType::String("Alice".to_string())],
                    1,
                ),
            ]
        );
    }

    #[test]
    fn outer_pattern_stream_fans_into_every_or_branch() {
        let plan = query_plan(
            "[:find ?name
              :where
              [?e :name ?name]
              (or [?e :age 30]
                  [?e :age 40])]",
        );
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));

        append(
            &handle,
            [
                (triple(1, NAME, DataType::String("Alice".to_string())), 1),
                (triple(1, AGE, DataType::Long(30)), 1),
                (triple(2, NAME, DataType::String("Bob".to_string())), 1),
                (triple(2, AGE, DataType::Long(40)), 1),
                (triple(3, NAME, DataType::String("Cara".to_string())), 1),
                (triple(3, AGE, DataType::Long(50)), 1),
            ],
        );
        circuit.transaction().unwrap();

        let mut rows = decode_output_rows(&output.consolidate()).unwrap();
        rows.sort_by_key(|row| format!("{:?}", row));
        assert_eq!(
            rows,
            vec![
                (vec![DataType::String("Alice".to_string())], 1),
                (vec![DataType::String("Bob".to_string())], 1),
            ]
        );
    }

    #[test]
    fn not_stream_uses_negative_key_presence() {
        let plan = query_plan("[:find ?name :where [?e :name ?name] (not [?e :age 30])]");
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));

        append(
            &handle,
            [
                (triple(1, NAME, DataType::String("Alice".to_string())), 1),
                (triple(1, AGE, DataType::Long(30)), 2),
            ],
        );
        circuit.transaction().unwrap();
        assert!(decode_output_rows(&output.consolidate())
            .unwrap()
            .is_empty());

        append(&handle, [(triple(1, AGE, DataType::Long(30)), -1)]);
        circuit.transaction().unwrap();
        assert!(decode_output_rows(&output.consolidate())
            .unwrap()
            .is_empty());

        append(&handle, [(triple(1, AGE, DataType::Long(30)), -1)]);
        circuit.transaction().unwrap();
        assert_eq!(
            decode_output_rows(&output.consolidate()).unwrap(),
            vec![(vec![DataType::String("Alice".to_string())], 1)]
        );
    }

    #[test]
    fn double_not_stream_tracks_nested_presence() {
        let plan = query_plan("[:find ?name :where [?e :name ?name] (not (not [?e :age 30]))]");
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));

        append(
            &handle,
            [(triple(1, NAME, DataType::String("Alice".to_string())), 1)],
        );
        circuit.transaction().unwrap();
        assert!(decode_output_rows(&output.consolidate())
            .unwrap()
            .is_empty());

        append(&handle, [(triple(1, AGE, DataType::Long(30)), 1)]);
        circuit.transaction().unwrap();
        assert_eq!(
            decode_output_rows(&output.consolidate()).unwrap(),
            vec![(vec![DataType::String("Alice".to_string())], 1)]
        );

        append(&handle, [(triple(1, AGE, DataType::Long(30)), -1)]);
        circuit.transaction().unwrap();
        assert_eq!(
            decode_output_rows(&output.consolidate()).unwrap(),
            vec![(vec![DataType::String("Alice".to_string())], -1)]
        );
    }

    #[test]
    #[should_panic(expected = "running relation layout does not match planned incoming layout")]
    fn assembly_rejects_incoming_layout_mismatch() {
        let plan = query_plan("[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]");
        let RelPlanKind::Chain(chain) = plan.where_plan.kind else {
            panic!("expected chain plan");
        };
        let extending_pattern = chain.children[1].clone();

        let _ = build_test_circuit(move |circuit| {
            let (facts, _) = circuit.add_input_zset::<EncodedTriple>();
            let (stream, _) = circuit.add_input_zset::<EncodedRow>();
            let incoming = PlannedWhereStream {
                stream,
                vars: vec!["?name".to_var(), "?e".to_var()],
            };
            let _ = rel_stream(&facts, &extending_pattern, Some(incoming));
            Ok(())
        });
    }

    #[test]
    fn joins_through_ref_value() {
        let plan = query_plan(
            "[:find ?friend-name
              :where
              [?e :follows ?friend]
              [?friend :name ?friend-name]]",
        );
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_where_circuit(circuit, plan.clone()));

        append(
            &handle,
            [
                (triple(1, FOLLOWS, DataType::Long(2)), 1),
                (triple(2, NAME, DataType::String("Bob".to_string())), 1),
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
        let plan = query_plan(
            "[:find ?name ?age ?friend
              :where
              [?e :name ?name]
              [?e :age ?age]
              [?e :follows ?friend]]",
        );
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_where_circuit(circuit, plan.clone()));

        append(
            &handle,
            [
                (triple(42, NAME, DataType::String("Alice".to_string())), 1),
                (triple(42, AGE, DataType::Long(30)), 1),
                (triple(42, FOLLOWS, DataType::Long(43)), 1),
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
        let plan = query_plan(
            "[:find ?name ?age
              :where
              [?e :name ?name]
              [?other :age ?age]]",
        );
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_where_circuit(circuit, plan.clone()));

        append(
            &handle,
            [
                (triple(1, NAME, DataType::String("Alice".to_string())), 1),
                (triple(2, NAME, DataType::String("Bob".to_string())), 1),
                (triple(3, AGE, DataType::Long(30)), 1),
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
        let plan = query_plan("[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]");
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));

        append(
            &handle,
            [
                (triple(42, NAME, DataType::String("Alice".to_string())), 1),
                (triple(42, AGE, DataType::Long(30)), 1),
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
        let plan = query_plan("[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]");
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));

        append(
            &handle,
            [
                (triple(42, NAME, DataType::String("Alice".to_string())), -1),
                (triple(42, AGE, DataType::Long(30)), 1),
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
    fn empty_transaction_decodes_to_no_rows() {
        let plan = query_plan("[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]");
        let (mut circuit, (_handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));

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
