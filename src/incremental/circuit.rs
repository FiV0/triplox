mod aggregates;

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;

use anyhow::{anyhow, Result};
use dbsp::circuit::{
    CircuitConfig, CircuitStorageConfig, StorageCacheConfig, StorageConfig, StorageOptions,
};
use dbsp::{
    typed_batch::IndexedZSetReader, utils::Tup2, DBSPHandle, OrdZSet, OutputHandle, RootCircuit,
    Runtime, Stream, ZSetHandle, ZWeight,
};
use edn::query::Variable;

pub(super) use aggregates::AggregateError;
use aggregates::{aggregate_find_stream, decode_aggregate_output, AggregateOutput};

use crate::codec::{Decode, Encode};
use crate::expr::{evaluate, evaluate_as_bool, expr_variables, EvalContext, Expr};
use crate::inc_query::{IncrementalQueryPlan, PatternPlan, PatternSlot, RelPlan, RelPlanKind};
use crate::incremental::{EncodedRow, EncodedTriple};
use crate::ops::DataType;
use crate::query::{FindPlan, Projection};

// Standard row -> weight
pub(crate) type RowZSet = OrdZSet<EncodedRow>;
// Output row
type OutputRow = Vec<AggregateOutput>;
// Output row -> weight
type OutputZSet = OrdZSet<OutputRow>;

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

fn expression_variable_positions(vars: &[Variable], expr: &Expr) -> Vec<(Variable, usize)> {
    expr_variables(expr)
        .into_iter()
        .map(|variable| {
            let position = vars
                .iter()
                .position(|candidate| candidate == &variable)
                .expect("expression variable must be present in the incoming row");
            (variable, position)
        })
        .collect()
}

fn update_expression_bindings(
    row: &EncodedRow,
    variable_positions: &[(Variable, usize)],
    bindings: &mut HashMap<Variable, DataType>,
) {
    for (variable, position) in variable_positions {
        let value =
            DataType::decode(&row[*position]).expect("incremental expression value should decode");
        if let Some(binding) = bindings.get_mut(variable) {
            *binding = value;
        } else {
            bindings.insert(variable.clone(), value);
        }
    }
}

fn evaluate_row_expression(
    row: &EncodedRow,
    variable_positions: &[(Variable, usize)],
    bindings: &mut HashMap<Variable, DataType>,
    expr: &Expr,
) -> Option<DataType> {
    update_expression_bindings(row, variable_positions, bindings);
    evaluate(expr, &EvalContext::new(bindings)).map(|result| result.into_owned())
}

fn filter_predicate_stream(
    stream: Stream<RootCircuit, RowZSet>,
    incoming_vars: &[Variable],
    expr: Expr,
) -> Stream<RootCircuit, RowZSet> {
    let variable_positions = expression_variable_positions(incoming_vars, &expr);
    let bindings = RefCell::new(HashMap::with_capacity(variable_positions.len()));

    stream.filter(move |row| {
        let mut bindings = bindings.borrow_mut();
        update_expression_bindings(row, &variable_positions, &mut bindings);
        evaluate_as_bool(&expr, &EvalContext::new(&bindings))
    })
}

fn apply_function_stream(
    stream: Stream<RootCircuit, RowZSet>,
    incoming_vars: &[Variable],
    expr: Expr,
    output_var: &Variable,
) -> Stream<RootCircuit, RowZSet> {
    let variable_positions = expression_variable_positions(incoming_vars, &expr);
    match incoming_vars
        .iter()
        .position(|variable| variable == output_var)
    {
        Some(output_position) => {
            let bindings = RefCell::new(HashMap::with_capacity(variable_positions.len()));
            stream.filter(move |row| {
                let mut bindings = bindings.borrow_mut();
                evaluate_row_expression(row, &variable_positions, &mut bindings, &expr).is_some_and(
                    |result| result.encode().as_slice() == row[output_position].as_slice(),
                )
            })
        }
        None => {
            let mut bindings = HashMap::with_capacity(variable_positions.len());
            stream.flat_map(move |row| {
                let result =
                    evaluate_row_expression(row, &variable_positions, &mut bindings, &expr)?;
                let mut output = row.clone();
                output.push(result.encode());
                Some(output)
            })
        }
    }
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
    // `antijoin` only looks at the negative keys, so carrying row values would be wasted work.
    let negative_indexed = negative
        .stream
        .map_index(move |row| (select_row_positions(row, &negative_key_positions), ()));

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
        RelPlanKind::Filter { expr } => {
            let incoming = incoming.expect("filter plan requires an incoming relation");
            filter_predicate_stream(incoming.stream, &incoming.vars, expr.clone())
        }
        RelPlanKind::Function { expr, output_var } => {
            let incoming = incoming.expect("function plan requires an incoming relation");
            apply_function_stream(incoming.stream, &incoming.vars, expr.clone(), output_var)
        }
        RelPlanKind::Chain { children } => {
            let relation = children
                .iter()
                .fold(incoming, |running, child| {
                    Some(rel_stream(fact_input, child, running))
                })
                .expect("chain plan must contain at least one child");
            project_stream(relation.stream, &relation.vars, &plan.output_vars)
        }
        RelPlanKind::Difference { key_vars, negative } => {
            let positive = incoming.expect("difference plan requires an incoming relation");
            let negative_seed_stream =
                project_stream(positive.stream.clone(), &positive.vars, key_vars);
            let negative_seed = PlannedWhereStream {
                stream: negative_seed_stream,
                vars: key_vars.clone(),
            };
            let negative = rel_stream(fact_input, negative, Some(negative_seed));
            difference_stream(positive, negative, key_vars)
        }
        RelPlanKind::Union { branches } => {
            let mut branches = branches
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

fn project_find_stream(
    where_stream: PlannedWhereStream,
    plan: &FindPlan,
) -> Stream<RootCircuit, OutputZSet> {

    let find_positions = plan
        .projections
        .iter()
        .map(|projection| match projection {
            Projection::GroupVar(group_position) => plan.group_key_indices[*group_position],
            Projection::Aggregate(_, _) => unreachable!("projection plan has no aggregates"),
        })
        .collect::<Vec<_>>();

    // TODO: After find every position is an AggregateOutput. This should likely be named differently.
    where_stream.stream.map(move |row| {
        select_row_positions(row, &find_positions)
            .into_iter()
            .map(AggregateOutput::Value)
            .collect()
    })
}

// Creates the DBSP stream of rows projected and aggregated according to the find plan.
fn query_find_stream(
    where_stream: PlannedWhereStream,
    plan: &FindPlan,
) -> Stream<RootCircuit, OutputZSet> {
    if plan.has_aggregates {
        aggregate_find_stream(where_stream, plan)
    } else {
        project_find_stream(where_stream, plan)
    }
}

// Decodes a DBSP output batch into user-facing values and signed weights.
fn decode_output_rows(batch: &OutputZSet) -> Result<Vec<(Vec<DataType>, isize)>> {
    batch
        .iter()
        .map(|(row, (), weight)| {
            let decoded = row
                .iter()
                .map(decode_aggregate_output)
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
    _output: OutputHandle<OutputZSet>,
}

impl QueryCircuit {
    // Builds a DBSP circuit for one incremental query plan.
    pub(super) fn build(plan: IncrementalQueryPlan, storage_path: &Path) -> Result<Self> {
        let config = storage_circuit_config(storage_path)?;
        let (circuit, (input, output)) = Runtime::init_circuit(config, move |circuit| {
            let (input, handle) = circuit.add_input_zset::<EncodedTriple>();
            let where_stream = query_where_stream(&input, &plan);
            let stream = query_find_stream(where_stream, &plan.find_plan);
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
    ) -> anyhow::Result<(ZSetHandle<EncodedTriple>, OutputHandle<OutputZSet>)> {
        let (input, handle) = circuit.add_input_zset::<EncodedTriple>();
        let where_stream = query_where_stream(&input, &plan);
        let stream = query_find_stream(where_stream, &plan.find_plan);
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
    fn predicate_stream_filters_rows_and_preserves_retractions() {
        let plan = query_plan("[:find ?age :where [?e :age ?age] [(< ?age 50)]]");
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));

        append(
            &handle,
            [
                (triple(1, AGE, DataType::Long(30)), 1),
                (triple(2, AGE, DataType::Long(50)), 1),
            ],
        );
        circuit.transaction().unwrap();
        assert_eq!(
            decode_output_rows(&output.consolidate()).unwrap(),
            vec![(vec![DataType::Long(30)], 1)]
        );

        append(&handle, [(triple(1, AGE, DataType::Long(30)), -1)]);
        circuit.transaction().unwrap();
        assert_eq!(
            decode_output_rows(&output.consolidate()).unwrap(),
            vec![(vec![DataType::Long(30)], -1)]
        );
    }

    #[test]
    fn function_stream_appends_results_and_preserves_retractions() {
        let plan = query_plan("[:find ?half :where [?e :age ?age] [(quot ?age 2) ?half]]");
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));

        append(
            &handle,
            [
                (triple(1, AGE, DataType::Long(30)), 1),
                (triple(2, AGE, DataType::Long(40)), 1),
            ],
        );
        circuit.transaction().unwrap();
        let mut rows = decode_output_rows(&output.consolidate()).unwrap();
        rows.sort_by_key(|(row, _weight)| format!("{:?}", row));
        assert_eq!(
            rows,
            vec![(vec![DataType::Long(15)], 1), (vec![DataType::Long(20)], 1)]
        );

        append(&handle, [(triple(1, AGE, DataType::Long(30)), -1)]);
        circuit.transaction().unwrap();
        assert_eq!(
            decode_output_rows(&output.consolidate()).unwrap(),
            vec![(vec![DataType::Long(15)], -1)]
        );
    }

    #[test]
    fn function_stream_filters_already_bound_results() {
        let plan = query_plan(
            "[:find ?name
              :where
              [?e :age ?age]
              [?e :name ?name]
              [(str ?age) ?name]]",
        );
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));

        append(
            &handle,
            [
                (triple(1, AGE, DataType::Long(30)), 1),
                (triple(1, NAME, DataType::String("30".to_string())), 1),
                (triple(2, AGE, DataType::Long(40)), 1),
                (triple(2, NAME, DataType::String("forty".to_string())), 1),
            ],
        );
        circuit.transaction().unwrap();
        assert_eq!(
            decode_output_rows(&output.consolidate()).unwrap(),
            vec![(vec![DataType::String("30".to_string())], 1)]
        );

        append(
            &handle,
            [(triple(2, NAME, DataType::String("40".to_string())), 1)],
        );
        circuit.transaction().unwrap();
        assert_eq!(
            decode_output_rows(&output.consolidate()).unwrap(),
            vec![(vec![DataType::String("40".to_string())], 1)]
        );
    }

    #[test]
    fn function_stream_drops_rows_when_evaluation_fails() {
        let plan = query_plan("[:find ?result :where [?e :age ?age] [(quot ?age 0) ?result]]");
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));

        append(&handle, [(triple(1, AGE, DataType::Long(30)), 1)]);
        circuit.transaction().unwrap();

        assert!(decode_output_rows(&output.consolidate())
            .unwrap()
            .is_empty());
    }

    #[test]
    fn function_stream_composes_with_union() {
        let plan = query_plan(
            "[:find ?result
              :where
              [?e :age ?age]
              (or [(+ ?age 1) ?result]
                  [(- ?age 1) ?result])]",
        );
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));

        append(&handle, [(triple(1, AGE, DataType::Long(30)), 1)]);
        circuit.transaction().unwrap();

        let mut rows = decode_output_rows(&output.consolidate()).unwrap();
        rows.sort_by_key(|(row, _weight)| format!("{:?}", row));
        assert_eq!(
            rows,
            vec![(vec![DataType::Long(29)], 1), (vec![DataType::Long(31)], 1)]
        );
    }

    #[test]
    fn function_stream_composes_with_difference() {
        let plan = query_plan(
            "[:find ?name
              :where
              [?e :age ?age]
              [?e :name ?name]
              (not [(str ?age) ?name])]",
        );
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));

        append(
            &handle,
            [
                (triple(1, AGE, DataType::Long(30)), 1),
                (triple(1, NAME, DataType::String("30".to_string())), 1),
                (triple(2, AGE, DataType::Long(40)), 1),
                (triple(2, NAME, DataType::String("forty".to_string())), 1),
            ],
        );
        circuit.transaction().unwrap();

        assert_eq!(
            decode_output_rows(&output.consolidate()).unwrap(),
            vec![(vec![DataType::String("forty".to_string())], 1)]
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
        let RelPlanKind::Chain { children } = plan.where_plan.kind else {
            panic!("expected chain plan");
        };
        let extending_pattern = children[1].clone();

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
    fn global_aggregates_share_a_group_and_preserve_find_order() {
        let plan = query_plan(
            "[:find (sum ?age) (min ?age) (max ?age) (count ?age)
              (count-distinct ?age) (avg ?age)
              :where [?e :age ?age]]",
        );
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));

        append(
            &handle,
            [
                (triple(1, AGE, DataType::Long(3)), 1),
                (triple(2, AGE, DataType::Long(1)), 1),
                (triple(3, AGE, DataType::Long(1)), 1),
                (triple(4, AGE, DataType::Long(1)), 1),
            ],
        );
        circuit.transaction().unwrap();

        assert_eq!(
            decode_output_rows(&output.consolidate()).unwrap(),
            vec![(
                vec![
                    DataType::Long(6),
                    DataType::Long(1),
                    DataType::Long(3),
                    DataType::Long(4),
                    DataType::Long(2),
                    DataType::Double(1.5),
                ],
                1,
            )]
        );
    }

    #[test]
    fn grouped_aggregates_reassemble_arbitrary_find_order() {
        let plan = query_plan(
            "[:find (sum ?age) ?type (count ?e) (max ?age)
              :where [?e :type ?type] [?e :age ?age]]",
        );
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));

        append(
            &handle,
            [
                (triple(1, TYPE, DataType::Keyword(kw!(:alpha))), 1),
                (triple(1, AGE, DataType::Long(10)), 1),
                (triple(2, TYPE, DataType::Keyword(kw!(:alpha))), 1),
                (triple(2, AGE, DataType::Long(20)), 1),
                (triple(3, TYPE, DataType::Keyword(kw!(:beta))), 1),
                (triple(3, AGE, DataType::Long(7)), 1),
            ],
        );
        circuit.transaction().unwrap();

        let rows = decode_output_rows(&output.consolidate()).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&(
            vec![
                DataType::Long(30),
                DataType::Keyword(kw!(:alpha)),
                DataType::Long(2),
                DataType::Long(20),
            ],
            1,
        )));
        assert!(rows.contains(&(
            vec![
                DataType::Long(7),
                DataType::Keyword(kw!(:beta)),
                DataType::Long(1),
                DataType::Long(7),
            ],
            1,
        )));
    }

    #[test]
    fn aggregate_changes_retract_the_old_row_and_add_the_new_row() {
        let plan = query_plan(
            "[:find (sum ?age) (count ?age) (avg ?age)
              :where [?e :age ?age]]",
        );
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));

        append(
            &handle,
            [
                (triple(1, AGE, DataType::Long(10)), 1),
                (triple(2, AGE, DataType::Long(20)), 1),
            ],
        );
        circuit.transaction().unwrap();
        assert_eq!(
            decode_output_rows(&output.consolidate()).unwrap(),
            vec![(
                vec![
                    DataType::Long(30),
                    DataType::Long(2),
                    DataType::Double(15.0),
                ],
                1,
            )]
        );

        append(&handle, [(triple(3, AGE, DataType::Long(30)), 1)]);
        circuit.transaction().unwrap();
        let rows = decode_output_rows(&output.consolidate()).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&(
            vec![
                DataType::Long(30),
                DataType::Long(2),
                DataType::Double(15.0),
            ],
            -1,
        )));
        assert!(rows.contains(&(
            vec![
                DataType::Long(60),
                DataType::Long(3),
                DataType::Double(20.0),
            ],
            1,
        )));
    }

    #[test]
    fn empty_global_zero_aggregates_produce_a_row() {
        let plan = query_plan(
            "[:find (count ?age) (count-distinct ?age) (sum ?age)
              :where [?e :age ?age]]",
        );
        let (mut circuit, (_handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));

        circuit.transaction().unwrap();

        assert_eq!(
            decode_output_rows(&output.consolidate()).unwrap(),
            vec![(
                vec![DataType::Long(0), DataType::Long(0), DataType::Long(0)],
                1,
            )]
        );
    }

    #[test]
    fn aggregate_row_is_removed_when_a_value_becomes_undefined() {
        let plan = query_plan("[:find (count ?age) (avg ?age) :where [?e :age ?age]]");
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));
        let age = triple(1, AGE, DataType::Long(10));

        append(&handle, [(age.clone(), 1)]);
        circuit.transaction().unwrap();
        assert_eq!(
            decode_output_rows(&output.consolidate()).unwrap(),
            vec![(vec![DataType::Long(1), DataType::Double(10.0)], 1,)]
        );

        append(&handle, [(age, -1)]);
        circuit.transaction().unwrap();
        assert_eq!(
            decode_output_rows(&output.consolidate()).unwrap(),
            vec![(vec![DataType::Long(1), DataType::Double(10.0)], -1,)]
        );
    }

    #[test]
    fn numeric_aggregates_promote_mixed_inputs() {
        let plan = query_plan(
            "[:find (min ?age) (max ?age) (sum ?age) (avg ?age)
              :where [?e :age ?age]]",
        );
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));

        append(
            &handle,
            [
                (triple(1, AGE, DataType::Long(10)), 1),
                (triple(2, AGE, DataType::Double(1.5)), 1),
            ],
        );
        circuit.transaction().unwrap();

        assert_eq!(
            decode_output_rows(&output.consolidate()).unwrap(),
            vec![(
                vec![
                    DataType::Double(1.5),
                    DataType::Long(10),
                    DataType::Double(11.5),
                    DataType::Double(5.75),
                ],
                1,
            )]
        );
    }

    #[test]
    fn min_rejects_incompatible_type_families() {
        let plan = query_plan(
            "[:find (min ?value)
              :where (or [?e :age ?value] [?e :name ?value])]",
        );
        let (mut circuit, (handle, output), _storage) =
            build_test_circuit(move |circuit| build_find_circuit(circuit, plan.clone()));

        append(
            &handle,
            [
                (triple(1, AGE, DataType::Long(10)), 1),
                (triple(2, NAME, DataType::String("Alice".to_string())), 1),
            ],
        );
        circuit.transaction().unwrap();

        let err = decode_output_rows(&output.consolidate()).unwrap_err();
        assert!(err
            .to_string()
            .contains("aggregate encountered uncomparable values"));
    }

    #[test]
    fn decode_errors_surface() {
        let batch =
            OutputZSet::from_keys((), vec![Tup2(vec![AggregateOutput::Value(vec![0xff])], 1)]);
        let err = decode_output_rows(&batch).unwrap_err();

        assert!(err.to_string().contains("DecodeError"));
    }
}
