use std::collections::{HashMap, HashSet};

use anyhow::{ensure, Result};
use bytes::Bytes;
use edn::query::Variable;

pub(crate) type BindingRow = Vec<Bytes>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BindingBag {
    pub(crate) variables: Vec<Variable>,
    pub(crate) rows: Vec<BindingRow>,
    column_indexes: HashMap<Variable, usize>,
}

impl BindingBag {
    pub(crate) fn new(variables: Vec<Variable>, rows: Vec<BindingRow>) -> Result<Self> {
        let mut column_indexes = HashMap::with_capacity(variables.len());
        for (index, variable) in variables.iter().enumerate() {
            ensure!(
                column_indexes.insert(variable.clone(), index).is_none(),
                "BindingBag variables must be unique: {variable}"
            );
        }
        for (index, row) in rows.iter().enumerate() {
            ensure!(
                row.len() == variables.len(),
                "BindingBag row {index} has arity {}, expected {}",
                row.len(),
                variables.len()
            );
        }

        Ok(Self {
            variables,
            rows,
            column_indexes,
        })
    }

    fn with_rows(&self, rows: Vec<BindingRow>) -> Self {
        Self {
            variables: self.variables.clone(),
            rows,
            column_indexes: self.column_indexes.clone(),
        }
    }

    pub(crate) fn unit() -> Self {
        Self {
            variables: Vec::new(),
            rows: vec![Vec::new()],
            column_indexes: HashMap::new(),
        }
    }

    pub(crate) fn empty(variables: Vec<Variable>) -> Result<Self> {
        Self::new(variables, Vec::new())
    }

    pub(crate) fn column_index(&self, variable: &Variable) -> Result<usize> {
        self.column_indexes
            .get(variable)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("Unknown BindingBag variable: {variable}"))
    }

    fn projection_indexes(&self, variables: &[Variable]) -> Result<Vec<usize>> {
        let mut seen = HashSet::with_capacity(variables.len());
        variables
            .iter()
            .map(|variable| {
                ensure!(
                    seen.insert(variable),
                    "Projection variables must be unique: {variable}"
                );
                self.column_index(variable)
            })
            .collect()
    }

    pub(crate) fn select_rows(&self, row_indexes: &[usize]) -> Result<Self> {
        let rows = row_indexes
            .iter()
            .map(|row_index| {
                self.rows
                    .get(*row_index)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("Unknown BindingBag row index: {row_index}"))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(self.with_rows(rows))
    }

    pub(crate) fn extend_rows(
        &self,
        added_variables: Vec<Variable>,
        extensions: Vec<Vec<BindingRow>>,
    ) -> Result<Self> {
        ensure!(
            extensions.len() == self.rows.len(),
            "BindingBag extensions have {} input rows, expected {}",
            extensions.len(),
            self.rows.len()
        );

        let mut seen: HashSet<Variable> = self.variables.iter().cloned().collect();
        for variable in &added_variables {
            ensure!(
                seen.insert(variable.clone()),
                "Extended BindingBag variables must be unique: {variable}"
            );
        }

        let mut rows = Vec::new();
        for (input, input_extensions) in self.rows.iter().zip(extensions) {
            for mut extension in input_extensions {
                ensure!(
                    extension.len() == added_variables.len(),
                    "BindingBag extension has arity {}, expected {}",
                    extension.len(),
                    added_variables.len()
                );
                let mut row = input.clone();
                row.append(&mut extension);
                rows.push(row);
            }
        }

        let mut variables = self.variables.clone();
        variables.extend(added_variables);
        Self::new(variables, rows)
    }

    pub(crate) fn project(&self, variables: &[Variable]) -> Result<Self> {
        let indexes = self.projection_indexes(variables)?;
        let rows = self
            .rows
            .iter()
            .map(|row| indexes.iter().map(|index| row[*index].clone()).collect())
            .collect();
        Self::new(variables.to_vec(), rows)
    }

    pub(crate) fn reorder(&self, variables: &[Variable]) -> Result<Self> {
        ensure!(
            variables.len() == self.variables.len(),
            "Reordered layout must contain exactly the current variables"
        );
        self.project(variables)
    }

    pub(crate) fn distinct(&self) -> Self {
        let mut seen = HashSet::with_capacity(self.rows.len());
        let rows = self
            .rows
            .iter()
            .filter(|row| seen.insert((*row).clone()))
            .cloned()
            .collect();
        Self {
            variables: self.variables.clone(),
            rows,
            column_indexes: self.column_indexes.clone(),
        }
    }

    pub(crate) fn index_by(
        &self,
        variables: &[Variable],
    ) -> Result<HashMap<BindingRow, Vec<usize>>> {
        let indexes = self.projection_indexes(variables)?;
        let mut result: HashMap<BindingRow, Vec<usize>> = HashMap::new();
        for (row_index, row) in self.rows.iter().enumerate() {
            let key = indexes.iter().map(|index| row[*index].clone()).collect();
            result.entry(key).or_default().push(row_index);
        }
        Ok(result)
    }

    fn shared_variables(&self, other: &Self) -> Vec<Variable> {
        self.variables
            .iter()
            .filter(|variable| other.column_indexes.contains_key(*variable))
            .cloned()
            .collect()
    }

    fn row_key(row: &BindingRow, indexes: &[usize]) -> BindingRow {
        indexes.iter().map(|index| row[*index].clone()).collect()
    }

    fn joined_row(
        left_row: &BindingRow,
        right_row: &BindingRow,
        right_only_indexes: &[usize],
    ) -> BindingRow {
        let mut row = left_row.clone();
        row.extend(
            right_only_indexes
                .iter()
                .map(|index| right_row[*index].clone()),
        );
        row
    }

    pub(crate) fn natural_join(&self, other: &Self) -> Result<Self> {
        let shared_variables = self.shared_variables(other);
        let right_only_indexes: Vec<usize> = other
            .variables
            .iter()
            .enumerate()
            .filter(|(_, variable)| !self.column_indexes.contains_key(*variable))
            .map(|(index, _)| index)
            .collect();

        let mut variables = self.variables.clone();
        variables.extend(
            right_only_indexes
                .iter()
                .map(|index| other.variables[*index].clone()),
        );

        let mut rows = Vec::new();
        if self.rows.len() < other.rows.len() {
            let left_index = self.index_by(&shared_variables)?;
            let right_shared_indexes = other.projection_indexes(&shared_variables)?;
            let mut right_row_indexes_by_left = vec![Vec::new(); self.rows.len()];
            for (right_row_index, right_row) in other.rows.iter().enumerate() {
                let key = Self::row_key(right_row, &right_shared_indexes);
                if let Some(left_row_indexes) = left_index.get(&key) {
                    for left_row_index in left_row_indexes {
                        right_row_indexes_by_left[*left_row_index].push(right_row_index);
                    }
                }
            }
            for (left_row_index, right_row_indexes) in right_row_indexes_by_left.iter().enumerate()
            {
                for right_row_index in right_row_indexes {
                    rows.push(Self::joined_row(
                        &self.rows[left_row_index],
                        &other.rows[*right_row_index],
                        &right_only_indexes,
                    ));
                }
            }
        } else {
            let left_shared_indexes = self.projection_indexes(&shared_variables)?;
            let right_index = other.index_by(&shared_variables)?;
            for left_row in &self.rows {
                let key = Self::row_key(left_row, &left_shared_indexes);
                if let Some(right_row_indexes) = right_index.get(&key) {
                    for right_row_index in right_row_indexes {
                        rows.push(Self::joined_row(
                            left_row,
                            &other.rows[*right_row_index],
                            &right_only_indexes,
                        ));
                    }
                }
            }
        }

        Self::new(variables, rows)
    }

    pub(crate) fn semijoin(&self, other: &Self) -> Result<Self> {
        let shared_variables = self.shared_variables(other);
        let left_shared_indexes = self.projection_indexes(&shared_variables)?;
        let right_index = other.index_by(&shared_variables)?;
        let rows = self
            .rows
            .iter()
            .filter(|row| right_index.contains_key(&Self::row_key(row, &left_shared_indexes)))
            .cloned()
            .collect();
        Ok(self.with_rows(rows))
    }

    pub(crate) fn antijoin(&self, other: &Self) -> Result<Self> {
        let shared_variables = self.shared_variables(other);
        let left_shared_indexes = self.projection_indexes(&shared_variables)?;
        let right_index = other.index_by(&shared_variables)?;
        let rows = self
            .rows
            .iter()
            .filter(|row| !right_index.contains_key(&Self::row_key(row, &left_shared_indexes)))
            .cloned()
            .collect();
        Ok(self.with_rows(rows))
    }

    pub(crate) fn union(&self, other: &Self) -> Result<Self> {
        ensure!(
            self.variables == other.variables,
            "Union requires identical BindingBag layouts"
        );
        let mut rows = self.rows.clone();
        rows.extend(other.rows.iter().cloned());
        Ok(self.with_rows(rows))
    }

    pub(crate) fn distinct_union(&self, other: &Self) -> Result<Self> {
        Ok(self.union(other)?.distinct())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use edn::query::ToVariable;

    use super::BindingBag;

    fn bytes(value: &str) -> Bytes {
        Bytes::copy_from_slice(value.as_bytes())
    }

    fn binding_bag(variables: &[&str], rows: &[&[&str]]) -> BindingBag {
        BindingBag::new(
            variables.iter().map(|variable| variable.to_var()).collect(),
            rows.iter()
                .map(|row| row.iter().map(|value| bytes(value)).collect())
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn construction_rejects_duplicate_variables_and_wrong_row_arity() {
        let x = "?x".to_var();

        assert!(BindingBag::new(vec![x.clone(), x], vec![]).is_err());
        assert!(BindingBag::new(vec!["?x".to_var()], vec![vec![]]).is_err());
    }

    #[test]
    fn column_lookup_reports_unknown_variables() {
        let bindings = binding_bag(&["?x", "?y"], &[&["a", "b"]]);

        assert_eq!(bindings.column_index(&"?x".to_var()).unwrap(), 0);
        assert_eq!(bindings.column_index(&"?y".to_var()).unwrap(), 1);
        assert!(bindings.column_index(&"?z".to_var()).is_err());
    }

    #[test]
    fn unit_and_empty_have_relational_identity_layouts() {
        let unit = BindingBag::unit();
        let empty = BindingBag::empty(vec!["?x".to_var()]).unwrap();

        assert!(unit.variables.is_empty());
        assert_eq!(unit.rows, &[Vec::<Bytes>::new()]);
        assert_eq!(empty.variables, ["?x".to_var()]);
        assert!(empty.rows.is_empty());
    }

    #[test]
    fn row_selection_preserves_requested_order_and_multiplicity() {
        let bindings = binding_bag(&["?x"], &[&["a"], &["b"], &["c"]]);

        let selected = bindings.select_rows(&[2, 0, 2]).unwrap();

        assert_eq!(selected, binding_bag(&["?x"], &[&["c"], &["a"], &["c"]]));
        assert!(bindings.select_rows(&[3]).is_err());
    }

    #[test]
    fn row_extension_supports_one_to_many_results() {
        let bindings = binding_bag(&["?x"], &[&["a"], &["b"]]);

        let extended = bindings
            .extend_rows(
                vec!["?y".to_var()],
                vec![vec![vec![bytes("1")], vec![bytes("2")]], vec![]],
            )
            .unwrap();

        assert_eq!(
            extended,
            binding_bag(&["?x", "?y"], &[&["a", "1"], &["a", "2"]])
        );
        assert!(bindings
            .extend_rows(vec!["?y".to_var()], vec![vec![]])
            .is_err());
        assert!(bindings
            .extend_rows(
                vec!["?y".to_var()],
                vec![vec![vec![]], vec![vec![bytes("1")]]],
            )
            .is_err());
        assert!(bindings
            .extend_rows(vec!["?x".to_var()], vec![vec![], vec![]])
            .is_err());
    }

    #[test]
    fn projection_and_reorder_address_columns_by_variable() {
        let bindings = binding_bag(
            &["?x", "?y", "?z"],
            &[&["x1", "y1", "z1"], &["x2", "y2", "z2"]],
        );

        assert_eq!(
            bindings.project(&["?z".to_var(), "?x".to_var()]).unwrap(),
            binding_bag(&["?z", "?x"], &[&["z1", "x1"], &["z2", "x2"]])
        );
        assert_eq!(
            bindings
                .reorder(&["?z".to_var(), "?x".to_var(), "?y".to_var()])
                .unwrap(),
            binding_bag(
                &["?z", "?x", "?y"],
                &[&["z1", "x1", "y1"], &["z2", "x2", "y2"]],
            )
        );
        assert!(bindings.project(&["?missing".to_var()]).is_err());
        assert!(bindings.project(&["?x".to_var(), "?x".to_var()]).is_err());
        assert!(bindings.reorder(&["?x".to_var(), "?y".to_var()]).is_err());
    }

    #[test]
    fn distinct_removes_complete_row_duplicates_stably() {
        let bindings = binding_bag(
            &["?x", "?y"],
            &[&["a", "1"], &["b", "2"], &["a", "1"], &["a", "3"]],
        );

        assert_eq!(
            bindings.distinct(),
            binding_bag(&["?x", "?y"], &[&["a", "1"], &["b", "2"], &["a", "3"]],)
        );
    }

    #[test]
    fn row_index_preserves_all_source_row_indexes() {
        let bindings = binding_bag(&["?x", "?y"], &[&["a", "1"], &["b", "1"], &["c", "2"]]);

        let by_y = bindings.index_by(&["?y".to_var()]).unwrap();
        assert_eq!(by_y.get(&vec![bytes("1")]), Some(&vec![0, 1]));
        assert_eq!(by_y.get(&vec![bytes("2")]), Some(&vec![2]));

        let by_nothing = bindings.index_by(&[]).unwrap();
        assert_eq!(by_nothing.get(&vec![]), Some(&vec![0, 1, 2]));
        assert!(bindings.index_by(&["?missing".to_var()]).is_err());
    }

    #[test]
    fn natural_join_indexes_smaller_receiver_and_preserves_layout_and_multiplicity() {
        let left = binding_bag(&["?x", "?left"], &[&["a", "l1"], &["a", "l2"]]);
        let right = binding_bag(
            &["?x", "?y"],
            &[&["a", "1"], &["a", "1"], &["a", "2"], &["c", "3"]],
        );

        let joined = left.natural_join(&right).unwrap();

        assert_eq!(
            joined,
            binding_bag(
                &["?x", "?left", "?y"],
                &[
                    &["a", "l1", "1"],
                    &["a", "l1", "1"],
                    &["a", "l1", "2"],
                    &["a", "l2", "1"],
                    &["a", "l2", "1"],
                    &["a", "l2", "2"],
                ],
            )
        );
    }

    #[test]
    fn natural_join_row_order_is_independent_of_build_side() {
        let left = binding_bag(&["?x", "?left"], &[&["a", "l1"], &["a", "l2"]]);
        let right = binding_bag(&["?x", "?right"], &[&["a", "1"], &["a", "2"]]);
        let larger_right = binding_bag(&["?x", "?right"], &[&["a", "1"], &["a", "2"], &["b", "3"]]);

        assert_eq!(
            left.natural_join(&right).unwrap(),
            left.natural_join(&larger_right).unwrap()
        );
    }

    #[test]
    fn natural_join_indexes_smaller_other_and_preserves_receiver_layout() {
        let left = binding_bag(
            &["?x", "?left"],
            &[&["a", "l1"], &["a", "l2"], &["b", "l3"]],
        );
        let right = binding_bag(&["?right", "?x"], &[&["r1", "a"]]);

        assert_eq!(
            left.natural_join(&right).unwrap(),
            binding_bag(
                &["?x", "?left", "?right"],
                &[&["a", "l1", "r1"], &["a", "l2", "r1"]],
            )
        );
    }

    #[test]
    fn natural_join_without_shared_variables_is_a_cartesian_product() {
        let left = binding_bag(&["?x"], &[&["a"], &["b"]]);
        let right = binding_bag(&["?y"], &[&["1"], &["2"]]);

        assert_eq!(
            left.natural_join(&right).unwrap(),
            binding_bag(
                &["?x", "?y"],
                &[&["a", "1"], &["a", "2"], &["b", "1"], &["b", "2"]],
            )
        );
    }

    #[test]
    fn semijoin_and_antijoin_filter_without_multiplying_left_rows() {
        let left = binding_bag(
            &["?x", "?value"],
            &[&["a", "1"], &["a", "1"], &["b", "2"], &["c", "3"]],
        );
        let right = binding_bag(&["?x"], &[&["a"], &["a"], &["c"]]);

        assert_eq!(
            left.semijoin(&right).unwrap(),
            binding_bag(&["?x", "?value"], &[&["a", "1"], &["a", "1"], &["c", "3"]],)
        );
        assert_eq!(
            left.antijoin(&right).unwrap(),
            binding_bag(&["?x", "?value"], &[&["b", "2"]])
        );
    }

    #[test]
    fn union_has_bag_semantics_and_requires_identical_layouts() {
        let left = binding_bag(&["?x"], &[&["a"], &["a"]]);
        let right = binding_bag(&["?x"], &[&["a"], &["b"]]);

        assert_eq!(
            left.union(&right).unwrap(),
            binding_bag(&["?x"], &[&["a"], &["a"], &["a"], &["b"]])
        );
        assert_eq!(
            left.distinct_union(&right).unwrap(),
            binding_bag(&["?x"], &[&["a"], &["b"]])
        );
        assert!(left.union(&binding_bag(&["?y"], &[&["a"]])).is_err());
    }

    #[test]
    fn zero_column_relations_follow_relational_identity_rules() {
        let unit = BindingBag::unit();
        let empty = BindingBag::empty(vec![]).unwrap();
        let values = binding_bag(&["?x"], &[&["a"], &["b"]]);

        assert_eq!(unit.natural_join(&values).unwrap(), values);
        assert_eq!(values.natural_join(&unit).unwrap(), values);
        assert_eq!(
            empty.natural_join(&values).unwrap(),
            BindingBag::empty(vec!["?x".to_var()]).unwrap()
        );
        assert_eq!(values.semijoin(&unit).unwrap(), values);
        assert_eq!(
            values.semijoin(&empty).unwrap(),
            BindingBag::empty(vec!["?x".to_var()]).unwrap()
        );
        assert_eq!(
            values.antijoin(&unit).unwrap(),
            BindingBag::empty(vec!["?x".to_var()]).unwrap()
        );
        assert_eq!(values.antijoin(&empty).unwrap(), values);
        assert_eq!(unit.union(&unit).unwrap().rows.len(), 2);
        assert_eq!(unit.distinct_union(&unit).unwrap(), unit);
    }
}
