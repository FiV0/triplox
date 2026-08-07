use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{bail, ensure, Result};
use bytes::{Bytes, BytesMut};
use edn::query::Variable;
use slatedb::{DbMetadataOps, DbReadOps};
use tokio::runtime::Handle;

use crate::codec;
use crate::index::IndexType;
use crate::iterator::slate_iterator::{Extractor, Index, SlateIterator};
use crate::iterator::temporal_filter_iterator::TemporalFilterIterator;
use crate::query::binding_bag::{BindingBag, BindingRow};
use crate::query::exec_pattern::{ExecPattern, PatternIndex, Proposal};
use crate::util::make_extractor;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TripleTerm {
    Variable(Variable),
    Constant(Bytes),
}

impl TripleTerm {
    fn is_variable(&self) -> bool {
        match self {
            Self::Variable(_) => true,
            Self::Constant(_) => false,
        }
    }

    fn variable(&self) -> Option<&Variable> {
        match self {
            Self::Variable(variable) => Some(variable),
            Self::Constant(_) => None,
        }
    }

    fn resolve(&self, input: &BindingBag, row: &BindingRow) -> Result<Option<Bytes>> {
        match self {
            Self::Variable(variable) => {
                if input.variables.contains(variable) {
                    Ok(Some(row[input.column_index(variable)?].clone()))
                } else {
                    Ok(None)
                }
            }
            Self::Constant(value) => Ok(Some(value.clone())),
        }
    }
}

#[derive(Clone, Copy)]
enum TriplePosition {
    Entity,
    Value,
}

/// Immutable database value used by triple-pattern scans.
/// Maybe unify with DB struct in node.rs
pub(crate) struct DbValue<D, M>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
    slate: Arc<D>,
    handle: Handle,
    as_of: i64,
    range_stats: Arc<slatedb_estimates::RangeStats<M>>,
}

impl<D, M> DbValue<D, M>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
    pub(crate) fn new(
        slate: Arc<D>,
        handle: Handle,
        as_of: i64,
        range_stats: Arc<slatedb_estimates::RangeStats<M>>,
    ) -> Self {
        Self {
            slate,
            handle,
            as_of,
            range_stats,
        }
    }
}

pub(crate) struct TriplePattern<D, M>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
    index: PatternIndex,
    variables: Vec<Variable>,
    entity: TripleTerm,
    attribute: i64,
    value: TripleTerm,
    db: Arc<DbValue<D, M>>,
}

impl<D, M> TriplePattern<D, M>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
    pub(crate) fn new(
        index: PatternIndex,
        entity: TripleTerm,
        attribute: i64,
        value: TripleTerm,
        db: Arc<DbValue<D, M>>,
    ) -> Result<Self> {
        let mut variables = Vec::new();
        if let Some(variable) = entity.variable() {
            variables.push(variable.clone());
        }
        if let Some(variable) = value.variable() {
            ensure!(
                !variables.contains(variable),
                "Triple pattern {index} repeats variable {variable}"
            );
            variables.push(variable.clone());
        }

        Ok(Self {
            index,
            variables,
            entity,
            attribute,
            value,
            db,
        })
    }

    fn base_prefix(&self, index_type: IndexType) -> Result<Vec<u8>> {
        let mut prefix = vec![codec::index_type_to_prefix(index_type)?];
        codec::encode_i64(self.attribute, &mut prefix);
        Ok(prefix)
    }

    fn term_is_resolved(term: &TripleTerm, input: &BindingBag) -> bool {
        match term {
            TripleTerm::Variable(variable) => input.column_indexes.contains_key(variable),
            TripleTerm::Constant(_) => true,
        }
    }

    fn index_type(position: TriplePosition, other_resolved: bool) -> IndexType {
        match (position, other_resolved) {
            (TriplePosition::Entity, false) => IndexType::AE,
            (TriplePosition::Entity, true) => IndexType::AVE,
            (TriplePosition::Value, false) => IndexType::AV,
            (TriplePosition::Value, true) => IndexType::AEV,
        }
    }

    fn estimate_count(&self, prefix: &[u8]) -> Result<usize> {
        let count = self
            .db
            .handle
            .block_on(self.db.range_stats.estimate_key_count_with_prefix(prefix))?;
        Ok(usize::try_from(count)?)
    }

    fn position_for_variable(&self, variable: &Variable) -> Option<TriplePosition> {
        if self.entity.variable() == Some(variable) {
            Some(TriplePosition::Entity)
        } else if self.value.variable() == Some(variable) {
            Some(TriplePosition::Value)
        } else {
            None
        }
    }

    fn create_iterator(
        &self,
        prefix: Bytes,
        index_type: IndexType,
        extractor: Extractor,
    ) -> Result<Box<dyn Index>> {
        match index_type {
            IndexType::AE | IndexType::AV => Ok(Box::new(SlateIterator::new(
                &prefix,
                self.db.slate.as_ref(),
                self.db.handle.clone(),
                extractor,
                self.db.range_stats.clone(),
            )?)),
            IndexType::EAV | IndexType::AVE | IndexType::AEV | IndexType::VAE => {
                Ok(Box::new(TemporalFilterIterator::new(
                    &prefix,
                    self.db.slate.as_ref(),
                    self.db.handle.clone(),
                    extractor,
                    self.db.as_of,
                    self.db.range_stats.clone(),
                )?))
            }
        }
    }

    // serves to iterate AE/AV indexes
    fn attr_var_iterator(&self, index_type: IndexType) -> Result<Box<dyn Index>> {
        ensure!(
            matches!(index_type, IndexType::AE | IndexType::AV),
            "Component iterator requires an AE or AV index"
        );
        let prefix = Bytes::from(self.base_prefix(index_type)?);
        let extractor: Extractor = Box::new(move |key| make_extractor(1, index_type)(key));
        self.create_iterator(prefix, index_type, extractor)
    }

    // serves to iterate AVE/AEV indexes
    fn attr_var_pair_iterator(&self, index_type: IndexType) -> Result<Box<dyn Index>> {
        ensure!(
            matches!(index_type, IndexType::AEV | IndexType::AVE),
            "Pair iterator requires an AEV or AVE index"
        );
        let prefix = Bytes::from(self.base_prefix(index_type)?);
        let prefix_len = prefix.len();
        let extractor: Extractor =
            Box::new(move |key| key.slice(prefix_len..key.len() - codec::TX_EID_OP_SUFFIX));
        self.create_iterator(prefix, index_type, extractor)
    }

    fn candidate_extensions(
        &self,
        input: &BindingBag,
        position: TriplePosition,
    ) -> Result<Vec<Vec<BindingRow>>> {
        if input.rows.is_empty() {
            return Ok(Vec::new());
        }

        let other = match position {
            TriplePosition::Entity => &self.value,
            TriplePosition::Value => &self.entity,
        };

        let other_resolved = Self::term_is_resolved(other, input);
        let index_type = Self::index_type(position, other_resolved);

        if let Some(other_var) = other.variable() {
            // TODO: once we pass to a sorted columnar trie, the BTreeMap will likely go away
            // We likely also can get rid of the to vec<index> mapping
            let mut groups = BTreeMap::new();
            let index = input.column_index(other_var)?;
            for (row_index, row) in input.rows.iter().enumerate() {
                groups
                    .entry(&row[index])
                    .or_insert_with(Vec::new)
                    .push(row_index);
            }

            let mut extensions = vec![Vec::new(); input.rows.len()];
            if groups.is_empty() {
                return Ok(extensions);
            }

            let mut iterator = self.attr_var_pair_iterator(index_type)?;
            // a bound E or V is constraining, we still scan once through a sorted iteration
            for (first, row_indexes) in groups {
                if !iterator.has_next() {
                    break;
                }
                iterator.seek(first.clone())?;

                let mut group_extensions = Vec::new();
                while let Some(var_pair) = iterator.get_value()? {
                    if !var_pair.starts_with(first) {
                        break;
                    }
                    // the second part of the pair are the new values
                    group_extensions.push(vec![var_pair.slice(first.len()..)]);
                    iterator.next()?;
                }

                for row_index in row_indexes {
                    extensions[row_index] = group_extensions.clone();
                }
            }
            Ok(extensions)
        } else {
            // only the attribute is constraining in this case. We simply walk the iterator.
            let mut iterator = self.attr_var_iterator(index_type)?;
            let mut candidates = Vec::new();
            while let Some(value) = iterator.get_value()? {
                candidates.push(vec![value]);
                iterator.next()?;
            }
            Ok(vec![candidates; input.rows.len()])
        }
    }

    // Partial two-variable checks use additive candidates; the full pair is validated later.
    fn validate(&self, input: &BindingBag) -> Result<BindingBag> {
        if input.rows.is_empty() {
            return input.select_rows(&Vec::new());
        }

        let entity_has_value = Self::term_is_resolved(&self.entity, input);
        let value_has_value = Self::term_is_resolved(&self.value, input);

        let entity_is_variable = self.entity.is_variable();
        let value_is_variable = self.value.is_variable();

        let matches = match (
            entity_is_variable,
            entity_has_value,
            value_is_variable,
            value_has_value,
        ) {
            // Constant entity, constant value.
            (false, true, false, true) => {
                let TripleTerm::Constant(entity) = &self.entity else {
                    unreachable!()
                };
                let TripleTerm::Constant(value) = &self.value else {
                    unreachable!()
                };
                let mut prefix = self.base_prefix(IndexType::AEV)?;
                prefix.extend_from_slice(entity);
                let extractor: Extractor =
                    Box::new(move |key| make_extractor(2, IndexType::AEV)(key));
                let mut iterator =
                    self.create_iterator(Bytes::from(prefix), IndexType::AEV, extractor)?;
                let has_match = if iterator.has_next() {
                    iterator.seek(value.clone())?;
                    iterator.get_value()?.as_ref() == Some(value)
                } else {
                    false
                };
                vec![has_match; input.rows.len()]
            }

            // Bound entity variable, constant value.
            (true, true, false, true) => {
                let mut groups = BTreeMap::new();
                let TripleTerm::Variable(entity_var) = &self.entity else {
                    unreachable!()
                };
                let TripleTerm::Constant(value) = &self.value else {
                    unreachable!()
                };
                let entity_index = input.column_index(entity_var)?;
                for (row_index, row) in input.rows.iter().enumerate() {
                    let mut pair = BytesMut::with_capacity(value.len() + row[entity_index].len());
                    pair.extend_from_slice(value);
                    pair.extend_from_slice(&row[entity_index]);
                    groups
                        .entry(pair.freeze())
                        .or_insert_with(Vec::new)
                        .push(row_index);
                }

                let mut iterator = self.attr_var_pair_iterator(IndexType::AVE)?;
                let mut matches = vec![false; input.rows.len()];
                for (expected, row_indexes) in groups {
                    if !iterator.has_next() {
                        break;
                    }
                    iterator.seek(expected.clone())?;
                    if iterator
                        .get_value()?
                        .as_ref()
                        .is_some_and(|actual| actual == &expected)
                    {
                        for row_index in row_indexes {
                            matches[row_index] = true;
                        }
                    }
                }
                matches
            }

            // Constant entity, bound value variable.
            (false, true, true, true) => {
                let mut groups = BTreeMap::new();
                let TripleTerm::Constant(entity) = &self.entity else {
                    unreachable!()
                };
                let TripleTerm::Variable(value_var) = &self.value else {
                    unreachable!()
                };
                let value_index = input.column_index(value_var)?;
                for (row_index, row) in input.rows.iter().enumerate() {
                    let mut pair = BytesMut::with_capacity(entity.len() + row[value_index].len());
                    pair.extend_from_slice(entity);
                    pair.extend_from_slice(&row[value_index]);
                    groups
                        .entry(pair.freeze())
                        .or_insert_with(Vec::new)
                        .push(row_index);
                }

                let mut iterator = self.attr_var_pair_iterator(IndexType::AEV)?;
                let mut matches = vec![false; input.rows.len()];
                for (expected, row_indexes) in groups {
                    if !iterator.has_next() {
                        break;
                    }
                    iterator.seek(expected.clone())?;
                    if iterator
                        .get_value()?
                        .as_ref()
                        .is_some_and(|actual| actual == &expected)
                    {
                        for row_index in row_indexes {
                            matches[row_index] = true;
                        }
                    }
                }
                matches
            }
            // Bound entity variable, unbound value variable.
            (true, true, true, false) => {
                let mut groups = BTreeMap::new();
                for (row_index, row) in input.rows.iter().enumerate() {
                    let value = self.entity.resolve(input, row)?.ok_or_else(|| {
                        anyhow::anyhow!("Cannot group rows by an unbound triple term")
                    })?;
                    groups.entry(value).or_insert_with(Vec::new).push(row_index);
                }

                let mut iterator = self.attr_var_iterator(IndexType::AE)?;
                let mut matches = vec![false; input.rows.len()];
                for (expected, row_indexes) in groups {
                    if !iterator.has_next() {
                        break;
                    }
                    iterator.seek(expected.clone())?;
                    if iterator
                        .get_value()?
                        .as_ref()
                        .is_some_and(|actual| actual == &expected)
                    {
                        for row_index in row_indexes {
                            matches[row_index] = true;
                        }
                    }
                }
                matches
            }
            // Unbound entity variable, bound value variable.
            (true, false, true, true) => {
                let mut groups = BTreeMap::new();
                for (row_index, row) in input.rows.iter().enumerate() {
                    let value = self.value.resolve(input, row)?.ok_or_else(|| {
                        anyhow::anyhow!("Cannot group rows by an unbound triple term")
                    })?;
                    groups.entry(value).or_insert_with(Vec::new).push(row_index);
                }

                let mut iterator = self.attr_var_iterator(IndexType::AV)?;
                let mut matches = vec![false; input.rows.len()];
                for (expected, row_indexes) in groups {
                    if !iterator.has_next() {
                        break;
                    }
                    iterator.seek(expected.clone())?;
                    if iterator
                        .get_value()?
                        .as_ref()
                        .is_some_and(|actual| actual == &expected)
                    {
                        for row_index in row_indexes {
                            matches[row_index] = true;
                        }
                    }
                }
                matches
            }
            // Bound entity variable, bound value variable.
            (true, true, true, true) => {
                let mut groups = BTreeMap::new();
                for (row_index, row) in input.rows.iter().enumerate() {
                    let entity = self
                        .entity
                        .resolve(input, row)?
                        .ok_or_else(|| anyhow::anyhow!("Cannot group rows by an unbound entity"))?;
                    let value = self
                        .value
                        .resolve(input, row)?
                        .ok_or_else(|| anyhow::anyhow!("Cannot group rows by an unbound value"))?;
                    let mut pair = entity.to_vec();
                    pair.extend_from_slice(&value);
                    groups
                        .entry(Bytes::from(pair))
                        .or_insert_with(Vec::new)
                        .push(row_index);
                }

                let mut iterator = self.attr_var_pair_iterator(IndexType::AEV)?;
                let mut matches = vec![false; input.rows.len()];
                for (expected, row_indexes) in groups {
                    if !iterator.has_next() {
                        break;
                    }
                    iterator.seek(expected.clone())?;
                    if iterator
                        .get_value()?
                        .as_ref()
                        .is_some_and(|actual| actual == &expected)
                    {
                        for row_index in row_indexes {
                            matches[row_index] = true;
                        }
                    }
                }
                matches
            }
            _ => bail!(
                "Triple pattern {} has no bound variables to validate",
                self.index
            ),
        };
        let matched: Vec<usize> = matches
            .into_iter()
            .enumerate()
            .filter_map(|(row_index, matches)| matches.then_some(row_index))
            .collect();

        input.select_rows(&matched)
    }

    fn propose(&self, input: &BindingBag, added: &[Variable]) -> Result<BindingBag> {
        ensure!(
            added.len() == 1,
            "Triple pattern {} can propose exactly one variable, got {added:?}",
            self.index
        );
        ensure!(
            !input.variables.contains(&added[0]),
            "Triple pattern {} cannot add already-bound variable {}",
            self.index,
            added[0]
        );
        let position = self.position_for_variable(&added[0]).ok_or_else(|| {
            anyhow::anyhow!(
                "Triple pattern {} cannot propose variable {}",
                self.index,
                added[0]
            )
        })?;
        input.extend_rows(added.to_vec(), self.candidate_extensions(input, position)?)
    }
}

impl<D, M> ExecPattern for TriplePattern<D, M>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
    fn index(&self) -> PatternIndex {
        self.index
    }

    fn variables(&self) -> &[Variable] {
        &self.variables
    }

    fn count(
        &self,
        input: &BindingBag,
        added: &[Variable],
        proposals: &mut [Proposal],
    ) -> Result<()> {
        ensure!(
            proposals.len() == input.rows.len(),
            "Triple pattern {} received {} proposals for {} input rows",
            self.index,
            proposals.len(),
            input.rows.len()
        );
        ensure!(
            added.len() == 1,
            "Triple pattern {} can count exactly one variable, got {added:?}",
            self.index
        );
        let Some(position) = self.position_for_variable(&added[0]) else {
            let added = &added[0];
            let vars = &self.variables;
            bail!("Triple pattern can only count variables it can propose. Received {added}, variables {vars:?}")
        };
        if input.rows.is_empty() {
            return Ok(());
        }

        let other = match position {
            TriplePosition::Entity => &self.value,
            TriplePosition::Value => &self.entity,
        };
        let other_resolved = Self::term_is_resolved(other, input);
        let index_type = Self::index_type(position, other_resolved);
        let base_prefix = self.base_prefix(index_type)?;

        if let Some(other_var) = other.variable() {
            // 1 variable already bound
            if input.variables.contains(other_var) {
                let index = input.column_index(other_var)?;
                for (row_index, row) in input.rows.iter().enumerate() {
                    let mut prefix = base_prefix.clone();
                    prefix.extend_from_slice(&row[index]);
                    let count = self.estimate_count(&prefix)?;
                    proposals[row_index].consider(self.index, count);
                }
            // nothing bound
            } else {
                let count = self.estimate_count(&base_prefix)?;
                for proposal in proposals {
                    proposal.consider(self.index, count);
                }
            }
        // other is a constant
        } else {
            let mut prefix = base_prefix.clone();
            let constant = match other {
                TripleTerm::Constant(bytes) => bytes,
                TripleTerm::Variable(_) => unreachable!(),
            };
            prefix.extend_from_slice(constant);
            let count = self.estimate_count(&prefix)?;
            for proposal in proposals {
                proposal.consider(self.index, count);
            }
        }
        Ok(())
    }

    fn join(
        &self,
        input: &BindingBag,
        added: &[Variable],
        target_variables: &[Variable],
    ) -> Result<BindingBag> {
        let res = if added.is_empty() {
            self.validate(input)
        } else {
            self.propose(input, added)
        }?;
        ensure!(
            target_variables == res.variables,
            "Triple pattern target_layout {:?} doesnt' match the computed layout {:?}",
            target_variables,
            res.variables
        );
        res.reorder(target_variables)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use bytes::Bytes;
    use edn::query::{ToVariable, Variable};
    use slatedb::Db;

    use super::{DbValue, TriplePattern, TripleTerm};
    use crate::codec::{self, Encode};
    use crate::ops::DataType;
    use crate::query::binding_bag::BindingBag;
    use crate::query::exec_pattern::{ExecPattern, Proposal};
    use crate::slate::in_memory_slate;

    fn encoded(value: DataType) -> Bytes {
        Bytes::from(value.encode())
    }

    fn binding_bag(variables: &[&str], rows: Vec<Vec<Bytes>>) -> BindingBag {
        BindingBag::new(
            variables.iter().map(|variable| variable.to_var()).collect(),
            rows,
        )
        .unwrap()
    }

    async fn insert_version(
        slate: &Db,
        attribute: i64,
        entity: i64,
        value: &DataType,
        tx_eid: i64,
        op: u8,
    ) -> Result<()> {
        let entity = DataType::Long(entity).encode();
        let value = value.encode();
        let attribute = codec::encode_i64_bytes(attribute);
        let tx_eid = codec::encode_i64_bytes(tx_eid);

        let mut aev = vec![codec::AEV];
        aev.extend_from_slice(&attribute);
        aev.extend_from_slice(&entity);
        aev.extend_from_slice(&value);
        aev.extend_from_slice(&tx_eid);
        aev.push(op);
        slate.put(&aev, b"").await?;

        let mut ave = vec![codec::AVE];
        ave.extend_from_slice(&attribute);
        ave.extend_from_slice(&value);
        ave.extend_from_slice(&entity);
        ave.extend_from_slice(&tx_eid);
        ave.push(op);
        slate.put(&ave, b"").await?;

        if op == codec::ADD {
            let mut ae = vec![codec::AE];
            ae.extend_from_slice(&attribute);
            ae.extend_from_slice(&entity);
            slate.put(&ae, b"").await?;

            let mut av = vec![codec::AV];
            av.extend_from_slice(&attribute);
            av.extend_from_slice(&value);
            slate.put(&av, b"").await?;
        }
        Ok(())
    }

    fn variables(entity: &str, value: &str) -> (TripleTerm, TripleTerm) {
        (
            TripleTerm::Variable(entity.to_var()),
            TripleTerm::Variable(value.to_var()),
        )
    }

    #[test]
    fn proposes_each_row_through_the_matching_index() -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        let components = runtime.block_on(in_memory_slate());
        runtime.block_on(async {
            insert_version(
                components.db.as_ref(),
                42,
                1,
                &DataType::String("alice".into()),
                10,
                codec::ADD,
            )
            .await?;
            insert_version(
                components.db.as_ref(),
                42,
                1,
                &DataType::String("ally".into()),
                10,
                codec::ADD,
            )
            .await?;
            insert_version(
                components.db.as_ref(),
                42,
                2,
                &DataType::String("bob".into()),
                10,
                codec::ADD,
            )
            .await
        })?;

        let (entity, value) = variables("?e", "?v");
        let db = Arc::new(DbValue::new(
            components.db,
            runtime.handle().clone(),
            10,
            components.range_stats,
        ));
        let pattern = TriplePattern::new(7, entity, 42, value, db)?;

        let mut entity_proposal = vec![Proposal::default()];
        pattern.count(&BindingBag::unit(), &["?e".to_var()], &mut entity_proposal)?;
        assert_eq!(entity_proposal[0].proposer(), Some(7));
        assert_eq!(entity_proposal[0].count(), 0);
        assert_eq!(
            pattern.join(&BindingBag::unit(), &["?e".to_var()], &["?e".to_var()])?,
            binding_bag(
                &["?e"],
                vec![
                    vec![encoded(DataType::Long(2))],
                    vec![encoded(DataType::Long(1))]
                ],
            )
        );

        let input = binding_bag(
            &["?outer", "?e"],
            vec![
                vec![encoded(DataType::Long(90)), encoded(DataType::Long(1))],
                vec![encoded(DataType::Long(91)), encoded(DataType::Long(2))],
                vec![encoded(DataType::Long(93)), encoded(DataType::Long(1))],
                vec![encoded(DataType::Long(92)), encoded(DataType::Long(3))],
            ],
        );
        let joined = pattern.join(
            &input,
            &["?v".to_var()],
            &["?v".to_var(), "?outer".to_var(), "?e".to_var()],
        )?;
        assert_eq!(
            joined,
            binding_bag(
                &["?v", "?outer", "?e"],
                vec![
                    vec![
                        encoded(DataType::String("alice".into())),
                        encoded(DataType::Long(90)),
                        encoded(DataType::Long(1)),
                    ],
                    vec![
                        encoded(DataType::String("ally".into())),
                        encoded(DataType::Long(90)),
                        encoded(DataType::Long(1)),
                    ],
                    vec![
                        encoded(DataType::String("bob".into())),
                        encoded(DataType::Long(91)),
                        encoded(DataType::Long(2)),
                    ],
                    vec![
                        encoded(DataType::String("alice".into())),
                        encoded(DataType::Long(93)),
                        encoded(DataType::Long(1)),
                    ],
                    vec![
                        encoded(DataType::String("ally".into())),
                        encoded(DataType::Long(93)),
                        encoded(DataType::Long(1)),
                    ],
                ],
            )
        );

        let value_input = binding_bag(
            &["?outer", "?v"],
            vec![
                vec![
                    encoded(DataType::Long(94)),
                    encoded(DataType::String("bob".into())),
                ],
                vec![
                    encoded(DataType::Long(95)),
                    encoded(DataType::String("alice".into())),
                ],
                vec![
                    encoded(DataType::Long(96)),
                    encoded(DataType::String("missing".into())),
                ],
            ],
        );
        assert_eq!(
            pattern.join(
                &value_input,
                &["?e".to_var()],
                &["?e".to_var(), "?outer".to_var(), "?v".to_var()],
            )?,
            binding_bag(
                &["?e", "?outer", "?v"],
                vec![
                    vec![
                        encoded(DataType::Long(2)),
                        encoded(DataType::Long(94)),
                        encoded(DataType::String("bob".into())),
                    ],
                    vec![
                        encoded(DataType::Long(1)),
                        encoded(DataType::Long(95)),
                        encoded(DataType::String("alice".into())),
                    ],
                ],
            )
        );
        Ok(())
    }

    #[test]
    fn count_uses_one_range_estimate_for_duplicate_bound_terms() -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        let components = runtime.block_on(in_memory_slate());
        runtime.block_on(async {
            insert_version(
                components.db.as_ref(),
                42,
                1,
                &DataType::String("alice".into()),
                10,
                codec::ADD,
            )
            .await?;
            insert_version(
                components.db.as_ref(),
                42,
                1,
                &DataType::String("alice".into()),
                20,
                codec::RETRACT,
            )
            .await?;
            components
                .db
                .flush_with_options(slatedb::config::FlushOptions {
                    flush_type: slatedb::config::FlushType::MemTable,
                })
                .await?;
            Ok::<_, anyhow::Error>(())
        })?;

        let entity_1 = encoded(DataType::Long(1));
        let mut prefix = vec![codec::AEV];
        prefix.extend_from_slice(&codec::encode_i64_bytes(42));
        prefix.extend_from_slice(&entity_1);
        let expected = usize::try_from(
            runtime.block_on(
                components
                    .range_stats
                    .estimate_key_count_with_prefix(&prefix),
            )?,
        )?;

        let (entity, value) = variables("?e", "?v");
        let pattern = TriplePattern::new(
            7,
            entity,
            42,
            value,
            Arc::new(DbValue::new(
                components.db,
                runtime.handle().clone(),
                20,
                components.range_stats,
            )),
        )?;
        let input = binding_bag(&["?e"], vec![vec![entity_1.clone()], vec![entity_1]]);
        let mut proposals = vec![Proposal::default(); 2];

        pattern.count(&input, &["?v".to_var()], &mut proposals)?;

        assert_eq!(proposals[0].proposer(), Some(7));
        assert_eq!(proposals[0].count(), expected);
        assert_eq!(proposals[1], proposals[0]);
        Ok(())
    }

    #[test]
    fn partial_validation_is_additive_and_full_validation_is_temporal() -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        let components = runtime.block_on(in_memory_slate());
        runtime.block_on(async {
            insert_version(
                components.db.as_ref(),
                42,
                1,
                &DataType::String("alice".into()),
                10,
                codec::ADD,
            )
            .await?;
            insert_version(
                components.db.as_ref(),
                42,
                2,
                &DataType::String("bob".into()),
                10,
                codec::ADD,
            )
            .await?;
            insert_version(
                components.db.as_ref(),
                42,
                2,
                &DataType::String("bob".into()),
                20,
                codec::RETRACT,
            )
            .await
        })?;

        let (entity, value) = variables("?e", "?v");
        let db = Arc::new(DbValue::new(
            components.db,
            runtime.handle().clone(),
            20,
            components.range_stats,
        ));
        let pattern = TriplePattern::new(7, entity, 42, value, db)?;
        let partial = binding_bag(
            &["?outer", "?e"],
            vec![
                vec![encoded(DataType::Long(9)), encoded(DataType::Long(1))],
                vec![encoded(DataType::Long(9)), encoded(DataType::Long(1))],
                vec![encoded(DataType::Long(8)), encoded(DataType::Long(2))],
            ],
        );
        assert_eq!(pattern.join(&partial, &[], &partial.variables)?, partial);

        let partial_values = binding_bag(
            &["?outer", "?v"],
            vec![
                vec![
                    encoded(DataType::Long(9)),
                    encoded(DataType::String("alice".into())),
                ],
                vec![
                    encoded(DataType::Long(8)),
                    encoded(DataType::String("bob".into())),
                ],
            ],
        );
        assert_eq!(
            pattern.join(&partial_values, &[], &partial_values.variables)?,
            partial_values
        );

        let full = binding_bag(
            &["?v", "?outer", "?e"],
            vec![
                vec![
                    encoded(DataType::String("alice".into())),
                    encoded(DataType::Long(9)),
                    encoded(DataType::Long(1)),
                ],
                vec![
                    encoded(DataType::String("wrong".into())),
                    encoded(DataType::Long(8)),
                    encoded(DataType::Long(1)),
                ],
                vec![
                    encoded(DataType::String("bob".into())),
                    encoded(DataType::Long(7)),
                    encoded(DataType::Long(2)),
                ],
            ],
        );
        assert_eq!(
            pattern.join(&full, &[], &full.variables)?,
            binding_bag(
                &["?v", "?outer", "?e"],
                vec![vec![
                    encoded(DataType::String("alice".into())),
                    encoded(DataType::Long(9)),
                    encoded(DataType::Long(1)),
                ]],
            )
        );
        Ok(())
    }

    #[test]
    fn validation_rejects_patterns_without_bound_variables() -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        let components = runtime.block_on(in_memory_slate());
        let db = Arc::new(DbValue::new(
            components.db,
            runtime.handle().clone(),
            10,
            components.range_stats,
        ));
        let entity_unbound = TriplePattern::new(
            1,
            TripleTerm::Variable("?e".to_var()),
            42,
            TripleTerm::Constant(encoded(DataType::String("alice".into()))),
            db.clone(),
        )?;
        let value_unbound = TriplePattern::new(
            2,
            TripleTerm::Constant(encoded(DataType::Long(1))),
            42,
            TripleTerm::Variable("?v".to_var()),
            db,
        )?;

        assert_eq!(
            entity_unbound
                .validate(&BindingBag::unit())
                .unwrap_err()
                .to_string(),
            "Triple pattern 1 has no bound variables to validate"
        );
        assert_eq!(
            value_unbound
                .validate(&BindingBag::unit())
                .unwrap_err()
                .to_string(),
            "Triple pattern 2 has no bound variables to validate"
        );
        Ok(())
    }

    #[test]
    fn constant_pattern_validation_matches_exact_triple() -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        let components = runtime.block_on(in_memory_slate());
        runtime.block_on(insert_version(
            components.db.as_ref(),
            42,
            1,
            &DataType::String("alice".into()),
            10,
            codec::ADD,
        ))?;

        let db = Arc::new(DbValue::new(
            components.db,
            runtime.handle().clone(),
            10,
            components.range_stats,
        ));
        let existing = TriplePattern::new(
            1,
            TripleTerm::Constant(encoded(DataType::Long(1))),
            42,
            TripleTerm::Constant(encoded(DataType::String("alice".into()))),
            db.clone(),
        )?;
        let wrong_entity = TriplePattern::new(
            2,
            TripleTerm::Constant(encoded(DataType::Long(2))),
            42,
            TripleTerm::Constant(encoded(DataType::String("alice".into()))),
            db.clone(),
        )?;
        let wrong_value = TriplePattern::new(
            3,
            TripleTerm::Constant(encoded(DataType::Long(1))),
            42,
            TripleTerm::Constant(encoded(DataType::String("bob".into()))),
            db,
        )?;

        assert_eq!(existing.validate(&BindingBag::unit())?, BindingBag::unit());
        assert_eq!(
            wrong_entity.validate(&BindingBag::unit())?,
            BindingBag::empty(Vec::<Variable>::new())?
        );
        assert_eq!(
            wrong_value.validate(&BindingBag::unit())?,
            BindingBag::empty(Vec::<Variable>::new())?
        );
        Ok(())
    }

    #[test]
    fn constants_propose_the_other_position_and_constant_only_patterns_validate() -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        let components = runtime.block_on(in_memory_slate());
        runtime.block_on(insert_version(
            components.db.as_ref(),
            42,
            1,
            &DataType::String("alice".into()),
            10,
            codec::ADD,
        ))?;

        let db = Arc::new(DbValue::new(
            components.db,
            runtime.handle().clone(),
            10,
            components.range_stats,
        ));
        let entity_pattern = TriplePattern::new(
            1,
            TripleTerm::Variable("?e".to_var()),
            42,
            TripleTerm::Constant(encoded(DataType::String("alice".into()))),
            db.clone(),
        )?;
        let entities =
            entity_pattern.join(&BindingBag::unit(), &["?e".to_var()], &["?e".to_var()])?;
        assert_eq!(
            entities,
            binding_bag(&["?e"], vec![vec![encoded(DataType::Long(1))]])
        );
        assert_eq!(
            entity_pattern
                .validate(&BindingBag::unit())
                .unwrap_err()
                .to_string(),
            "Triple pattern 1 has no bound variables to validate"
        );

        let value_pattern = TriplePattern::new(
            2,
            TripleTerm::Constant(encoded(DataType::Long(1))),
            42,
            TripleTerm::Variable("?v".to_var()),
            db.clone(),
        )?;
        let values = value_pattern.join(&BindingBag::unit(), &["?v".to_var()], &["?v".to_var()])?;
        assert_eq!(
            values,
            binding_bag(
                &["?v"],
                vec![vec![encoded(DataType::String("alice".into()))]]
            )
        );
        assert_eq!(
            value_pattern
                .validate(&BindingBag::unit())
                .unwrap_err()
                .to_string(),
            "Triple pattern 2 has no bound variables to validate"
        );

        let existing = TriplePattern::new(
            3,
            TripleTerm::Constant(encoded(DataType::Long(1))),
            42,
            TripleTerm::Constant(encoded(DataType::String("alice".into()))),
            db.clone(),
        )?;
        assert_eq!(
            existing.join(&BindingBag::unit(), &[], &[])?,
            BindingBag::unit()
        );

        let missing = TriplePattern::new(
            4,
            TripleTerm::Constant(encoded(DataType::Long(2))),
            42,
            TripleTerm::Constant(encoded(DataType::String("alice".into()))),
            db,
        )?;
        assert_eq!(
            missing.join(&BindingBag::unit(), &[], &[])?,
            BindingBag::empty(Vec::<Variable>::new())?
        );
        Ok(())
    }

    #[test]
    fn historical_basis_observes_add_and_retract_boundaries() -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        let components = runtime.block_on(in_memory_slate());
        runtime.block_on(async {
            insert_version(
                components.db.as_ref(),
                42,
                1,
                &DataType::String("alice".into()),
                10,
                codec::ADD,
            )
            .await?;
            insert_version(
                components.db.as_ref(),
                42,
                1,
                &DataType::String("alice".into()),
                20,
                codec::RETRACT,
            )
            .await
        })?;

        let make_pattern = |index, as_of| {
            TriplePattern::new(
                index,
                TripleTerm::Constant(encoded(DataType::Long(1))),
                42,
                TripleTerm::Constant(encoded(DataType::String("alice".into()))),
                Arc::new(DbValue::new(
                    components.db.clone(),
                    runtime.handle().clone(),
                    as_of,
                    components.range_stats.clone(),
                )),
            )
        };

        assert_eq!(
            make_pattern(1, 9)?.join(&BindingBag::unit(), &[], &[])?,
            BindingBag::empty(Vec::<Variable>::new())?
        );
        assert_eq!(
            make_pattern(2, 10)?.join(&BindingBag::unit(), &[], &[])?,
            BindingBag::unit()
        );
        assert_eq!(
            make_pattern(3, 20)?.join(&BindingBag::unit(), &[], &[])?,
            BindingBag::empty(Vec::<Variable>::new())?
        );
        Ok(())
    }

    #[test]
    fn constructor_rejects_repeated_variables() -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        let components = runtime.block_on(in_memory_slate());
        let db = Arc::new(DbValue::new(
            components.db,
            runtime.handle().clone(),
            10,
            components.range_stats,
        ));
        let new = |entity, value| TriplePattern::new(7, entity, 42, value, db.clone());

        assert!(new(
            TripleTerm::Variable("?x".to_var()),
            TripleTerm::Variable("?x".to_var())
        )
        .is_err());
        Ok(())
    }
}
