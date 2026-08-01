use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{ensure, Result};
use bytes::Bytes;
use edn::query::Variable;
use slatedb::{DbMetadataOps, DbReadOps};
use tokio::runtime::Handle;

use crate::codec;
use crate::index::IndexType;
use crate::iterator::slate_iterator::{Extractor, Index, SlateIterator};
use crate::iterator::temporal_filter_iterator::TemporalFilterIterator;
use crate::query::binding_bag::{BindingBag, BindingRow};
use crate::query::exec_pattern::{ExecPattern, ParticipantIndex, Proposal};
use crate::util::make_extractor;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TripleTerm {
    Variable(Variable),
    Constant(Bytes),
    Placeholder,
}

impl TripleTerm {
    fn variable(&self) -> Option<&Variable> {
        match self {
            Self::Variable(variable) => Some(variable),
            Self::Constant(_) | Self::Placeholder => None,
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
            Self::Placeholder => Ok(None),
        }
    }
}

#[derive(Clone, Copy)]
enum TriplePosition {
    Entity,
    Value,
}

struct ScanSpec {
    prefix: Bytes,
    index_type: IndexType,
    extractor_position: usize,
}

pub(crate) struct TriplePattern<D, M>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
    variables: Vec<Variable>,
    entity: TripleTerm,
    attribute: i64,
    value: TripleTerm,
    slate: Arc<D>,
    handle: Handle,
    as_of: i64,
    range_stats: Arc<slatedb_estimates::RangeStats<M>>,
}

impl<D, M> TriplePattern<D, M>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
    pub(crate) fn new(
        entity: TripleTerm,
        attribute: i64,
        value: TripleTerm,
        slate: Arc<D>,
        handle: Handle,
        as_of: i64,
        range_stats: Arc<slatedb_estimates::RangeStats<M>>,
    ) -> Result<Self> {
        ensure!(
            !matches!(entity, TripleTerm::Placeholder),
            "Triple pattern does not support an entity placeholder"
        );
        ensure!(
            !matches!(value, TripleTerm::Placeholder),
            "Triple pattern does not support a value placeholder"
        );

        let mut variables = Vec::new();
        if let Some(variable) = entity.variable() {
            variables.push(variable.clone());
        }
        if let Some(variable) = value.variable() {
            ensure!(
                !variables.contains(variable),
                "Triple pattern repeats variable {variable}"
            );
            variables.push(variable.clone());
        }

        Ok(Self {
            variables,
            entity,
            attribute,
            value,
            slate,
            handle,
            as_of,
            range_stats,
        })
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

    fn base_prefix(&self, index_type: IndexType) -> Result<Vec<u8>> {
        let mut prefix = vec![codec::index_type_to_prefix(index_type)?];
        codec::encode_i64(self.attribute, &mut prefix);
        Ok(prefix)
    }

    fn proposal_scan(
        &self,
        input: &BindingBag,
        row: &BindingRow,
        position: TriplePosition,
    ) -> Result<ScanSpec> {
        match position {
            TriplePosition::Entity => {
                if let Some(value) = self.value.resolve(input, row)? {
                    let mut prefix = self.base_prefix(IndexType::AVE)?;
                    prefix.extend_from_slice(&value);
                    Ok(ScanSpec {
                        prefix: Bytes::from(prefix),
                        index_type: IndexType::AVE,
                        extractor_position: 2,
                    })
                } else {
                    Ok(ScanSpec {
                        prefix: Bytes::from(self.base_prefix(IndexType::AE)?),
                        index_type: IndexType::AE,
                        extractor_position: 1,
                    })
                }
            }
            TriplePosition::Value => {
                if let Some(entity) = self.entity.resolve(input, row)? {
                    let mut prefix = self.base_prefix(IndexType::AEV)?;
                    prefix.extend_from_slice(&entity);
                    Ok(ScanSpec {
                        prefix: Bytes::from(prefix),
                        index_type: IndexType::AEV,
                        extractor_position: 2,
                    })
                } else {
                    Ok(ScanSpec {
                        prefix: Bytes::from(self.base_prefix(IndexType::AV)?),
                        index_type: IndexType::AV,
                        extractor_position: 1,
                    })
                }
            }
        }
    }

    fn validation_scan(
        &self,
        input: &BindingBag,
        row: &BindingRow,
    ) -> Result<(ScanSpec, Option<Bytes>)> {
        let entity = self.entity.resolve(input, row)?;
        let value = self.value.resolve(input, row)?;

        if let Some(entity) = entity {
            let mut prefix = self.base_prefix(IndexType::AEV)?;
            prefix.extend_from_slice(&entity);
            Ok((
                ScanSpec {
                    prefix: Bytes::from(prefix),
                    index_type: IndexType::AEV,
                    extractor_position: 2,
                },
                value,
            ))
        } else {
            let mut prefix = self.base_prefix(IndexType::AVE)?;
            if let Some(value) = value {
                prefix.extend_from_slice(&value);
            }
            Ok((
                ScanSpec {
                    prefix: Bytes::from(prefix),
                    index_type: IndexType::AVE,
                    extractor_position: 2,
                },
                None,
            ))
        }
    }

    fn create_iterator(&self, spec: ScanSpec) -> Result<Box<dyn Index>> {
        let index_type = spec.index_type;
        let position = spec.extractor_position;
        let extractor: Extractor = Box::new(move |key| make_extractor(position, index_type)(key));

        match index_type {
            IndexType::AE | IndexType::AV => Ok(Box::new(SlateIterator::new(
                &spec.prefix,
                self.slate.as_ref(),
                self.handle.clone(),
                extractor,
                self.range_stats.clone(),
            )?)),
            IndexType::EAV | IndexType::AVE | IndexType::AEV | IndexType::VAE => {
                Ok(Box::new(TemporalFilterIterator::new(
                    &spec.prefix,
                    self.slate.as_ref(),
                    self.handle.clone(),
                    extractor,
                    self.as_of,
                    self.range_stats.clone(),
                )?))
            }
        }
    }

    fn candidate_extensions(
        &self,
        input: &BindingBag,
        row: &BindingRow,
        position: TriplePosition,
    ) -> Result<Vec<BindingRow>> {
        let mut iterator = self.create_iterator(self.proposal_scan(input, row, position)?)?;
        let mut seen = HashSet::new();
        let mut extensions = Vec::new();

        while let Some(value) = iterator.get_value()? {
            if seen.insert(value.clone()) {
                extensions.push(vec![value]);
            }
            iterator.next()?;
        }
        Ok(extensions)
    }

    fn matches(&self, input: &BindingBag, row: &BindingRow) -> Result<bool> {
        let (scan, expected) = self.validation_scan(input, row)?;
        let mut iterator = self.create_iterator(scan)?;
        if let Some(expected) = expected {
            if !iterator.has_next() {
                return Ok(false);
            }
            iterator.seek(expected.clone())?;
            Ok(iterator.get_value()?.as_ref() == Some(&expected))
        } else {
            Ok(iterator.has_next())
        }
    }
}

impl<D, M> ExecPattern for TriplePattern<D, M>
where
    D: DbReadOps + Send + Sync + 'static,
    M: DbMetadataOps + Send + Sync + 'static,
{
    fn variables(&self) -> &[Variable] {
        &self.variables
    }

    fn count(
        &self,
        input: &BindingBag,
        added: &[Variable],
        participant_index: ParticipantIndex,
        proposals: &mut [Proposal],
    ) -> Result<()> {
        ensure!(
            proposals.len() == input.rows.len(),
            "Triple pattern received {} proposals for {} input rows",
            proposals.len(),
            input.rows.len()
        );
        if added.len() != 1 || input.variables.contains(&added[0]) {
            return Ok(());
        }
        let Some(position) = self.position_for_variable(&added[0]) else {
            return Ok(());
        };

        for (row, proposal) in input.rows.iter().zip(proposals) {
            let count = self.candidate_extensions(input, row, position)?.len();
            proposal.consider(participant_index, count);
        }
        Ok(())
    }

    fn join(
        &self,
        input: &BindingBag,
        added: &[Variable],
        target_variables: &[Variable],
    ) -> Result<BindingBag> {
        if added.is_empty() {
            ensure!(
                target_variables == input.variables,
                "Triple pattern validation must preserve the input layout"
            );
            let mut matches = Vec::new();
            for (row_index, row) in input.rows.iter().enumerate() {
                if self.matches(input, row)? {
                    matches.push(row_index);
                }
            }
            return input.select_rows(&matches);
        }

        ensure!(
            added.len() == 1,
            "Triple pattern can propose exactly one variable, got {added:?}"
        );
        ensure!(
            !input.variables.contains(&added[0]),
            "Triple pattern cannot add already-bound variable {}",
            added[0]
        );
        let position = self.position_for_variable(&added[0]).ok_or_else(|| {
            anyhow::anyhow!("Triple pattern cannot propose variable {}", added[0])
        })?;
        let extensions = input
            .rows
            .iter()
            .map(|row| self.candidate_extensions(input, row, position))
            .collect::<Result<Vec<_>>>()?;

        input
            .extend_rows(added.to_vec(), extensions)?
            .reorder(target_variables)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use bytes::Bytes;
    use edn::query::{ToVariable, Variable};
    use slatedb::Db;

    use super::{TriplePattern, TripleTerm};
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
    fn proposes_and_counts_each_row_through_the_matching_index() -> Result<()> {
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
        let pattern = TriplePattern::new(
            entity,
            42,
            value,
            components.db,
            runtime.handle().clone(),
            10,
            components.range_stats,
        )?;

        let mut entity_proposal = vec![Proposal::default()];
        pattern.count(
            &BindingBag::unit(),
            &["?e".to_var()],
            7,
            &mut entity_proposal,
        )?;
        assert_eq!(entity_proposal[0].count(), 2);
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

        let mut value_proposal = vec![Proposal::default()];
        pattern.count(
            &BindingBag::unit(),
            &["?v".to_var()],
            7,
            &mut value_proposal,
        )?;
        assert_eq!(value_proposal[0].count(), 3);

        let input = binding_bag(
            &["?outer", "?e"],
            vec![
                vec![encoded(DataType::Long(90)), encoded(DataType::Long(1))],
                vec![encoded(DataType::Long(91)), encoded(DataType::Long(2))],
                vec![encoded(DataType::Long(92)), encoded(DataType::Long(3))],
            ],
        );
        let mut proposals = vec![Proposal::default(); 3];

        pattern.count(&input, &["?v".to_var()], 7, &mut proposals)?;
        assert_eq!(proposals[0].count(), 2);
        assert_eq!(proposals[1].count(), 1);
        assert_eq!(proposals[2].proposer(), None);

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
                ],
            )
        );
        Ok(())
    }

    #[test]
    fn validation_is_temporal_existential_and_layout_independent() -> Result<()> {
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
        let pattern = TriplePattern::new(
            entity,
            42,
            value,
            components.db,
            runtime.handle().clone(),
            20,
            components.range_stats,
        )?;
        let partial = binding_bag(
            &["?outer", "?e"],
            vec![
                vec![encoded(DataType::Long(9)), encoded(DataType::Long(1))],
                vec![encoded(DataType::Long(9)), encoded(DataType::Long(1))],
                vec![encoded(DataType::Long(8)), encoded(DataType::Long(2))],
            ],
        );
        assert_eq!(
            pattern.join(&partial, &[], &partial.variables)?,
            binding_bag(
                &["?outer", "?e"],
                vec![
                    vec![encoded(DataType::Long(9)), encoded(DataType::Long(1))],
                    vec![encoded(DataType::Long(9)), encoded(DataType::Long(1))],
                ],
            )
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

        let entity_pattern = TriplePattern::new(
            TripleTerm::Variable("?e".to_var()),
            42,
            TripleTerm::Constant(encoded(DataType::String("alice".into()))),
            components.db.clone(),
            runtime.handle().clone(),
            10,
            components.range_stats.clone(),
        )?;
        let entities =
            entity_pattern.join(&BindingBag::unit(), &["?e".to_var()], &["?e".to_var()])?;
        assert_eq!(
            entities,
            binding_bag(&["?e"], vec![vec![encoded(DataType::Long(1))]])
        );

        let value_pattern = TriplePattern::new(
            TripleTerm::Constant(encoded(DataType::Long(1))),
            42,
            TripleTerm::Variable("?v".to_var()),
            components.db.clone(),
            runtime.handle().clone(),
            10,
            components.range_stats.clone(),
        )?;
        let values = value_pattern.join(&BindingBag::unit(), &["?v".to_var()], &["?v".to_var()])?;
        assert_eq!(
            values,
            binding_bag(
                &["?v"],
                vec![vec![encoded(DataType::String("alice".into()))]]
            )
        );

        let existing = TriplePattern::new(
            TripleTerm::Constant(encoded(DataType::Long(1))),
            42,
            TripleTerm::Constant(encoded(DataType::String("alice".into()))),
            components.db.clone(),
            runtime.handle().clone(),
            10,
            components.range_stats.clone(),
        )?;
        assert_eq!(
            existing.join(&BindingBag::unit(), &[], &[])?,
            BindingBag::unit()
        );

        let missing = TriplePattern::new(
            TripleTerm::Constant(encoded(DataType::Long(2))),
            42,
            TripleTerm::Constant(encoded(DataType::String("alice".into()))),
            components.db,
            runtime.handle().clone(),
            10,
            components.range_stats,
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

        let make_pattern = |as_of| {
            TriplePattern::new(
                TripleTerm::Constant(encoded(DataType::Long(1))),
                42,
                TripleTerm::Constant(encoded(DataType::String("alice".into()))),
                components.db.clone(),
                runtime.handle().clone(),
                as_of,
                components.range_stats.clone(),
            )
        };

        assert_eq!(
            make_pattern(9)?.join(&BindingBag::unit(), &[], &[])?,
            BindingBag::empty(Vec::<Variable>::new())?
        );
        assert_eq!(
            make_pattern(10)?.join(&BindingBag::unit(), &[], &[])?,
            BindingBag::unit()
        );
        assert_eq!(
            make_pattern(20)?.join(&BindingBag::unit(), &[], &[])?,
            BindingBag::empty(Vec::<Variable>::new())?
        );
        Ok(())
    }

    #[test]
    fn constructor_rejects_unsupported_and_repeated_shapes() -> Result<()> {
        let runtime = tokio::runtime::Runtime::new()?;
        let components = runtime.block_on(in_memory_slate());
        let new = |entity, value| {
            TriplePattern::new(
                entity,
                42,
                value,
                components.db.clone(),
                runtime.handle().clone(),
                10,
                components.range_stats.clone(),
            )
        };

        assert!(new(TripleTerm::Placeholder, TripleTerm::Variable("?v".to_var())).is_err());
        assert!(new(TripleTerm::Variable("?e".to_var()), TripleTerm::Placeholder).is_err());
        assert!(new(
            TripleTerm::Variable("?x".to_var()),
            TripleTerm::Variable("?x".to_var())
        )
        .is_err());
        Ok(())
    }
}
