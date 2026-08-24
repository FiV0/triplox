//! Mentat-style :db.unique/identity upsert resolution.
//!
//! This module implements the upsert resolution algorithm described at
//! <https://github.com/mozilla/mentat/wiki/Transacting:-upsert-resolution-algorithm>.
//!
//! Inputs are `DatomWithTempids` produced by the earlier `expand_tx_ops` /
//! `into_datoms_with_tempids` stages in `tx.rs`. The output is a `Vec<Datom>` with
//! every tempid resolved to a concrete entid.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::Result;
use indexmap::IndexSet;

use crate::ops::{DataType, Datom, DatomOp, Entid};
use crate::schema::{Schema, Unique};
use crate::tx::{DatomWithTempids, IdOrTempId, ValueWithTempIds};
use crate::union_find::UnionFind;

pub(crate) type TempIdMap = HashMap<String, Entid>;
pub(crate) type UniqueLookup = (Entid, DataType);

/// A "Simple upsert" that looks like `[:db/add TEMPID a v]`, where `a` is `:db.unique/identity`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct UpsertE(String, Entid, DataType);

/// A "Complex upsert" that looks like `[:db/add TEMPID a OTHERTEMPID]`, where `a` is
/// `:db.unique/identity`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct UpsertEV(String, Entid, String);

/// A generation collects entities into populations at a single evolutionary step in the upsert
/// resolution evolution process.
///
/// The upsert resolution process is only concerned with `[:db/add ...]` entities until the final
/// entid allocations. That's why we separate into special simple and complex upsert types
/// immediately, and then collect the more general datom types for final resolution.
#[derive(Debug, Default)]
pub(crate) struct Generation {
    /// "Simple upserts" that look like `[:db/add TEMPID a v]`, where `a` is `:db.unique/identity`.
    upserts_e: Vec<UpsertE>,

    /// "Complex upserts" that look like `[:db/add TEMPID a OTHERTEMPID]`, where `a` is
    /// `:db.unique/identity`.
    upserts_ev: Vec<UpsertEV>,

    /// Entities that look like:
    /// - `[:db/add TEMPID b OTHERTEMPID]`. `b` may be `:db.unique/identity` if it has failed to upsert.
    /// - `[:db/add TEMPID b v]`. `b` may be `:db.unique/identity` if it has failed to upsert.
    /// - `[:db/add e b OTHERTEMPID]`.
    allocations: Vec<DatomWithTempids>,

    /// Entities that upserted and no longer reference tempids. These assertions are guaranteed to
    /// be in the store.
    upserted: Vec<Datom>,

    /// Entities that resolved due to other upserts and no longer reference tempids. These
    /// assertions may or may not be in the store.
    resolved: Vec<Datom>,
}

#[derive(Debug, Default)]
pub(crate) struct FinalPopulations {
    /// Upserts that upserted.
    pub(crate) upserted: Vec<Datom>,

    /// Allocations that resolved due to other upserts.
    pub(crate) resolved: Vec<Datom>,

    /// Allocations that required new entid allocations.
    pub(crate) allocated: Vec<Datom>,
}

fn datom_with_entid_attr(d: &DatomWithTempids, schema: &Schema) -> Result<(Entid, bool)> {
    let (attr_eid, attr) = schema
        .get_attribute(&d.attribute)
        .ok_or_else(|| anyhow::anyhow!("Unknown attribute: {}", d.attribute))?;
    Ok((attr_eid, attr.unique == Some(Unique::Identity)))
}

fn substitute_datom(d: DatomWithTempids, temp_id_map: &TempIdMap) -> Result<EitherDatom> {
    let entity = match d.entity {
        IdOrTempId::Id(id) => IdOrTempId::Id(id),
        IdOrTempId::TempId(s) => match temp_id_map.get(&s) {
            Some(eid) => IdOrTempId::Id(*eid),
            None => IdOrTempId::TempId(s),
        },
    };
    let value = match d.value {
        ValueWithTempIds::Data(dt) => ValueWithTempIds::Data(dt),
        ValueWithTempIds::TempRef(s) => match temp_id_map.get(&s) {
            Some(eid) => ValueWithTempIds::Data(DataType::Long(*eid)),
            None => ValueWithTempIds::TempRef(s),
        },
    };

    match (&entity, &value) {
        (IdOrTempId::Id(entity), ValueWithTempIds::Data(value)) => {
            Ok(EitherDatom::Concrete(Datom {
                entity: *entity,
                attribute: d.attribute,
                value: value.clone(),
                op: d.op,
            }))
        }
        _ => Ok(EitherDatom::WithTempids(DatomWithTempids {
            entity,
            attribute: d.attribute,
            value,
            op: d.op,
        })),
    }
}

enum EitherDatom {
    Concrete(Datom),
    WithTempids(DatomWithTempids),
}

impl Generation {
    /// Split datoms into a generation of populations that need to evolve to have their tempids
    /// resolved or allocated, and a population of inert datoms that do not reference tempids.
    pub(crate) fn from(
        datoms: Vec<DatomWithTempids>,
        schema: &Schema,
    ) -> Result<(Self, Vec<Datom>)> {
        let mut generation = Generation::default();
        let mut inert = Vec::new();

        for d in datoms {
            let (attr_eid, identity) = datom_with_entid_attr(&d, schema)?;
            match (&d.entity, &d.value) {
                (IdOrTempId::TempId(t), ValueWithTempIds::Data(v))
                    if d.op == DatomOp::Assert && identity =>
                {
                    generation
                        .upserts_e
                        .push(UpsertE(t.clone(), attr_eid, v.clone()));
                }
                (IdOrTempId::TempId(t1), ValueWithTempIds::TempRef(t2))
                    if d.op == DatomOp::Assert && identity =>
                {
                    generation
                        .upserts_ev
                        .push(UpsertEV(t1.clone(), attr_eid, t2.clone()));
                }
                (IdOrTempId::Id(entity), ValueWithTempIds::Data(value)) => {
                    inert.push(Datom {
                        entity: *entity,
                        attribute: d.attribute,
                        value: value.clone(),
                        op: d.op,
                    });
                }
                // Retracts, non-identity tempid asserts, and `(Id, TempRef)` — all still
                // reference a tempid that needs later allocation or substitution.
                _ => generation.allocations.push(d),
            }
        }

        Ok((generation, inert))
    }

    /// Return true if it's possible to evolve this generation further.
    ///
    /// Note that there can be complex upserts but no simple upserts to help resolve them, and in
    /// this case, we cannot evolve further.
    pub(crate) fn can_evolve(&self) -> bool {
        !self.upserts_e.is_empty()
    }

    /// Collect tempid -> [a v] pairs that might upsert at this evolutionary step.
    pub(crate) fn temp_id_avs(&self) -> Vec<(String, UniqueLookup)> {
        self.upserts_e
            .iter()
            .map(|UpsertE(t, a, v)| (t.clone(), (*a, v.clone())))
            .collect()
    }

    /// Evolve potential upserts that haven't resolved into allocations.
    pub(crate) fn allocate_unresolved_upserts(&mut self, schema: &Schema) -> Result<()> {
        for UpsertEV(t1, attr_eid, t2) in self.upserts_ev.drain(..) {
            let attr = schema
                .entid_map
                .get(&attr_eid)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Unknown attribute entid: {}", attr_eid))?;
            self.allocations.push(DatomWithTempids {
                entity: IdOrTempId::TempId(t1),
                attribute: attr,
                value: ValueWithTempIds::TempRef(t2),
                op: DatomOp::Assert,
            });
        }
        Ok(())
    }

    /// Evolve this generation one step further by rewriting the existing `:db/add` datoms using
    /// the given temporary IDs.
    ///
    /// Tempids resolved in earlier generations (tracked in `resolved_tempids`) are also honored,
    /// so that a tempid which surfaces a second time (e.g. via an `UpsertEV` promotion) does not
    /// get re-allocated a fresh entid.
    pub(crate) fn evolve_one_step(
        self,
        temp_id_map: &TempIdMap,
        resolved_tempids: &BTreeMap<String, Entid>,
        schema: &Schema,
    ) -> Result<Generation> {
        let mut next = Generation {
            resolved: self.resolved,
            ..Generation::default()
        };

        for UpsertE(t, a, v) in self.upserts_e {
            let attr = schema
                .entid_map
                .get(&a)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Unknown attribute entid: {}", a))?;
            match temp_id_map.get(&t) {
                Some(eid) => next.upserted.push(Datom {
                    entity: *eid,
                    attribute: attr,
                    value: v,
                    op: DatomOp::Assert,
                }),
                None if resolved_tempids.contains_key(&t) => next.resolved.push(Datom {
                    entity: resolved_tempids[&t],
                    attribute: attr,
                    value: v,
                    op: DatomOp::Assert,
                }),
                None => next.allocations.push(DatomWithTempids {
                    entity: IdOrTempId::TempId(t),
                    attribute: attr,
                    value: ValueWithTempIds::Data(v),
                    op: DatomOp::Assert,
                }),
            }
        }

        for UpsertEV(t1, a, t2) in self.upserts_ev {
            let attr = schema
                .entid_map
                .get(&a)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Unknown attribute entid: {}", a))?;
            let t1_eid = temp_id_map.get(&t1).or_else(|| resolved_tempids.get(&t1));
            let t2_eid = temp_id_map.get(&t2).or_else(|| resolved_tempids.get(&t2));
            match (t1_eid, t2_eid) {
                // Even though we can resolve entirely when both tempids are known, it's possible
                // that the remaining upsert could conflict. Moving straight to resolved doesn't
                // give us a chance to search the store for the conflict.
                (Some(_), Some(t2_eid)) | (None, Some(t2_eid)) => {
                    next.upserts_e.push(UpsertE(t1, a, DataType::Long(*t2_eid)));
                }
                (Some(t1_eid), None) => next.allocations.push(DatomWithTempids {
                    entity: IdOrTempId::Id(*t1_eid),
                    attribute: attr,
                    value: ValueWithTempIds::TempRef(t2),
                    op: DatomOp::Assert,
                }),
                (None, None) => next.upserts_ev.push(UpsertEV(t1, a, t2)),
            }
        }

        let combined: TempIdMap = resolved_tempids
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .chain(temp_id_map.iter().map(|(k, v)| (k.clone(), *v)))
            .collect();
        for d in self.allocations {
            match substitute_datom(d, &combined)? {
                EitherDatom::Concrete(d) => next.resolved.push(d),
                EitherDatom::WithTempids(d) => next.allocations.push(d),
            }
        }

        Ok(next)
    }

    /// After evolution is complete, yield the set of tempids that require entid allocation.
    ///
    /// Some of the tempids may be identified, so we also provide a map from tempid to a dense set
    /// of contiguous integer labels.
    pub(crate) fn temp_ids_in_allocations(
        &self,
        schema: &Schema,
    ) -> Result<BTreeMap<String, usize>> {
        let mut tempids = BTreeSet::new();
        let mut value_temprefs = BTreeSet::new();
        let mut identity_groups: HashMap<(Entid, ValueWithTempIds), Vec<String>> = HashMap::new();

        for d in &self.allocations {
            if d.op == DatomOp::Retract
                && (matches!(d.entity, IdOrTempId::TempId(_))
                    || matches!(d.value, ValueWithTempIds::TempRef(_)))
            {
                return Err(anyhow::anyhow!(
                    "[:db/retract ...] referenced tempid that did not upsert"
                ));
            }

            if d.op != DatomOp::Assert {
                continue;
            }

            if let IdOrTempId::TempId(t) = &d.entity {
                tempids.insert(t.clone());
            }
            if let ValueWithTempIds::TempRef(t) = &d.value {
                value_temprefs.insert(t.clone());
            }

            let (attr_eid, identity) = datom_with_entid_attr(d, schema)?;
            if identity {
                if let IdOrTempId::TempId(t) = &d.entity {
                    identity_groups
                        .entry((attr_eid, d.value.clone()))
                        .or_default()
                        .push(t.clone());
                }
            }
        }

        for t in value_temprefs {
            if !tempids.contains(&t) {
                return Err(anyhow::anyhow!(
                    "Tempid {} referenced only in value position",
                    t
                ));
            }
        }

        // Now we union-find all the known tempids. Two tempids are unioned if they both appear as
        // the entity of an `[a v]` upsert, including when the value column `v` is itself a tempid.
        // Our `UnionFind` operates on contiguous indices, so we maintain the map from tempids to
        // indices ourselves (sorted via `BTreeSet` for deterministic results).
        let tempid_indices: BTreeMap<String, usize> = tempids
            .into_iter()
            .enumerate()
            .map(|(i, t)| (t, i))
            .collect();
        let mut uf = UnionFind::new(tempid_indices.len());

        for group in identity_groups.values() {
            if let Some(first) = group.first().and_then(|t| tempid_indices.get(t)) {
                for t in group {
                    if let Some(i) = tempid_indices.get(t) {
                        uf.union(*first, *i);
                    }
                }
            }
        }

        // Now that we have aggregated tempids, label them using the smallest number of contiguous
        // labels possible. We allocate labels for tempids in sorted order (driven by the
        // `BTreeMap` iteration above) so that "a" gets a smaller label than "b", which is pleasant
        // for testing.
        let mut dense_labels: IndexSet<usize> = IndexSet::new();
        let mut tempid_labels = BTreeMap::new();
        for (tempid, index) in tempid_indices {
            let rep = uf.find(index);
            let (label, _) = dense_labels.insert_full(rep);
            tempid_labels.insert(tempid, label);
        }

        Ok(tempid_labels)
    }

    /// After evolution is complete, use the provided allocated entids to segment `self` into
    /// populations, each with no references to tempids.
    pub(crate) fn into_final_populations(
        self,
        temp_id_map: &TempIdMap,
    ) -> Result<FinalPopulations> {
        let mut populations = FinalPopulations {
            upserted: self.upserted,
            resolved: self.resolved,
            allocated: Vec::new(),
        };

        for d in self.allocations {
            match substitute_datom(d, temp_id_map)? {
                EitherDatom::Concrete(d) => populations.allocated.push(d),
                EitherDatom::WithTempids(_) => {
                    return Err(anyhow::anyhow!("Unresolved tempid after allocation"))
                }
            }
        }

        Ok(populations)
    }
}
