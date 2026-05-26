use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use dbsp::{utils::Tup2, ZWeight};
use log::error;
use slatedb::object_store::ObjectStore;
use slatedb::WalReader;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

use crate::codec::Encode;
use crate::inc_query::IncrementalQueryPlan;
use crate::incremental::{EncodedTriple, IncrementalQueryService};
use crate::indexer::{eav_key_to_parts, Indexer};
use crate::ops::{DataType, Datom, DatomOp};
use crate::schema::Schema;
use crate::slate::cdc::{CdcCursor, CdcStream};
use crate::slate::DEFAULT_SCAN_OPTIONS;
use crate::transaction::{TxBasis, TxKey};
use crate::{codec, util::concat_bytes};

const CDC_POLL_INTERVAL: Duration = Duration::from_millis(200);

pub(crate) fn datoms_to_tuples(
    datoms: &[Datom],
    schema: &Schema,
) -> Result<Vec<Tup2<EncodedTriple, ZWeight>>> {
    // Triplox transaction semantics currently guarantee that the tuples going
    // into the circuit form a set, so this conversion does not consolidate.
    datoms
        .iter()
        .map(|datom| {
            let (attribute, _) = schema
                .get_attribute(&datom.attribute)
                .ok_or_else(|| anyhow!("Unknown attribute: {}", datom.attribute))?;
            let weight: ZWeight = match datom.op {
                DatomOp::Assert => 1,
                DatomOp::Retract => -1,
            };
            Ok(Tup2(
                EncodedTriple {
                    entity: DataType::Long(datom.entity).encode(),
                    attribute,
                    value: datom.value.encode(),
                },
                weight,
            ))
        })
        .collect::<Result<Vec<_>>>()
}

pub(crate) fn spawn_writer_cdc_loop(
    path: String,
    object_store: Arc<dyn ObjectStore>,
    indexer: Arc<RwLock<Indexer>>,
    service: IncrementalQueryService,
    registration_gate: Arc<Mutex<()>>,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        if let Err(err) = run_writer_cdc_loop(
            path,
            object_store,
            indexer,
            service,
            registration_gate,
            cancel,
        )
        .await
        {
            error!("Incremental query CDC loop stopped: {:#}", err);
        }
    });
}

async fn run_writer_cdc_loop(
    path: String,
    object_store: Arc<dyn ObjectStore>,
    indexer: Arc<RwLock<Indexer>>,
    service: IncrementalQueryService,
    registration_gate: Arc<Mutex<()>>,
    cancel: CancellationToken,
) -> Result<()> {
    let wal_reader = WalReader::new(path, object_store);
    let mut stream =
        CdcStream::new(wal_reader, CdcCursor::default(), CDC_POLL_INTERVAL, cancel).await?;

    while let Some(tx) = stream.next_transaction().await? {
        let seq = tx.seq;
        let (datoms, basis) = {
            let indexer = indexer.read().await;
            let datoms =
                crate::slate::cdc::datoms_from_cdc_transaction(&tx, &indexer.metadata().schema)?;
            let basis = tx_basis_from_datoms(&datoms);
            (datoms, basis)
        };

        if let Some(basis) = basis {
            let waiter = indexer.read().await.tx_waiter();
            waiter.await_indexed(basis.tx_key).await?;
        }

        let tuples = {
            let indexer = indexer.read().await;
            datoms_to_tuples(&datoms, &indexer.metadata().schema)?
        };
        let _registration_guard = registration_gate.lock().await;
        service.apply_triples(basis, seq, tuples).await?;
        // The registration gate is released before polling the next WAL transaction.
    }

    Ok(())
}

pub(crate) fn tx_basis_from_datoms(datoms: &[Datom]) -> Option<TxBasis> {
    let mut by_entity: HashMap<i64, (Option<i64>, Option<crate::clock::Instant>)> = HashMap::new();
    for datom in datoms {
        let entry = by_entity.entry(datom.entity).or_default();
        match &datom.value {
            DataType::Long(tx_id) if datom.attribute == edn::kw!(:db/txId) => {
                entry.0 = Some(*tx_id);
            }
            DataType::Instant(instant) if datom.attribute == edn::kw!(:db/txInstant) => {
                entry.1 = Some(*instant);
            }
            _ => {}
        }
    }

    by_entity
        .into_iter()
        .find_map(|(tx_eid, (tx_id, instant))| {
            Some(TxBasis {
                tx_key: TxKey {
                    tx_id: tx_id?,
                    system_time: instant?,
                },
                tx_eid,
            })
        })
}

pub(crate) async fn scan_current_triples<D>(
    db: &D,
    plan: &IncrementalQueryPlan,
    as_of_tx_eid: i64,
) -> Result<Vec<Tup2<EncodedTriple, ZWeight>>>
where
    D: slatedb::DbReadOps + Sync,
{
    let attributes = plan
        .patterns
        .iter()
        .map(|pattern| pattern.attribute)
        .collect::<HashSet<_>>();
    let mut latest_by_triple: HashMap<EncodedTriple, (i64, u8)> = HashMap::new();
    let mut iter = db
        .scan_with_options(
            concat_bytes(&[&[codec::EAV]])..vec![codec::EAV_END],
            &DEFAULT_SCAN_OPTIONS,
        )
        .await?;

    while let Some(kv) = iter.next().await? {
        let (entity, attribute, value, tx_eid, op) = eav_key_to_parts(kv.key)?;
        if tx_eid > as_of_tx_eid || !attributes.contains(&attribute) {
            continue;
        }

        let entity = match entity {
            DataType::Long(entity) => DataType::Long(entity).encode(),
            other => return Err(anyhow!("Expected Long entity in EAV key, got {:?}", other)),
        };
        match op {
            codec::ADD | codec::RETRACT => {}
            other => return Err(anyhow!("Unknown op byte: {}", other)),
        }

        let triple = EncodedTriple {
            entity,
            attribute,
            value: value.encode(),
        };
        let should_replace = latest_by_triple
            .get(&triple)
            .is_none_or(|(latest_tx_eid, _)| tx_eid >= *latest_tx_eid);
        if should_replace {
            latest_by_triple.insert(triple, (tx_eid, op));
        }
    }

    let mut triples = latest_by_triple
        .into_iter()
        .filter_map(|(triple, (_tx_eid, op))| (op == codec::ADD).then_some(Tup2(triple, 1)))
        .collect::<Vec<_>>();
    triples.sort();
    Ok(triples)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use edn::kw;

    use super::*;
    use crate::schema::{Attribute, Schema, ValueType};

    fn test_schema() -> Schema {
        let name = kw!(:name);
        let age = kw!(:age);
        let mut ident_map = HashMap::new();
        ident_map.insert(name.clone(), 10);
        ident_map.insert(age.clone(), 11);

        let mut entid_map = HashMap::new();
        entid_map.insert(10, name);
        entid_map.insert(11, age);

        let mut attribute_map = HashMap::new();
        attribute_map.insert(
            10,
            Attribute {
                value_type: ValueType::String,
                multival: true,
                unique: None,
            },
        );
        attribute_map.insert(
            11,
            Attribute {
                value_type: ValueType::Long,
                multival: true,
                unique: None,
            },
        );

        Schema {
            entid_map,
            ident_map,
            attribute_map,
        }
    }

    #[test]
    fn assert_datom_becomes_positive_encoded_triple() {
        let schema = test_schema();
        let datoms = [Datom {
            entity: 42,
            attribute: kw!(:name),
            value: DataType::String("Alice".to_string()),
            op: DatomOp::Assert,
        }];

        let tuples = datoms_to_tuples(&datoms, &schema).unwrap();

        assert_eq!(
            tuples,
            vec![Tup2(
                EncodedTriple {
                    entity: DataType::Long(42).encode(),
                    attribute: 10,
                    value: DataType::String("Alice".to_string()).encode(),
                },
                1,
            )]
        );
    }

    #[test]
    fn retract_datom_becomes_negative_encoded_triple() {
        let schema = test_schema();
        let datoms = [Datom {
            entity: 42,
            attribute: kw!(:age),
            value: DataType::Long(30),
            op: DatomOp::Retract,
        }];

        let tuples = datoms_to_tuples(&datoms, &schema).unwrap();

        assert_eq!(
            tuples,
            vec![Tup2(
                EncodedTriple {
                    entity: DataType::Long(42).encode(),
                    attribute: 11,
                    value: DataType::Long(30).encode(),
                },
                -1,
            )]
        );
    }

    #[test]
    fn unknown_attribute_errors() {
        let schema = test_schema();
        let datoms = [Datom {
            entity: 42,
            attribute: kw!(:unknown),
            value: DataType::Long(30),
            op: DatomOp::Assert,
        }];

        let err = datoms_to_tuples(&datoms, &schema).unwrap_err();
        assert!(err.to_string().contains("Unknown attribute: :unknown"));
    }
}
