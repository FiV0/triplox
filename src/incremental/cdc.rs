use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use dbsp::{utils::Tup2, ZWeight};
use edn::kw;
use log::info;
use slatedb::object_store::ObjectStore;
use slatedb::WalReader;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::codec::Encode;
use crate::inc_query::IncrementalQueryPlan;
use crate::incremental::{EncodedTriple, IncrementalQueryService};
use crate::indexer::eav_key_to_parts;
use crate::node::SchemaProvider;
use crate::ops::{DataType, Datom, DatomOp};
use crate::partition::{extract_counter, extract_partition, TX_PARTITION};
use crate::schema::Schema;
use crate::slate::cdc::{CdcCursor, CdcStream};
use crate::slate::DEFAULT_SCAN_OPTIONS;
use crate::transaction::TxKey;
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

pub(crate) fn spawn_cdc_loop<N>(
    object_path: String,
    object_store: Arc<dyn ObjectStore>,
    node: Arc<N>,
    service: IncrementalQueryService,
    registration_gate: Arc<Mutex<()>>,
    cancel: CancellationToken,
) -> JoinHandle<Result<()>>
where
    N: SchemaProvider,
{
    tokio::spawn(run_cdc_loop(
        object_path,
        object_store,
        node,
        service,
        registration_gate,
        cancel,
    ))
}

async fn run_cdc_loop<N>(
    object_path: String,
    object_store: Arc<dyn ObjectStore>,
    node: Arc<N>,
    service: IncrementalQueryService,
    registration_gate: Arc<Mutex<()>>,
    cancel: CancellationToken,
) -> Result<()>
where
    N: SchemaProvider,
{
    let wal_reader = WalReader::new(object_path, object_store);
    let mut stream =
        CdcStream::new(wal_reader, CdcCursor::default(), CDC_POLL_INTERVAL, cancel).await?;

    while let Some(tx) = stream.next_transaction().await? {
        let seq = tx.seq;
        let schema = node.schema().await;
        let datoms = crate::slate::cdc::datoms_from_cdc_transaction(&tx, &schema)?;
        if datoms.is_empty() {
            continue;
        }
        let tx_key = tx_key_from_datoms(&datoms)?;
        let tuples = datoms_to_tuples(&datoms, &schema)?;
        let _registration_guard = registration_gate.lock().await;
        service.apply_triples(tx_key, seq, tuples).await?;
        // The registration gate is released before polling the next WAL transaction.
    }

    info!("Incremental query CDC stream exited normally");
    Ok(())
}

/// Recover the `TxKey` for a CDC-streamed transaction from its datoms.
pub(crate) fn tx_key_from_datoms(datoms: &[Datom]) -> Result<TxKey> {
    datoms
        .iter()
        .find_map(|datom| match &datom.value {
            DataType::Instant(instant)
                if datom.attribute == kw!(:db/txInstant)
                    && extract_partition(datom.entity) == TX_PARTITION =>
            {
                Some(TxKey {
                    tx_id: extract_counter(datom.entity),
                    system_time: *instant,
                })
            }
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("CDC transaction datoms missing transaction key"))
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
    // TODO: This needs to be done efficiently via AVE/AEV using attribute and query constants. See #329.
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
    use std::sync::Arc;

    use edn::kw;
    use tokio::sync::{Mutex, RwLock};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::clock::st_from_unix_epoch;
    use crate::indexer::{Indexer, DEFAULT_TX_COMPLETION_CAPACITY};
    use crate::metadata::{Metadata, PartitionMap};
    use crate::partition::{make_entity_id, tx_eid_from_tx_id, USER_PARTITION};
    use crate::schema::{Attribute, Schema, ValueType};

    #[tokio::test]
    async fn cdc_loop_exits_ok_when_cancelled() {
        let slate = crate::slate::in_memory_slate().await;
        let indexer = Arc::new(RwLock::new(Indexer::new(
            slate.db.clone(),
            Metadata::new(test_schema(), PartitionMap::new()),
            *crate::bootstrap::BOOTSTRAP_TX_KEY,
            DEFAULT_TX_COMPLETION_CAPACITY,
        )));
        let service = IncrementalQueryService::new(
            tempfile::tempdir().unwrap().path().to_path_buf(),
            tokio::runtime::Handle::current(),
            CancellationToken::new(),
            slate.object_path.clone(),
            slate.object_store.clone(),
        );
        let cancel = CancellationToken::new();
        cancel.cancel();

        let result = run_cdc_loop(
            slate.object_path,
            slate.object_store,
            indexer,
            service,
            Arc::new(Mutex::new(())),
            cancel,
        )
        .await;

        assert!(result.is_ok());
    }

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
    fn tx_key_from_datoms_extracts_transaction_key() {
        let instant = st_from_unix_epoch(123);
        // A real tx entity's id is `tx_eid_from_tx_id(tx_id)`, so `tx_id` is
        // recovered by masking the entity id rather than reading `db/txId`.
        let tx_eid = tx_eid_from_tx_id(42);
        let datoms = [
            Datom {
                entity: tx_eid,
                attribute: kw!(:db/txId),
                value: DataType::Long(42),
                op: DatomOp::Assert,
            },
            Datom {
                entity: tx_eid,
                attribute: kw!(:db/txInstant),
                value: DataType::Instant(instant),
                op: DatomOp::Assert,
            },
        ];

        let tx_key = tx_key_from_datoms(&datoms).unwrap();

        assert_eq!(
            tx_key,
            TxKey {
                tx_id: 42,
                system_time: instant,
            }
        );
    }

    #[test]
    fn tx_key_from_datoms_errors_without_transaction_key() {
        let datoms = [Datom {
            entity: 42,
            attribute: kw!(:name),
            value: DataType::String("Alice".to_string()),
            op: DatomOp::Assert,
        }];

        let err = tx_key_from_datoms(&datoms).unwrap_err();

        assert!(err
            .to_string()
            .contains("CDC transaction datoms missing transaction key"));
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
