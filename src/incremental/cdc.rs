use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use dbsp::{utils::Tup2, ZWeight};
use edn::kw;
use futures::{stream, StreamExt, TryStreamExt};
use log::info;
use slatedb::object_store::ObjectStore;
use slatedb::WalReader;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::codec::{self, Encode};
use crate::inc_query::{IncrementalQueryPlan, PatternPlan, PatternSlot};
use crate::incremental::{EncodedTriple, IncrementalQueryService};
use crate::indexer::{aev_key_to_parts, ave_key_to_parts, IndexerHandle};
use crate::ops::{DataType, Datom, DatomOp};
use crate::partition::{extract_counter, extract_partition, TX_PARTITION};
use crate::schema::Schema;
use crate::slate::cdc::{CdcCursor, CdcStream};
use crate::slate::DEFAULT_SCAN_OPTIONS;
use crate::transaction::TxKey;

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

pub(crate) fn spawn_cdc_loop(
    object_path: String,
    object_store: Arc<dyn ObjectStore>,
    indexer: IndexerHandle,
    service: IncrementalQueryService,
    registration_gate: Arc<Mutex<()>>,
    cancel: CancellationToken,
    poll_interval: Duration,
) -> JoinHandle<Result<()>> {
    tokio::spawn(run_cdc_loop(
        object_path,
        object_store,
        indexer,
        service,
        registration_gate,
        cancel,
        poll_interval,
    ))
}

async fn run_cdc_loop(
    object_path: String,
    object_store: Arc<dyn ObjectStore>,
    indexer: IndexerHandle,
    service: IncrementalQueryService,
    registration_gate: Arc<Mutex<()>>,
    cancel: CancellationToken,
    poll_interval: Duration,
) -> Result<()> {
    let wal_reader = WalReader::new(object_path, object_store);
    let mut stream =
        CdcStream::new(wal_reader, CdcCursor::default(), poll_interval, cancel).await?;

    while let Some(tx) = stream.next_transaction().await? {
        let Some(tx_id) = crate::slate::cdc::tx_id_from_cdc_transaction(&tx)? else {
            continue;
        };
        // The WAL can hold a tx before its schema update is published, so wait for it.
        let schema = indexer.await_indexed_state(tx_id).await?.schema;
        let datoms = crate::slate::cdc::datoms_from_cdc_transaction(&tx, &schema)?;
        if datoms.is_empty() {
            continue;
        }
        let tx_key = tx_key_from_datoms(&datoms)?;
        let tuples = datoms_to_tuples(&datoms, &schema)?;
        let _registration_guard = registration_gate.lock().await;
        service.apply_triples(tx_key, tuples).await?;
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

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct InitialScan {
    attribute: i64,
    entity: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
}

impl InitialScan {
    fn from_pattern(pattern: &PatternPlan) -> Self {
        let constant = |slot: &PatternSlot| match slot {
            PatternSlot::Constant(value) => Some(value.clone()),
            PatternSlot::Variable(_) => None,
        };
        Self {
            attribute: pattern.attribute,
            entity: constant(&pattern.entity),
            value: constant(&pattern.value),
        }
    }

    fn covers(&self, other: &Self) -> bool {
        self.attribute == other.attribute
            && self
                .entity
                .as_ref()
                .is_none_or(|e| other.entity.as_ref() == Some(e))
            && self
                .value
                .as_ref()
                .is_none_or(|v| other.value.as_ref() == Some(v))
    }

    fn index(&self) -> u8 {
        if self.entity.is_none() && self.value.is_some() {
            codec::AVE
        } else {
            codec::AEV
        }
    }

    fn prefix(&self) -> Vec<u8> {
        let mut prefix = vec![self.index()];
        codec::encode_i64(self.attribute, &mut prefix);
        if let Some(entity) = &self.entity {
            prefix.extend_from_slice(entity);
        }
        if let Some(value) = &self.value {
            prefix.extend_from_slice(value);
        }
        prefix
    }
}

fn initial_scans(plan: &IncrementalQueryPlan) -> Vec<InitialScan> {
    let mut scans = plan
        .leaf_patterns()
        .into_iter()
        .map(InitialScan::from_pattern)
        .collect::<Vec<_>>();
    scans.sort();
    scans.dedup();
    scans
        .iter()
        .filter(|scan| {
            !scans
                .iter()
                .any(|other| other != *scan && other.covers(scan))
        })
        .cloned()
        .collect()
}

fn keep_latest(
    latest_by_triple: &mut HashMap<EncodedTriple, (i64, u8)>,
    triple: EncodedTriple,
    tx_eid: i64,
    op: u8,
) {
    let should_replace = latest_by_triple
        .get(&triple)
        .is_none_or(|(latest_tx_eid, _)| tx_eid >= *latest_tx_eid);
    if should_replace {
        latest_by_triple.insert(triple, (tx_eid, op));
    }
}

async fn scan_latest_triples<D>(
    db: &D,
    scan: &InitialScan,
    as_of_tx_eid: i64,
) -> Result<HashMap<EncodedTriple, (i64, u8)>>
where
    D: slatedb::DbReadOps + Sync,
{
    let index = scan.index();
    let mut iter = db
        .scan_prefix_with_options(scan.prefix(), .., &DEFAULT_SCAN_OPTIONS)
        .await?;

    let mut latest_by_triple = HashMap::new();
    while let Some(kv) = iter.next().await? {
        let (attribute, entity, value, tx_eid, op) = match index {
            codec::AEV => aev_key_to_parts(kv.key)?,
            codec::AVE => {
                let (attribute, value, entity, tx_eid, op) = ave_key_to_parts(kv.key)?;
                (attribute, entity, value, tx_eid, op)
            }
            other => unreachable!("initial scans use AEV or AVE, got index {other}"),
        };
        if tx_eid > as_of_tx_eid {
            continue;
        }

        let entity = match entity {
            DataType::Long(entity) => DataType::Long(entity).encode(),
            other => {
                return Err(anyhow!(
                    "Expected Long entity in index key, got {:?}",
                    other
                ))
            }
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
        keep_latest(&mut latest_by_triple, triple, tx_eid, op);
    }
    Ok(latest_by_triple)
}

/// Upper bound on prefix scans in flight while priming a subscription.
const MAX_CONCURRENT_INITIAL_SCANS: usize = 8;

pub(crate) async fn scan_current_triples<D>(
    db: &D,
    plan: &IncrementalQueryPlan,
    as_of_tx_eid: i64,
) -> Result<Vec<Tup2<EncodedTriple, ZWeight>>>
where
    D: slatedb::DbReadOps + Sync,
{
    let latest_by_triple = stream::iter(initial_scans(plan))
        .map(|scan| async move { scan_latest_triples(db, &scan, as_of_tx_eid).await })
        .buffer_unordered(MAX_CONCURRENT_INITIAL_SCANS)
        // Overlapping scans contribute each live triple only once.
        .try_fold(
            HashMap::new(),
            |mut latest_by_triple, scan_latest| async move {
                for (triple, (tx_eid, op)) in scan_latest {
                    keep_latest(&mut latest_by_triple, triple, tx_eid, op);
                }
                Ok(latest_by_triple)
            },
        )
        .await?;

    Ok(latest_by_triple
        .into_iter()
        .filter_map(|(triple, (_tx_eid, op))| (op == codec::ADD).then_some(Tup2(triple, 1)))
        .collect())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use edn::kw;
    use tokio::sync::Mutex;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::clock::st_from_unix_epoch;
    use crate::inc_query::test_support::{query_plan, test_schema, AGE_ATTR_ID, NAME_ATTR_ID};
    use crate::indexer::{Indexer, DEFAULT_TX_COMPLETION_CAPACITY};
    use crate::metadata::{Metadata, PartitionMap};
    use crate::partition::tx_eid_from_tx_id;

    async fn write_scan_datoms(db: &slatedb::Db, tx: i64, datoms: &[Datom]) {
        let mut batch = slatedb::WriteBatch::new();
        crate::indexer::write_index_entries(&mut batch, datoms, &test_schema(), tx).unwrap();
        db.write(batch).await.unwrap();
    }

    fn scan_datom(entity: i64, attribute: edn::Keyword, value: DataType, op: DatomOp) -> Datom {
        Datom {
            entity,
            attribute,
            value,
            op,
        }
    }

    #[tokio::test]
    async fn initial_scans_follow_history_at_each_basis() {
        let slate = crate::slate::in_memory_slate().await;
        let name = |entity, value: &str, op| scan_datom(entity, kw!(:name), value.into(), op);
        write_scan_datoms(
            &slate.db,
            1,
            &[
                name(42, "Alice", DatomOp::Assert),
                name(42, "Ada", DatomOp::Assert),
            ],
        )
        .await;
        write_scan_datoms(
            &slate.db,
            2,
            &[
                name(42, "Alice", DatomOp::Retract),
                name(43, "Alice", DatomOp::Assert),
            ],
        )
        .await;
        write_scan_datoms(&slate.db, 3, &[name(42, "Alice", DatomOp::Assert)]).await;
        write_scan_datoms(&slate.db, 4, &[name(42, "Alice", DatomOp::Retract)]).await;

        // Live :name triples expected at bases 0 through 4.
        for (query, by_basis) in [
            (
                r#"{:find [?n]
                    :where [[42 :name ?n]]}"#,
                [
                    vec![],
                    vec![(42, "Alice"), (42, "Ada")],
                    vec![(42, "Ada")],
                    vec![(42, "Alice"), (42, "Ada")],
                    vec![(42, "Ada")],
                ],
            ),
            (
                r#"{:find [?e]
                    :where [[?e :name "Alice"]]}"#,
                [
                    vec![],
                    vec![(42, "Alice")],
                    vec![(43, "Alice")],
                    vec![(42, "Alice"), (43, "Alice")],
                    vec![(43, "Alice")],
                ],
            ),
            (
                r#"{:find [?age]
                    :where [[42 :name "Alice"]
                            [42 :age ?age]]}"#,
                [
                    vec![],
                    vec![(42, "Alice")],
                    vec![],
                    vec![(42, "Alice")],
                    vec![],
                ],
            ),
            (
                r#"{:find [?e ?v]
                    :where [[?e :name "Alice"]
                            [42 :name ?v]]}"#,
                [
                    vec![],
                    vec![(42, "Alice"), (42, "Ada")],
                    vec![(42, "Ada"), (43, "Alice")],
                    vec![(42, "Alice"), (42, "Ada"), (43, "Alice")],
                    vec![(42, "Ada"), (43, "Alice")],
                ],
            ),
        ] {
            let plan = query_plan(query);
            for (basis, live) in by_basis.into_iter().enumerate() {
                let mut actual = scan_current_triples(slate.db.as_ref(), &plan, basis as i64)
                    .await
                    .unwrap();
                actual.sort();
                let live = live
                    .into_iter()
                    .map(|(entity, value)| name(entity, value, DatomOp::Assert))
                    .collect::<Vec<_>>();
                let mut expected = datoms_to_tuples(&live, &test_schema()).unwrap();
                expected.sort();
                assert_eq!(actual, expected, "query {query}, basis {basis}");
            }
        }
        slate.db.close().await.unwrap();
    }

    #[test]
    fn initial_scans_pick_index_prefixes() {
        fn prefix(index: u8, attribute: i64, parts: &[DataType]) -> Vec<u8> {
            let mut prefix = vec![index];
            codec::encode_i64(attribute, &mut prefix);
            for part in parts {
                prefix.extend(part.encode());
            }
            prefix
        }

        for (label, query, expected) in [
            (
                "attribute",
                r#"{:find [?e]
                    :where [[?e :name _]]}"#,
                vec![prefix(codec::AEV, NAME_ATTR_ID, &[])],
            ),
            (
                "entity",
                r#"{:find [?v]
                    :where [[42 :name ?v]]}"#,
                vec![prefix(codec::AEV, NAME_ATTR_ID, &[DataType::Long(42)])],
            ),
            (
                "value",
                r#"{:find [?e]
                    :where [[?e :name "Alice"]]}"#,
                vec![prefix(codec::AVE, NAME_ATTR_ID, &["Alice".into()])],
            ),
            (
                "full triple and age",
                r#"{:find [?age]
                    :where [[42 :name "Alice"]
                            [42 :age ?age]]}"#,
                vec![
                    prefix(
                        codec::AEV,
                        NAME_ATTR_ID,
                        &[DataType::Long(42), "Alice".into()],
                    ),
                    prefix(codec::AEV, AGE_ATTR_ID, &[DataType::Long(42)]),
                ],
            ),
        ] {
            let prefixes = initial_scans(&query_plan(query))
                .iter()
                .map(InitialScan::prefix)
                .collect::<Vec<_>>();
            assert_eq!(prefixes, expected, "{label}");
        }
    }

    #[test]
    fn initial_scans_normalize_nested_patterns() {
        let plan = query_plan(
            r#"{:find [?e]
                :where [[?e :name _]
                        (or [?e :name "Alice"] [?e :name "Bob"])
                        (not [42 :name "Alice"])
                        (not [?e :age 30])]}"#,
        );
        let scans = initial_scans(&plan);
        assert_eq!(
            scans,
            vec![
                InitialScan {
                    attribute: NAME_ATTR_ID,
                    entity: None,
                    value: None
                },
                InitialScan {
                    attribute: AGE_ATTR_ID,
                    entity: None,
                    value: Some(DataType::Long(30).encode())
                },
            ]
        );

        let plan = query_plan(
            r#"{:find [?e ?v]
                :where [[?e :name "Alice"]
                        [42 :name ?v]
                        [42 :name "Alice"]
                        [43 :name "Alice"]
                        (not [?e :name "Alice"])]}"#,
        );
        let scans = initial_scans(&plan);
        assert_eq!(scans.len(), 2);
        assert!(!scans[0].covers(&scans[1]));
        assert!(!scans[1].covers(&scans[0]));
    }

    #[tokio::test]
    async fn cdc_loop_exits_ok_when_cancelled() {
        let slate = crate::slate::in_memory_slate().await;
        let indexer = Indexer::new(
            slate.db.clone(),
            Metadata::new(test_schema(), PartitionMap::new()),
            *crate::bootstrap::BOOTSTRAP_TX_KEY,
            DEFAULT_TX_COMPLETION_CAPACITY,
        );
        let service = IncrementalQueryService::new(
            tempfile::tempdir().unwrap().path().to_path_buf(),
            CancellationToken::new(),
            slate.object_path.clone(),
            slate.object_store.clone(),
            Duration::from_micros(250),
        );
        let cancel = CancellationToken::new();
        cancel.cancel();

        let result = run_cdc_loop(
            slate.object_path,
            slate.object_store,
            indexer.handle(),
            service,
            Arc::new(Mutex::new(())),
            cancel,
            Duration::from_micros(250),
        )
        .await;

        assert!(result.is_ok());
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
                    attribute: NAME_ATTR_ID,
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
                    attribute: AGE_ATTR_ID,
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
