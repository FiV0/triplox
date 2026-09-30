use std::collections::HashMap;
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

use crate::codec::{self, Encode};
use crate::inc_query::{IncrementalQueryPlan, PatternPlan, PatternSlot};
use crate::incremental::{EncodedTriple, IncrementalQueryService};
use crate::indexer::{aev_key_to_parts, ave_key_to_parts};
use crate::node::SchemaProvider;
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

pub(crate) fn spawn_cdc_loop<N>(
    object_path: String,
    object_store: Arc<dyn ObjectStore>,
    node: Arc<N>,
    service: IncrementalQueryService,
    registration_gate: Arc<Mutex<()>>,
    cancel: CancellationToken,
    poll_interval: Duration,
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
        poll_interval,
    ))
}

async fn run_cdc_loop<N>(
    object_path: String,
    object_store: Arc<dyn ObjectStore>,
    node: Arc<N>,
    service: IncrementalQueryService,
    registration_gate: Arc<Mutex<()>>,
    cancel: CancellationToken,
    poll_interval: Duration,
) -> Result<()>
where
    N: SchemaProvider,
{
    let wal_reader = WalReader::new(object_path, object_store);
    let mut stream =
        CdcStream::new(wal_reader, CdcCursor::default(), poll_interval, cancel).await?;

    while let Some(tx) = stream.next_transaction().await? {
        let schema = node.schema().await;
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

    fn prefix(&self) -> Vec<u8> {
        let index = if self.entity.is_none() && self.value.is_some() {
            codec::AVE
        } else {
            codec::AEV
        };
        let mut prefix = vec![index];
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

pub(crate) async fn scan_current_triples<D>(
    db: &D,
    plan: &IncrementalQueryPlan,
    as_of_tx_eid: i64,
) -> Result<Vec<Tup2<EncodedTriple, ZWeight>>>
where
    D: slatedb::DbReadOps + Sync,
{
    let mut latest_by_triple: HashMap<EncodedTriple, (i64, u8)> = HashMap::new();
    for scan in initial_scans(plan) {
        let prefix = scan.prefix();
        let mut iter = db
            .scan_prefix_with_options(&prefix, .., &DEFAULT_SCAN_OPTIONS)
            .await?;

        while let Some(kv) = iter.next().await? {
            let (attribute, entity, value, tx_eid, op) = if prefix[0] == codec::AVE {
                let (attribute, value, entity, tx_eid, op) = ave_key_to_parts(kv.key)?;
                (attribute, entity, value, tx_eid, op)
            } else {
                aev_key_to_parts(kv.key)?
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
            // Overlapping scans contribute each live triple only once.
            let should_replace = latest_by_triple
                .get(&triple)
                .is_none_or(|(latest_tx_eid, _)| tx_eid >= *latest_tx_eid);
            if should_replace {
                latest_by_triple.insert(triple, (tx_eid, op));
            }
        }
    }

    Ok(latest_by_triple
        .into_iter()
        .filter_map(|(triple, (_tx_eid, op))| (op == codec::ADD).then_some(Tup2(triple, 1)))
        .collect())
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
    use crate::inc_query::test_support::test_schema as scan_schema;
    use crate::indexer::{Indexer, DEFAULT_TX_COMPLETION_CAPACITY};
    use crate::metadata::{Metadata, PartitionMap};
    use crate::partition::tx_eid_from_tx_id;
    use crate::schema::{Attribute, Schema, ValueType};

    fn scan_plan(query: &str) -> IncrementalQueryPlan {
        crate::inc_query::plan_query(&edn::parse::parse_query(query).unwrap(), &scan_schema())
            .unwrap()
    }

    async fn write_scan_datoms(db: &slatedb::Db, tx: i64, datoms: &[Datom]) {
        let mut batch = slatedb::WriteBatch::new();
        crate::indexer::write_index_entries(&mut batch, datoms, &scan_schema(), tx).unwrap();
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

    async fn full_eav_reference(
        db: &slatedb::Db,
        plan: &IncrementalQueryPlan,
        basis: i64,
    ) -> Vec<Tup2<EncodedTriple, ZWeight>> {
        let mut iter = db.scan_prefix([codec::EAV], ..).await.unwrap();
        let mut latest = HashMap::new();
        while let Some(kv) = iter.next().await.unwrap() {
            let (entity, attribute, value, tx, op) =
                crate::indexer::eav_key_to_parts(kv.key).unwrap();
            if tx <= basis {
                let triple = EncodedTriple {
                    entity: entity.encode(),
                    attribute,
                    value: value.encode(),
                };
                let version = latest.entry(triple).or_insert((tx, op));
                *version = (*version).max((tx, op));
            }
        }
        let matches = |slot: &PatternSlot, value: &[u8]| match slot {
            PatternSlot::Variable(_) => true,
            PatternSlot::Constant(constant) => constant == value,
        };
        let patterns = plan.leaf_patterns();
        let mut triples = latest
            .into_iter()
            .filter_map(|(triple, (_, op))| {
                (op == codec::ADD
                    && patterns.iter().any(|pattern| {
                        pattern.attribute == triple.attribute
                            && matches(&pattern.entity, &triple.entity)
                            && matches(&pattern.value, &triple.value)
                    }))
                .then_some(Tup2(triple, 1))
            })
            .collect::<Vec<_>>();
        triples.sort();
        triples
    }

    #[tokio::test]
    async fn initial_scans_match_history_at_each_basis() {
        let slate = crate::slate::in_memory_slate().await;
        let alice = scan_datom(42, kw!(:name), "Alice".into(), DatomOp::Assert);
        let age = scan_datom(42, kw!(:age), DataType::Long(30), DatomOp::Assert);
        write_scan_datoms(
            &slate.db,
            1,
            &[
                alice.clone(),
                age.clone(),
                scan_datom(43, kw!(:name), "Bob".into(), DatomOp::Assert),
                scan_datom(
                    42,
                    kw!(:type),
                    DataType::Keyword(kw!(:person)),
                    DatomOp::Assert,
                ),
                scan_datom(42, kw!(:follows), DataType::Long(43), DatomOp::Assert),
            ],
        )
        .await;
        write_scan_datoms(
            &slate.db,
            2,
            &[
                Datom {
                    op: DatomOp::Retract,
                    ..alice.clone()
                },
                Datom {
                    op: DatomOp::Retract,
                    ..age
                },
                scan_datom(42, kw!(:age), DataType::Long(31), DatomOp::Assert),
            ],
        )
        .await;
        write_scan_datoms(&slate.db, 3, std::slice::from_ref(&alice)).await;
        write_scan_datoms(
            &slate.db,
            4,
            &[Datom {
                op: DatomOp::Retract,
                ..alice
            }],
        )
        .await;

        for query in [
            r#"{:find [?e ?name]
                 :where [[?e :name ?name]]}"#,
            r#"{:find [?name]
                 :where [[42 :name ?name]]}"#,
            r#"{:find [?e]
                 :where [[?e :name "Alice"]]}"#,
            r#"{:find [?e]
                 :where [[?e :age 30] [42 :name "Alice"]]}"#,
            r#"{:find [?e ?name]
                 :where [[?e :name "Alice"] [42 :name ?name]]}"#,
            r#"{:find [?e]
                 :where [(or [?e :name "Alice"] [?e :name "Bob"])
                         (not [?e :age 30])]}"#,
            r#"{:find [?e]
                 :where [[?e :type :person] [?e :follows 43]]}"#,
        ] {
            let plan = scan_plan(query);
            for basis in 0..=4 {
                let mut actual = scan_current_triples(slate.db.as_ref(), &plan, basis)
                    .await
                    .unwrap();
                actual.sort();
                let expected = full_eav_reference(&slate.db, &plan, basis).await;
                assert_eq!(actual, expected, "query {query}, basis {basis}");
            }
        }
        slate.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn initial_scans_select_stored_triples_by_constants() {
        let slate = crate::slate::in_memory_slate().await;
        let datoms = [
            scan_datom(42, kw!(:name), "Alice".into(), DatomOp::Assert),
            scan_datom(42, kw!(:name), "Alice Jr".into(), DatomOp::Assert),
            scan_datom(43, kw!(:name), "Alice".into(), DatomOp::Assert),
            scan_datom(43, kw!(:name), "".into(), DatomOp::Assert),
            scan_datom(43, kw!(:age), DataType::Long(-30), DatomOp::Assert),
        ];
        write_scan_datoms(&slate.db, 1, &datoms).await;
        for (query, selected) in [
            (
                r#"{:find [?e]
                  :where [[?e :name _]]}"#,
                vec![0, 1, 2, 3],
            ),
            (
                r#"{:find [?v]
                  :where [[42 :name ?v]]}"#,
                vec![0, 1],
            ),
            (
                r#"{:find [?e]
                  :where [[?e :name "Alice"]]}"#,
                vec![0, 2],
            ),
            (
                r#"{:find [?e]
                  :where [[?e :age -30]
                          [42 :name "Alice"]]}"#,
                vec![0, 4],
            ),
            (
                r#"{:find [?e ?v]
                  :where [[?e :name "Alice"]
                          [42 :name ?v]]}"#,
                vec![0, 1, 2],
            ),
            (
                r#"{:find [?e]
                  :where [[?e :name ""]]}"#,
                vec![3],
            ),
            (
                r#"{:find [?e]
                  :where [[?e :name "Ali"]]}"#,
                vec![],
            ),
        ] {
            let plan = scan_plan(query);
            let mut actual = scan_current_triples(slate.db.as_ref(), &plan, 1)
                .await
                .unwrap();
            actual.sort();
            let selected = selected
                .into_iter()
                .map(|i| datoms[i].clone())
                .collect::<Vec<_>>();
            let mut expected = datoms_to_tuples(&selected, &test_schema()).unwrap();
            expected.sort();
            assert_eq!(actual, expected, "{query}");
        }
        slate.db.close().await.unwrap();
    }

    #[test]
    fn initial_scans_pick_index_prefixes() {
        use crate::inc_query::test_support::{AGE_ATTR_ID, NAME_ATTR_ID};

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
            (
                "overlap",
                r#"{:find [?e ?v]
                    :where [[?e :name "Alice"]
                            [42 :name ?v]]}"#,
                vec![
                    prefix(codec::AVE, NAME_ATTR_ID, &["Alice".into()]),
                    prefix(codec::AEV, NAME_ATTR_ID, &[DataType::Long(42)]),
                ],
            ),
            (
                "covered scans",
                r#"{:find [?e]
                    :where [[?e :name _]
                            (or [?e :name "Alice"]
                                [?e :name "Alice Jr"])]}"#,
                vec![prefix(codec::AEV, NAME_ATTR_ID, &[])],
            ),
        ] {
            let prefixes = initial_scans(&scan_plan(query))
                .iter()
                .map(InitialScan::prefix)
                .collect::<Vec<_>>();
            assert_eq!(prefixes, expected, "{label}");
        }
    }

    #[test]
    fn initial_scans_normalize_nested_patterns() {
        let plan = scan_plan(
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
                    attribute: 10,
                    entity: None,
                    value: None
                },
                InitialScan {
                    attribute: 11,
                    entity: None,
                    value: Some(DataType::Long(30).encode())
                },
            ]
        );

        let plan = scan_plan(
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
        let indexer = Arc::new(RwLock::new(Indexer::new(
            slate.db.clone(),
            Metadata::new(test_schema(), PartitionMap::new()),
            *crate::bootstrap::BOOTSTRAP_TX_KEY,
            DEFAULT_TX_COMPLETION_CAPACITY,
        )));
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
            indexer,
            service,
            Arc::new(Mutex::new(())),
            cancel,
            Duration::from_micros(250),
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
