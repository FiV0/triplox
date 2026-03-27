use std::sync::Arc;
use slatedb::{Db, IsolationLevel};
use crate::clock::{self, st_from_unix_epoch};
use crate::codec;
use crate::indexer::write_index_entries;
use crate::ops::tx_ops_to_datoms;
use crate::partition::{PartitionCounters, extract_partition, extract_counter};
use crate::schema::{bootstrap_schema_tx, load_schema_from_indices, SchemaCache};
use crate::slate::{DEFAULT_SCAN_OPTIONS, DEFAULT_WRITE_OPTIONS};
use crate::util::concat_bytes;

const META_KEY_VERSION: &[u8] = b"version";

/// Reserved counter space for bootstrap entities in DB_PARTITION.
/// New user-defined schema attributes start at this counter value,
/// leaving room below for future bootstrap entities.
const DB_PARTITION_COUNTER_FLOOR: i64 = 1000;

/// Scan all EAV keys and return per-partition counters (max counter + 1 for each partition).
/// The DB_PARTITION counter is clamped to at least DB_PARTITION_COUNTER_FLOOR.
pub(crate) async fn scan_partition_counters(slatedb: &Db) -> PartitionCounters {
    let mut counters = PartitionCounters::new();
    let mut iter = slatedb
        .scan_prefix_with_options(&[codec::EAV], &DEFAULT_SCAN_OPTIONS)
        .await
        .expect("Failed to scan EAV index");

    while let Some(kv) = iter.next().await.expect("Failed to read EAV key") {
        // EAV key: [EAV:1] [entity_id:ENTITY_LENGTH] ...
        let mut cursor: &[u8] = &kv.key[1..1 + codec::ENTITY_LENGTH];
        let eid = match codec::decode_datatype(&mut cursor).expect("Failed to decode entity ID from EAV key") {
            crate::ops::DataType::Long(id) => id,
            other => panic!("Expected Long entity ID in EAV key, got {:?}", other),
        };
        let partition = extract_partition(eid);
        let counter = extract_counter(eid);
        let entry = counters.entry(partition).or_insert(0);
        if counter + 1 > *entry {
            *entry = counter + 1;
        }
    }

    // Reserve space for future bootstrap entities (only if DB_PARTITION has entries)
    if let Some(db_counter) = counters.get_mut(&crate::partition::DB_PARTITION) {
        if *db_counter < DB_PARTITION_COUNTER_FLOOR {
            *db_counter = DB_PARTITION_COUNTER_FLOOR;
        }
    }

    counters
}

/// Initialize the database and return a ready `SchemaCache` and per-partition counters.
///
/// - **Fresh DB**: processes the bootstrap schema transaction, writes index entries
///   directly to SlateDB, writes the version, and returns the populated cache.
/// - **Existing DB**: loads the schema from indices via the Datalog query engine,
///   derives counters by scanning the EAV index.
pub async fn init_db(slatedb: Arc<Db>) -> (SchemaCache, PartitionCounters) {
    let version_key = concat_bytes(&[&[codec::META_INDEX], META_KEY_VERSION]);

    match slatedb.get(&version_key).await.expect("Failed to read version from META_INDEX") {
        Some(_bytes) => {
            // Existing DB — load schema from indices, derive counters from EAV scan
            let cache = load_schema_from_indices(slatedb.clone()).await;
            let counters = scan_partition_counters(&slatedb).await;

            (cache, counters)
        }
        None => {
            // Fresh DB — bootstrap schema (ops have explicit db/id values)
            let tx_ops = bootstrap_schema_tx();
            let datoms = tx_ops_to_datoms(&tx_ops, st_from_unix_epoch(0)).unwrap();
            let mut cache = SchemaCache::new();
            cache.process_tx(SchemaCache::validate_schema_attrs(&datoms).unwrap());

            let txn = slatedb.begin(IsolationLevel::Snapshot).await.unwrap();
            write_index_entries(&txn, &datoms, &cache, clock::st_from_unix_epoch(0)).unwrap();
            // Write version
            let version = env!("CARGO_PKG_VERSION");
            txn.put(&version_key, version.as_bytes()).unwrap();
            txn.commit_with_options(&DEFAULT_WRITE_OPTIONS).await.unwrap();

            // Derive counters from the just-written index
            let counters = scan_partition_counters(&slatedb).await;

            (cache, counters)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::DB_PARTITION;
    use crate::slate::in_memory_slate;

    #[tokio::test]
    async fn test_init_db_fresh() {
        let slatedb = Arc::new(in_memory_slate().await);
        let (cache, counters) = init_db(slatedb).await;
        // Bootstrap defines 3 schema attributes (db/ident, db/valueType, db/cardinality)
        assert_eq!(cache.len(), 3);
        assert!(cache.get("db/ident").is_some());
        assert!(cache.get("db/valueType").is_some());
        assert!(cache.get("db/cardinality").is_some());
        // Counter is clamped to DB_PARTITION_COUNTER_FLOOR (room for future bootstrap entities)
        assert_eq!(counters[&DB_PARTITION], DB_PARTITION_COUNTER_FLOOR);
    }

    #[tokio::test]
    async fn test_init_db_existing() {
        let slatedb = Arc::new(in_memory_slate().await);
        let (cache1, counters1) = init_db(slatedb.clone()).await;
        // Second call takes the existing-DB path (scan EAV for counters)
        let (cache2, counters2) = init_db(slatedb).await;
        assert_eq!(cache1.len(), cache2.len());
        assert_eq!(cache1.len(), 3);
        assert_eq!(counters1[&DB_PARTITION], counters2[&DB_PARTITION]);
    }

    #[tokio::test]
    async fn test_init_db_preserves_old_version() {
        let slatedb = Arc::new(in_memory_slate().await);

        // Write an older version directly (simulates existing DB without bootstrap indices)
        let key = concat_bytes(&[&[codec::META_INDEX], META_KEY_VERSION]);
        slatedb.put(&key, b"0.0.1").await.unwrap();

        // init_db takes the existing-DB path — loads from indices (empty, since no bootstrap ran)
        let (cache, counters) = init_db(slatedb).await;
        assert_eq!(cache.len(), 0);
        // No EAV entries → empty counters
        assert!(counters.is_empty());
    }

}
