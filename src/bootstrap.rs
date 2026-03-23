use std::sync::Arc;
use slatedb::{Db, IsolationLevel};
use crate::clock::{self, st_from_unix_epoch};
use crate::codec;
use crate::indexer::write_index_entries;
use crate::ops::{assign_entity_ids, tx_ops_to_datoms};
use crate::schema::{bootstrap_schema_tx, load_schema_from_indices, SchemaCache};
use crate::slate::DEFAULT_WRITE_OPTIONS;
use crate::util::concat_bytes;

const META_KEY_VERSION: &[u8] = b"version";
pub(crate) const META_KEY_NEXT_T: &[u8] = b"next_t";

/// Initialize the database and return a ready `SchemaCache` and the next entity counter value.
///
/// - **Fresh DB**: processes the bootstrap schema transaction, writes index entries
///   directly to SlateDB, writes the version and next_t, and returns the populated cache.
/// - **Existing DB**: loads the schema from indices via the Datalog query engine,
///   reads next_t from META_INDEX.
pub async fn init_db(slatedb: Arc<Db>) -> (SchemaCache, i64) {
    let version_key = concat_bytes(&[&[codec::META_INDEX], META_KEY_VERSION]);
    let next_t_key = concat_bytes(&[&[codec::META_INDEX], META_KEY_NEXT_T]);

    match slatedb.get(&version_key).await.expect("Failed to read version from META_INDEX") {
        Some(_bytes) => {
            // Existing DB — load schema from indices
            let cache = load_schema_from_indices(slatedb.clone()).await;

            // Read persisted next_t, fallback to 32 for pre-partition databases
            let next_t = match slatedb.get(&next_t_key).await.expect("Failed to read next_t") {
                Some(bytes) => {
                    let arr: [u8; 8] = bytes.as_ref().try_into().expect("next_t must be 8 bytes");
                    i64::from_le_bytes(arr)
                }
                None => 32, // fallback for pre-partition databases
            };

            (cache, next_t)
        }
        None => {
            // Fresh DB — bootstrap schema
            let tx_ops = bootstrap_schema_tx();
            let mut next_t: i64 = 0;
            let resolved_ops = assign_entity_ids(&tx_ops, &mut next_t).unwrap();
            let datoms = tx_ops_to_datoms(&resolved_ops, st_from_unix_epoch(0)).unwrap();
            let mut cache = SchemaCache::new();
            cache.process_tx(SchemaCache::validate_schema_attrs(&datoms).unwrap());

            let txn = slatedb.begin(IsolationLevel::Snapshot).await.unwrap();
            write_index_entries(&txn, &datoms, &cache, clock::st_from_unix_epoch(0)).unwrap();
            // Write version
            let version = env!("CARGO_PKG_VERSION");
            txn.put(&version_key, version.as_bytes()).unwrap();
            // Write next_t
            txn.put(&next_t_key, &next_t.to_le_bytes()).unwrap();
            txn.commit_with_options(&DEFAULT_WRITE_OPTIONS).await.unwrap();

            (cache, next_t)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::slate::in_memory_slate;

    #[tokio::test]
    async fn test_init_db_fresh() {
        let slatedb = Arc::new(in_memory_slate().await);
        let (cache, next_t) = init_db(slatedb).await;
        // Bootstrap defines 3 schema attributes (db/ident, db/valueType, db/cardinality)
        assert_eq!(cache.len(), 3);
        assert!(cache.get("db/ident").is_some());
        assert!(cache.get("db/valueType").is_some());
        assert!(cache.get("db/cardinality").is_some());
        // next_t should be past all bootstrap entities (19 entities: 3 attrs + 14 type enums + 2 card enums)
        assert_eq!(next_t, 19);
    }

    #[tokio::test]
    async fn test_init_db_existing() {
        let slatedb = Arc::new(in_memory_slate().await);
        let (cache1, next_t1) = init_db(slatedb.clone()).await;
        // Second call takes the existing-DB path (load from indices)
        let (cache2, next_t2) = init_db(slatedb).await;
        assert_eq!(cache1.len(), cache2.len());
        assert_eq!(cache1.len(), 3);
        assert_eq!(next_t1, next_t2);
    }

    #[tokio::test]
    async fn test_init_db_preserves_old_version() {
        let slatedb = Arc::new(in_memory_slate().await);

        // Write an older version directly (simulates existing DB without bootstrap indices)
        let key = concat_bytes(&[&[codec::META_INDEX], META_KEY_VERSION]);
        slatedb.put(&key, b"0.0.1").await.unwrap();

        // init_db takes the existing-DB path — loads from indices (empty, since no bootstrap ran)
        let (cache, next_t) = init_db(slatedb).await;
        assert_eq!(cache.len(), 0);
        // No next_t persisted → falls back to 32
        assert_eq!(next_t, 32);
    }

    #[tokio::test]
    async fn test_next_t_round_trip() {
        let slatedb = Arc::new(in_memory_slate().await);
        let (_cache1, next_t1) = init_db(slatedb.clone()).await;
        let (_cache2, next_t2) = init_db(slatedb).await;
        assert_eq!(next_t1, next_t2);
    }
}
