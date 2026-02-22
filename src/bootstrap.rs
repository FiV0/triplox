use std::sync::Arc;
use slatedb::Db;
use crate::codec;
use crate::indexer::build_index_write_batch;
use crate::schema::{bootstrap_schema_tx, load_schema_from_indices, SchemaCache};
use crate::slate::DEFAULT_WRITE_OPTIONS;
use crate::util::concat_bytes;

const META_KEY_VERSION: &[u8] = b"version";

/// Initialize the database and return a ready `SchemaCache`.
///
/// - **Fresh DB**: processes the bootstrap schema transaction, writes index entries
///   directly to SlateDB, writes the version, and returns the populated cache.
/// - **Existing DB**: loads the schema from indices via the Datalog query engine.
pub async fn init_db(slatedb: Arc<Db>) -> SchemaCache {
    let key = concat_bytes(&[&[codec::META_INDEX], META_KEY_VERSION]);

    match slatedb.get(&key).await.expect("Failed to read version from META_INDEX") {
        Some(_bytes) => {
            // Existing DB — load schema from indices
            load_schema_from_indices(slatedb).await
        }
        None => {
            // Fresh DB — bootstrap schema
            let tx_ops = bootstrap_schema_tx();
            let mut cache = SchemaCache::new();
            cache.process_tx(&tx_ops).unwrap();

            let write_batch = build_index_write_batch(&tx_ops, &cache).unwrap();
            slatedb.write_with_options(write_batch, &DEFAULT_WRITE_OPTIONS).await.unwrap();

            // Write version
            let version = env!("CARGO_PKG_VERSION");
            slatedb.put(&key, version.as_bytes()).await.expect("Failed to write version to META_INDEX");

            cache
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
        let cache = init_db(slatedb).await;
        // Bootstrap defines 3 schema attributes (db/ident, db/valueType, db/cardinality)
        assert_eq!(cache.len(), 3);
        assert!(cache.get("db/ident").is_some());
        assert!(cache.get("db/valueType").is_some());
        assert!(cache.get("db/cardinality").is_some());
    }

    #[tokio::test]
    async fn test_init_db_existing() {
        let slatedb = Arc::new(in_memory_slate().await);
        let cache1 = init_db(slatedb.clone()).await;
        // Second call takes the existing-DB path (load from indices)
        let cache2 = init_db(slatedb).await;
        assert_eq!(cache1.len(), cache2.len());
        assert_eq!(cache1.len(), 3);
    }

    #[tokio::test]
    async fn test_init_db_preserves_old_version() {
        let slatedb = Arc::new(in_memory_slate().await);

        // Write an older version directly (simulates existing DB without bootstrap indices)
        let key = concat_bytes(&[&[codec::META_INDEX], META_KEY_VERSION]);
        slatedb.put(&key, b"0.0.1").await.unwrap();

        // init_db takes the existing-DB path — loads from indices (empty, since no bootstrap ran)
        let cache = init_db(slatedb).await;
        assert_eq!(cache.len(), 0);
    }
}
