use anyhow::{bail, Context, Result};

use crate::codec;
use crate::metadata::{Metadata, PartitionMap};
use crate::partition::{
    extract_counter, partition_entity_prefix, DB_PARTITION, TX_PARTITION, USER_PARTITION,
};
use crate::schema::load_schema_from_indices;
use crate::slate::{SlateComponents, DEFAULT_SCAN_OPTIONS};
use crate::util::concat_bytes;
use slatedb::{Db, WriteBatch};

const META_KEY_VERSION: &[u8] = b"version";

/// Reserved counter space for bootstrap entities in DB_PARTITION.
/// New user-defined schema attributes start at this counter value,
/// leaving room below for future bootstrap entities.
const DB_PARTITION_COUNTER_FLOOR: i64 = 1000;

pub(crate) fn version_key() -> Vec<u8> {
    concat_bytes(&[&[codec::META_INDEX], META_KEY_VERSION])
}

pub(crate) fn write_version_marker(batch: &mut WriteBatch) {
    batch.put(version_key(), env!("CARGO_PKG_VERSION").as_bytes());
}

pub(crate) async fn is_initialized(slate: &SlateComponents) -> Result<bool> {
    Ok(slate
        .db
        .get(&version_key())
        .await
        .context("Failed to read version from META_INDEX")?
        .is_some())
}

pub(crate) async fn load_existing_metadata(slate: &SlateComponents) -> Result<Metadata> {
    let schema = load_schema_from_indices(slate).await;
    let pm = scan_partition_counters(&slate.db).await?;
    Ok(Metadata::new(schema, pm))
}

/// Scan each partition's EAV prefix and return per-partition counters (max counter + 1).
/// With descending encoding, the first entity per partition has the highest counter.
/// The DB_PARTITION counter is clamped to at least DB_PARTITION_COUNTER_FLOOR.
/// Partitions with no entities are initialized to counter 0.
pub(crate) async fn scan_partition_counters(slatedb: &Db) -> Result<PartitionMap> {
    let mut pm = PartitionMap::new();
    for partition in [DB_PARTITION, TX_PARTITION, USER_PARTITION] {
        pm.insert(partition, 0);

        let prefix = concat_bytes(&[&[codec::EAV], &partition_entity_prefix(partition)]);
        let mut iter = slatedb
            .scan_prefix_with_options(&prefix, &DEFAULT_SCAN_OPTIONS)
            .await
            .context("Failed to scan EAV partition prefix")?;

        if let Some(kv) = iter.next().await.context("Failed to read EAV key")? {
            let mut cursor: &[u8] =
                &kv.key[codec::CODEC_LENGTH..codec::CODEC_LENGTH + codec::ENTITY_LENGTH];
            let eid = match codec::decode_datatype(&mut cursor)
                .context("Failed to decode entity ID from EAV key")?
            {
                crate::ops::DataType::Long(id) => id,
                other => bail!("Expected Long entity ID in EAV key, got {:?}", other),
            };
            let counter = extract_counter(eid);
            pm.insert(partition, counter + 1);
        }
    }

    // Reserve space for future bootstrap entities
    if let Some(db_counter) = pm.get_mut(&crate::partition::DB_PARTITION) {
        if *db_counter < DB_PARTITION_COUNTER_FLOOR {
            *db_counter = DB_PARTITION_COUNTER_FLOOR;
        }
    }

    Ok(pm)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::st_from_unix_epoch;
    use crate::indexer::Indexer;
    use crate::partition::{DB_PARTITION, TX_PARTITION, USER_PARTITION};
    use crate::schema::bootstrap_schema_tx;
    use crate::slate::in_memory_slate;
    use crate::transaction::TxKey;
    use edn::kw;

    async fn index_bootstrap(slate: &SlateComponents) {
        let mut indexer = Indexer::new_bootstrapping(slate.db.clone());
        indexer
            .transact_bootstrap_tx(
                TxKey {
                    tx_id: 0,
                    system_time: st_from_unix_epoch(0),
                },
                bootstrap_schema_tx(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_is_initialized_tracks_version_marker() {
        let slate = in_memory_slate().await;
        assert!(!is_initialized(&slate).await.unwrap());

        index_bootstrap(&slate).await;

        assert!(is_initialized(&slate).await.unwrap());
    }

    #[tokio::test]
    async fn test_load_existing_metadata_reads_indices() {
        let slate = in_memory_slate().await;
        index_bootstrap(&slate).await;
        let loaded = load_existing_metadata(&slate).await.unwrap();

        // Bootstrap defines 8 schema attributes (4 core + 4 tx)
        assert_eq!(loaded.schema.len(), 8);
        assert!(loaded.schema.get_attribute(&kw!(:db/ident)).is_some());
        assert!(loaded.schema.get_attribute(&kw!(:db/valueType)).is_some());
        assert!(loaded.schema.get_attribute(&kw!(:db/cardinality)).is_some());
        assert!(loaded.schema.get_attribute(&kw!(:db/unique)).is_some());
        assert!(loaded.schema.get_attribute(&kw!(:db/txInstant)).is_some());
        assert!(loaded.schema.get_attribute(&kw!(:db/txId)).is_some());
        assert!(loaded.schema.get_attribute(&kw!(:db/txResult)).is_some());
        assert!(loaded.schema.get_attribute(&kw!(:db/txError)).is_some());
        // Counter is clamped to DB_PARTITION_COUNTER_FLOOR (room for future bootstrap entities)
        assert_eq!(
            loaded.partition_map[&DB_PARTITION],
            DB_PARTITION_COUNTER_FLOOR
        );
        assert_eq!(loaded.partition_map[&TX_PARTITION], 1);
        assert_eq!(loaded.partition_map[&USER_PARTITION], 0);
        // Enum entities are in ident_map but not attribute_map
        assert!(loaded.schema.ident_map.contains_key(&kw!(:db.type/string)));
        assert!(loaded.schema.get_attribute(&kw!(:db.type/string)).is_none());
    }

    #[tokio::test]
    async fn test_load_existing_metadata_preserves_old_version() {
        let slate = in_memory_slate().await;

        // Write an older version directly (simulates existing DB without bootstrap indices)
        let key = version_key();
        slate.db.put(&key, b"0.0.1").await.unwrap();

        let metadata = load_existing_metadata(&slate).await.unwrap();
        assert_eq!(metadata.schema.len(), 0);
        // No EAV entries → partition counters initialized to 0 (DB clamped to floor)
        assert_eq!(
            metadata.partition_map[&DB_PARTITION],
            DB_PARTITION_COUNTER_FLOOR
        );
        assert_eq!(metadata.partition_map[&TX_PARTITION], 0);
        assert_eq!(metadata.partition_map[&USER_PARTITION], 0);
    }
}
