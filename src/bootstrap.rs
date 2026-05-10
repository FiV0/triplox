use anyhow::{bail, Context, Result};

use crate::clock::st_from_unix_epoch;
use crate::codec;
use crate::indexer::{build_tx_entity_datoms, write_index_entries};
use crate::metadata::{Metadata, PartitionMap};
use crate::partition::{
    extract_counter, partition_entity_prefix, DB_PARTITION, TX_PARTITION, USER_PARTITION,
};
use crate::schema::{bootstrap_schema, bootstrap_schema_tx, load_schema_from_indices, Schema};
use crate::slate::{SlateComponents, DEFAULT_SCAN_OPTIONS, DEFAULT_WRITE_OPTIONS};
use crate::tempids;
use crate::transaction::TxKey;
use crate::tx;
use crate::util::concat_bytes;
use slatedb::{Db, WriteBatch};

const META_KEY_VERSION: &[u8] = b"version";

/// Reserved counter space for bootstrap entities in DB_PARTITION.
/// New user-defined schema attributes start at this counter value,
/// leaving room below for future bootstrap entities.
const DB_PARTITION_COUNTER_FLOOR: i64 = 1000;

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

/// Initialize the database and return ready `Metadata`.
///
/// - **Fresh DB**: processes the bootstrap schema transaction, writes index entries
///   including a transaction entity directly to SlateDB, writes the version, and
///   returns populated metadata.
/// - **Existing DB**: loads the schema from indices via the Datalog query engine,
///   derives counters by scanning the EAV index.
pub async fn init_db(slate: &SlateComponents) -> Result<Metadata> {
    let version_key = concat_bytes(&[&[codec::META_INDEX], META_KEY_VERSION]);

    match slate
        .db
        .get(&version_key)
        .await
        .context("Failed to read version from META_INDEX")?
    {
        Some(_bytes) => {
            // Existing DB — load schema from indices, derive counters from EAV scan
            let schema = load_schema_from_indices(slate).await;
            let pm = scan_partition_counters(&slate.db).await?;
            Ok(Metadata::new(schema, pm))
        }
        None => {
            // Fresh DB — build schema from constants, then bootstrap
            let bootstrap_schema = bootstrap_schema();
            let tx_ops = bootstrap_schema_tx();
            let tx_key = TxKey {
                tx_id: 0,
                system_time: st_from_unix_epoch(0),
            };
            let mut boot_pm = PartitionMap::new();
            let tx_eid = boot_pm.allocate_entid(TX_PARTITION);

            // Same normalization and tempid-resolution stages as the indexer.
            let expanded = tx::expand_tx_ops(&tx_ops, &bootstrap_schema).unwrap();
            let with_tempids = tx::resolve_lookup_refs(expanded, &bootstrap_schema, &slate.db)
                .await
                .unwrap();
            let mut datoms =
                tempids::resolve_tempids(with_tempids, &bootstrap_schema, &slate.db, &mut boot_pm)
                    .await
                    .unwrap();
            datoms.extend(build_tx_entity_datoms(tx_eid, tx_key, true, None));

            // Validate against pre-built schema, then derive the bootstrap schema delta.
            let validation = bootstrap_schema.validate_datoms(&datoms).unwrap();
            assert!(validation.schema_changes_detected);
            let update = Schema::default().prepare_schema_update(&datoms).unwrap();

            // Verify: tx-derived schema changes must match pre-built schema
            let mut schema_from_tx = Schema::default();
            schema_from_tx.apply_schema_update(update);
            assert_eq!(
                schema_from_tx.ident_map, bootstrap_schema.ident_map,
                "bootstrap ident_map mismatch"
            );
            assert_eq!(
                schema_from_tx.attribute_map, bootstrap_schema.attribute_map,
                "bootstrap attribute_map mismatch"
            );

            let mut batch = WriteBatch::new();
            write_index_entries(&mut batch, &datoms, &bootstrap_schema, tx_eid).unwrap();
            // Write version
            let version = env!("CARGO_PKG_VERSION");
            batch.put(&version_key, version.as_bytes());
            slate
                .db
                .write_with_options(batch, &DEFAULT_WRITE_OPTIONS)
                .await
                .unwrap();

            // Derive counters from the just-written index
            let pm = scan_partition_counters(&slate.db).await?;
            Ok(Metadata::new(bootstrap_schema, pm))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::{DB_PARTITION, TX_PARTITION, USER_PARTITION};
    use crate::slate::in_memory_slate;
    use edn::kw;

    #[tokio::test]
    async fn test_init_db_fresh() {
        let slate = in_memory_slate().await;
        let metadata = init_db(&slate).await.unwrap();
        // Bootstrap defines 8 schema attributes (4 core + 4 tx)
        assert_eq!(metadata.schema.len(), 8);
        assert!(metadata.schema.get_attribute(&kw!(:db/ident)).is_some());
        assert!(metadata.schema.get_attribute(&kw!(:db/valueType)).is_some());
        assert!(metadata
            .schema
            .get_attribute(&kw!(:db/cardinality))
            .is_some());
        assert!(metadata.schema.get_attribute(&kw!(:db/unique)).is_some());
        assert!(metadata.schema.get_attribute(&kw!(:db/txInstant)).is_some());
        assert!(metadata.schema.get_attribute(&kw!(:db/txId)).is_some());
        assert!(metadata.schema.get_attribute(&kw!(:db/txResult)).is_some());
        assert!(metadata.schema.get_attribute(&kw!(:db.tx/error)).is_some());
        // Counter is clamped to DB_PARTITION_COUNTER_FLOOR (room for future bootstrap entities)
        assert_eq!(
            metadata.partition_map[&DB_PARTITION],
            DB_PARTITION_COUNTER_FLOOR
        );
        assert_eq!(metadata.partition_map[&TX_PARTITION], 1);
        assert_eq!(metadata.partition_map[&USER_PARTITION], 0);
        // Enum entities are in ident_map but not attribute_map
        assert!(metadata
            .schema
            .ident_map
            .contains_key(&kw!(:db.type/string)));
        assert!(metadata
            .schema
            .get_attribute(&kw!(:db.type/string))
            .is_none());
    }

    #[tokio::test]
    async fn test_init_db_existing() {
        let slate = in_memory_slate().await;
        let metadata1 = init_db(&slate).await.unwrap();
        // Second call takes the existing-DB path (scan EAV for counters)
        let metadata2 = init_db(&slate).await.unwrap();
        assert_eq!(metadata1.schema.len(), metadata2.schema.len());
        assert_eq!(metadata1.schema.len(), 8);
        assert_eq!(
            metadata1.partition_map[&DB_PARTITION],
            metadata2.partition_map[&DB_PARTITION]
        );
        assert_eq!(
            metadata1.partition_map[&TX_PARTITION],
            metadata2.partition_map[&TX_PARTITION]
        );
    }

    #[tokio::test]
    async fn test_init_db_preserves_old_version() {
        let slate = in_memory_slate().await;

        // Write an older version directly (simulates existing DB without bootstrap indices)
        let key = concat_bytes(&[&[codec::META_INDEX], META_KEY_VERSION]);
        slate.db.put(&key, b"0.0.1").await.unwrap();

        // init_db takes the existing-DB path — loads from indices (empty, since no bootstrap ran)
        let metadata = init_db(&slate).await.unwrap();
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
