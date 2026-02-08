#![allow(dead_code, unused)]

use slatedb::{Db, KeyValue};
use slatedb::object_store::{ObjectStore, memory::InMemory, path::Path};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::config::{DurabilityLevel, ReadOptions, ScanOptions, WriteOptions};
use bincode;
use std::sync::Arc;
use std::collections::HashMap;
use std::ops::{Bound, RangeBounds};
use bytes::Bytes;

use crate::util::{random_string, concat_bytes};
use crate::codec;


pub async fn in_memory_slate() -> Db {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let kv_store = Db::open(
        format!("tmp/{}", random_string(10)),
        object_store,
    )
    .await
    .unwrap();
    kv_store
}

pub async fn local_slate(path: &str) -> Db {
    let object_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(path).unwrap());
    let kv_store = Db::open(path, object_store).await.unwrap();
    kv_store
}

pub const DEFAULT_READ_OPTIONS: ReadOptions = ReadOptions {
    durability_filter: DurabilityLevel::Memory,
    dirty: true,
    cache_blocks: true,
};

pub const DEFAULT_WRITE_OPTIONS: WriteOptions = WriteOptions {
    await_durable: false,
};

pub const DEFAULT_SCAN_OPTIONS: ScanOptions = ScanOptions {
    durability_filter: DurabilityLevel::Memory,
    dirty: true,
    read_ahead_bytes: 1,
    cache_blocks: false,
    max_fetch_tasks: 1,
};

pub async fn read_attribute_map(slatedb: Arc<Db>) -> HashMap<String, u64> {
    let mut attribute_to_id = HashMap::new();

    let mut iter = slatedb.scan_prefix_with_options(&[codec::ATTRIBUTE_TO_ID], &DEFAULT_SCAN_OPTIONS).await.unwrap();

    while let Some(KeyValue { key, ..}) = iter.next().await.unwrap() {
        let attribute_range = codec::CODEC_LENGTH as usize..key.len() - codec::CODEC_LENGTH - 8;
        let id_range = key.len() - 8 - codec::CODEC_LENGTH..key.len() - codec::CODEC_LENGTH;

        let attribute = bincode::deserialize::<String>(&key.slice(attribute_range)).unwrap();
        let id = bincode::deserialize::<u64>(&key.slice(id_range)).unwrap();
        attribute_to_id.insert(attribute, id);
    }

    attribute_to_id
}

pub fn get_and_create_attribute_id(slatedb: Arc<Db>, attribute: &str, attribute_map: &mut HashMap<String, u64>) -> u64 {
    let size = attribute_map.len() as u64;
    let attribute_id = match attribute_map.get(attribute) {
        Some(id) => id,
        None => {
            attribute_map.insert(attribute.to_string(), size);
            let attribute = bincode::serialize(attribute).unwrap();
            let id = bincode::serialize(&size).unwrap();
            slatedb.put(&concat_bytes(&[&[codec::ATTRIBUTE_TO_ID], &attribute, &id]), &[]);
            return size
        }
    };
    *attribute_id
}

// TODO write a test for this