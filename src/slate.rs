#![allow(dead_code, unused)]

use slatedb::db::Db;
use object_store::{ObjectStore, memory::InMemory, path::Path}; 
use object_store::local::LocalFileSystem;
use slatedb::config::{DbOptions, ReadLevel, ReadOptions, ScanOptions, WriteOptions};
use bincode;
use std::sync::Arc;
use std::collections::HashMap;
use std::ops::{Bound, RangeBounds};
use bytes::Bytes;

use crate::util::{random_string, concat_bytes, create_prefix_range};
use crate::codec;


pub async fn in_memory_slate() -> Db {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let options = DbOptions::default();
    let kv_store = Db::open_with_opts(
        Path::from(format!("tmp/{}", random_string(10))),
        options,
        object_store,
    )
    .await
    .unwrap();
    kv_store
}

pub async fn local_slate(path: &str) -> Db {
    let object_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(path).unwrap());
    let options = DbOptions::default();
    let kv_store = Db::open_with_opts(Path::from(path), options, object_store).await.unwrap();
    kv_store
}

pub const DEFAULT_READ_OPTIONS: ReadOptions = ReadOptions {
    read_level: ReadLevel::Uncommitted,
};

pub const DEFAULT_WRITE_OPTIONS: WriteOptions = WriteOptions {
    await_durable: false,
};

pub const DEFAULT_SCAN_OPTIONS: ScanOptions = ScanOptions {
    read_level: ReadLevel::Uncommitted,
    read_ahead_bytes: 1,
    cache_blocks: false
};

pub async fn read_attribute_map(slatedb: Arc<Db>) -> HashMap<String, u64> {
    let attribute_to_id = HashMap::new();

    let range = create_prefix_range(b"a");

    let mut iter = slatedb.scan_with_options(range, &DEFAULT_SCAN_OPTIONS).await.unwrap();

    while let Some((key, _)) = iter.next().await.unwrap() {
        let attribute = bincode::deserialize::<String>(&key.slice(codec::CODEC_LENGTH as usize..key.len() - codec::CODEC_LENGTH - 8)).unwrap();
        let id = bincode::deserialize::<u64>(&key.slice(key.len() - 8 - codec::CODEC_LENGTH..key.len() - codec::CODEC_LENGTH)).unwrap();
        attribute_to_id.insert(attribute, id);
    }

    attribute_to_id
}

pub async fn get_and_create_attribute_id(slatedb: Arc<Db>, attribute: &str, attribute_map: &HashMap<String, u64>) -> u64 {
    let len = attribute_map.len() as u64;
    let attribute_id = match attribute_map.get(attribute) {
        Some(id) => id,
        None => {
            attribute_map.insert(attribute.to_string(), len);
            let attribute = bincode::serialize(attribute).unwrap();
            let id = bincode::serialize(&len).unwrap();
            slatedb.put(&concat_bytes(&[&[codec::ATTRIBUTE_TO_ID], &attribute, &id]), &[]);
            return len
        }
    };
    attribute_id
}