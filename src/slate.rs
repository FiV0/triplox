#![allow(dead_code, unused)]

use slatedb::db::Db;
use object_store::{ObjectStore, memory::InMemory, path::Path}; 
use object_store::local::LocalFileSystem;
use slatedb::config::{DbOptions, ReadLevel, ReadOptions, ScanOptions, WriteOptions};

use crate::util::random_string;

use std::sync::Arc;
use std::collections::HashMap;

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

pub fn read_attribute_to_id(_slatedb: Arc<Db>) -> HashMap<String, u64> {
    let attribute_to_id = HashMap::new();
    attribute_to_id
}