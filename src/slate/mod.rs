#![allow(dead_code, unused)]

pub mod cdc;

use slatedb::config::{DurabilityLevel, ReadOptions, ScanOptions, WriteOptions};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb::Db;
use std::sync::Arc;

use crate::util::random_string;

pub async fn in_memory_slate() -> Db {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    Db::builder(format!("tmp/triplox-{}", random_string(10)), object_store)
        .build()
        .await
        .unwrap()
}

pub async fn local_slate(path: &str) -> Db {
    let object_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(path).unwrap());
    Db::builder("triplox", object_store).build().await.unwrap()
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
