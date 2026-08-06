#![allow(dead_code, unused)]

pub mod cdc;

use slatedb::config::{
    DurabilityLevel, ObjectStoreCacheOptions, ReadOptions, ScanOptions, Settings, WriteOptions,
};
use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
use slatedb::db_cache::{DbCache, SplitCache};
use slatedb::object_store::aws::AmazonS3Builder;
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb::Db;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::util::random_string;

pub const DEFAULT_BLOCK_CACHE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub const DEFAULT_META_CACHE_BYTES: u64 = 512 * 1024 * 1024;
pub const DEFAULT_RANGE_STATS_CACHE_BYTES: u64 = 512 * 1024 * 1024;

pub fn default_db_cache() -> Arc<dyn DbCache> {
    let block_cache = FoyerCache::new_with_opts(FoyerCacheOptions {
        max_capacity: DEFAULT_BLOCK_CACHE_BYTES,
        ..Default::default()
    });
    let meta_cache = FoyerCache::new_with_opts(FoyerCacheOptions {
        max_capacity: DEFAULT_META_CACHE_BYTES,
        ..Default::default()
    });
    Arc::new(
        SplitCache::new()
            .with_block_cache(Some(Arc::new(block_cache)))
            .with_meta_cache(Some(Arc::new(meta_cache)))
            .build(),
    )
}

pub fn default_range_stats_cache() -> Arc<dyn DbCache> {
    let meta_cache = FoyerCache::new_with_opts(FoyerCacheOptions {
        max_capacity: DEFAULT_RANGE_STATS_CACHE_BYTES,
        ..Default::default()
    });
    Arc::new(
        SplitCache::new()
            .with_meta_cache(Some(Arc::new(meta_cache)))
            .build(),
    )
}

pub struct SlateComponents {
    pub db: Arc<Db>,
    pub object_path: String,
    pub object_store: Arc<dyn ObjectStore>,
    pub range_stats: Arc<slatedb_estimates::RangeStats>,
}

impl std::fmt::Debug for SlateComponents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlateComponents")
            .field("object_path", &self.object_path)
            .finish_non_exhaustive()
    }
}

pub async fn in_memory_slate() -> SlateComponents {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = format!("tmp/triplox-{}", random_string(10));
    let db = Arc::new(
        Db::builder(path.clone(), object_store.clone())
            .with_db_cache(default_db_cache())
            .build()
            .await
            .unwrap(),
    );
    let range_stats = Arc::new(slatedb_estimates::RangeStats::new(
        db.clone(),
        path.clone(),
        object_store.clone(),
        Some(default_range_stats_cache()),
        None,
    ));
    SlateComponents {
        db,
        object_path: path,
        object_store,
        range_stats,
    }
}

pub async fn local_slate(root_path: &Path) -> SlateComponents {
    let object_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(root_path).unwrap());
    let slate_path = "triplox".to_string();
    let db = Arc::new(
        Db::builder(slate_path.clone(), object_store.clone())
            .with_db_cache(default_db_cache())
            .build()
            .await
            .unwrap(),
    );
    let range_stats = Arc::new(slatedb_estimates::RangeStats::new(
        db.clone(),
        slate_path.clone(),
        object_store.clone(),
        Some(default_range_stats_cache()),
        None,
    ));
    SlateComponents {
        db,
        object_path: slate_path,
        object_store,
        range_stats,
    }
}

pub async fn remote_slate(
    endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    cache_path: &Path,
) -> Result<SlateComponents, anyhow::Error> {
    let s3 = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_bucket_name(bucket)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .with_region(region)
        .with_allow_http(true) // TODO: make configurable for production use
        .build()
        .expect("failed to build S3 object store");

    let object_store: Arc<dyn ObjectStore> = Arc::new(s3);
    std::fs::create_dir_all(cache_path)?;
    let settings = Settings {
        flush_interval: Some(Duration::new(0, 100000)),
        max_unflushed_bytes: 2 * 1024 * 1024 * 1024,
        l0_max_ssts: 16,
        wal_enabled: true,
        // compactor_options: None,
        object_store_cache_options: ObjectStoreCacheOptions {
            root_folder: Some(cache_path.to_path_buf()),
            cache_on_flush: true,
            cache_on_compaction: true,
            ..ObjectStoreCacheOptions::default()
        },
        ..Settings::default()
    };
    // TODO: hardcoded path — reconsider when adding database creation functions
    let db = Arc::new(
        Db::builder("triplox", object_store.clone())
            .with_settings(settings)
            .with_db_cache(default_db_cache())
            .build()
            .await?,
    );
    let path = "triplox".to_string();
    let range_stats = Arc::new(slatedb_estimates::RangeStats::new(
        db.clone(),
        path.clone(),
        object_store.clone(),
        Some(default_range_stats_cache()),
        None,
    ));
    Ok(SlateComponents {
        db,
        object_path: path,
        object_store,
        range_stats,
    })
}

pub const DEFAULT_WRITE_OPTIONS: WriteOptions = WriteOptions {
    await_durable: false,
    seqnum: 0,
};

pub const DEFAULT_SCAN_OPTIONS: ScanOptions = ScanOptions {
    durability_filter: DurabilityLevel::Memory,
    dirty: false,
    read_ahead_bytes: 1,
    cache_blocks: true,
    max_fetch_tasks: 1,
    order: slatedb::IterationOrder::Ascending,
    filter_context: None,
};
