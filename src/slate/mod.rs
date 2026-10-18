#![allow(dead_code, unused)]

pub mod cdc;

use slatedb::config::{
    CompactorOptions, DurabilityLevel, GarbageCollectorDirectoryOptions, GarbageCollectorOptions,
    ObjectStoreCacheOptions, ReadOptions, ScanOptions, Settings, WriteOptions,
};
use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
use slatedb::db_cache::{DbCache, SplitCache};
use slatedb::object_store::aws::AmazonS3Builder;
use slatedb::object_store::client::SpawnedReqwestConnector;
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb::Db;
use std::path::Path;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::runtime::{Handle, Runtime};

use crate::config::RemoteStorageConfig;
use crate::util::random_string;

pub const DEFAULT_BLOCK_CACHE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub const DEFAULT_META_CACHE_BYTES: u64 = 512 * 1024 * 1024;
pub const DEFAULT_RANGE_STATS_CACHE_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_LOCAL_WAL_FLUSH_INTERVAL: Duration = Duration::from_millis(10);
const OBJECT_STORE_IO_THREADS: usize = 2;

// Drives all S3 HTTP I/O, so a pooled connection doesn't depend on the runtime that opened it.
static OBJECT_STORE_IO_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(OBJECT_STORE_IO_THREADS)
        .thread_name("tpx-io")
        .enable_all()
        .build()
        .expect("failed to start the object store I/O runtime")
});

const GC_INTERVAL: Duration = Duration::from_secs(5 * 60);
const GC_MIN_AGE: Duration = Duration::from_secs(5 * 60);
// A single SlateDB read (scan/get) overlapping a compaction must finish within
// this window, otherwise GC may delete the SSTs it still reads.
const COMPACTOR_CHECKPOINT_LIFETIME: Duration = Duration::from_secs(5 * 60);

fn default_compactor_options() -> CompactorOptions {
    CompactorOptions {
        checkpoint_lifetime: COMPACTOR_CHECKPOINT_LIFETIME,
        ..CompactorOptions::default()
    }
}

fn default_garbage_collector_options() -> GarbageCollectorOptions {
    let dir = GarbageCollectorDirectoryOptions {
        interval: Some(GC_INTERVAL),
        min_age: GC_MIN_AGE,
        dry_run: false,
    };
    // WAL fence GC keeps SlateDB's default (dry run), deleting fences can lose data.
    GarbageCollectorOptions {
        manifest_options: Some(dir),
        wal_options: Some(dir),
        compacted_options: Some(dir),
        compactions_options: Some(dir),
        ..GarbageCollectorOptions::default()
    }
}

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

async fn build_in_memory_slate(write_runtime: Option<&Handle>) -> SlateComponents {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = format!("tmp/triplox-{}", random_string(10));
    let mut builder = Db::builder(path.clone(), object_store.clone())
        .with_settings(Settings {
            flush_interval: Some(DEFAULT_LOCAL_WAL_FLUSH_INTERVAL),
            ..Settings::default()
        })
        .with_db_cache(default_db_cache(), 0);
    if let Some(write_runtime) = write_runtime {
        builder = builder.with_write_runtime(write_runtime.clone());
    }
    let db = Arc::new(builder.build().await.unwrap());
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

pub async fn in_memory_slate() -> SlateComponents {
    build_in_memory_slate(None).await
}

// SlateDB's batch-writer, which every write and `Db::close` go through, runs on `write_runtime`.
pub async fn in_memory_slate_with_write_runtime(write_runtime: &Handle) -> SlateComponents {
    build_in_memory_slate(Some(write_runtime)).await
}

pub async fn local_slate(root_path: &Path, write_runtime: &Handle) -> SlateComponents {
    let object_store: Arc<dyn ObjectStore> = Arc::new(
        LocalFileSystem::new_with_prefix(root_path)
            .unwrap()
            .with_fsync(true),
    );
    let slate_path = "triplox".to_string();
    let db = Arc::new(
        Db::builder(slate_path.clone(), object_store.clone())
            .with_settings(Settings {
                flush_interval: Some(DEFAULT_LOCAL_WAL_FLUSH_INTERVAL),
                compactor_options: Some(default_compactor_options()),
                garbage_collector_options: Some(default_garbage_collector_options()),
                ..Settings::default()
            })
            .with_db_cache(default_db_cache(), 0)
            .with_write_runtime(write_runtime.clone())
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
    storage: &RemoteStorageConfig,
    write_runtime: &Handle,
) -> Result<SlateComponents, anyhow::Error> {
    let s3 = AmazonS3Builder::new()
        .with_endpoint(&storage.endpoint)
        .with_bucket_name(&storage.bucket)
        .with_access_key_id(&storage.access_key)
        .with_secret_access_key(&storage.secret_key)
        .with_region(&storage.region)
        .with_allow_http(true) // TODO: make configurable for production use
        .with_http_connector(SpawnedReqwestConnector::new(
            OBJECT_STORE_IO_RUNTIME.handle().clone(),
        ))
        .build()
        .expect("failed to build S3 object store");

    let object_store: Arc<dyn ObjectStore> = Arc::new(s3);
    let cache_path = storage.cache_path.join("cache");
    std::fs::create_dir_all(&cache_path)?;
    let settings = Settings {
        flush_interval: Some(Duration::from_micros(storage.wal_flush_interval_us.get())),
        max_unflushed_bytes: 2 * 1024 * 1024 * 1024,
        l0_max_ssts: 16,
        wal_enabled: true,
        compactor_options: Some(default_compactor_options()),
        garbage_collector_options: Some(default_garbage_collector_options()),
        object_store_cache_options: ObjectStoreCacheOptions {
            root_folder: Some(cache_path),
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
            .with_db_cache(default_db_cache(), 0)
            .with_write_runtime(write_runtime.clone())
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

pub const DEFAULT_WRITE_OPTIONS: WriteOptions = WriteOptions { seqnum: 0 };

pub const DEFAULT_SCAN_OPTIONS: ScanOptions = ScanOptions {
    durability_filter: DurabilityLevel::Memory,
    dirty: false,
    read_ahead_bytes: 1,
    cache_blocks: true,
    max_fetch_tasks: 1,
    order: slatedb::IterationOrder::Ascending,
    filter_context: None,
    tracing_options: None,
};
