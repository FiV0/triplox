use slatedb::db::Db;
use object_store::{ObjectStore, memory::InMemory, path::Path};
use slatedb::config::DbOptions;
use std::sync::Arc;

use crate::util::random_string;

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