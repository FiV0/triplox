use std::sync::Arc;
use slatedb::Db;
use crate::codec;
use crate::util::concat_bytes;

const META_KEY_VERSION: &[u8] = b"version";

/// Initialize the database. Returns `(version, is_fresh)`.
/// If no version exists in META_INDEX, writes the current crate version and
/// returns `(version, true)`. If a version already exists, returns `(version, false)`.
///
/// When `is_fresh` is true, the caller should transact the bootstrap schema.
pub async fn init_db(slatedb: Arc<Db>) -> (String, bool) {
    let key = concat_bytes(&[&[codec::META_INDEX], META_KEY_VERSION]);

    match slatedb.get(&key).await.expect("Failed to read version from META_INDEX") {
        Some(bytes) => {
            // Existing DB
            let version = String::from_utf8(bytes.to_vec()).expect("Invalid UTF-8 in stored version");
            (version, false)
        }
        None => {
            // Fresh DB — write current version
            let version = env!("CARGO_PKG_VERSION");
            slatedb.put(&key, version.as_bytes()).await.expect("Failed to write version to META_INDEX");
            (version.to_string(), true)
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
        let (version, is_fresh) = init_db(slatedb).await;
        assert_eq!(version, env!("CARGO_PKG_VERSION"));
        assert!(is_fresh);
    }

    // TODO(triplox-6x7): Replace with a local_node test that transacts schema,
    // closes the node, reopens it, and verifies the version is preserved.
    #[tokio::test]
    async fn test_init_db_existing() {
        let slatedb = Arc::new(in_memory_slate().await);
        let (version1, is_fresh1) = init_db(slatedb.clone()).await;
        let (version2, is_fresh2) = init_db(slatedb).await;
        assert_eq!(version1, version2);
        assert!(is_fresh1);
        assert!(!is_fresh2);
    }

    #[tokio::test]
    async fn test_init_db_preserves_old_version() {
        let slatedb = Arc::new(in_memory_slate().await);

        // Write an older version directly
        let key = concat_bytes(&[&[codec::META_INDEX], META_KEY_VERSION]);
        slatedb.put(&key, b"0.0.1").await.unwrap();

        // init_db should return the existing version, not overwrite it
        let (version, is_fresh) = init_db(slatedb).await;
        assert_eq!(version, "0.0.1");
        assert!(!is_fresh);
    }
}
