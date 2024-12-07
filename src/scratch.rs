use bytes::Bytes;
use slatedb::{db::Db, config::ScanOptions, config::ReadLevel, error::SlateDBError};
use slatedb::object_store::{ObjectStore, memory::InMemory};
use std::sync::Arc;

use std::ops::{Bound, RangeBounds};

fn create_prefix_range(prefix: &[u8]) -> (Bound<Bytes>, Bound<Bytes>) {
    let start = Bound::Included(Bytes::copy_from_slice(prefix));
    
    // Create end by appending 0xFF to the prefix
    let mut end_bytes = prefix.to_vec();
    end_bytes.push(0xFF);
    let end = Bound::Included(Bytes::copy_from_slice(&end_bytes));
    
    (start, end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_scan() -> Result<(), SlateDBError> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::open("test_db", object_store).await?;
        db.put(b"ab", b"ab_value").await?;
        db.put(b"ac", b"ac_value").await?;
        db.put(b"b", b"b_value").await?;

        let range = create_prefix_range(b"a");

        let mut iter = db.scan_with_options(range, &ScanOptions {
            read_level: ReadLevel::Uncommitted,
            ..ScanOptions::default()
        }).await?;
        assert_eq!(Some((b"ab" as &[u8], b"ab_value" as &[u8]).into()) , iter.next().await?);
        assert_eq!(Some((b"ac" as &[u8], b"ac_value" as &[u8]).into()) , iter.next().await?);
        assert_eq!(None , iter.next().await?);
        Ok(())
    }
}
