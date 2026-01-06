#![allow(unused)]

use bytes::Bytes;
use slatedb::{Db, Error as SlateDBError, config::{ScanOptions, DurabilityLevel}};
use slatedb::object_store::{ObjectStore, memory::InMemory};
use std::sync::Arc;

use std::ops::Bound;

#[cfg(test)]
mod tests {
    use slatedb::KeyValue;
    use crate::util::create_prefix_range;
    use anyhow::Error;

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
            durability_filter: DurabilityLevel::Memory,
            dirty: true,
            ..ScanOptions::default()
        }).await?;
        
        if let Some(kv1) = iter.next().await? {
            assert_eq!(kv1.key, Bytes::from("ab"));
            assert_eq!(kv1.value, Bytes::from("ab_value"));
        }
        if let Some(kv2) = iter.next().await? {
            assert_eq!(kv2.key, Bytes::from("ac"));
            assert_eq!(kv2.value, Bytes::from("ac_value"));
        }
        assert_eq!(None , iter.next().await?);
        Ok(())
    }
    
    use bincode;

    #[tokio::test]
    async fn test_anything() -> Result<(), Error> {
        let value = 1 as u64;
        let attribute_length = bincode::serialize(&value)?;
        println!("{:?}", attribute_length.len());
        Ok(())
    }
}
