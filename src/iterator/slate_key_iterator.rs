use anyhow::Result;
use bytes::Bytes;

use crate::slate::DEFAULT_SCAN_OPTIONS;

pub(crate) struct SlateKeyIterator {
    inner: slatedb::DbIterator,
    current_key: Option<Bytes>,
}

impl SlateKeyIterator {
    pub async fn scan_prefix(db: &slatedb::Db, prefix: &[u8]) -> Result<Self> {
        let mut inner = db
            .scan_prefix_with_options(prefix, &DEFAULT_SCAN_OPTIONS)
            .await?;
        let current_key = inner.next().await?.map(|kv| kv.key);
        Ok(Self { inner, current_key })
    }

    pub async fn seek(&mut self, key: &[u8]) -> Result<()> {
        if let Some(current) = &self.current_key {
            if current.as_ref() >= key {
                return Ok(());
            }
        }

        self.inner.seek(Bytes::copy_from_slice(key)).await?;
        self.current_key = self.inner.next().await?.map(|kv| kv.key);
        Ok(())
    }

    pub async fn next(&mut self) -> Result<Option<Bytes>> {
        self.current_key = self.inner.next().await?.map(|kv| kv.key);
        Ok(self.current_key.clone())
    }

    pub fn peek(&self) -> Option<&Bytes> {
        self.current_key.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::slate::in_memory_slate;

    const PFX: &[u8] = b"\x05";
    const OTHER_PFX: &[u8] = b"\x06";

    fn key(prefix: &[u8], suffix: &[u8]) -> Vec<u8> {
        let mut key = prefix.to_vec();
        key.extend_from_slice(suffix);
        key
    }

    #[tokio::test]
    async fn test_slate_key_iterator_peek_and_next() -> Result<()> {
        let slate = in_memory_slate().await.db;
        let first = key(PFX, b"aa");
        let second = key(PFX, b"bb");

        slate.put(&first, b"").await?;
        slate.put(&second, b"").await?;
        slate.put(&key(OTHER_PFX, b"xx"), b"").await?;

        let mut iter = SlateKeyIterator::scan_prefix(slate.as_ref(), PFX).await?;

        assert_eq!(iter.peek(), Some(&Bytes::from(first)));
        assert_eq!(iter.next().await?, Some(Bytes::from(second)));
        assert_eq!(iter.next().await?, None);
        assert_eq!(iter.peek(), None);
        Ok(())
    }

    #[tokio::test]
    async fn test_slate_key_iterator_seek_forwards_to_next_key() -> Result<()> {
        let slate = in_memory_slate().await.db;
        let first = key(PFX, b"aa");
        let second = key(PFX, b"cc");

        slate.put(&first, b"").await?;
        slate.put(&second, b"").await?;

        let mut iter = SlateKeyIterator::scan_prefix(slate.as_ref(), PFX).await?;
        iter.seek(&key(PFX, b"bb")).await?;

        assert_eq!(iter.peek(), Some(&Bytes::from(second)));
        Ok(())
    }

    #[tokio::test]
    async fn test_slate_key_iterator_seek_keeps_overshot_current_key() -> Result<()> {
        let slate = in_memory_slate().await.db;
        let overshot = key(PFX, b"bb-full-key");

        slate.put(&overshot, b"").await?;

        let mut iter = SlateKeyIterator::scan_prefix(slate.as_ref(), PFX).await?;
        iter.seek(&key(PFX, b"aa")).await?;
        iter.seek(&key(PFX, b"bb")).await?;

        assert_eq!(iter.peek(), Some(&Bytes::from(overshot)));
        Ok(())
    }

    #[tokio::test]
    async fn test_slate_key_iterator_next_after_seek_advances_past_landed_key() -> Result<()> {
        let slate = in_memory_slate().await.db;
        let first = key(PFX, b"aa");
        let second = key(PFX, b"bb");
        let third = key(PFX, b"cc");

        slate.put(&first, b"").await?;
        slate.put(&second, b"").await?;
        slate.put(&third, b"").await?;

        let mut iter = SlateKeyIterator::scan_prefix(slate.as_ref(), PFX).await?;
        iter.seek(&key(PFX, b"bb")).await?;

        assert_eq!(iter.peek(), Some(&Bytes::from(second)));
        assert_eq!(iter.next().await?, Some(Bytes::from(third.clone())));
        assert_eq!(iter.peek(), Some(&Bytes::from(third)));
        Ok(())
    }

    #[tokio::test]
    async fn test_slate_key_iterator_seek_beyond_end() -> Result<()> {
        let slate = in_memory_slate().await.db;

        slate.put(&key(PFX, b"aa"), b"").await?;

        let mut iter = SlateKeyIterator::scan_prefix(slate.as_ref(), PFX).await?;
        iter.seek(&key(PFX, b"zz")).await?;

        assert_eq!(iter.peek(), None);
        Ok(())
    }
}
