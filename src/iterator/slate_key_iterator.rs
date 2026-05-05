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

    pub fn get_value(&self) -> Option<&Bytes> {
        self.current_key.as_ref()
    }
}
