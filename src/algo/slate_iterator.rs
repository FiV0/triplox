use bytes::Bytes;
use std::sync::Arc;

use anyhow::Error;
use slatedb::{DbIterator};

use crate::slate::DEFAULT_SCAN_OPTIONS;
use crate::util::create_prefix_range;

pub (crate) trait Index {
    async fn seek(&mut self, key: Bytes) -> Result<(), Error>;
    async fn next(&mut self) -> Result<Option<Bytes>, Error>;
    fn get_value(&self) -> Result<Option<Bytes>, Error>;
    fn has_next(&self) -> bool;
}

pub (crate) struct SlateIterator<'a> {
    inner : slatedb::DbIterator<'a>,
    current_key: Option<Bytes>
}

impl<'a> SlateIterator<'a> {
    pub async fn new(prefix: &[u8], slate: &'a slatedb::Db) -> Result<Self, Error> {
        let range = create_prefix_range(prefix);
        let mut iterator = slate.scan_with_options(range, &DEFAULT_SCAN_OPTIONS).await?;
        let mut current_key = None;
        if let Some(next_key) = iterator.next().await? {
            current_key = Some(next_key.key.clone());
        }
        Ok(Self { inner: iterator, current_key })
    }
}

impl<'a> Index for SlateIterator<'a> {
    async fn seek(&mut self, key: Bytes) -> Result<(), Error> {
        self.inner.seek(key).await?;
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<Bytes>, Error> {
        let next_key = self.inner.next().await?;
        if let Some(next_key) = next_key {
            self.current_key = Some(next_key.key.clone());
        } else {
            self.current_key = None;
        }
        Ok(self.current_key.clone())
    }

    fn get_value(&self) -> Result<Option<Bytes>, Error> {
        Ok(self.current_key.clone())
    }

    fn has_next(&self) -> bool {
        self.current_key.is_some()
    }
}
