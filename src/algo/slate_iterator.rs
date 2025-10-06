use bytes::Bytes;
use std::sync::Arc;
use tokio::runtime::Handle;

use anyhow::Error;
use slatedb::{DbIterator};

use crate::slate::DEFAULT_SCAN_OPTIONS;
use crate::util::create_prefix_range;

pub (crate) trait Index {
    // TODO: move back to async
    fn seek(&mut self, key: Bytes) -> Result<(), Error>;
    fn next(&mut self) -> Result<Option<Bytes>, Error>;
    fn get_value(&self) -> Result<Option<Bytes>, Error>;
    fn has_next(&self) -> bool;
}

pub (crate) struct SlateIterator<'a> {
    inner : slatedb::DbIterator<'a>,
    current_key: Option<Bytes>
}

impl<'a> SlateIterator<'a> {
    pub fn new(prefix: &[u8], slate: &'a slatedb::Db) -> Result<Self, Error> {
        let range = create_prefix_range(prefix);
        let mut iterator = Handle::current().block_on(slate.scan_with_options(range, &DEFAULT_SCAN_OPTIONS))?;
        let mut current_key = None;
        if let Some(next_key) = Handle::current().block_on(iterator.next())? {
            current_key = Some(next_key.key.clone());
        }
        Ok(Self { inner: iterator, current_key })
    }
}

// TODO: move to &[u8]
impl<'a> Index for SlateIterator<'a> {
    fn seek(&mut self, key: Bytes) -> Result<(), Error> {
        Handle::current().block_on(self.inner.seek(key))?;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Bytes>, Error> {
        let next_key = Handle::current().block_on(self.inner.next())?;
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
