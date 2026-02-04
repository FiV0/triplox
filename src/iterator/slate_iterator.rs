use bytes::Bytes;

use anyhow::Error;
use tokio::runtime::Handle;

use crate::slate::DEFAULT_SCAN_OPTIONS;
use crate::util::{self, create_prefix_range, extract_next_component};

pub(crate) trait Index {
    fn count(&self) -> Result<u64, Error>;
    fn seek(&mut self, key: Bytes) -> Result<(), Error>;
    fn next(&mut self) -> Result<Option<Bytes>, Error>;
    fn get_value(&self) -> Result<Option<Bytes>, Error>;
    fn has_next(&self) -> bool;
}

pub(crate) struct SlateIterator {
    inner: slatedb::DbIterator,
    current_key: Option<Bytes>,
    prefix: Bytes,
    handle: Handle,
    count: u64,
}

impl SlateIterator {
    pub fn new(prefix: &[u8], slate: &slatedb::Db, handle: Handle) -> Result<Self, Error> {
        let prefix_bytes = Bytes::from(prefix.to_vec());
        let prefix_range = create_prefix_range(prefix);
        let count = handle.block_on(slate.estimate_key_count(prefix_range.clone()))?;
        let mut iterator = handle.block_on(slate.scan_with_options(prefix_range, &DEFAULT_SCAN_OPTIONS))?;
        let mut current_key = None;
        if let Some(next_key) = handle.block_on(iterator.next())? {
            current_key = Some(next_key.key.clone());
        }
        Ok(Self {
            inner: iterator,
            current_key,
            prefix: prefix_bytes,
            handle,
            count,
        })
    }
}

impl Index for SlateIterator {
    fn count(&self) -> Result<u64, Error> {
        Ok(self.count)
    }

    fn seek(&mut self, extension: Bytes) -> Result<(), Error> {
        let mut full_key = self.prefix.to_vec();
        full_key.extend_from_slice(&extension);
        let full_key = Bytes::from(full_key);

        // SlateDB forbids backward seeks. If already at or past the target, skip the seek.
        match &self.current_key {
            Some(current) if current.as_ref() >= full_key.as_ref() => return Ok(()),
            _ => {}
        }

        self.handle.block_on(self.inner.seek(full_key))?;

        // Update current_key so get_value() reflects the new position.
        let next_entry = self.handle.block_on(self.inner.next())?;
        self.current_key = next_entry.map(|e| e.key.clone());
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Bytes>, Error> {
        let next_key = self.handle.block_on(self.inner.next())?;
        if let Some(next_key) = next_key {
            self.current_key = Some(next_key.key.clone());
            Ok(Some(extract_next_component(&next_key.key, &self.prefix)?))
        } else {
            self.current_key = None;
            Ok(None)
        }
    }

    fn get_value(&self) -> Result<Option<Bytes>, Error> {
        match &self.current_key {
            Some(key) => Ok(Some(extract_next_component(key, &self.prefix)?)),
            None => Ok(None),
        }
    }

    fn has_next(&self) -> bool {
        self.current_key.is_some()
    }
}

// vec is assumed to be sorted
pub(crate) struct VecIterator {
    vec: Vec<Bytes>,
    index: usize,
}

impl VecIterator {
    pub fn new(vec: Vec<Bytes>) -> Self {
        Self { vec, index: 0 }
    }
}

impl Index for VecIterator {
    fn count(&self) -> Result<u64, Error> {
        Ok(self.vec.len() as u64)
    }

    fn seek(&mut self, key: Bytes) -> Result<(), Error> {
        match self.vec.binary_search(&key) {
            Ok(pos) | Err(pos) => {
                self.index = pos;
                Ok(())
            }
        }
    }

    fn next(&mut self) -> Result<Option<Bytes>, Error> {
        if self.index < self.vec.len() {
            let result = Some(self.vec[self.index].clone());
            self.index += 1;
            Ok(result)
        } else {
            Ok(None)
        }
    }

    fn get_value(&self) -> Result<Option<Bytes>, Error> {
        if self.index < self.vec.len() {
            Ok(Some(self.vec[self.index].clone()))
        } else {
            Ok(None)
        }
    }

    fn has_next(&self) -> bool {
        self.index < self.vec.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::codec::ADD;
    use crate::slate::in_memory_slate;

    #[test]
    fn test_vec_iterator_basic_operations() {
        let vec = vec![
            Bytes::from("a"),
            Bytes::from("b"),
            Bytes::from("c"),
            Bytes::from("d"),
            Bytes::from("e"),
        ];
        let mut iter = VecIterator::new(vec);

        assert_eq!(iter.count().unwrap(), 5);
        assert!(iter.has_next());

        assert_eq!(iter.next().unwrap(), Some(Bytes::from("a")));
        assert_eq!(iter.next().unwrap(), Some(Bytes::from("b")));
        assert_eq!(iter.next().unwrap(), Some(Bytes::from("c")));
        assert_eq!(iter.next().unwrap(), Some(Bytes::from("d")));
        assert_eq!(iter.next().unwrap(), Some(Bytes::from("e")));
        assert_eq!(iter.next().unwrap(), None);
        assert!(!iter.has_next());
    }

    #[test]
    fn test_vec_iterator_seek() {
        let vec = vec![
            Bytes::from("a"),
            Bytes::from("c"),
            Bytes::from("e"),
            Bytes::from("g"),
            Bytes::from("i"),
        ];
        let mut iter = VecIterator::new(vec);

        // Seek to existing value
        iter.seek(Bytes::from("c")).unwrap();
        assert_eq!(iter.next().unwrap(), Some(Bytes::from("c")));
        assert_eq!(iter.next().unwrap(), Some(Bytes::from("e")));

        // Seek to non-existing value (lands on next greater)
        iter.seek(Bytes::from("f")).unwrap();
        assert_eq!(iter.next().unwrap(), Some(Bytes::from("g")));

        // Seek beyond end
        iter.seek(Bytes::from("z")).unwrap();
        assert_eq!(iter.next().unwrap(), None);
    }

    #[test]
    fn test_vec_iterator_empty() {
        let mut iter = VecIterator::new(vec![]);

        assert_eq!(iter.count().unwrap(), 0);
        assert!(!iter.has_next());
        assert_eq!(iter.next().unwrap(), None);
        assert_eq!(iter.get_value().unwrap(), None);
        iter.seek(Bytes::from("a")).unwrap();
        assert_eq!(iter.next().unwrap(), None);
    }

    #[test]
    fn test_vec_iterator_get_value() {
        let vec = vec![Bytes::from("x"), Bytes::from("y")];
        let mut iter = VecIterator::new(vec);

        assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("x")));
        iter.next().unwrap();
        assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("y")));
        iter.next().unwrap();
        assert_eq!(iter.get_value().unwrap(), None);
    }

    /// Build a key: prefix + value + ADD byte
    fn make_key(prefix: &[u8], value: &[u8]) -> Vec<u8> {
        let mut key = prefix.to_vec();
        key.extend_from_slice(value);
        key.push(ADD);
        key
    }

    const PFX: &[u8] = b"\x01";
    const OTHER_PFX: &[u8] = b"\x02";

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_slate_iterator_basic() {
        let slate = in_memory_slate().await;
        let handle = Handle::current();

        slate.put(&make_key(PFX, b"aa"), b"").await;
        slate.put(&make_key(PFX, b"bb"), b"").await;
        slate.put(&make_key(PFX, b"cc"), b"").await;
        slate.put(&make_key(OTHER_PFX, b"xx"), b"").await;

        tokio::task::spawn_blocking(move || {
            let mut iter = SlateIterator::new(PFX, &slate, handle).unwrap();

            assert!(iter.has_next());
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("aa")));

            assert_eq!(iter.next().unwrap(), Some(Bytes::from("bb")));
            assert_eq!(iter.next().unwrap(), Some(Bytes::from("cc")));
            assert_eq!(iter.next().unwrap(), None);
            assert!(!iter.has_next());
        }).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_slate_iterator_seek() {
        let slate = in_memory_slate().await;
        let handle = Handle::current();

        slate.put(&make_key(PFX, b"aa"), b"").await;
        slate.put(&make_key(PFX, b"cc"), b"").await;
        slate.put(&make_key(PFX, b"ee"), b"").await;

        tokio::task::spawn_blocking(move || {
            let mut iter = SlateIterator::new(PFX, &slate, handle).unwrap();

            iter.seek(Bytes::from("cc")).unwrap();
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("cc")));
            assert_eq!(iter.next().unwrap(), Some(Bytes::from("ee")));
            assert_eq!(iter.next().unwrap(), None);
        }).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_slate_iterator_empty_prefix() {
        let slate = in_memory_slate().await;
        let handle = Handle::current();

        slate.put(&make_key(OTHER_PFX, b"aa"), b"").await;

        tokio::task::spawn_blocking(move || {
            let iter = SlateIterator::new(PFX, &slate, handle).unwrap();
            assert!(!iter.has_next());
            assert_eq!(iter.get_value().unwrap(), None);
        }).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_slate_iterator_count() {
        let slate = in_memory_slate().await;
        let handle = Handle::current();

        slate.put(&make_key(PFX, b"aa"), b"").await;
        slate.put(&make_key(PFX, b"bb"), b"").await;
        slate.put(&make_key(PFX, b"cc"), b"").await;

        tokio::task::spawn_blocking(move || {
            let iter = SlateIterator::new(PFX, &slate, handle).unwrap();
            let count = iter.count().unwrap();
            // estimate_key_count is an approximation, so just check it's reasonable
            assert!(count >= 1, "count should be at least 1, got {}", count);
        }).await.unwrap();
    }
}
