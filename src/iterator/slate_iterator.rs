use bytes::Bytes;

use anyhow::Error;
use tokio::runtime::Handle;

use crate::slate::DEFAULT_SCAN_OPTIONS;
use crate::util::{self, create_prefix_range};

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
    handle: Handle,
    count: u64,
}

impl SlateIterator {
    pub fn new(prefix: &[u8], slate: &slatedb::Db, handle: Handle) -> Result<Self, Error> {
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
            handle,
            count,
        })
    }
}

impl Index for SlateIterator {
    fn count(&self) -> Result<u64, Error> {
        Ok(self.count)
    }

    fn seek(&mut self, key: Bytes) -> Result<(), Error> {
        self.handle.block_on(self.inner.seek(key))?;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Bytes>, Error> {
        let next_key = self.handle.block_on(self.inner.next())?;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_slate_iterator_basic() {
        let slate = in_memory_slate().await;
        let handle = Handle::current();

        slate.put(b"pfx/a", b"").await;
        slate.put(b"pfx/b", b"").await;
        slate.put(b"pfx/c", b"").await;
        slate.put(b"other/x", b"").await;

        tokio::task::spawn_blocking(move || {
            let mut iter = SlateIterator::new(b"pfx/", &slate, handle).unwrap();

            assert!(iter.has_next());
            assert_eq!(iter.get_value().unwrap(), Some(Bytes::from("pfx/a")));

            assert_eq!(iter.next().unwrap(), Some(Bytes::from("pfx/b")));
            assert_eq!(iter.next().unwrap(), Some(Bytes::from("pfx/c")));
            assert_eq!(iter.next().unwrap(), None);
            assert!(!iter.has_next());
        }).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_slate_iterator_seek() {
        let slate = in_memory_slate().await;
        let handle = Handle::current();

        slate.put(b"pfx/a", b"").await;
        slate.put(b"pfx/c", b"").await;
        slate.put(b"pfx/e", b"").await;

        tokio::task::spawn_blocking(move || {
            let mut iter = SlateIterator::new(b"pfx/", &slate, handle).unwrap();

            iter.seek(Bytes::from("pfx/c")).unwrap();
            assert_eq!(iter.next().unwrap(), Some(Bytes::from("pfx/c")));
            assert_eq!(iter.next().unwrap(), Some(Bytes::from("pfx/e")));
            assert_eq!(iter.next().unwrap(), None);
        }).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_slate_iterator_empty_prefix() {
        let slate = in_memory_slate().await;
        let handle = Handle::current();

        slate.put(b"other/a", b"").await;

        tokio::task::spawn_blocking(move || {
            let iter = SlateIterator::new(b"pfx/", &slate, handle).unwrap();
            assert!(!iter.has_next());
            assert_eq!(iter.get_value().unwrap(), None);
        }).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_slate_iterator_count() {
        let slate = in_memory_slate().await;
        let handle = Handle::current();

        slate.put(b"pfx/a", b"").await;
        slate.put(b"pfx/b", b"").await;
        slate.put(b"pfx/c", b"").await;

        tokio::task::spawn_blocking(move || {
            let iter = SlateIterator::new(b"pfx/", &slate, handle).unwrap();
            let count = iter.count().unwrap();
            // estimate_key_count is an approximation, so just check it's reasonable
            assert!(count >= 1, "count should be at least 1, got {}", count);
        }).await.unwrap();
    }
}
