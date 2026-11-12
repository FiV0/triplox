use bytes::Bytes;

use anyhow::Error;

pub(crate) trait Index {
    fn count(&self) -> Result<u64, Error>;
    fn seek(&mut self, key: Bytes) -> Result<(), Error>;
    fn next(&mut self) -> Result<Option<Bytes>, Error>;
    fn get_value(&self) -> Result<Option<Bytes>, Error>;
    fn has_next(&self) -> bool;
}

/// Type alias for extractor functions that extract a component from a full key.
pub type Extractor = Box<dyn Fn(Bytes) -> Bytes + Send + Sync>;

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
}
