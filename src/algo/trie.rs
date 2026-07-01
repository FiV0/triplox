use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

pub(crate) struct Trie<T> {
    root: TrieNode<T>,
}

impl<T> Default for Trie<T> {
    fn default() -> Self {
        Self {
            root: TrieNode::default(),
        }
    }
}

impl<T> Trie<T>
where
    T: Eq + Hash,
{
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn insert<I>(&mut self, values: I)
    where
        I: IntoIterator<Item = T>,
    {
        let mut node = &mut self.root;
        for value in values {
            node = node.children.entry(value).or_default();
        }
    }

    pub(crate) fn contains_prefix<'a, Q, I>(&self, values: I) -> bool
    where
        T: Borrow<Q>,
        Q: Eq + Hash + ?Sized + 'a,
        I: IntoIterator<Item = &'a Q>,
    {
        let mut node = &self.root;
        for value in values {
            let Some(child) = node.children.get(value) else {
                return false;
            };
            node = child;
        }
        true
    }
}

struct TrieNode<T> {
    children: HashMap<T, TrieNode<T>>,
}

impl<T> Default for TrieNode<T> {
    fn default() -> Self {
        Self {
            children: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Trie;

    #[test]
    fn empty_prefix_is_always_present() {
        let trie = Trie::<&str>::new();

        assert!(trie.contains_prefix(std::iter::empty::<&str>()));
    }

    #[test]
    fn contains_inserted_full_and_partial_prefixes() {
        let mut trie = Trie::new();
        trie.insert(["a", "x"]);
        trie.insert(["a", "y"]);
        trie.insert(["b", "z"]);

        assert!(trie.contains_prefix(["a"].iter()));
        assert!(trie.contains_prefix(["a", "x"].iter()));
        assert!(trie.contains_prefix(["a", "y"].iter()));
        assert!(trie.contains_prefix(["b", "z"].iter()));
    }

    #[test]
    fn missing_prefixes_return_false() {
        let mut trie = Trie::new();
        trie.insert(["a", "x"]);

        assert!(!trie.contains_prefix(["b"].iter()));
        assert!(!trie.contains_prefix(["a", "z"].iter()));
    }

    #[test]
    fn duplicate_inserts_keep_prefix_present() {
        let mut trie = Trie::new();
        trie.insert(["a", "x"]);
        trie.insert(["a", "x"]);

        assert!(trie.contains_prefix(["a"].iter()));
        assert!(trie.contains_prefix(["a", "x"].iter()));
    }
}
