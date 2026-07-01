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
            node = node.insert(value);
        }
    }

    pub(crate) fn contains_prefix<'a, Q, I>(&self, values: I) -> bool
    where
        T: Borrow<Q>,
        Q: Eq + Hash + ?Sized + 'a,
        I: IntoIterator<Item = &'a Q>,
    {
        self.node(values).is_some()
    }

    pub(crate) fn node<'a, Q, I>(&self, values: I) -> Option<&TrieNode<T>>
    where
        T: Borrow<Q>,
        Q: Eq + Hash + ?Sized + 'a,
        I: IntoIterator<Item = &'a Q>,
    {
        let mut node = &self.root;
        for value in values {
            node = node.children.get(value)?;
        }
        Some(node)
    }

    pub(crate) fn node_mut<'a, Q, I>(&mut self, values: I) -> Option<&mut TrieNode<T>>
    where
        T: Borrow<Q>,
        Q: Eq + Hash + ?Sized + 'a,
        I: IntoIterator<Item = &'a Q>,
    {
        let mut node = &mut self.root;
        for value in values {
            node = node.children.get_mut(value)?;
        }
        Some(node)
    }
}

pub(crate) struct TrieNode<T> {
    children: HashMap<T, TrieNode<T>>,
}

impl<T> Default for TrieNode<T> {
    fn default() -> Self {
        Self {
            children: HashMap::new(),
        }
    }
}

impl<T> TrieNode<T>
where
    T: Eq + Hash,
{
    pub(crate) fn insert(&mut self, value: T) -> &mut TrieNode<T> {
        self.children.entry(value).or_default()
    }

    pub(crate) fn insert_all(&mut self, values: impl IntoIterator<Item = T>) {
        for value in values {
            self.insert(value);
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

    #[test]
    fn node_insert_adds_one_child_value() {
        let mut trie = Trie::new();

        let a = trie.root.insert("a");
        a.insert("x");

        assert!(trie.contains_prefix(["a"].iter()));
        assert!(trie.contains_prefix(["a", "x"].iter()));
        assert!(!trie.contains_prefix(["x"].iter()));
    }

    #[test]
    fn node_insert_all_adds_non_copy_child_values() {
        let mut trie = Trie::new();

        let a = trie.root.insert(String::from("a"));
        a.insert_all(vec![String::from("x"), String::from("y")]);

        assert!(trie.contains_prefix([String::from("a")].iter()));
        assert!(trie.contains_prefix([String::from("a"), String::from("x")].iter()));
        assert!(trie.contains_prefix([String::from("a"), String::from("y")].iter()));
        assert!(!trie.contains_prefix([String::from("x")].iter()));
    }

    #[test]
    fn node_finds_existing_prefix() {
        let mut trie = Trie::new();
        trie.insert(["a", "x"]);

        assert!(trie.node(["a"].iter()).is_some());
        assert!(trie.node(["a", "x"].iter()).is_some());
        assert!(trie.node(["a", "z"].iter()).is_none());
    }

    #[test]
    fn node_mut_allows_insertion_from_existing_prefix() {
        let mut trie = Trie::new();
        trie.insert(["a", "x"]);

        let a = trie.node_mut(["a"].iter()).unwrap();
        a.insert("y");

        assert!(trie.contains_prefix(["a", "x"].iter()));
        assert!(trie.contains_prefix(["a", "y"].iter()));
        assert!(!trie.contains_prefix(["y"].iter()));
    }
}
