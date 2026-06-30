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
        let values: Vec<T> = values.into_iter().collect();
        if self
            .node_for(values.iter())
            .is_some_and(|node| node.is_terminal)
        {
            return;
        }

        let mut node = &mut self.root;
        node.terminal_descendants += 1;
        for value in values {
            node = node.insert_child(value);
            node.terminal_descendants += 1;
        }
        node.is_terminal = true;
    }

    pub(crate) fn node_for<'a, Q, I>(&self, values: I) -> Option<&TrieNode<T>>
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

    pub(crate) fn node_for_mut<'a, Q, I>(&mut self, values: I) -> Option<&mut TrieNode<T>>
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
    is_terminal: bool,
    terminal_descendants: usize,
}

impl<T> Default for TrieNode<T> {
    fn default() -> Self {
        Self {
            children: HashMap::new(),
            is_terminal: false,
            terminal_descendants: 0,
        }
    }
}

impl<T> TrieNode<T>
where
    T: Eq + Hash,
{
    pub(crate) fn children(&self) -> &HashMap<T, TrieNode<T>> {
        &self.children
    }

    pub(crate) fn descendant_count(&self) -> usize {
        self.terminal_descendants
    }

    pub(crate) fn insert_child(&mut self, value: T) -> &mut TrieNode<T> {
        self.children.entry(value).or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::Trie;

    #[test]
    fn root_lookup_accepts_empty_prefix() {
        let trie = Trie::<&str>::new();

        let root = trie.node_for(std::iter::empty::<&str>());

        assert!(root.is_some());
        assert!(root.unwrap().children().is_empty());
        assert_eq!(root.unwrap().descendant_count(), 0);
    }

    #[test]
    fn finds_inserted_full_and_partial_prefixes() {
        let mut trie = Trie::new();
        trie.insert(["a", "x"]);
        trie.insert(["a", "y"]);
        trie.insert(["b", "z"]);

        let root = trie.node_for(std::iter::empty::<&str>()).unwrap();
        let a = trie.node_for(["a"].iter()).unwrap();
        let ax = trie.node_for(["a", "x"].iter()).unwrap();

        assert_eq!(root.children().len(), 2);
        assert_eq!(a.children().len(), 2);
        assert!(ax.children().is_empty());
        assert_eq!(root.descendant_count(), 3);
        assert_eq!(a.descendant_count(), 2);
        assert_eq!(ax.descendant_count(), 1);
    }

    #[test]
    fn missing_prefixes_return_none() {
        let mut trie = Trie::new();
        trie.insert(["a", "x"]);

        assert!(trie.node_for(["b"].iter()).is_none());
        assert!(trie.node_for(["a", "z"].iter()).is_none());
    }

    #[test]
    fn duplicate_inserts_do_not_duplicate_children() {
        let mut trie = Trie::new();
        trie.insert(["a", "x"]);
        trie.insert(["a", "x"]);
        trie.insert(["a", "y"]);

        let root = trie.node_for(std::iter::empty::<&str>()).unwrap();
        let a = trie.node_for(["a"].iter()).unwrap();

        assert_eq!(root.children().len(), 1);
        assert_eq!(a.children().len(), 2);
        assert_eq!(root.descendant_count(), 2);
        assert_eq!(a.descendant_count(), 2);
    }

    #[test]
    fn mutable_node_lookup_can_insert_child() {
        let mut trie = Trie::new();
        trie.insert(["branch"]);

        let branch = trie.node_for_mut(["branch"].iter()).unwrap();
        branch.insert_child("value");

        assert!(trie.node_for(["branch", "value"].iter()).is_some());
    }
}
