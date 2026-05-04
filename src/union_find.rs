/// Disjoint-set forest with path compression. Used by the upsert resolver to
/// coalesce tempids that assert the same unresolved unique-identity value.
#[derive(Debug)]
pub struct UnionFind {
    parent: Vec<usize>,
}

impl UnionFind {
    pub fn new(len: usize) -> Self {
        Self {
            parent: (0..len).collect(),
        }
    }

    pub fn find(&mut self, x: usize) -> usize {
        if self.parent[x] != x {
            self.parent[x] = self.find(self.parent[x]);
        }
        self.parent[x]
    }

    pub fn union(&mut self, a: usize, b: usize) {
        let a_root = self.find(a);
        let b_root = self.find(b);
        if a_root != b_root {
            self.parent[b_root] = a_root;
        }
    }
}
