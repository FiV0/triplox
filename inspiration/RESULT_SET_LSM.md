# Result Sets in Datatoad: Salad, FactLSM, Forest, and Layers

This document describes how datatoad represents sets of facts — both at rest
(named relations) and in flight (partial results moving through a rule's join
pipeline). It covers the full representation stack, how the structures compact,
how partial results branch into independent consumers without copying, and the
mechanics that make each of these cheap.

The stack, top to bottom:

```
Salad<T>                      in-flight facts + column names        rules/exec.rs
 └─ FactLSM<Forest<Terms>>    log-structured set of forests         facts/mod.rs
     └─ Forest<Terms>         one trie: Vec<Rc<Layer<Terms>>>       facts/trie.rs
         └─ Layer<Terms>      one column: a list of sorted lists    facts/trie.rs
             └─ Lists<C>      columnar Vecs<C, Strides>             facts/mod.rs
                 └─ Terms     Lists<Vec<u8>>: flat bytes + offsets  facts/mod.rs
```

Two design commitments drive everything below:

1. **All fact data is immutable once built.** Structures are updated by
   constructing replacements and swapping pointers, never by writing in place.
   This is what makes branching a pointer copy.
2. **All fact data is dense and positional.** Layers are packed arrays that
   address each other by position, not by pointer or row id. This is what makes
   the join/filter kernels fast — and what forces whole-layer rebuilds when a
   layer's contents change.

---

## 1. The columnar foundation: `Terms`, `Lists`, `Strides`

```rust
pub type Lists<C> = Vecs<C, Strides>;   // facts/mod.rs
pub type Terms    = Lists<Vec<u8>>;
```

A `Terms` is a sequence of byte slices stored columnar-style: one flat `Vec<u8>`
holding every value back to back, plus a `Strides` offset structure recording
where each value ends. `Lists<Terms>` adds a second level of bounds, giving a
sequence of *lists* of byte slices. There are no per-row allocations and no
pointers in the data plane; "a column of a million 4-byte keys" is one 4 MB
buffer plus offsets.

`Strides` (from the `columnar` crate) compresses the common case where every
entry has the same width into a single stored stride. Two consequences:

- `upgrade_hint` / `upgrade::<K>` (facts/mod.rs) can detect a fixed width `K`
  and recast a general `Lists<Terms>` as `Lists<Vec<[u8; K]>>` — a zero-copy
  reinterpretation. The layer kernels (`union`, `intersection`,
  `retain_items`, `retain_lists`, `sort_terms` in facts/trie.rs) dispatch on
  this and run width-specialized code (including a radix-sort path for `[u8;4]`
  via `u32_sort`).
- `advance_bounds` — the workhorse that projects a set of index ranges from one
  layer to the next — becomes two multiplications per range when strided
  (facts/trie.rs, `layers::advance_bounds`).

Everything also has a *borrowed* form (`<Lists<C> as Borrow>::Borrowed<'_>`):
a struct of slices viewing the same memory. Compute kernels take borrowed
views, so reading shared data never copies and never bumps a refcount. Rust
lifetimes confine these views to the duration of a kernel call; they are never
stored in persistent state.

## 2. `Layer`: one column of a trie

```rust
pub struct Layer<C> { pub list: Lists<C> }   // facts/trie.rs
```

A layer is a list of sorted, distinct lists. List `i` of layer `k` holds the
values that extend item `i` of layer `k-1`. That sentence is the entire
addressing scheme: **correspondence between layers is positional**. There are
no child pointers, no row ids, no tombstones. Layer `k` must have exactly as
many lists as layer `k-1` has items — checked in `Forest::push_layer` and the
`TryFrom<Vec<Rc<Layer<C>>>>` constructor.

Positional addressing is a trade:

- Reading is fast and cache-friendly: walking from a prefix to its extensions
  is `advance_bounds`, i.e. range arithmetic over offsets.
- Any change to a layer's *contents* (dropping an item, say) shifts positions,
  so the layer — and every layer leafward of it — must be rebuilt as a new
  packed array. There is no in-place editing and no view-based deletion.

## 3. `Forest`: a trie of layers

```rust
pub struct Forest<C> { layers: Vec<Rc<Layer<C>>> }   // facts/trie.rs
```

A `Forest` is a non-empty collection of facts of arity `n`, as `n` layers. The
first layer has a single list (an implicit unit root); a forest with zero
layers represents one 0-ary fact. `Forest::len()` is the item count of the last
layer.

Properties that matter downstream:

- **Prefix sharing.** Each distinct prefix appears once in its layer regardless
  of how many facts extend it. A forest over `(A, B, C)` stores each distinct
  `(a, b)` once; appending a `C` layer does not blow up the prefix storage.
- **Sorted everywhere.** Each list is sorted and distinct, so the whole forest
  enumerates facts in lexicographic order. Set operations (union, intersection,
  semijoin) are ordered merges, accelerated by galloping (`facts::gallop`).
- **`Rc` per layer.** The vector holds `Rc<Layer>` — shared-ownership pointers
  to immutable layers. `Forest::clone()` clones the `Vec`, which clones each
  `Rc`: refcount bumps, no data movement. Two forests may share any subset of
  their layers; sharing granularity is the whole layer.

Cheap structural operations: `push_layer` / `pop_layer` append or remove a
column without touching the others (used by the WCOJ count protocol to attach
and detach its metadata column, and by logic atoms to append a derived column).
`truncate` drops trailing columns.

## 4. `FactLSM`: the log-structured set

```rust
pub struct FactLSM<F> { layers: Vec<F> }   // facts/mod.rs
```

A `FactLSM<Forest<Terms>>` is a possibly-empty list of non-empty forests, each
sorted and internally distinct, logically representing their union. It is the
type of every result set: each named relation's stable data, every in-flight
salad, every join output.

### Compaction invariant

`tidy()` maintains the classic LSM shape: after sorting by size, any two
adjacent layers within a factor of two are merged, repeatedly. The result:

- at most a logarithmic number of forests;
- total stored facts at most twice the number of distinct facts (the largest
  layer dominates and the rest telescope).

`push` and `extend` re-tidy on every insertion, so the invariant always holds.
The amortized cost argument is the standard one: a fact participates in
`O(log N)` merges over its lifetime, because each merge it joins at least
doubles the size of the layer containing it. **This is the mechanism that makes
"small change, repeated forever" cheap**: new facts enter as their own small
forest next to the big ones; the big forests are not touched until geometry
says a merge is due.

Two other entry points:

- `flatten()` merges everything into a single forest and takes it (callers use
  this when a kernel needs one contiguous trie to work on — e.g. before a join
  — and push the result back afterwards).
- `tidy_through(size)` merges all layers smaller than `size`, used to bound
  tidying work by the size of incoming data (`FactSet::advance` calls
  `stable.tidy_through(2 * to_add.len())` — compaction proportional to the work
  that prompted it, never a full rebuild on a whim).

### Merging two forests

`Forest::merge` (facts/trie.rs, `impl Merge for Forest<Terms>`) unions two
tries layer by layer in one forward pass. A `VecDeque<Report>` carries
alignment between consecutive layers:

```rust
enum Report {
    This(usize, usize),   // range exclusive to self
    That(usize, usize),   // range exclusive to other
    Both(usize, usize),   // one matching index in each
}
```

Seeded with `Both(0, 0)` (the unit roots), each layer's `union` kernel consumes
the parent-level reports and emits child-level ones: ranges exclusive to one
side are **bulk-copied** with `extend_from_self` (a memcpy of a contiguous
region, including all their descendant data in later layers), and only genuinely
overlapping lists are walked value by value — with galloping to skip runs. The
last layer skips report generation entirely (`next: false`). Merging is
therefore proportional to the *overlap structure*, not strictly to total size,
and produces a fully deduplicated forest. This same merge is what `flatten` and
`tidy` invoke, so LSM compaction doubles as deduplication.

## 5. `FactSet` and `Relations`: result sets at rest

```rust
pub struct FactSet<F> {        // facts/mod.rs
    pub stable: FactLSM<F>,    // facts known in prior rounds
    pub recent: Option<F>,     // facts new this round (one forest)
    pub to_add: FactLSM<F>,    // facts arriving, not yet processed
}
```

This is the semi-naive lifecycle. `advance()` retires `recent` into `stable`,
then promotes `to_add` to the new `recent` — after `antijoin`ing it against
`stable` so `recent` contains only genuinely novel facts. Rules then join the
small `recent` delta against `stable`, instead of re-deriving everything.
Relations only accumulate; there is no retraction, so at-rest forests are never
edited — only extended by new LSM layers and periodically merged.

`Relations` (facts/mod.rs) maps names to fact sets, and additionally maintains
`Forms`: for each `Action` (a filter/projection/permutation recipe — see §8) a
separately maintained `FactSet` holding the transformed facts. These are the
"indexes" the planner requests via `ensure_action` so that joins find their
inputs already sorted in the column order a plan needs. Each form tracks the
base relation's stable/recent split, with recent deduplicated against stable.

## 6. `Salad`: result sets in flight

```rust
pub struct Salad<T> {                       // rules/exec.rs
    pub facts: FactLSM<Forest<Terms>>,      // the rows
    pub terms: Vec<T>,                      // the column names, in order
}
```

A salad is a partial result moving through a rule: the bindings of the
variables bound so far. The `terms` vector is the schema — column `i` of every
forest holds the values of variable `terms[i]`. There is **no binding
environment anywhere in the engine**: "variable `e` is bound" means "the salad
has a column named `e`". A clone of the salad therefore *is* a snapshot of all
current bindings.

Operations:

- `align_to(order)` / `prune_to(order)` permute columns to match a desired
  prefix (prune additionally drops columns not named). Both detect identity
  permutations and do nothing; otherwise they flatten and run a permutation
  `Action` through `act_on`. Column 0 doubles as the partition key in
  multi-worker runs, so permutations that change it trigger an exchange.
- `truncate(arity)` drops trailing columns (cheap: pops `Rc`s).
- `extend(forests)` pushes forests into the LSM (arity-checked).

A rule executes as a sequence of stages (`run_wco_stages`, rules/mod.rs): the
seed atom produces an initial salad; each stage's `wco_join` (rules/exec.rs)
narrows it (semijoins), extends it with newly introduced variables, and prunes
it to the columns later stages demand; finally `emit_head_facts` projects it
onto each head atom (with a zero-copy fast path when a head matches the salad's
layout exactly) and feeds the named relations.

## 7. Branching: handing one partial result to many consumers

Partial results fork in several places:

- **View disjuncts (`or` branches).** A view used inside a rule evaluates each
  defining rule against the caller's bound prefix: `View::join_seeded`
  (rules/atoms/view.rs) canonicalizes the input salad once, then derives one
  salad per disjunct, runs each disjunct's plan independently, projects each
  result back to the view's head shape, and unions them.
- **WCOJ proposer shards.** Within one stage, when several atoms could propose
  values for the new variable, `wco_join_inner` (rules/exec.rs) *partitions*
  the salad: a 4-byte metadata column (`[log1p(count), atom_index, 255, 255]`,
  the "potato") is appended via `push_layer`; each atom's `count()` stamps
  itself wherever it beats the current minimum; the salad is then split into
  one disjoint shard per atom, each extended by its atom and validated by the
  others, and the shards are concatenated back into a fresh LSM. Partition is
  stronger than sharing — each fact is processed by exactly one branch.
- **Column sequestering.** `wco_join` clones the salad and truncates the clone
  to the columns the stage's atoms reference, joining the wide original back in
  afterwards.
- **Shared inputs.** Every atom reading a relation holds `Forest` clones of the
  stored data; many rules and branches read the same forests concurrently.

All of these rely on the same two facts:

1. **Cloning is pointer-copying.** `Salad::clone` → `FactLSM::clone` →
   `Forest::clone` → `Vec<Rc<Layer>>::clone`. The fork itself costs a few
   refcount bumps per layer, independent of data size. (`act_on` short-circuits
   identity actions to `self.clone().into()` — facts/trie.rs.)
2. **Nobody can write through a shared pointer.** Rust's `Rc` statically
   forbids mutable access while the refcount exceeds one. Every kernel either
   builds entirely new layers, or uses `Rc::make_mut` — clone-on-write,
   mutating in place only when provably the sole owner (the potato-column
   updates in rules/atoms/data.rs and rules/atoms/logic.rs are the notable
   users; the layer is freshly built there, so the fast path applies).

So after a fork, branches hold private pointer-vectors aimed at common layers:

```
branch1.layers: [ →A, →B ]      branch2.layers: [ →A, →B ]     A.rc=2, B.rc=2
```

A branch "modifies" the result set by building replacement layers and swapping
its own pointers:

```
branch1.layers: [ →A, →B' ]     branch2.layers: [ →A, →B ]     A.rc=2, B.rc=1, B'.rc=1
```

Branch 2 observes nothing; layer `B` lives until its last holder drops. There
is no notion of a branch holding a *view over a larger result set* — a layer is
either shared whole (the identical heap object) or replaced whole (a dense
private copy of the survivors). Isolation between branches is not an added
mechanism; it is the absence of any shared mutable state to leak through.
Branches reconverge only by set union of complete, identically-shaped rows
(`FactLSM::extend` + merge), which cannot re-associate a value from one branch
with a prefix from another.

## 8. Filtering: masks, `retain_items`, `retain_lists`

Filtering is where a branch diverges from shared layers. Every filter — a
semijoin against another trie, a literal constraint, a logic predicate —
reduces to the same shape: *compute a survival mask at one layer (reading
shared data), then rebuild the affected layers*.

**Computing the mask.** A semijoin (`Forest::retain_join` / `retain_inner`,
facts/trie.rs) intersects the two tries layer by layer: `intersection` is an
ordered merge with galloping over borrowed views, producing matched index pairs
that seed the next layer's intersection. The result is `include`, one bool per
item at the constrained layer. Literal/equality filters (`Forest::filter`,
driven by an `Action`'s `lit_filter`/`var_filter`) instead carry an `active`
index list shrunk by `filter_items`. Logic atoms produce a keep-mask from
per-row `count()` results. All converge on `retain_core`.

**Applying the mask** (`retain_core`, facts/trie.rs):

- Fast paths first: mask all-true → return `self` unchanged (pure `Rc`
  clones); all-false → empty LSM.
- **Leafward** of the constrained layer: the mask's true-runs become
  `(lower, upper)` list-index ranges; `retain_lists` builds a new packed layer
  by bulk `extend_from_self` over each surviving range, rewriting the range
  buffer in place into item ranges for the next layer down. The same small
  buffer cascades to the leaves.
- **Rootward**, walking in reverse: `retain_items` builds a new layer keeping
  flagged items and *refills the mask* with one bool per list — "did this list
  keep anything?" — which is exactly the per-item mask for the parent layer.
  Survival propagates up: a prefix dies only when all its extensions died. The
  loop exits early the moment the propagated mask is all-true; every layer
  above that point stays shared.

The post-filter forest is thus a **shared rootward prefix plus a private
leafward suffix**, with the boundary as high as the data allows. The `Action`
pipeline in `act_on` composes this with reordering: filter, then `permute`
(distinct columns into output order), then `embellish` (re-insert literal and
repeated columns).

Cost shape: mask computation is a gallop-accelerated merge over the layers it
reads; rebuilding is bulk copies proportional to surviving data in the rebuilt
layers. (TODOs in facts/trie.rs note the retain kernels should become
output-linear rather than input-linear.)

## 9. Extension: joins that build new tries

Extending `(A, B)` with `C` produces new layers — including rebuilt prefix
layers, since the join restricts prefixes to those with matches. `join_cols`
(facts/trie.rs) implements the n-way variant behind `join_many`:

1. Align the first `arity` columns of `self` against each other trie by
   layer-wise `intersection`.
2. Compute, per aligned prefix, the output cardinality (`advance_bounds` on
   both sides), giving a `counts` vector.
3. Peel off aligned prefixes in chunks whose output size stays under `thresh`
   (the per-worker memory budget, `Comms::thresh` = budget / peers). Each chunk
   produces its output layers via `sort_terms` / `expand` and is pushed into an
   output `FactLSM` and a `Conduit` — so deduplication (LSM merge) and, in
   multi-worker runs, exchange happen incrementally rather than after one giant
   materialization. Peak memory is bounded by `thresh`, not by output size.

`sort_terms` is the kernel that mints each output layer: given (group, value)
pairs it sorts, deduplicates within groups, and seals list boundaries at group
changes — with the fixed-width upgrade dispatching to radix-sort
implementations (`u32_sort`, paged LSB sort in `facts::radix_sort`) for narrow
columns. The same machinery serves column permutation (`permute_subset`, which
also has a zero-reshuffle fast path for an identity prefix of the projection,
and a `thresh`-chunked trampoline for oversized expansions).

The trie keeps extension affordable: rebuilding the `(A, B)` prefix layers
costs per *distinct surviving prefix*, not per output row, and appending a
column to an otherwise-unchanged forest is just `push_layer`.

## 10. Cost model summary

| Operation | Cost | Why |
|---|---|---|
| Fork a salad to a branch | O(#layers) refcount bumps | `Vec<Rc<Layer>>` clone |
| Read shared data in a kernel | zero-copy | borrowed views |
| Filter (semijoin/literal) | merge over read layers + bulk copy of survivors in rebuilt layers | mask + `retain_*`; all-true masks and rootward layers above the change stay shared |
| Extend with a column (join) | per distinct surviving prefix + per output row, in `thresh`-bounded chunks | `join_cols` staging |
| Insert new facts into a relation | amortized O(log N) merges per fact | LSM geometry; big layers untouched until geometry triggers |
| One round of a recursive rule | proportional to the delta, not the relation | semi-naive: `recent` vs `stable` |
| Union of branch results | ordered merge, bulk-copying non-overlap | `Forest::merge` with `Report`s + galloping |
| Point-edit a large forest | O(data leafward of the change) — deliberately unsupported in place | positional layers force whole-layer rebuilds |

The last row is the trade-off accepted knowingly: dense positional layers give
up cheap point updates to make scans, joins, and merges run over contiguous
memory. The structures above it are how the engine ensures the expensive case
rarely arises — changes enter as new small LSM layers (never edits), rule
evaluation processes small deltas (semi-naive), in-flight filtering is batched
(one mask per stage, not per tuple), and branching shares everything a branch
doesn't actually change.

---

*Code references: `src/facts/mod.rs` (FactLSM, FactSet, Relations, Lists/Terms,
radix sorts, gallop), `src/facts/trie.rs` (Forest, Layer, merge/union,
retain/permute/join kernels), `src/rules/exec.rs` (Salad, wco_join),
`src/rules/mod.rs` (stage execution, head emission), `src/rules/atoms/`
(data/logic/view atoms), `src/comms.rs` (thresh, Conduit, exchange/broadcast).*
