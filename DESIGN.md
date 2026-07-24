# Standard Query Engine Rewrite

Status: proposed

## Context

[Issue 374](https://github.com/FiV0/triplox/issues/374) demonstrates that the
current prefix-at-a-time `GenericJoin` loses OR branch identity. A value proposed
by one branch can be validated by a predicate from another branch because
`GenericOrPrefixExtender` unions each level independently.

[Issue 414](https://github.com/FiV0/triplox/issues/414) proposes moving from
single prefixes to result sets. Hooray2 implemented that direction in
[PR 40](https://github.com/FiV0/hooray2/pull/40), with a row-oriented
`BindingSet`, logical planning stages, executable patterns, and a staged
`GenericJoinEngine`.

Triplox should adopt that final shape rather than porting either issue 414 or
the Hooray2 implementation literally. Triplox has different storage access,
query types, error handling, and no Clojure/Kotlin boundary, but the semantic
split applies:

```text
ParsedQuery + arguments
        |
        v
runtime-independent descriptors
        |
        v
logical plans and stages
        |
        v
runtime pattern assembly
        |
        v
GenericJoinEngine(BindingSet)
        |
        v
find/aggregate/order/limit
```

The old `PrefixExtender` stack remains the active standard engine until the
last integration stage. The preparatory changes are added alongside it and are
independently testable.

## Goals

- Fix OR branch leakage by keeping each branch's rows correlated throughout
  branch execution.
- Replace prefix-at-a-time execution with named, ordered `BindingSet` batches.
- Retain worst-case optimal proposer selection where multiple patterns can
  introduce the next variable.
- Separate query description, logical planning, runtime pattern construction,
  and stage execution.
- Make every migration stage reviewable and mergeable while the existing
  standard engine continues to work.
- Preserve the current public query result, aggregation, ordering, limit,
  temporal, and error semantics unless this document explicitly says
  otherwise.

## Non-goals

- The incremental query engine is unchanged.
- This does not enable tuple or relation `:in` bindings. `RelationPattern` is
  required internally for existing scalar/collection inputs and nested
  correlation, but removing the current public validation error for
  `BindTuple` or `BindRel` is separate work.
- This does not replace `BindingSet` rows with the trie-backed result set from
  issue 373. The execution contracts must allow that optimization later.
- This does not introduce a cost-based global optimizer. Existing query
  variable order remains the deterministic tie-breaker.
- This does not remove the separate `join` or `leapfrog` modules merely because
  they are adjacent to `generic_join`.
- This does not change the client protocol or public `QueryResult` type.

## Decisions

### `BindingSet` is the execution value

A binding set is a relation with an ordered variable layout:

```rust
pub(crate) type BindingRow = Vec<Bytes>;

pub(crate) struct BindingSet {
    variables: Vec<Variable>,
    rows: Vec<BindingRow>,
    column_indexes: HashMap<Variable, usize>,
}
```

Rows continue to contain encoded `Bytes`. This avoids decoding values that are
only moved between indices and joins. Predicate and function patterns decode
the columns they evaluate, and final projection continues to decode public
results into `DataType`.

Column order is part of the contract. Patterns address columns by `Variable`,
not by assuming that every input is a prefix in global query order. This lets a
stage preserve unrelated outer columns without projecting and reattaching them.

`BindingSet` has bag semantics. Operations preserve row multiplicity unless the
operation name explicitly says otherwise. It provides:

- checked construction and variable-to-column lookup;
- `unit()`, containing zero variables and one empty row;
- `empty(variables)`, containing a layout and no rows;
- row selection and extension;
- projection and layout-only reorder;
- row indexing by a variable subset;
- natural join;
- existential semijoin and antijoin;
- union and distinct union;
- explicit complete-row deduplication.

Natural join keeps the left layout followed by right-only variables. Semijoin
and antijoin never multiply left rows. Zero-column relations follow relational
identity rules: the unit relation joins as identity, an empty zero-column
relation annihilates a join, and semijoin/antijoin without shared variables
depend only on whether the right side is empty.

`Bytes` clones are cheap, but row vectors and hash keys are not. The first
version favors a small, verifiable representation. Builders, borrowed row
views, columnar storage, or a trie-backed implementation can replace internals
after profiling without changing the planner or pattern contracts.

### Proposal metadata is a sidecar

Issue 414 describes storing proposal metadata in the result set. Triplox will
instead use a temporary sidecar aligned with the input rows:

```rust
pub(crate) type PatternId = usize;

pub(crate) struct Proposal {
    proposer: Option<PatternId>,
    count: usize,
}
```

Each pattern updates a row only when it has a strictly cheaper, positive count.
Equal counts preserve participant order. A row with no positive proposer is
dropped. Keeping this metadata out of `BindingSet` keeps ordinary relational
operations independent of engine bookkeeping. A None proposer means that the row
was not extended by any proposer.

### `ExecPattern` operates on whole binding sets

The runtime pattern contract is:

```rust
pub(crate) trait ExecPattern: Send + Sync {
    fn id(&self) -> PatternId;
    fn variables(&self) -> &[Variable];

    fn count(
        &self,
        input: &BindingSet,
        added: &[Variable],
        proposals: &mut [Proposal],
    ) -> anyhow::Result<()>;

    fn join(
        &self,
        input: &BindingSet,
        added: &[Variable],
        target_variables: &[Variable],
    ) -> anyhow::Result<BindingSet>;
}
```

`count` is a no-op when the pattern cannot propose the complete `added` list.
`join` has two modes:

- non-empty `added` extends the input and returns exactly `target_variables`;
- empty `added` validates or filters rows without changing the input layout.

There is no `seed` method. Execution starts with `BindingSet::unit()`, and the
first proposing stage produces the initial variables. This keeps one execution
path for seeded, externally bound, and nested queries.

Storage, codec, expression, and layout errors are returned rather than turned
into panics. Invalid planner/engine contracts are also reported at their
boundary with the pattern or stage identity included.

### A stage is one layout transition

A runtime stage contains:

```rust
pub(crate) struct Stage {
    added: Vec<Variable>,
    participants: Vec<Arc<dyn ExecPattern>>,
    target_variables: Vec<Variable>,
}
```

Its invariants are:

- `added` and `target_variables` contain no duplicates;
- no `added` variable is present in the input;
- the target variable set is exactly the input variable set plus `added`;
- participants are non-empty and have distinct IDs;
- a proposer returns the exact target layout;
- a validator preserves the layout it receives.

An empty `added` list is a first-class validation-only stage. It is required
for constant-only patterns, fully bound predicates/functions, OR/NOT filters,
and constraints that become runnable only after the final variable was added.

### Planning remains runtime-independent

Planning uses owned Rust data. It does not construct SlateDB iterators,
`ExecPattern` trait objects, or stages containing runtime patterns.

Descriptors preserve the recursive query structure:

```text
Triple
Relation
Predicate
Function
Or { branches: Vec<Vec<Descriptor>> }
Not { children: Vec<Descriptor> }
```

Every descriptor has a stable `PatternId`, ordered variables, and a
`groundable(bound)` operation. A complete logical plan contains descriptor
data, recursively planned nested scopes, and logical stages whose participants
are IDs. Each scope also records its ordered incoming variables:

```rust
pub(crate) struct LogicalScope {
    incoming_variables: Option<Vec<Variable>>,
    stages: Vec<LogicalStage>,
}

pub(crate) struct LogicalStage {
    added: Vec<Variable>,
    proposers: Vec<ParticipantRef>,
    participants: Vec<ParticipantRef>,
    target_variables: Vec<Variable>,
}

pub(crate) enum ParticipantRef {
    Pattern(PatternId),
    Incoming,
}
```

`incoming_variables` is `None` for the top-level scope and `Some` for an OR
branch or NOT body, including when the projected variable list is empty.
`ParticipantRef::Incoming` refers to that scope's single projected outer
relation. The planner treats it like a relation descriptor with
relation-prefix groundability and may place it in several stages. Its presence
in `proposers` records a proposing role; presence only in `participants`
records a validating role. The separate variant avoids assigning a magic
`PatternId` to a planning-only participant.

Runtime assembly converts a logical stage into a `StageTemplate`:

```rust
pub(crate) enum ParticipantTemplate {
    Pattern(Arc<dyn ExecPattern>),
    Incoming,
}

pub(crate) struct StageTemplate {
    added: Vec<Variable>,
    participants: Vec<ParticipantTemplate>,
    target_variables: Vec<Variable>,
}
```

`ParticipantTemplate::Incoming` is a deferred slot, not an executable runtime
participant. Assembly cannot construct its `RelationPattern` because the outer
rows do not exist until the owning OR or NOT pattern is invoked. A top-level
template cannot contain this slot and is immediately converted to a concrete
`Stage`.

For each nested invocation, the owning OR or NOT pattern:

1. projects the current outer `BindingSet` to the scope's
   `incoming_variables`;
2. creates one `Arc<RelationPattern>` from that projection;
3. clones that arc into every `ParticipantTemplate::Incoming` slot, preserving
   participant order;
4. converts every `StageTemplate` into a concrete `Stage` and validates that
   its participants are non-empty and have distinct IDs;
5. passes only those concrete stages to `GenericJoinEngine`.

The reusable templates remain on the composite pattern; the concrete stages
exist only for that invocation. `GenericJoinEngine` therefore never observes
an `Incoming` variant. The incoming relation can use the owning composite
pattern's ID because that ID does not occur among its descendant patterns.

Planner tests compare ordinary descriptors and logical plans. They do not open
a database or construct fake executable patterns.

### Groundability belongs to the planner

Groundability answers only what variables a descriptor can derive from the
currently bound set:

| Descriptor | Groundability |
|---|---|
| Triple | Its unbound entity/value variables. The planner normally selects the next one by variable order. |
| Relation | The next unbound relation variable, only when already bound relation variables form a prefix of the relation layout. |
| Predicate | None. All referenced variables must be bound before validation. |
| Function | Its output, only when all input variables are bound and the output is not. |
| OR | All missing OR variables, but only when every branch can derive the complete missing set. Otherwise none. |
| NOT | None. All correlated variables must be supplied by the outer scope. |

OR branch groundability is computed to a fixed point within each branch before
the branch results are compared. It is all-or-nothing: an OR must not advertise
a partial set merely because different branches can derive different subsets.

The planner owns stage composition, proposer selection, validators, and
participant placement. Those concerns do not belong in `ExecPattern`.

### Planner algorithm

Each top-level query, OR branch, and NOT body is a conjunctive planning scope.
Conjunction is represented by a scope's stages; there is no `AndPattern`.

For a scope:

1. Move relevant incoming variables to the front of the scope variable order.
   Incoming variables not mentioned in the scope are excluded.
2. Start with no bound variables and no completed descriptors.
3. Emit any validation-only stage whose remaining descriptors are already
   fully bound.
4. Visit remaining variables in query order and choose the first variable for
   which at least one unfinished non-OR descriptor can propose.
5. Add every eligible proposer and every unfinished descriptor that becomes
   fully validatable in the target layout as participants.
6. If no ordinary descriptor can propose the next variable, select the first
   OR that can derive its complete missing set. It adds that set as the sole
   participant and proposer.
7. Recompute immediate groundability after each stage. Query order is a
   tie-breaker, not a requirement that overrides data dependencies.
8. After all variables are bound, emit a final validation-only stage for any
   now-runnable descriptors.
9. Fail planning if a variable cannot be grounded or a descriptor is never
   placed.

An OR or NOT descriptor participates in exactly one outer stage. A leaf pattern
may participate in several stages as its variables are introduced one at a
time.

Nested scopes are planned using the outer layout known at the composite
descriptor's stage. When an OR proposes, its incoming layout is the outer input
before the grouped variables are added. When OR or NOT validates, its incoming
layout is the fully bound stage input. Only variables present in both that
layout and the nested scope are passed as `Incoming`.

### Runtime pattern semantics

| Pattern | Execution |
|---|---|
| `RelationPattern` | Wraps a materialized `BindingSet` and indexes distinct relation rows in relation-variable order. It proposes only the next relation-prefix variables and validates prefixes existentially. Input columns can be reordered or interleaved with unrelated variables. |
| `TriplePattern` | Uses the current SlateDB indices and `as_of` filtering. It proposes entity/value variables through the appropriate AE, AV, AEV, or AVE access path and validates bound shapes existentially. The attribute remains a fixed ident or entid, and the current rejection of entity/value placeholders remains unchanged. Constant-only and partially bound variable patterns validate correctly. |
| `PredicatePattern` | Never proposes. Once all expression variables are bound, it decodes those columns, evaluates the existing `Expr`, and filters rows. |
| `FunctionPattern` | Proposes only its output after all expression inputs are bound. With the output already bound, it evaluates and compares instead. |
| `OrPattern` | Requires the same variable set in every branch, while allowing different internal introduction orders. It executes every branch as an independent nested plan, aligns branch layouts, distinct-unions the branch relations, and correlates that relation back to the complete outer input. It can propose all missing OR variables only as a stage's sole participant; otherwise it validates with a semijoin. |
| `NotPattern` | Never proposes. It executes its nested plan with the projected outer bindings and antijoins the matches from the complete outer input. |

`RelationPattern` uses the existing internal trie where it helps prefix lookup,
but this is an implementation detail. The relation wrapped by a pattern has set
semantics even though the surrounding `BindingSet` retains bag multiplicity.

`TriplePattern` should share the existing SlateDB iterator, temporal filtering,
codec, and range-statistics infrastructure. The new implementation can live
beside `GenericPrefixExtender` during migration; it must not route through the
old `PrefixExtender` API.

### OR branch isolation

For issue 374, the two branches are evaluated independently:

```text
outer rows projected to OR variables
        |                         |
        v                         v
branch A stages              branch B stages
        |                         |
        +---- distinct union -----+
                     |
                     v
          join/semijoin with outer rows
```

The predicate from branch A can only filter rows produced inside branch A, and
the predicate from branch B can only filter rows produced inside branch B.
The union happens after each branch has completed. Unrelated outer columns and
their multiplicity are restored by joining or semijoining the union with the
original input.

This deliberately realizes the OR branch result for the current correlated
input. It does not realize an uncorrelated global OR relation unless the OR has
no incoming bindings. Correct branch identity is more important than retaining
single-variable laziness across a disjunction.

### Engine execution

`GenericJoinEngine` folds stages over an input binding set.

For a validation-only stage, it calls every participant with `added = []`,
checks that each result preserves its input layout, and finally applies the
stage reorder.

For a proposing stage with one participant, it calls `join` directly. This is
the path used by grouped OR proposals and avoids a meaningless OR count.

For a proposing stage with several participants:

1. Initialize one empty proposal per input row.
2. Let each participant update proposal counts.
3. Drop rows without a positive proposer.
4. Group row indexes by winning pattern ID.
5. Let each winner extend its selected input shard.
6. Validate the extended shard with every other participant using
   `added = []`.
7. Union the validated shards in target layout without deduplicating them.

The winning proposer is selected per input row, not once per stage. This keeps
the useful WCO behavior of the current engine while allowing a pattern to add
more than one variable when the logical plan requires it.

### Assembly and query integration

Runtime assembly is a separate pass over the complete logical plan. It:

- resolves fixed attributes through `IdentMap`;
- constructs Slate-backed triple patterns with the current database basis;
- converts input arguments into encoded relation patterns;
- constructs expression patterns from the existing `Expr` conversion;
- recursively assembles OR and NOT branch plan templates;
- replaces `ParticipantRef::Pattern` IDs with shared
  `Arc<dyn ExecPattern>` values;
- converts `ParticipantRef::Incoming` to a `ParticipantTemplate::Incoming`
  slot to be materialized by its owning composite pattern.

`execute_query` then becomes orchestration:

1. validate the query and arguments;
2. derive the existing query variable order;
3. build descriptors and a complete logical plan;
4. assemble the executable plan for the requested database basis;
5. execute it from `BindingSet::unit()`;
6. pass the final rows to the existing find, aggregation, order, and limit
   code.

The final binding layout must equal the planned query variable order. The
existing post-processing code can initially consume `binding_set.rows`
directly; it should not be redesigned as part of this migration.

## Source layout

The new code should be isolated under the existing query module:

```text
src/query.rs
src/query/standard/
  mod.rs
  binding_set.rs
  exec_pattern.rs
  stage.rs
  plan.rs
  engine.rs
  patterns/
    mod.rs
    relation.rs
    triple.rs
    predicate.rs
    function.rs
    or.rs
    not.rs
```

Names can be adjusted if Rust ownership makes a different split clearer, but
the boundaries above must remain visible. In particular, planner tests must
not depend on runtime pattern modules, and `engine.rs` must not contain query
AST lowering.

## Migration stages

### Stage 1: `BindingSet`

Land the relation value and its tests without changing query execution.

Scope:

- checked layouts, row arity, and column lookup;
- unit and empty relations;
- select, extend, project, reorder, and distinct;
- row indexing;
- natural join, semijoin, antijoin, union, and distinct union;
- explicit bag and zero-column behavior.

Acceptance:

- tests cover invalid layouts and every relational multiplicity boundary;
- the current standard engine remains the only production caller;
- no `PrefixExtender` code moves or changes.

This should be one commit and one PR.

### Stage 2: execution contracts and leaf patterns

Add `Proposal`, `ExecPattern`, validated runtime `Stage`, and the leaf runtime
patterns without routing queries through them.

Recommended review slices:

1. contracts, stage invariants, and `RelationPattern`;
2. Slate-backed `TriplePattern`;
3. `PredicatePattern` and `FunctionPattern`.

Each slice is directly unit tested with `BindingSet` inputs. Triple tests use
the in-memory SlateDB setup and cover count, proposal, full validation, partial
existential validation, constants, rejected unsupported shapes, temporal
visibility, and layout independence. Expression tests cover both proposal and
already-bound validation.

OR and NOT are deferred because they are consumers of nested stage execution,
not independent leaf patterns.

### Stage 3: pure logical planner

Add descriptor construction, recursive logical plans, groundability, and
stage selection. Do not construct `ExecPattern`, `Stage`, SlateDB iterators, or
the engine.

Required planner cases:

- descriptor structure for every supported clause and input;
- relation-prefix groundability;
- function output dependency;
- fixed-point branch groundability;
- all-or-nothing OR groundability;
- multiple proposers and fully bound validators;
- validation-only final stages;
- grouped OR as sole proposer;
- OR validation only after all OR variables are bound;
- relevant incoming-variable projection and explicit `Incoming`
  participation;
- NOT requiring outer bindings;
- insufficient-binding and unplaced-pattern failures;
- deterministic query-order tie-breaking.

The expected plans should be asserted as plain Rust values. This stage is the
review point for semantics before runtime assembly exists.

### Stage 4: engine and composite patterns

Add `GenericJoinEngine`, `OrPattern`, and `NotPattern`, still without replacing
the active query path.

Engine tests use small test patterns to prove:

- sequential layout transitions;
- per-row cheapest-proposer selection and stable tie-breaking;
- row sharding by proposer;
- validators receiving an empty `added` list;
- validation-only stages;
- rows with no proposal being dropped;
- rejection of unknown proposer IDs, wrong proposal lengths, and layout
  changes.

Composite tests prove:

- OR branches with different internal variable orders align correctly;
- OR distinct-unions branch results;
- proposal and validation preserve unrelated outer columns and multiplicity;
- branch predicates cannot leak across branches;
- NOT uses antijoin and preserves unrelated columns;
- incoming relation slots are all materialized and no stage is left without a
  participant.

### Stage 5: assembly, cutover, and removal

Add runtime assembly, route `execute_query` through the new plan and engine,
and only then remove the old implementation.

The cutover must add the issue 374 query as a public standard-query regression
in `src/node.rs`. Existing standard query tests for triples, joins, predicates,
functions, OR, nested OR, NOT, scalar inputs, collection inputs, aggregation,
ordering, limits, and time travel remain the parity suite. Incremental
equivalence tests continue to compare against the standard engine but are not
otherwise changed.

Once parity passes, remove:

- `src/algo/generic_join.rs`;
- the `generic_*_prefix_extender.rs` modules and their exports;
- old extender compilation in `src/query.rs`;
- tests that only exercise the removed prefix API.

Do not remove unrelated join implementations or broaden input-binding support
in this PR.

## Verification

Every stage first runs its focused tests. Before a stage is considered ready,
run:

```bash
cargo fmt --check
cargo test --workspace
cargo clippy --workspace --all-targets --all-features
```

The final cutover additionally needs:

- the exact issue 374 regression;
- nested OR with branch-local predicates;
- OR that proposes all of its variables;
- OR correlated with already-bound outer variables;
- NOT correlated with a wider outer binding set;
- empty database, empty input collection, and empty branch result cases;
- duplicate witnesses and duplicate OR branch results;
- queries at a historical `as_of` basis.

## Risks

### Row materialization and copying

`BindingSet` materializes intermediate rows, while the current engine extends
one prefix at a time. `Bytes` limits value-copy cost, but row allocation and
hashing can still regress large joins. Keep the public execution contracts
representation-independent, benchmark after correctness, and optimize builders
or storage without changing planner semantics.

### Repeated SlateDB lookups

Naively running a triple lookup per input row can repeat identical seeks.
`TriplePattern` may group rows by the bound lookup key and reuse counts or
extensions, provided it preserves the input-row mapping and multiplicity.
Caching is an implementation optimization, not part of planning.

### OR intermediate size

Branch isolation necessarily retains branch results until the branch is
complete. Project incoming bindings to only OR variables, deduplicate the
branch union before correlating it with the wider input, and avoid carrying
unrelated outer columns through nested execution.

### Planner/runtime drift

The planner says which patterns may propose and where every pattern validates;
the engine enforces layouts and participant IDs. Keep runtime checks even after
planner tests pass so a later planner change fails at the boundary instead of
silently corrupting rows.

### Large final cutover

The final PR touches routing and deletes the old engine. All new components must
already be merged and directly tested, leaving that PR responsible only for
assembly, parity fixes, the production switch, and removal.

## Completion criteria

The rewrite is complete when:

- issue 374 passes through the public standard query API;
- the standard query parity suite passes through `GenericJoinEngine`;
- scalar and collection inputs behave as before;
- tuple and relation inputs remain explicitly unsupported;
- the old `PrefixExtender` standard engine and its composite extenders are
  removed;
- the incremental engine and public protocol are unchanged;
- workspace tests and clippy pass without warnings.
