# Staged Composite Execution for GenericJoin

Version 0.1

Status: Proposed

## Overview

The standard query engine should continue to use the existing
`PrefixExtender` implementations for ordinary query clauses. Disjunctions do
not fit that interface: an OR branch is a conjunction whose identity must be
preserved until every constraint in that branch has run.

This design adds execution stages around the existing GenericJoin engine. An
ordinary stage extends prefixes with the current extenders. An OR stage runs
each branch as an isolated nested join, distinct-unions the completed branch
relations, and correlates that relation with the outer prefixes. NOT uses the
same nested-scope mechanism so an OR below NOT remains isolated.

The design deliberately does not adopt the `ExecPattern`, `CandidateBindings`,
`TriplePattern`, or `RelationPattern` execution path from PRs #422 and #429.
Those types replace the leaf engine as well as solving composite execution.
Issue #374 only requires a boundary around composite clauses.

```text
ParsedQuery + QueryArg values + database basis
                    |
                    v
              validate_query
                    |
                    v
          compile staged scope
       existing leaf PrefixExtenders
       + recursive OR/NOT stages
                    |
                    v
            execute scope
    BindingBag between stage boundaries
    Vec<Prefix> inside GenericJoin
                    |
                    v
       projection / aggregation / order / limit
```

## Problem

`GenericOrPrefixExtender` unions proposals and intersections one variable at a
time. Once two branches produce the same prefix, the next level cannot tell
which branch produced it. A predicate from one branch can therefore validate a
prefix produced by another branch.

Issue #374 demonstrates the problem:

```clojure
[:find ?name ?age
 :where
 [?e :age ?age]
 (or
  (and [?e :name "A"]
       [(< ?age 30)])
  (and [?e :name "B"]
       [(< ?age 40)]))
 [?e :name ?name]]
```

For an entity named `A`, the first branch must apply `< 30`; it must not borrow
`< 40` from the `B` branch. Unioning branch extensions before the predicates
run loses that relationship.

The branch-aware trie explored in PR #415 retains identity between variable
levels, but it revalidates prefixes and makes branch state part of the
`PrefixExtender` contract. The general solution is to run the conjunction in
each branch before unioning the branch results.

## Goals

- Fix branch leakage for implicit `or` at every supported nesting depth.
- Keep existing storage-backed triple, predicate, function, and input
  extenders.
- Let an OR introduce several variables in one logical stage.
- Execute an OR only after its required outer variables are bound.
- Avoid repeating nested work for duplicate correlated outer bindings.
- Preserve outer bag multiplicity while giving the realized OR relation set
  semantics.
- Land the implementation in independently testable changes.

## Non-goals

- Supporting explicit `or-join`; validation continues to reject it.
- Replacing the leaf execution interface.
- Introducing runtime-independent descriptor and assembly phases.
- Changing variable ordering or making it cost based.
- Changing projection, aggregation, ordering, or limit behavior.
- Changing the incremental query engine.
- Optimizing the intermediate OR relation before correctness is established.

## Decision

### Compiled scopes and stages

The compiler produces a recursive scope around existing extenders:

```rust
struct CompiledScope {
    input_variables: Vec<Variable>,
    variable_order: Vec<Variable>,
    extenders: Vec<Box<dyn PrefixExtender>>,
    stages: Vec<JoinStage>,
}

struct JoinStage {
    target_variables: Vec<Variable>,
    kind: JoinStageKind,
}

enum JoinStageKind {
    Generic,
    Or {
        variables: Vec<Variable>,
        branches: Vec<CompiledScope>,
    },
    Not {
        variables: Vec<Variable>,
        body: Box<CompiledScope>,
    },
}
```

The names are illustrative but describe the intended ownership boundary:

- `CompiledScope` owns the leaf extenders for one conjunction.
- `Generic` runs a consecutive range of ordinary join levels.
- `Or` owns one compiled scope per branch.
- `Not` owns one compiled scope for its body.
- `target_variables` is the checked layout after a stage.
- A stage's input layout is the preceding target layout, or the scope's
  `input_variables` for the first stage.

The root scope starts with no variables. A nested scope starts with the
correlated variables supplied by its owning OR or NOT stage. Every scope layout
is a prefix of its `variable_order` so the existing positional extenders remain
valid.

## Planning

### Groundability

Groundability is computed directly over the query AST:

| Clause | Variables it can ground |
|--------|-------------------------|
| Scalar or collection `:in` | Its declared variable. |
| Triple | Its unbound variables in scope order. |
| Function | Its output once every input is bound. |
| Predicate | None. Every referenced variable must be bound. |
| OR | All missing OR variables when every branch can derive the complete missing set. |
| NOT | None. Every body variable must be supplied by the outer scope. |

OR groundability is a fixed-point calculation per branch. Starting with the
currently bound variables, repeatedly add variables groundable by that branch's
clauses. The OR can execute only if every branch reaches the complete OR
variable set. This is equivalent to waiting until every variable that a branch
cannot produce for itself has been bound outside the OR.

Nested OR clauses participate recursively in the fixed point. NOT contributes
no variables.

### Scope order

The root retains the current query variable order. A nested scope uses:

1. correlated input variables in outer order;
2. remaining variables in outer order;
3. variables first seen only in the nested scope, in occurrence order.

This is a deterministic dependency-aware order, not a new cost-based planner.

### Stage selection

For each scope, begin with its input variables bound and repeatedly:

1. Emit validation stages for source-ordered OR or NOT clauses whose variables
   are fully bound.
2. Select the next unbound variable in the scope order.
3. If an ordinary leaf can ground it, extend the current `Generic` stage or
   create a new one.
4. Otherwise, select the first source-ordered OR for which every branch can
   derive every missing OR variable.
5. Emit one OR stage that adds the missing variables together.
6. Fail planning if neither an ordinary leaf nor an OR can make progress.

Ordinary leaves are preferred over OR materialization. When another triple can
bind the next variable, the outer GenericJoin does so and the OR runs later as
a smaller proposing stage or a validation-only stage.

Variables added by one OR stage must be the next contiguous variables in the
scope order. The current implicit-OR ordering produces this property; planning
checks it rather than relying on it silently.

Adjacent ordinary levels are coalesced into one `Generic` stage. A composite
stage remains a barrier even when it adds no variables.

## GenericJoin Changes

The prefix algorithm remains responsible for ordinary levels. It gains two
entry points in addition to the current complete `join()` operation:

```rust
fn join_range(
    &self,
    prefixes: Vec<Prefix>,
    levels: Range<usize>,
) -> Vec<Prefix>;

fn validate_range(
    &self,
    rows: Vec<Prefix>,
    levels: Range<usize>,
) -> Vec<Prefix>;
```

`join_range` runs the existing proposer selection and intersection loop over a
range, starting from supplied prefixes. Duplicate input prefixes remain
duplicate.

`validate_range` checks columns that have already been materialized by an OR.
For every row and level, it passes the prefix before that level and the row's
single candidate value to each participating extender's `intersect`. The row is
dropped when any participant rejects it. This lets an OR introduce several
variables without changing `PrefixExtender::propose` to return tuples.

The existing `join()` delegates to `join_range(vec![vec![]], 0..levels)` so the
ordinary query path keeps its behavior while stages are introduced.

## Execution

### Scope execution

A scope accepts a `BindingBag` whose variables exactly match
`input_variables`. Before extending nested input, it validates the already-bound
input levels against that scope's direct leaf extenders. This applies branch
triples and predicates even when every branch variable arrived through
correlation.

Stages then fold over the current `BindingBag`:

- A `Generic` stage passes its rows to `join_range` and wraps the returned rows
  in the target layout.
- An `Or` stage realizes its branch relation and correlates it with the current
  bag.
- A `Not` stage realizes matches from its body and antijoins them from the
  current bag.

An empty input skips storage work but still advances through the planned target
layouts, producing an empty bag with the correct final schema.

### OR execution

Given an outer input:

1. Derive the correlated variables from the OR variables and current layout.
2. Project the outer input to those variables and apply `distinct`.
3. Execute each branch scope independently with that projected bag.
4. Reorder every branch result to the OR variable order.
5. Distinct-union the completed branch results.
6. If the OR adds no variables, semijoin the original outer input with the OR
   relation.
7. Otherwise, natural-join the OR relation with the original outer input,
   reorder to the stage target, and validate the newly added levels against the
   outer scope's leaf extenders.

```text
outer BindingBag
       |
       +-- project correlated variables -- distinct -- branch 1 scope --+
       |                                                               |
       +-- project correlated variables -- distinct -- branch 2 scope --+-- distinct union
                                                                       |
                                    outer natural join / semijoin <-----+
```

Projecting before branch execution prevents unrelated outer columns from
duplicating branch work. Joining back afterward restores those columns and
their multiplicity.

The OR relation itself has set semantics. Multiple witnesses or multiple
branches that produce the same complete OR tuple expose that tuple once. The
outer `BindingBag` retains bag semantics: duplicate outer rows remain duplicate
after a matching OR.

### NOT execution

NOT follows the same correlation boundary:

1. Project the input to the NOT variables and apply `distinct`.
2. Execute the body scope with that relation as input.
3. Project matches to the NOT variables.
4. Antijoin the matches from the original outer input.

NOT is included in the staged representation so an OR nested inside a NOT body
does not fall back to the branch-leaking prefix combinator. This changes the
execution boundary, not NOT semantics.

## Invariants and Error Boundaries

- Scope input, stage target, and final layouts contain unique variables.
- Every scope input and stage target is a prefix of its scope order.
- A `Generic` stage adds at least one variable.
- An OR stage's branches expose the same ordered OR variable set.
- A proposing OR adds exactly the OR variables missing from its input.
- A validating OR and every NOT stage preserve the outer layout.
- Branch results are aligned before union.
- Layout errors and ungroundable plans return query errors rather than silently
  dropping variables.
- Runtime storage and expression failures retain the existing error behavior;
  this design does not add another error abstraction.

## Alternatives Considered

### Continue evolving `GenericOrPrefixExtender`

Tracking branch identity in prefix annotations or per-branch tries can preserve
correctness, but composite state then leaks into every variable-level call.
Later levels must retain and revisit earlier branch decisions. This is the
complexity the stage boundary is intended to remove.

### Adopt the complete engine from PR #422

That engine correctly isolates branches, but it also replaces the leaf
execution contract, triple lookup implementation, proposal representation, and
planning/assembly boundary. The OR fix does not require those changes.

### Build on PR #429

`ExecPattern`, `CandidateBindings`, and `TriplePattern` provide a result-set
execution path, but the recent candidate-selection split demonstrates how much
new leaf machinery must be stabilized before reaching OR. Keeping
`PrefixExtender` leaves makes the composite boundary independently testable.

### Materialize every OR before the outer join

An uncorrelated full relation can do unnecessary work and cannot evaluate
branches that require outer predicate inputs. Delaying the OR until its
non-groundable variables are bound retains correlation and limits the realized
relation.

## Incremental Implementation

Each phase should be independently reviewable and leave existing standard
queries working.

### Phase 1: Seeded GenericJoin

Add `join_range` and `validate_range` without changing query compilation.

Acceptance criteria:

- `join()` produces the same results through `join_range`.
- A non-empty prefix bag can continue across a selected range.
- Exact-row validation filters rejected candidates without changing layout or
  multiplicity.
- Empty prefixes and levels with no participants have defined behavior.

Verification:

```bash
cargo test -p triplox algo::generic_join
```

### Phase 2: Stage planning

Add AST groundability, nested scope ordering, and stage selection. Planning may
be tested before it drives execution.

Acceptance criteria:

- Ordinary leaves are preferred when they can ground the next variable.
- Predicate-only OR variables force the OR to wait for outer bindings.
- An OR can plan the introduction of two variables in one stage.
- Fully bound OR and NOT clauses become validation stages.
- Nested OR scopes receive only relevant correlated variables.
- Ungroundable scopes fail with the blocked variable or composite clause.

Verification:

```bash
cargo test -p triplox query::staged_join::tests::planning
```

### Phase 3: Ordinary stage execution

Execute `CompiledScope` and `Generic` stages with `BindingBag` at stage
boundaries. Keep the public query path on its existing compiler.

Acceptance criteria:

- Root and correlated scope inputs are validated against their leaf extenders.
- Consecutive stages preserve layout and row multiplicity.
- Empty bags reach the planned final layout.
- Existing leaf extenders are used unchanged.

Verification:

```bash
cargo test -p triplox query::staged_join::tests::generic
```

### Phase 4: OR execution

Add isolated branch execution, distinct union, and outer correlation.

Acceptance criteria:

- The issue #374 branch-predicate regression passes.
- Predicate-only OR filters fully bound inputs.
- One OR stage can introduce two variables.
- Overlapping branches expose a complete OR tuple once.
- Duplicate outer rows retain their multiplicity.
- Unrelated outer columns are restored after branch execution.
- Nested OR branches remain isolated.

Verification:

```bash
cargo test -p triplox query::staged_join::tests::or
```

### Phase 5: NOT execution

Move NOT to correlated body execution so nested OR never uses the old OR
prefix combinator.

Acceptance criteria:

- Existing NOT query behavior remains unchanged.
- OR inside NOT is branch-safe.
- NOT inside an OR branch uses only rows from that branch.
- Duplicate outer rows are filtered without multiplication.

Verification:

```bash
cargo test -p triplox query::staged_join::tests::not
```

### Phase 6: Standard query cutover

Route `execute_query` through the compiled root scope. Keep find projection,
aggregation, order, and limit processing unchanged.

Add JVM integration coverage for:

- issue #374;
- predicate-only OR;
- two-variable OR introduction;
- multiple matching branches;
- correlated OR with unrelated outer variables;
- nested OR and OR below NOT;
- empty relations and historical query bases.

Run the complete verification suite:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets --all-features
cargo fmt --check
git diff --check
```

### Phase 7: Cleanup

After the cutover and parity tests pass, remove production compilation through
`GenericOrPrefixExtender`, `GenericAndPrefixExtender`, and
`GenericNotPrefixExtender`. Retain unrelated GenericJoin helpers and every
ordinary leaf extender.

Do not remove the old composite path earlier: keeping it until cutover makes
the preceding phases independently testable and easy to compare.

## Consequences

- OR and NOT materialize intermediate relations at explicit composite
  boundaries.
- Ordinary conjunctions remain prefix-at-a-time and storage-backed.
- Branch execution can repeat storage scans for different correlated bindings,
  although distinct projection avoids work caused only by outer duplicates.
- `BindingBag` is used where named layouts and relational correlation matter;
  GenericJoin continues using positional byte prefixes internally.
- The implementation is smaller than #422 because it adds no alternate leaf
  contract or runtime assembly layer.
- A future explicit `or-join` feature can extend `OrStage` with declared
  `join_variables` without changing ordinary extenders.

Performance work should follow correctness and use equivalent benchmarks. The
first candidates are caching branch results by correlated key and replacing
hash-backed intermediate relations with a trie when measurements justify it.
