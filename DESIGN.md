# Incoming Relations in Incremental Query Plans

Status: proposed

## Context

The incremental planner in `src/inc_query.rs` currently lowers every clause to
an independently producing `RelPlan`. `RelPlanKind::Join` then combines those
child relations, and `src/incremental/circuit.rs::rel_stream` recursively builds
each child from the global encoded-triple stream before joining the results.

That works while every supported clause can produce all of its values:

- A triple pattern produces a relation from the encoded-triple stream.
- An `or` produces each branch independently and unions the branch relations.
- An `and` is represented by independently producing inputs followed by joins.

It does not provide a stream for an operator that can only consume existing
bindings. This is required by:

- `not`, whose body must be evaluated against bindings from the positive
  relation;
- predicates, which filter an existing relation and cannot produce rows;
- functions, which consume bound arguments and either append or validate a
  result binding.

[Triplox PR #412](https://github.com/FiV0/triplox/pull/412) adds `not` by
building the negative relation independently. That is sufficient for the
triple-only negative bodies in that PR, but it leaves no way to evaluate a
predicate or function inside the negative scope.

[Hooray2 PR #33](https://github.com/FiV0/hooray2/pull/33) introduced
groundability metadata before changing planner behavior.
[Hooray2 PR #34](https://github.com/FiV0/hooray2/pull/34) then changed DBSP
planning and assembly so a plan node can extend an incoming relation. The final
form is in
`/home/finn/src/github.com/FiV0/hooray2/src/main/clojure/hooray/dbsp.clj`.

## Goals

- Separate query-clause description from physical relation planning.
- Record which variables a clause can ground itself.
- Choose a valid deterministic left-deep order from those capabilities.
- Represent the exact incoming and outgoing row layouts of every physical plan
  node.
- Pass the running DBSP row stream into nested operators during circuit
  assembly.
- Preserve query results, find-variable order, deterministic row layouts, and
  DBSP set semantics.
- Establish the planner and assembly contracts required by future `not`,
  predicate, and function support.

## Non-goals

- Add `not`, predicate, or function plan kinds.
- Add or change DBSP operators. `Chain` is only a physical plan shape; circuits
  continue to use the existing pattern `flat_map`, join, projection, sum, and
  distinct operators.
- Merge or reproduce the implementation from Triplox PR #412.
- Change shared query validation or expand the supported incremental query
  syntax.
- Change the subscription API, CDC path, or query lifecycle.
- Optimize join cost beyond the deterministic heuristic described below.

## Terminology

The two inputs involved in circuit construction must remain distinct:

- The **fact input** is the global
  `Stream<RootCircuit, OrdZSet<EncodedTriple>>`. Every triple pattern reads from
  this stream.
- The **incoming relation** is an optional
  `Stream<RootCircuit, RowZSet>` produced by the preceding node in the current
  query scope. Its row layout is a vector of query variables.

An incoming relation is an edge in the planned dataflow. It is not another fact
input and should not be represented as a fake triple-pattern leaf.

## Planner phases

### Phase 0: validation

Keep the existing validation boundary:

1. Reject incremental query shapes that are not supported yet.
2. Run shared `validate_query`.
3. Extract relational find variables.

Shared validation owns language-wide rules. The incremental planner should not
duplicate those checks, but it must still reject an internally unplannable
descriptor order as a planner invariant.

### Phase 1: semantic descriptors

Normalize supported `WhereClause` values into schema-resolved descriptors
without choosing a DBSP circuit shape.

A descriptor records:

- its semantic kind;
- its referenced variables in stable encounter order;
- its groundable variables in stable order;
- kind-specific data, such as the resolved attribute and pattern slots.

An `or` branch is a nested scope. An `and` branch is therefore represented by a
scope containing multiple descriptors rather than by a physical join.

The initial descriptor model only needs triple patterns and `or`. Future clause
kinds extend this model without changing the phase boundary.

Groundability is defined as follows:

| Descriptor | Referenced variables | Groundable variables |
| --- | --- | --- |
| Triple pattern | Variable entity/value slots | All referenced variables |
| Scope / `and` | Ordered union of child variables | Ordered union of child groundable variables |
| `or` | Canonical order from the first branch | Intersection of branch groundable variables |
| Future `not` | Variables mentioned by its body | None |
| Future predicate | Variables read by the expression | None |
| Future function | Input variables plus result variable | The result variable, unless it is also an input |

Set operations must preserve a canonical variable order. Hash-set iteration
must not determine row layouts or plan snapshots.

The distinction between `variables` and `groundable` expresses the binding
requirement:

```text
required(node) = variables(node) - groundable(node)
```

A node can be introduced only when every required variable is already grounded
by the incoming relation or by an earlier node in the same scope.

This phase should be testable without constructing a `RelPlan` or DBSP stream.

### Phase 2: physical relation planning

Lower descriptor scopes into a left-deep physical plan.

Every `RelPlan` records:

- `incoming_vars: Option<Vec<Variable>>`;
- `output_vars: Vec<Variable>`;
- a physical kind.

`None` means that the node produces the first relation in its scope.
`Some(vars)` means that the node consumes a running relation with exactly that
layout. An option is preferable to treating an empty vector as “no input”:
absence of a relation and a valid zero-column relation are different concepts.

The physical kinds needed by this change are:

- `Pattern`: produces a pattern relation, then joins it with the incoming
  relation when one is present;
- `Chain`: threads a running relation through its children;
- `Union`: assembles every branch against the same incoming relation and unions
  the resulting streams.

`RelPlanKind::Join` should become `Chain`. A chain describes dataflow order; it
does not contain a collection of independently producing inputs. The current
`JoinStep` metadata remains useful on a pattern that extends an incoming
relation:

- left layout: the node's `incoming_vars`;
- right layout: the triple pattern's variable layout;
- key variables: their intersection in left-layout order;
- output layout: left variables followed by newly grounded right variables.

An empty key remains a Cartesian product.

The current `PatternPlan::output_vars` must not be overloaded with both the
right-side pattern layout and the full result layout. Keep the pattern's
intrinsic layout as `pattern_vars` (or on its descriptor), and use
`RelPlan::output_vars` for the complete result after applying the incoming
relation.

#### Left-deep ordering

Plan each scope with an initial grounded set:

- empty at the top level;
- the incoming layout's variables for a nested scope.

Repeatedly:

1. Compute `required(node)` for every remaining descriptor.
2. Keep descriptors whose required variables are a subset of the grounded set.
3. Prefer the candidate sharing the most variables with the grounded set.
4. Break ties by the descriptor's order in its scope.
5. Append the selected descriptor and add all of its referenced variables to
   the grounded set.

If no descriptor can be introduced, planning fails with an insufficient-binding
error naming one missing variable.

The first node in a top-level scope has no incoming relation. Every later node
receives the preceding node's output layout. In a nested scope, the first node
also receives the enclosing incoming layout.

#### Layout rules

- A standalone pattern uses its pattern variable order.
- A node with incoming variables retains that order and appends only newly
  grounded variables in the descriptor's canonical order.
- A chain's output is its last child's output.
- A standalone union uses its canonical branch variable order.
- A union with an incoming relation retains the incoming layout and appends
  variables grounded by the union.
- Union branches may produce different natural column orders. Each branch is
  projected to the union's declared output layout before summation.

These rules keep layout decisions in the planner. Circuit assembly may verify
them but must not invent a different semantic order.

### Phase 3: circuit assembly

Change recursive assembly to accept both the fact input and an optional running
relation:

```text
assemble_rel(fact_input, plan, incoming) -> PlannedWhereStream
```

Before assembling a node, assert that:

- a plan with no `incoming_vars` received no running relation;
- a plan with `incoming_vars` received one;
- the received row layout exactly matches the planned layout.

A mismatch is a planner bug, not a query error.

Assembly by kind:

- `Pattern`: build the right-side pattern stream from the fact input. Return it
  directly when there is no incoming relation; otherwise join it with the
  incoming relation using the planned join metadata.
- `Chain`: fold over the children. The first child receives the chain's incoming
  relation and every later child receives its predecessor's output.
- `Union`: assemble every branch with a clone of the same incoming relation,
  project every branch to `output_vars`, sum the branches, and apply the
  existing `distinct`.

The top-level `query_where_stream` starts assembly with no incoming relation.

## Plan examples

For:

```clojure
[:find ?name
 :where
 [?e :name ?name]
 (or [?e :age 30]
     [?e :age 40])]
```

the current plan is conceptually:

```text
join(
  pattern(?e, ?name),
  union(pattern(?e), pattern(?e)))
```

The new plan is:

```text
chain
  pattern incoming=None output=[?e, ?name]
  union   incoming=[?e, ?name] output=[?e, ?name]
    pattern incoming=[?e, ?name] output=[?e, ?name]
    pattern incoming=[?e, ?name] output=[?e, ?name]
```

The first pattern's stream fans out into both union branches. Each branch
extends that same relation before the branch results are unioned.

An `or` without an enclosing relation remains standalone:

```text
union incoming=None output=[?e]
  pattern incoming=None output=[?e]
  pattern incoming=None output=[?e]
```

## Future operators

This change does not implement these operators. It fixes the contracts they
will use.

### `not`

A future `not` descriptor grounds no variables, so all variables mentioned by
its body must already be present in the running relation.

The physical difference node will:

1. Record the complete positive incoming layout.
2. Select the `not` variables from that layout as anti-join keys.
3. Project the positive stream to those keys.
4. Assemble the negative body with that key stream as its incoming relation.
5. Apply the difference/anti-semijoin algebra from Triplox PR #412.

The important change from PR #412 is the definition of the negative relation:
it is evaluated from the projected positive bindings, not solely from the fact
input. For triple-only bodies the results are equivalent; predicates and
functions inside the body require the seeded form.

### Predicates

A predicate has no groundable variables. It becomes introducible only after all
of its referenced variables are in the running layout. Its output layout is
identical to its incoming layout.

### Functions

A function's argument variables are required. Its result variable is
groundable unless the result is also one of its inputs.

The same physical function node can later choose its circuit behavior from the
incoming layout:

- append the computed result when the result variable is absent;
- filter for equality when the result variable is already bound.

No separate “produce function” and “validate function” semantic nodes are
needed.

## Source organization

The semantic phases should be reflected in the source layout:

- `src/inc_query.rs`: supported-query validation, `plan_query`, crate-visible
  plan types, and phase orchestration;
- `src/inc_query/descriptor.rs`: AST-to-descriptor normalization and
  groundability;
- `src/inc_query/planner.rs`: left-deep ordering and descriptor-to-`RelPlan`
  lowering;
- `src/incremental/circuit.rs`: `RelPlan`-to-DBSP assembly only.

`src/inc_query.rs` can declare the two submodules and re-export the
`pub(crate)` types consumed by circuit construction. This avoids a module-path
change for existing callers while making the phase boundary visible.

## Implementation sequence

### Commit 1: semantic descriptors and groundability

- Introduce the descriptor phase and source-file split.
- Add ordered variable and groundable metadata for the currently supported
  pattern, scope/`and`, and `or` shapes.
- Make the existing physical planner consume descriptors while preserving the
  current `Pattern`/`Join`/`Union` plans and circuit behavior.
- Add descriptor-only tests for variable ordering, scope union, `or`
  intersection, nested branches, and branch-specific variable order.

This commit must not change circuit assembly.

### Commit 2: incoming-aware planning and assembly

- Replace the independently producing `Join` plan with a `Chain`.
- Add exact incoming layouts and pattern-extension join metadata.
- Use groundability to choose the left-deep order.
- Thread incoming layouts through nested scope planning.
- Change circuit assembly to accept and verify an optional running relation.
- Fan the same incoming stream into all `or` branches.
- Update plan-shape tests and retain the existing result/delta tests.

This commit must not add a `Difference`, predicate, or function plan kind.

## Test strategy

Descriptor tests:

- triple variables are all groundable;
- a scope's groundable variables are the ordered union of its children;
- an `or` exposes only the ordered intersection of branch groundability;
- nested branch structure and scope order are preserved;
- branch encounter-order differences do not make layout order nondeterministic.

Physical planner tests:

- a single top-level pattern has no incoming layout;
- a multi-pattern scope is a chain and every child after the first receives the
  preceding output layout;
- a connected descriptor is preferred over a disconnected descriptor;
- disconnected patterns still plan as Cartesian products;
- a standalone `or` has no incoming layout;
- an `or` following a pattern and every one of its branches receive the same
  incoming layout;
- an `and` branch under `or` threads that incoming layout through its children;
- nested `or` preserves incoming and output layouts recursively;
- branch-specific row orders are normalized at the union boundary;
- the ordering helper reports an insufficient-binding error for a synthetic
  descriptor set with no introducible node.

Circuit tests:

- retain single-pattern, joins, Cartesian products, union distinctness, nested
  union, row-order normalization, and delta/retraction coverage;
- construct plans through the real planner where the incoming layout itself is
  under test;
- add an outer-pattern-plus-`or` case whose plan proves that the outer stream
  fans into every branch;
- assert that a planned/actual incoming-layout mismatch fails at the assembly
  boundary.

The implementation should finish with:

```text
cargo fmt --check
cargo test --workspace
cargo clippy --workspace --all-targets --all-features
```

## Alternatives rejected

### Special-case `not`

Passing a positive relation only to a future difference operator would solve
one immediate case but leave predicates, functions, and nested `or` with the
same missing-input problem.

### Keep independent children and add more joins

A predicate has no independently produced right-hand relation to join. A
function likewise needs row values from the running relation. Treating them as
independent children models the wrong dataflow.

### Add a fake incoming plan leaf

The incoming relation is an already assembled row stream with a known layout,
not another source derived from encoded triples. A fake leaf obscures ownership
and permits invalid standalone assembly.

### Infer incoming layouts during circuit assembly

This would make circuit construction another semantic planning pass and would
hide layout errors until runtime. The physical plan must be complete enough for
assembly to be mechanical and assertable.

## Consequences

- `inc_query.rs` becomes a coordinator rather than a combined AST normalizer,
  join planner, and physical-plan definition file.
- Physical plan snapshots become larger because incoming layouts are explicit.
- Circuit assembly becomes context-sensitive but simpler: every node either
  produces a base relation or extends the supplied relation.
- Existing positive queries should keep their results, although their physical
  operator trees change when an outer relation is pushed into `or` branches.
- Future operator PRs can add one descriptor kind, one physical kind, and one
  assembly case without redesigning how outer bindings reach nested scopes.
