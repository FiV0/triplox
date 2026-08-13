# Triplox Standard Query Engine

Version 0.1

## Overview

The standard query engine evaluates a parsed query against one immutable
database basis and returns a complete result. It is separate from the
incremental query engine described in
[INCREMENTAL_QUERIES.md](INCREMENTAL_QUERIES.md).

The engine is row-oriented. It plans a query without opening storage, assembles
that plan into runtime patterns for the selected database basis, and executes a
sequence of layout transitions over `BindingBag` values.

```text
ParsedQuery + QueryArg values + database basis
                    |
                    v
              validate_query
                    |
                    v
           build_logical_plan
       descriptors + logical scopes
                    |
                    v
              assemble_plan
      runtime patterns + concrete stages
                    |
                    v
            GenericJoinEngine
                 BindingBag
                    |
                    v
       projection / aggregation / order / limit
                    |
                    v
                QueryResult
```

`execute_query` in `src/query.rs` owns this orchestration. The planner and
runtime implementation live under `src/query/`; final result processing
remains in `src/query.rs`.

---

## Execution Value

`BindingBag` is the value passed between stages:

```rust
type BindingRow = Vec<Bytes>;

struct BindingBag {
    variables: Vec<Variable>,
    rows: Vec<BindingRow>,
    column_indexes: HashMap<Variable, usize>,
}
```

The variable vector is an ordered schema for every row. Construction rejects
duplicate variables and rows with the wrong arity. Patterns look columns up by
`Variable`; they do not assume that a row is a prefix of the global query
order. This is what lets nested patterns preserve unrelated outer columns.

Values stay encoded as `Bytes` while they move through storage lookups and
joins. Only expression patterns and final result processing decode values into
`DataType`.

In the future the row-oriented `BindingBag` will likely get replaced by a columnar
trie implementation that is more suited for joining a result set already sitting
in memory with a pattern that needs to retrieve values from SlateDB via seeks.
The bottleneck is likely retrieving and seeking through SlateDB iterators. The additional
cost of keeping variable results sorted in memory will pale in comparison.

### Relational operations

`BindingBag` provides the operations needed by executable patterns:

- row selection and one-to-many extension;
- projection and layout reorder;
- indexing by a variable subset;
- natural join;
- existential semijoin and antijoin;
- bag union, distinct union, and complete-row deduplication.

Natural join keeps the left layout and appends right-only variables. Semijoin
and antijoin preserve the complete left row and never multiply it.

`BindingBag::unit()` is the zero-column relation with one empty row.
`BindingBag::empty(variables)` has the requested layout and no rows. These
values follow the usual relational rules: unit is the identity for natural
join, while an empty zero-column relation annihilates it.

### Multiplicity

`BindingBag` has bag semantics. A natural join can multiply rows, and projection
does not implicitly deduplicate them. This multiplicity reaches ordinary
`:find` projection and aggregation.

Deduplication is explicit at boundaries that require set semantics:

- `RelationPattern` stores distinct relation rows;
- each triple pattern produces distinct candidate extensions per input row;
- `OrPattern` distinct-unions complete branch rows.

This distinction is important. Duplicate witnesses in a conjunction remain
observable, while the same complete result produced by two OR branches appears
once before it is correlated back to the outer input. See
[SEMANTICS.md](SEMANTICS.md) for the broader bag-versus-set discussion.

---

## Logical Planning

Planning is runtime-independent. `build_logical_plan` consumes owned query data
and arguments, but it does not construct SlateDB iterators, `ExecPattern` trait
objects, or runtime stages.

### Descriptors

Every input and where clause is lowered to a descriptor with a stable
`PatternId`, an ordered variable list, and descriptor-specific data:

```text
Triple(Pattern)
Relation { rows }
Predicate { expression }
Function { expression, input_variables, output }
Or { branches }
Not { children }
```

Scalar and collection `:in` arguments become relation descriptors. Their
values remain `DataType` values until runtime assembly encodes them.

OR and NOT preserve their recursive structure. Conjunction is represented by
the stages of a planning scope; there is no separate `AndPattern`.

### Variable order

The top-level variable order is deterministic:

1. variables supplied through `:in`;
2. variables introduced by positive triple and OR clauses in query order;
3. outputs from top-level function clauses, considered after the other
   top-level positive clauses.

Each nested scope derives its own relevant order. Correlated incoming variables
are moved to the front, variables not mentioned in that scope are left out,
and the top-level order remains the tie-breaker for the rest.

This is a dependency-aware deterministic order, not a cost-based global
optimizer.

### Groundability

Groundability answers which variables a participant can derive from the
currently bound variable set:

| Participant | Groundability |
|-------------|---------------|
| Triple | Any unbound entity or value variable in the pattern. One is selected per ordinary stage. |
| Relation | The next relation variable, provided already-bound relation variables form a prefix of its layout. |
| Predicate | None. Every referenced variable must already be bound. |
| Function | Its output when every input is bound and the output is not. |
| OR | Every missing OR variable, but only if every branch can derive the complete missing set. |
| NOT | None. All variables in the body must be supplied by the outer scope. |

OR groundability computes each branch to a fixed point. A branch may therefore
derive one variable from another inside the branch. The OR is groundable only
when every branch can reach the same complete variable set; it never advertises
a partial union of what different branches can derive.

### Scope planning and stages

`LogicalPlan` owns the top-level variable order and one recursive descriptor
tree. Planning one scope returns only its flat stage sequence:

```rust
struct LogicalPlan {
    variable_order: Vec<Variable>,
    descriptors: Vec<Descriptor>,
}

struct LogicalStage {
    added: Vec<Variable>,
    proposers: Vec<ParticipantRef>,
    participants: Vec<ParticipantRef>,
    target_variables: Vec<Variable>,
}

enum ParticipantRef {
    Pattern(PatternId),
    Incoming,
}
```

The planner does not build a second recursively planned descriptor tree.
Assembly invokes the same flat `plan_scope` function for the root, each OR
branch, and each NOT body. The root has no incoming relation. A nested scope
always has an incoming participant, including an explicitly empty relation for
an uncorrelated scope.

A planning scope is built as follows:

1. Emit a validation-only stage for any participant already fully bound.
2. Visit remaining variables in scope order. For each variable, first look for
   ordinary proposers and then for an OR that can derive the complete missing
   variable group.
3. Put every eligible ordinary proposer in that stage, or put the selected OR
   in the stage as its sole proposer and participant.
4. Also include unfinished participants that become fully validatable after
   an ordinary variable is added.
5. Continue until every participant has been placed.

Planning fails if a variable cannot be grounded or a participant cannot be
placed. A leaf descriptor may participate at several stages as its variables
become bound. An OR or NOT descriptor participates once in its owning scope.

Assembly verifies that proposers are also participants and translates their
symbolic references into positions in the ordered runtime participant list.
Only proposing stages may carry proposer positions, and every proposing stage
must carry at least one. Runtime execution therefore distinguishes patterns
that may propose from participants that only validate the completed binding.

---

## Runtime Assembly

`assemble_plan` binds the logical plan to one database basis. It:

- resolves fixed attribute idents through `IdentMap`;
- constructs Slate-backed `TriplePattern` values with `as_of`, the Tokio
  handle, and range statistics;
- encodes relation descriptor rows;
- constructs predicate and function patterns from the existing expression
  representation;
- plans and recursively assembles OR branches and NOT bodies;
- lazily constructs each runtime pattern when it first participates in a
  stage, then reuses the same `Arc<dyn ExecPattern>` in later stages;
- replaces descriptor ids with positions in ordered stage participant lists.

Assembly also checks that every runtime pattern exposes exactly the variables
recorded by its descriptor. When a composite pattern first participates,
assembly projects its incoming layout from the previous stage layout if it is
proposing, or from the current target layout if it is validating.

### Deferred incoming relations

`ParticipantRef::Incoming` is planning data, not a runtime pattern. Assembly
preserves it as a deferred slot:

```rust
enum ParticipantTemplate {
    Pattern(Arc<dyn ExecPattern>),
    Incoming,
}

struct StageTemplate {
    added: Vec<Variable>,
    participants: Vec<ParticipantTemplate>,
    proposer_positions: Vec<usize>,
    target_variables: Vec<Variable>,
}
```

Top-level templates cannot contain `Incoming` and are converted immediately
into concrete `Stage` values.

Nested templates remain on their owning `OrPattern` or `NotPattern`. On each
invocation, the composite pattern:

1. projects the current outer `BindingBag` to the planned incoming variables;
2. wraps that projection in one `RelationPattern`;
3. substitutes the same relation pattern into every `Incoming` slot;
4. validates and creates concrete stages;
5. executes those stages from `BindingBag::unit()`.

The templates are reusable, while the incoming relation and concrete stages
exist only for that invocation. `GenericJoinEngine` never receives an
`Incoming` enum variant.

---

## Runtime Contracts

### Executable patterns

All runtime patterns implement:

```rust
trait ExecPattern: Send + Sync {
    fn id(&self) -> PatternId;
    fn variables(&self) -> &[Variable];

    fn count(
        &self,
        input: &BindingBag,
        added: &[Variable],
        proposals: &mut [Proposal],
    ) -> anyhow::Result<()>;

    fn join(
        &self,
        input: &BindingBag,
        added: &[Variable],
        target_variables: &[Variable],
    ) -> anyhow::Result<BindingBag>;
}
```

`count` is called only on patterns designated as proposers for the stage. Each
proposer must consider every input row, including with a count of zero. Calling
`count` on a pattern that cannot propose is a contract error.

`join` has two modes:

- non-empty `added` extends the input and returns exactly
  `target_variables`;
- empty `added` validates or filters rows and must preserve the input layout.

Execution always starts from `BindingBag::unit()`. There is no separate seed
path for top-level inputs or nested correlations.

### Stages

A concrete `Stage` is one checked layout transition:

```rust
struct Stage {
    added: Vec<Variable>,
    participants: Vec<Arc<dyn ExecPattern>>,
    proposer_positions: Vec<usize>,
    target_variables: Vec<Variable>,
}
```

Its participant ids, proposer positions, added variables, and target
variables must be unique. Proposer positions are ordered and must refer to
participants in the stage. A stage has proposers if and only if it adds
variables. At execution time, the target variable set must equal the input
variables plus the added variables.

### Proposal selection

For a multi-participant proposing stage, the engine keeps one `Proposal` per
input row:

```rust
struct Proposal {
    proposer: Option<PatternId>,
    count: usize,
}
```

The initial value is `(None, usize::MAX)`. A proposer replaces it with its first
estimate or a strictly cheaper count, including zero. Equal counts retain the
earlier proposer in participant order. Valid proposer implementations consider
every row, so zero candidates are represented by a selected proposer with count
zero rather than by an absent proposer.

---

## Stage Execution

`GenericJoinEngine` folds concrete stages over an input `BindingBag`.

### Validation-only stage

Every participant receives `join(input, [], input_variables)` in stage order.
The engine checks that each participant preserves the layout, then reorders the
result to the stage target layout.

### Single-proposer stage

The engine calls the sole proposer’s `join` directly, checks the returned
layout, and then runs every other participant as a validator. This is also how a
grouped OR proposes all of its missing variables; OR does not implement a
proposal count.

### Multi-participant proposing stage

1. Allocate one empty proposal per input row.
2. Ask every designated proposer to count the requested extension.
3. Group row indexes by winning proposer `PatternId`.
4. Ask each winner to extend its input shard.
5. Run every other participant, including losing proposers, as a validator on
   that extended shard.
6. Bag-union the validated shards in the target layout.

The cheapest proposer is selected independently for every input row. Different
rows in one stage may therefore be extended by different patterns.

---

## Runtime Patterns

| Pattern | Current behavior |
|---------|------------------|
| `RelationPattern` | Wraps a distinct materialized relation. It proposes the next relation-prefix variable and validates bound prefixes existentially. |
| `TriplePattern` | Proposes entity or value variables from SlateDB indices and validates partial or complete triple bindings at the selected `as_of` basis. |
| `PredicatePattern` | Never proposes. It decodes referenced columns, evaluates an `Expr`, and filters rows once all variables are bound. |
| `FunctionPattern` | Proposes its single output after all expression inputs are bound. If the output is already bound, it evaluates and compares instead. |
| `OrPattern` | Executes each branch independently, aligns layouts, distinct-unions branch results, and then joins or semijoins them with the complete outer input. |
| `NotPattern` | Executes its body with projected outer bindings and antijoins matching bindings from the complete outer input. |

### Triple index access

The attribute position is fixed to an ident or entid during planning. Depending
on which entity/value position is being proposed and what is already bound,
`TriplePattern` chooses:

| Requested value | Already resolved | Index |
|-----------------|------------------|-------|
| Entity | Value | AVE |
| Entity | Neither | AE |
| Value | Entity | AEV |
| Value | Neither | AV |

Validation uses AEV when the entity is resolved and AVE otherwise. Full indices
are wrapped in `TemporalFilterIterator`, which resolves assertions and
retractions at `as_of`.

AE and AV are atemporal, additive projection indices and may over-propose
historical or retracted values. This does not change results: a two-variable
triple pattern participates again with a full temporal index after the other
variable is bound. A one-variable pattern with a constant in the other
position uses a full index directly. See
[COVERING_INDICES.md](COVERING_INDICES.md) for the storage layouts.

### OR correlation

OR branches are complete nested plans. A predicate in one branch can only
filter rows produced within that branch. Branch results are unioned only after
each branch has finished.

For a proposing OR, the branch union is naturally joined with the complete
outer input. For a validating OR, it is semijoined with the outer input.
Projecting the incoming relation keeps nested work limited to relevant
variables; joining or semijoining afterward restores unrelated outer columns
and their multiplicity.

### NOT correlation

Every variable referenced by a NOT body must already be bound by the outer
query. The body executes against the distinct projection of those variables.
Its result is used only as an existence relation: `NotPattern` antijoins it
from the original outer input, preserving unrelated columns and duplicate
outer rows.

---

## Result Processing

After the final stage, the binding layout must equal the planned top-level
variable order. `execute_query` then:

1. compiles the relational `:find` specification;
2. projects rows or accumulates aggregates;
3. decodes result values into `DataType`;
4. applies `:order`;
5. resolves and applies fixed or input-bound `:limit`;
6. returns `QueryResult`, currently `Vec<Vec<DataType>>`.

An all-aggregate query seeds one empty group, so aggregates such as `count`
still produce a result on empty input. A grouped aggregate produces no rows
when no group exists.

---

## Validation and Error Boundaries

`validate_query` runs before planning. It checks input arity and binding types,
OR variable agreement, positive binding of predicate and NOT variables,
function dependencies, aggregate arguments, ordering, and variable limits.

The later layers retain defensive checks:

- `BindingBag` checks schemas and row arity;
- planning rejects insufficient bindings and unplaced descriptors;
- assembly checks descriptor ids, pattern variables, and participant roles;
- stages check layout transitions and distinct participant ids;
- patterns check proposal sidecar lengths and their `join` mode;
- the engine checks proposer ids and every returned layout.

Storage, decoding, expression-conversion, and contract failures are returned
through `anyhow::Result` with pattern or stage context. A well-formed runtime
expression that is not applicable to a row evaluates to no value or false and
filters that row.

---

## Current Scope

The standard engine currently supports scalar and collection `:in` bindings,
triple patterns, predicates, scalar functions, implicit OR, NOT, relational
`:find`, aggregates, ordering, limits, and historical `as_of` bases.

Current restrictions include:

- tuple and relation `:in` bindings are rejected;
- query source and transaction positions are unsupported;
- attributes must be fixed idents or entids;
- entity and value placeholders are rejected;
- a triple pattern cannot repeat the same variable;
- explicit `or-join` is unsupported;
- every OR branch must bind and mention the same variable set;
- every NOT variable must be bound by positive outer clauses;
- a query must contain at least one variable;
- only relational `:find` is supported;
- pull and corresponding find elements are unsupported.

The incremental query path has its own planner and runtime. Standard-engine
changes do not automatically apply to subscriptions.

### Performance characteristics

The current implementation favors explicit contracts and correctness:

- intermediate `BindingBag` rows are materialized;
- projection and joins clone row vectors and cheap `Bytes` handles;
- triple proposals may repeat equivalent SlateDB scans for different input
  rows;
- nested OR materializes each branch result before union and correlation;
- variable order is deterministic but not globally cost-based.

These internals can be optimized without changing the logical-plan,
`ExecPattern`, or stage boundaries.

---

## Source Guide

| File | Responsibility |
|------|----------------|
| `src/query.rs` | Query orchestration, expression lowering, variable order, final projection, aggregation, ordering, and limits. |
| `src/query_validation.rs` | Validation shared by the standard query entry point. |
| `src/query/binding_bag.rs` | Checked row relation and relational operations. |
| `src/query/plan.rs` | Descriptors, groundability, and flat scope planning. |
| `src/query/plan/tests.rs` | Descriptor and scope-planning tests. |
| `src/query/assembly.rs` | Recursive scope assembly, database-basis-dependent pattern construction, and stage templates. |
| `src/query/exec_pattern.rs` | `PatternId`, proposal sidecar, and `ExecPattern` contract. |
| `src/query/stage.rs` | Concrete stages and deferred nested stage templates. |
| `src/query/engine.rs` | Stage fold and per-row proposer arbitration. |
| `src/query/patterns/` | Relation, triple, expression, OR, and NOT runtime patterns. |

Unit tests live beside each contract and runtime pattern. Public parity and
regression coverage for the standard engine also lives in
`triplox-jvm/src/test/clojure/xyz/triplox/integration/subscription_test.clj`.
