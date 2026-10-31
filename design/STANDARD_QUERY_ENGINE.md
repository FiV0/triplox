# Triplox Standard Query Engine

Version 0.1

## Overview

The standard query engine evaluates a parsed query against one immutable
database basis and returns a complete result. A database basis is the log
position that a read uses. The standard engine is separate from the
incremental query engine that [INCREMENTAL_QUERIES.md](INCREMENTAL_QUERIES.md)
describes.

The engine is row-oriented. It plans a query without opening storage. Then it
materializes that recursive plan for one database basis. Materialization turns
a plan into runtime objects. Last, the engine executes a sequence of scopes with
stages over a growing `BindingBag`.

A scope is the set of clauses at one nesting level. A stage is one step that
adds variables to rows or filters rows. Nested scopes exist to evaluate `or`,
`or-join`, `not`, and `not-join`.

```text
ParsedQuery + QueryArg values + database basis
                    |
                    v
              validate_query
                    |
                    v
               rewrite_query
                    |
                    v
           build_logical_plan
     recursive descriptors + stages
                    |
                    v
      LogicalPlan::materialize
         concrete runtime stages
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

`execute_query` in `src/query.rs` controls this sequence of steps. The planner
and the runtime implementation are in `src/query/`. The final result processing
stays in `src/query.rs`.

---

## Execution Value

`BindingBag` is the value that one stage passes to the next. It is a bag (a
collection that can contain duplicates) of binding rows. A binding row gives
one value to each variable of the bag.

```rust
type BindingRow = Vec<Bytes>;

struct BindingBag {
    variables: Vec<Variable>,
    rows: Vec<BindingRow>,
    column_indexes: HashMap<Variable, usize>,
}
```

The variable vector is the ordered column layout for every row. The constructor
rejects duplicate variables. It also rejects rows with the wrong arity (number
of columns). Patterns find columns by `Variable`. They do not assume that a row
is a prefix of the global query order. Because of this, nested patterns can keep
unrelated outer columns.

Values stay encoded as `Bytes` while they move through storage lookups and
joins. Only expression patterns and the final result processing decode values
into `DataType`.

In the future, a columnar trie implementation will probably replace the
row-oriented `BindingBag`. A trie is a tree that stores shared prefixes one
time. A columnar trie is a better fit for a join of two inputs. One input is a
result set that is already in memory. The other input is a pattern that gets
values from SlateDB through seeks.

The bottleneck is probably the reads and seeks through SlateDB iterators. In
comparison, the extra cost to keep variable results sorted in memory will be
small.

### Relational operations

`BindingBag` provides the operations that executable patterns need:

- Row selection and one-to-many extension
- Projection (keep a subset of columns) and layout reorder
- Indexing by a variable subset
- Natural join
- Existential semijoin and antijoin
- Bag union (keeps duplicates), distinct union (removes duplicates), and complete-row deduplication.

A natural join combines rows that agree on all shared variables. It keeps the
left layout and appends the right-only variables. A semijoin keeps each left row
that has a match on the right. An antijoin keeps each left row that has no match
on the right. Semijoin and antijoin keep the complete left row and never
multiply it.

`BindingBag::unit()` is the zero-column relation with one empty row.
`BindingBag::empty(variables)` has the requested layout and no rows. These
values follow the usual relational rules. Unit is the identity for natural join:
a join with unit gives the other input unchanged. An empty zero-column relation
annihilates natural join: a join with it gives no rows.

### Multiplicity

`BindingBag` has bag semantics (duplicate rows stay and count). A natural join
can multiply rows, and projection does not remove duplicates implicitly. This
multiplicity (the number of copies of a row) gets to the ordinary `:find`
projection and aggregation.

Deduplication is explicit at the boundaries that require set semantics (each
distinct row one time):

- `RelationPattern` stores distinct relation rows.
- Each triple pattern produces distinct candidate extensions for each input row.
- `OrPattern` projects branches to their exposed variables before the distinct union.

Duplicate witnesses (matches that produce a row) in a conjunction (clauses that
must all match) stay observable. But if two OR branches produce the same complete
result, that result appears one time before the engine correlates it back to the
outer input. For the general discussion of bag and set semantics, see
[SEMANTICS.md](SEMANTICS.md).

---

## Logical Planning

Planning does not depend on the runtime. `build_logical_plan` consumes owned
query data and arguments. It does not construct SlateDB iterators, `ExecPattern`
trait objects, or runtime stages.

### Descriptors

A descriptor is the runtime-free plan record of one clause. The planner lowers
each input and each where clause to a descriptor. Each descriptor has a stable
`PatternId`, an ordered variable list, and data that is specific to its kind:

```text
Triple(Pattern)
Relation { rows }
Predicate { expression }
Function { expression, input_variables, output }
Or { branches }
Not { children }
```

Scalar and collection `:in` arguments become relation descriptors. Their
values stay `DataType` values until materialization encodes them.

OR and NOT keep their recursive structure. The stages of a planning scope
represent conjunction. There is no separate `AndPattern`.

### Variable order

The top-level variable order is deterministic:

1. Variables supplied through `:in`.
2. Variables that positive (not negated) triple and OR clauses introduce, in query order.
3. Outputs from top-level function clauses. The planner considers these clauses
   after the other top-level positive clauses.

Each nested scope derives its own order of the relevant variables. Correlated
incoming variables (variables that the outer scope shares) come first. The order
leaves out variables that the scope does not mention. For the other variables,
the top-level order breaks ties.

This order is deterministic and follows dependencies. It is not a cost-based
global optimizer.

### Groundability

A participant is a pattern that takes part in a stage. A variable is bound
when each row already has a value for it. Groundability is the set of variables
that a participant can derive from the variables that are bound at that point:

| Participant | Groundability |
|-------------|---------------|
| Triple | Any unbound entity or value variable in the pattern. The planner selects one for each ordinary stage. |
| Relation | The next relation variable. This applies only if the already bound relation variables form a prefix of its layout. |
| Predicate | None. Every referenced variable must already be bound. |
| Function | Its output. This applies only if every input is bound and the output is not bound. |
| OR | Every missing OR variable. This applies only if every branch can derive the complete set of missing variables. |
| NOT | None. The outer scope must supply all variables in the body, or the declared `not-join` variables. |

For OR groundability, the planner computes each branch to a fixed point (the
state where no more variables become derivable). Thus, a branch can derive one
variable from another variable inside the branch. An OR is groundable (able to
derive its missing variables) only if every branch can get to the same complete
variable set. The OR never advertises a partial union of the variables that
different branches can derive.

### Scope planning and stages

Each `LogicalPlan` owns the descriptors and stages of one scope. Composite
descriptors (OR and NOT descriptors) contain complete child plans. Thus,
planning recurses one time, and the result needs no second planning
representation:

```rust
struct LogicalPlan {
    incoming_variables: Option<Vec<Variable>>,
    descriptors: Vec<LogicalDescriptor>,
    stages: Vec<LogicalStage>,
}

struct LogicalDescriptor {
    id: PatternId,
    variables: Vec<Variable>,
    kind: LogicalDescriptorKind,
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

The root records `incoming_variables: None`. A nested scope records
`Some(variables)`. An uncorrelated scope (it shares no variables with the outer
scope) records `Some(vec![])`. This difference keeps the explicit zero-column
incoming relation.

A proposer is a participant that produces candidate values for a new variable.
The planner builds a planning scope in these steps:

1. Emit a validation-only stage (it adds no variables) for any participant that
   is already fully bound.
2. Visit the remaining variables in scope order. For each variable, look first
   for ordinary (non-OR) proposers, then for an OR that can derive the complete
   group of missing variables.
3. Put every eligible ordinary proposer in that stage, or put the selected OR
   in the stage as its only proposer and participant.
4. Also include each unfinished participant that becomes fully validatable (all
   its variables bound) after the stage adds an ordinary variable.
5. Continue until every participant is placed.

If a variable cannot be grounded, or a participant cannot be placed, planning
fails. A leaf descriptor (not OR and not NOT) can participate in several stages
as its variables become bound. An OR or NOT descriptor participates one time in
the scope that owns it.

Planning records the incoming layout for a composite descriptor. If the
composite proposes, this layout is the layout of the previous stage. If the
composite validates, this layout is the current target layout. Then the planner
recursively plans each OR branch or NOT child with that layout.

Materialization makes sure that proposers are also participants. It translates
their symbolic references into positions in the ordered list of runtime
participants. A stage that does not propose must carry no proposer positions.
Every proposing stage must carry at least one. Thus, runtime execution
distinguishes the patterns that can propose from the participants that only
validate the completed binding.

---

## Runtime Materialization

`LogicalPlan::materialize` binds one already planned scope to a shared `DB`
value and to an optional incoming pattern. The method does these tasks:

- It resolves fixed attribute idents (keyword names of attributes) through the `IdentMap` stored in `DB`.
- It constructs Slate-backed `TriplePattern` values. These values share the
  database basis, the Tokio handle, and the range statistics from `DB`.
- It encodes the rows of relation descriptors.
- It constructs predicate and function patterns from the existing expression
  representation.
- It constructs OR and NOT patterns that own their already planned child scopes.
- It constructs each runtime pattern of the current scope one time, and it
  reuses the same `Arc<dyn ExecPattern>` in every stage.
- It replaces descriptor ids with positions in the ordered participant lists of
  the stages.

The root materializes with no incoming pattern. Nested plans require one. They
make sure that the ordered variables of that pattern equal their planned
incoming layout.

### Incoming relations

`ParticipantRef::Incoming` is logical planning data. It is not a runtime
pattern. Each time that an `OrPattern` or a `NotPattern` runs, it does these
steps:

1. It projects the current outer `BindingBag` to the planned incoming variables.
2. It wraps that projection in one `RelationPattern`.
3. It passes that same pattern to the `materialize` method of each child plan.
4. It resolves every `Incoming` reference while it creates the concrete stages.
5. It executes those stages from `BindingBag::unit()`.

OR constructs the projected relation one time and shares its `Arc` across all
branches. `GenericJoinEngine` receives only concrete `Stage` values. It never
receives an `Incoming` enum variant.

---

## Runtime Contracts

### Executable patterns

All runtime patterns implement this trait:

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

The engine calls `count` only on the patterns that the stage designates as
proposers. Each proposer must consider every input row, including the rows where
its count is zero. A call to `count` on a pattern that cannot propose is a
contract error.

`join` has two modes. If `added` is not empty, `join` extends the input and
returns exactly `target_variables`. If `added` is empty, `join` validates or
filters rows, and it must keep the input layout.

Execution always starts from `BindingBag::unit()`. There is no separate seed
path (a special way to supply the first rows) for top-level inputs or nested
correlations.

### Stages

A concrete `Stage` is one validated layout transition (a change from the input
layout to the target layout):

```rust
struct Stage {
    added: Vec<Variable>,
    participants: Vec<Arc<dyn ExecPattern>>,
    proposer_positions: Vec<usize>,
    target_variables: Vec<Variable>,
}
```

Its participant ids, proposer positions, added variables, and target variables
must be unique. Proposer positions are ordered, and they must refer to
participants in the stage. A stage has proposers if and only if it adds
variables. At execution time, the target variable set must equal the input
variables plus the added variables.

### Proposal selection

For a proposing stage with more than one participant, the engine keeps one
`Proposal` for each input row:

```rust
struct Proposal {
    proposer: Option<PatternId>,
    count: usize,
}
```

The initial value is `(None, usize::MAX)`. A proposer replaces it with its first
estimate or with a strictly cheaper count. This rule includes a count of zero.
If two counts are equal, the earlier proposer in participant order stays. Valid
proposer implementations consider every row. Thus, a row with zero candidates
has a selected proposer with count zero, not an absent proposer.

---

## Stage Execution

`GenericJoinEngine` folds concrete stages over an input `BindingBag`. That is,
it applies each stage in turn to the output of the previous stage.

### Validation-only stage

Every participant receives `join(input, [], input_variables)` in stage order.
The engine makes sure that each participant keeps the layout. Then it reorders
the result to the target layout of the stage.

### Single-proposer stage

The engine calls `join` on the only proposer directly. It makes sure that the
returned layout is correct. Then it runs every other participant as a validator
(a participant that only validates or filters rows). A grouped OR proposes all
of its missing variables in the same way. OR does not implement a proposal
count.

### Multi-participant proposing stage

1. Allocate one empty proposal for each input row.
2. Ask every designated proposer to count the requested extension.
3. Group the row indexes by the `PatternId` of the winning proposer.
4. Ask each winner to extend its input shard (the input rows that it won).
5. Run every other participant, including the losing proposers, as a validator
   on that extended shard.
6. Bag-union the validated shards in the target layout.

The engine selects the cheapest proposer independently for every input row.
Thus, different patterns can extend different rows in one stage.

---

## Runtime Patterns

| Pattern | Current behavior |
|---------|------------------|
| `RelationPattern` | Wraps a distinct materialized relation. It proposes the next relation-prefix variable. It validates bound prefixes existentially (it only looks for one match). |
| `TriplePattern` | Proposes entity or value variables from SlateDB indices. It validates partial or complete triple bindings at the selected `as_of` basis. |
| `PredicatePattern` | Never proposes. After all variables are bound, it decodes the referenced columns, evaluates an `Expr`, and filters rows. |
| `FunctionPattern` | Proposes its single output after all expression inputs are bound. If the output is already bound, it evaluates and compares instead. |
| `OrPattern` | Executes each branch independently and aligns the layouts. It takes the distinct union of the branch results. Then it joins or semijoins them with the complete outer input. |
| `NotPattern` | Executes its body with the projected outer bindings. It projects away the `not-join` locals (body variables that are not declared). Then it antijoins the matching bindings from the complete outer input. |

---

## Result Processing

After the final stage, the binding layout must equal the planned top-level
variable order. Then `execute_query` does these steps:

1. It compiles the relational `:find` specification.
2. It projects rows or accumulates aggregates.
3. It decodes result values into `DataType`.
4. It applies `:order`.
5. It resolves and applies a fixed or input-bound `:limit`.
6. It returns `QueryResult`, which is currently `Vec<Vec<DataType>>`.

An all-aggregate query (its `:find` has only aggregates) seeds one empty group.
Thus, aggregates such as `count` still produce a result for an empty input. If
no group exists, a grouped aggregate produces no rows.

---

## Validation and Error Boundaries

`validate_query` runs before planning. It validates these properties of the
query:

- Input arity and binding types
- OR variable agreement
- Positive binding of predicate and NOT variables
- Function dependencies
- Aggregate arguments
- Ordering
- Variable limits.

If the planner is correct, some states do not occur. Later layers keep defensive
code for these states. In most cases, the runtime data structures can represent
these states. But if everything works correctly, these states do not occur.

The engine returns storage, decoding, expression-conversion, and contract
failures through `anyhow::Result`, with pattern or stage context. If a
well-formed runtime expression does not apply to a row, it evaluates to no value
or to false. That result filters out the row.

---

## Source Guide

| File | Responsibility |
|------|----------------|
| `src/query.rs` | Query orchestration, expression lowering, variable order, final projection, aggregation, ordering, and limits. |
| `src/query_validation.rs` | Validation shared by the standard query entry point. |
| `src/query/binding_bag.rs` | Row relation and relational operations. |
| `src/query/plan.rs` | Descriptors, recursive planning, materialization that depends on the database basis, and their tests. |
| `src/query/exec_pattern.rs` | The main runtime `ExecPattern` contract, and helpers for proposals. |
| `src/query/stage.rs` | Concrete executable stages. |
| `src/query/engine.rs` | `GenericJoinEngine`, the main execution loop over `dyn ExecPattern`. |
| `src/query/patterns/` | Relation, triple, expression, OR, and NOT runtime patterns. |
