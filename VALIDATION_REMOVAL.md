# Query Validation Removal

## Context

PR [#422](https://github.com/FiV0/triplox/pull/422) replaced the standard prefix-extender query engine with the staged standard planner and runtime. The standard and incremental query planners now both track which variables a clause can introduce and which variables it requires.

This makes parts of `src/query_validation.rs` redundant. The safe boundary is to remove checks whose invariants are enforced by both planners while retaining feature gates and layout requirements that downstream code still assumes.

This document describes the state after #422. It does not propose removing query-language restrictions merely because another layer happens to fail later.

## Clause ordering

Textual clause order is already unrestricted for the supported clause types:

- `not` can appear before the positive clause that grounds its variables.
- Predicates can appear before the triple patterns that ground their inputs.
- Functions can appear before clauses that ground their inputs.

The planners place these clauses once their dependencies are available. The remaining ordering restriction is in `validate_fn_clauses`: every function input must precede its output in the derived global variable order. This is stricter than either execution pipeline requires.

## Remove or narrow now

### Function variable-order validation

Remove the requirement that a function input precede its output in the global variable order.

Both planners defer a function until its inputs are bound. Both runtimes also support a function whose output is already bound: the function becomes a validator that keeps only rows where the computed value equals the existing output binding. For example, this query shape is executable even though `?e` precedes `?age` in the variable order:

```clojure
[:find ?e
 :where
 [?e :age ?age]
 [(+ ?age 1) ?e]]
```

The standard runtime still rejects a function whose output is also one of the expression inputs. Until standard and incremental semantics are aligned, retain that restriction explicitly or continue to let standard materialization report it.

Input variables missing from every positive clause do not need a separate validator check. The standard planner rejects variables missing from the scope's global order, and the incremental planner reports an insufficient binding.

### Predicate binding validation

Remove the check that every predicate variable appears in the global positive-variable index.

Both planners already require predicate inputs to be grounded in the relevant scope. Planner-owned validation is more accurate for nested scopes than a query-wide membership check.

Keep the restriction that predicates reference at least one variable for now. The standard engine can evaluate a constant predicate, but the incremental planner can select a zero-variable predicate before establishing an incoming relation. Supporting constant predicates requires a small incremental ordering change and focused coverage first.

### NOT binding precheck

Remove `validate_not_clauses` from shared query validation without changing implicit `not` semantics.

Both planners already enforce the requirement:

- The standard planner cannot place a `not` descriptor until all of its variables are bound.
- The incremental planner treats all non-groundable `not` variables as required inputs.

Removing this precheck changes which layer reports the error, not which queries are accepted.

### Duplicate standard validation

Standard queries are validated once in `Db::query_with_args` and again in `execute_query`. Keep validation in `execute_query`, which also protects direct callers, and remove the earlier duplicate call unless validation before entering the blocking task is considered important enough to justify the repetition.

## Revisit with additional coverage

### Empty global variable order

`validate_query` rejects a query when `query_variable_order` is empty. This also rejects fully constant triple patterns and zero-column existence queries.

The standard triple runtime can validate an exact constant entity/attribute/value triple against the unit relation. The staged planner can represent the check as a validation-only stage. The incremental pattern path also has a zero-column representation, but it lacks direct end-to-end coverage for this query shape.

Before removing the check:

1. Define the result contract for an empty relational `:find` and a fully constant `:where`.
2. Add standard and incremental tests for matching and non-matching constant triples.
3. Keep planner errors for genuinely ungroundable predicates, functions, and `not` clauses.

### Constant predicates

The standard engine can evaluate a predicate with no variables against the unit or current input relation. The incremental planner currently requires an incoming positive relation and may choose a leading constant predicate before one exists.

To support constant predicates consistently, make incremental descriptor ordering prefer a positive relation when no incoming relation exists, then add tests for the predicate appearing before and after a triple pattern.

## Retain

### OR branch validation

Keep the following implicit `or` requirements:

- The OR has at least one branch.
- Every branch binds the same variable set.
- Every branch mentions the same variable set.

The standard `OrPattern` requires every branch plan to produce exactly the OR variable set and use the same incoming layout. The incremental union plan takes the first branch as its output layout and projects every other branch to that layout. Removing validation can therefore turn a query error into a materialization failure or projection assertion.

Branch-private variables should be introduced through explicit `or-join`, not by weakening implicit `or` validation.

### NOT correlation and explicit joins

Keep rejecting explicit `or-join` and `not-join`. Both planners currently ignore `UnifyVars`, so accepting these forms would apply the wrong correlation interface.

Also keep the semantic requirement that every variable in an implicit `not` is bound outside the `not`. The standard `NotPattern` requires the incoming layout to equal the complete NOT variable set. The incremental planner currently assumes the negative plan produces exactly its correlation keys.

Supporting NOT-local variables requires proper `not-join` planning and an explicit projection of the negative plan to its declared join keys.

### Pattern feature gates

Keep validation for:

- Source variables.
- Transaction positions.
- Variable or placeholder attributes.
- Entity and value placeholders.
- Repeated variables in one triple pattern.

These are unsupported execution shapes, not obsolete ordering restrictions. Some downstream paths deliberately use `unreachable!` or enforce uniqueness based on validation.

### Input, find, order, and limit validation

Keep validation for:

- `:in` binding count and argument type.
- Unsupported tuple and relation input bindings.
- Relational `:find` and aggregate argument shapes.
- Aggregate variables missing from the query output layout.
- `ORDER BY` variables missing from `:find`.
- Variable limits that are absent, non-scalar, non-`Long`, or negative.

These checks protect result processing and infallible assumptions outside the where-clause planners.

## Suggested implementation sequence

1. Remove the function input-before-output restriction and add a public query regression for an already-bound function output.
2. Delegate unbound predicate, function, and NOT variable errors to the planners while retaining any deliberate cross-engine compatibility restrictions.
3. Remove one of the two standard `validate_query` calls.
4. Add zero-column and constant-predicate coverage before relaxing those restrictions.
5. Implement explicit `or-join` and `not-join` as separate features before allowing branch-private or NOT-local variables.

The initial audit ran the focused query-validation, standard-planner, incremental-planner, function-runtime, and constant-triple suites. All 86 selected tests passed.
