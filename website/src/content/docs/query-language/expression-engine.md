---
title: Expression Engine
description: Built-in predicates and functions.
---

The expression engine evaluates the predicates and functions that appear inside
Datalog `:where` clauses. It is a row-at-a-time evaluator over a tree of
expressions built from variables, literals and supported predicates and functions.

There are two ways to use an expression in a clause:

- **Predicates** — a boolean expression that filters rows.
  ```clojure
  [(< ?age 30)]
  ```
- **Function binding** — an expression whose result is bound to a new
  variable.
  ```clojure
  [(+ ?x 1) ?y]
  [(upper ?name) ?upper-name]
  ```

## Predicates

Predicates return a `boolean` and are used as filters.

| Predicate    | Syntax                            | Argument types                    |
| ------------ | --------------------------------- | --------------------------------- |
| `<`          | `(< a b)`                         | any comparable, same kind         |
| `<=`         | `(<= a b)`                        | any comparable, same kind         |
| `>`          | `(> a b)`                         | any comparable, same kind         |
| `>=`         | `(>= a b)`                        | any comparable, same kind         |
| `=`          | `(= a b)`                         | any                               |
| `not=`, `!=` | `(not= a b)`                      | any                               |
| `not`        | `(not x)`                         | `boolean`                         |
| `regexp_like` | `(regexp_like subject "pattern")` | `string`, string literal pattern |

`regexp_like` uses Rust regular expression syntax. Its pattern must be a string
literal and is compiled when the query is planned. It is only available as a
predicate, not as a function binding.

## Functions

Functions return a value and are used in a function binding to
introduce a new variable.

### Arithmetic

| Function    | Syntax       | Argument types                                | Result  |
| ----------- | ------------ | --------------------------------------------- | ------- |
| `+`         | `(+ a b)`    | `long`/`double`                               | `long`/`double` |
| `-`         | `(- a b)`    | `long`/`double`                               | `long`/`double` |
| `*`         | `(* a b)`    | `long`/`double`                               | `long`/`double` |
| `/`, `quot` | `(/ a b)`    | `long`/`double`                               | `long`/`double` |
| `mod`       | `(mod a b)`  | `long`/`double`                               | `long`/`double` |
| `abs`       | `(abs x)`    | `long`/`double`                               | `long`/`double` |

Notes:

- Integer arithmetic uses checked operations — overflow makes the row drop out.
- Division or modulo by zero is treated as no result rather than an error.
- Mixed `long`/`double` arithmetic promotes both sides to `double`.

### String

| Function | Syntax           | Argument types                                                                                  | Result   |
| -------- | ---------------- | ----------------------------------------------------------------------------------------------- | -------- |
| `concat` | `(concat a b)`   | `string`, `string`                                                                              | `string` |
| `upper`  | `(upper s)`      | `string`                                                                                        | `string` |
| `lower`  | `(lower s)`      | `string`                                                                                        | `string` |
| `strlen` | `(strlen s)`     | `string`                                                                                        | `long`   |
| `str`    | `(str x)`        | `long`, `bigint`, `double`, `float`, `boolean`, `string`, `uuid`                                | `string` |

Notes:

- `str` falls back to a debug-style representation for any argument type not
  listed above.

### Date/Time

| Function | Syntax      | Argument type | Result |
| -------- | ----------- | ------------- | ------ |
| `year`   | `(year t)`  | `instant`     | `long` |
| `month`  | `(month t)` | `instant`     | `long` |

### Conditional

| Function | Syntax                         | Argument types            | Result          |
| -------- | ------------------------------ | ------------------------- | --------------- |
| `if`     | `(if condition then else)`     | `boolean`, any, any       | selected branch |

`if` evaluates to the `then` expression only when its condition produces
`boolean true`; otherwise it evaluates to the `else` expression.

## Evaluation semantics

- Expressions evaluate against a tuple's variable bindings.
  A type mismatch, or an arithmetic failure (overflow, divide-by-zero) or generally an argument that
  is nonsensical for a predicate or function all
  produce "no result". The row is excluded from a predicate and produces no
  binding from a function.
- Predicate evaluation returns `true` only when the expression yields
  `boolean true`; anything else is treated as `false`.
- Expressions can be nested freely, for example:
  ```clojure
  [(not (< ?x 10))]
  [(+ (* ?x 2) 10) ?y]
  ```

:::note
The evaluation of an expression just "silently failing" might be a bit confusing to users and is
different to most EDN Datalog flavoured engines. We are aware of this and will likely
consider different error semantics for the expression engine at some point.
:::
