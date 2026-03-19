# Mentat Entity Representation and Transaction API

## Overview

Mentat supported multiple ways to submit entities (documents) for transacting.
The EDN map notation `{:db/id 1 :person/name "foo"}` was the most familiar syntax
from a Datomic/Clojure perspective, but Rust callers typically used either raw EDN
strings or a programmatic builder pattern.

---

## 1. EDN String Transacting

The simplest approach was passing EDN text to `Conn::transact` (`src/conn.rs`):

```rust
conn.transact(&mut sqlite, r#"[{
    :db/id "tempid"
    :person/name "foo"
}]"#).unwrap();
```

Under the hood this called `edn::parse::entities()` to produce `Entity` variants,
which were then handed to the transactor.

Both map notation and explicit assertions were supported:

```rust
// Map notation
conn.transact(&mut sqlite, r#"[{
    :db/id "tempid"
    :person/name "foo"
    :person/age  42
}]"#)?;

// Explicit add/retract assertions
conn.transact(&mut sqlite, r#"[
    [:db/add "tempid" :person/name "foo"]
    [:db/add "tempid" :person/age  42]
]"#)?;
```

---

## 2. Builder Pattern (Programmatic, No Parsing)

For constructing entities in Rust without string parsing overhead, Mentat provided
a builder API in `transaction/src/entity_builder.rs`.

### TermBuilder (standalone, batched)

```rust
let mut builder = TermBuilder::new();
let e_x = builder.named_tempid("x");
let e_y = builder.named_tempid("y");

builder.add(e_x.clone(), kw!(:person/name), TypedValue::typed_string("foo"))?;
builder.add(e_x.clone(), kw!(:person/age),  TypedValue::Long(42))?;
builder.add(e_y.clone(), kw!(:person/friend), e_x.clone())?;

let (terms, tempids) = builder.build()?;
let report = in_progress.transact_entities(terms)?;
```

### InProgressBuilder (integrated with transaction lifecycle)

```rust
let in_progress = conn.begin_transaction(&mut sqlite)?;
let mut builder = in_progress.builder();

let e = builder.named_tempid("x");
builder.add(e.clone(), kw!(:person/name), TypedValue::typed_string("foo"))?;
builder.add(e.clone(), kw!(:person/age),  TypedValue::Long(42))?;

builder.commit()?;
```

### EntityBuilder (fluent, single-entity focused)

```rust
let mut builder = TermBuilder::new();
let mut entity = builder.describe_tempid("x");

entity.add(kw!(:person/name), TypedValue::typed_string("foo"))?;
entity.add(kw!(:person/age),  TypedValue::Long(42))?;

let builder = entity.finish();
let (terms, tempids) = builder.build()?;
```

### Key builder traits and types

```rust
// transaction/src/entity_builder.rs
pub trait BuildTerms {
    fn named_tempid<I>(&mut self, name: I) -> ValueRc<TempId> where I: Into<String>;
    fn describe_tempid(self, name: &str) -> EntityBuilder<Self>;
    fn describe<E>(self, entity: E) -> EntityBuilder<Self>
        where E: Into<EntityPlace<TypedValue>>;
    fn add<E, A, V>(&mut self, e: E, a: A, v: V) -> Result<()>
        where E: Into<EntityPlace<TypedValue>>,
              A: Into<AttributePlace>,
              V: Into<ValuePlace<TypedValue>>;
    fn retract<E, A, V>(&mut self, e: E, a: A, v: V) -> Result<()>
        where E: Into<EntityPlace<TypedValue>>,
              A: Into<AttributePlace>,
              V: Into<ValuePlace<TypedValue>>;
}
```

---

## 3. Internal Representation: `MapNotation`

The EDN map syntax `{:db/id 1 :person/name "foo"}` parsed into an internal
`BTreeMap` type. Defined in `edn/src/entities.rs`:

```rust
pub type MapNotation<V> = BTreeMap<EntidOrIdent, ValuePlace<V>>;

pub enum Entity<V> {
    AddOrRetract {
        op: OpType,
        e: EntityPlace<V>,
        a: AttributePlace,
        v: ValuePlace<V>,
    },
    MapNotation(MapNotation<V>),
}
```

Keys were `EntidOrIdent` (an entid number or a `Keyword` like `:person/name`),
values were `ValuePlace<V>` (atoms, tempids, lookup refs, nested maps, vectors, etc.).

**Nobody constructed `MapNotation` BTreeMaps by hand in Rust.** The EDN parser
produced them from strings, and for programmatic use the builder pattern was
preferred. During transaction processing (`db/src/tx.rs`), `MapNotation` variants
were expanded into individual `AddOrRetract` terms.

### Entity and Value place types

```rust
pub enum EntityPlace<V> {
    Entid(EntidOrIdent),
    TempId(ValueRc<TempId>),
    LookupRef(LookupRef<V>),
    TxFunction(TxFunction),
}

pub enum ValuePlace<V> {
    Entid(EntidOrIdent),
    TempId(ValueRc<TempId>),
    LookupRef(LookupRef<V>),
    TxFunction(TxFunction),
    Vector(Vec<ValuePlace<V>>),
    Atom(V),
    MapNotation(MapNotation<V>),
}
```

### TypedValue (concrete value type)

```rust
// core-traits/lib.rs
pub enum TypedValue {
    Ref(Entid),
    Boolean(bool),
    Long(i64),
    Double(OrderedFloat<f64>),
    Instant(DateTime<Utc>),
    String(ValueRc<String>),
    Keyword(ValueRc<Keyword>),
    Uuid(Uuid),
}
```

---

## 4. The `kw!` Macro

Defined in `src/lib.rs:96`, the `kw!` macro provides EDN-like syntax for
constructing `Keyword` values at compile time using `stringify!` and `concat!`
(zero runtime parsing overhead):

```rust
kw!(:person/name)       // => Keyword::namespaced("person", "name")
kw!(:db.type/long)      // => Keyword::namespaced("db.type", "long")
kw!(:foo/bar.baz)       // => Keyword::namespaced("foo", "bar.baz")
kw!(:foo)               // => Keyword::plain("foo")
```

---

## 5. Transaction Result

All transaction methods returned a `TxReport` (`core/src/tx_report.rs`):

```rust
pub struct TxReport {
    pub tx_id: Entid,
    pub tx_instant: DateTime<Utc>,
    pub tempids: BTreeMap<String, Entid>,
}
```

The `tempids` map resolved named tempids (e.g. `"x"`) to the permanent `Entid`
values assigned by the transactor.

---

## 6. Summary Table

| Method | Input | Typical Use |
|--------|-------|-------------|
| `conn.transact(sqlite, edn_string)` | EDN text | Schema setup, test fixtures, FFI |
| `InProgressBuilder::add(e, a, v)` | Rust types directly | Application logic in Rust |
| `TermBuilder` + `transact_entities` | Rust types directly | Batched programmatic transactions |
| `MapNotation` BTreeMap | Parsed from EDN `{...}` | Internal representation only |

## 7. Key Files

| File | Purpose |
|------|---------|
| `edn/src/entities.rs` | `Entity`, `MapNotation`, `ValuePlace`, `EntityPlace` types |
| `transaction/src/entity_builder.rs` | `TermBuilder`, `EntityBuilder`, `BuildTerms` trait |
| `transaction/src/lib.rs` | `InProgress`, `InProgressBuilder` |
| `src/conn.rs` | `Conn::transact()` main API entry point |
| `src/lib.rs` | `kw!` macro definition |
| `db/src/tx.rs` | `transact()` and `transact_terms()` core functions |
| `core-traits/lib.rs` | `TypedValue` enum |
| `core/src/tx_report.rs` | `TxReport` struct |
| `tests/entity_builder.rs` | Comprehensive builder pattern examples |
