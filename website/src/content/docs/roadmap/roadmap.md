---
title: Roadmap
description: Planned work and direction for Triplox.
---

In an initial version, Triplox will have quite the bare bones API. We want to focus on transactions, queries and incremental queries.
In later steps the APIs can be extended to support a wider set of features. This includes the `pull` and `entity` API as well as querying the complete transaction history as data and doing so incrementally.

Some parts which are (currently) explicitly left unsupported are:
- Triplox Datalog doesn't support the `:with` clause. As everything is bag based for now (see [bag vs. set semantics](/roadmap/open-questions/#bag-vs-set-semantics)) there is no need or rather no use for a `:with` clause.
- No support for variables in the attribute position. This makes the current join algorithm a little easier. We will likely add support for variables in attribute position.
- No support for entity ids in attribute position. This just makes planning a little easier. Even though underneath the idents are resolved to entity ids, we are currently only support idents in attribute position.

### Features on the Roadmap

The following is a non-exhaustive list of things that we plan to add to Triplox eventually.

- Reader node support. Currently the node can only run as primary node.
- query API features
  - `or-join`
  - `not-join`
  - rules
  - queries over the complete transaction history as data
- the `entity` API
- incremental queries (IQs)
  - `or`/`or-join`
  - `and`
  - `not`/`not-join`
  - `predicate`/`function` patterns
  - rules (rules for IQs require recursive circuits which are more tricky than simply building a circuit tree)
- explicit entity id partition support
- variable support in attribute position
- Extending the Expression Engine with more standard functions
- the `pull` API
- Views server side. Do people want views? Once you have the incremental queries correctly working,
it's kind of "easy" to add views. The question is mainly should it be a first class concept or do people maintain their views
themselves.
- database branching - SlateDB supports branching natively. Triplox branching should be build on top of that feature.
- Transaction Functions - This is likely quite far away and I don't know when (if ever) we get to that. It likely involves compiling transaction functions to WASM. A project to look into for this is Cranelift.

### Collapsing the "DBSP gap" and removing the standard query engine

When incremental queries get initialized the underlying DBSP circuits need to get bootstrapped. Circuits need to hold state that is needed to correctly function for future updates. The process of initializing this state is called bootstrapping. The initialization (simplifying a lot of things) runs the equivalent standard query through the corresponding DBSP circuit. The output of this "update" and the standard query result should be identical. This means that in theory, we should be able to run one-off queries and incremental queries through the same code path. I suspect (albeit I have not done a lot of testing on this) that the circuit bootstrapping will be a lot slower than standard queries. I call this gap the "DBSP gap". Any DBMS has it, in most it's just infinite as most systems don't support incremental queries 😉. The final boss challange will be closing this gap and then
we can get rid of the whole standard query pipeline. This might be a bit of pipe dream as the DBSP circuit does lots of bookkeeping
for future updates and in the standard query case this bookkeeping is pointless and hence an engine optimized for one-off queries should most of the time be faster as this overhead is not done. I think there is still value in unifying the two query engines and the bookkeeping could be disabled for standard queries. The DBSP circuit terminology and objects are very stateful and I think there is definitely some way to think about DBSP more functionally ([transducers](https://clojure.org/reference/transducers) I am looking at you) and my feeling is that there is a lot of simplification lurking in the dark.
