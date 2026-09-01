---
title: Rust API
description: The Triplox Rust client.
---

The Triplox Rust client is published on [crates.io](https://crates.io/crates/triplox-client) as `triplox-client`.
The full API reference is available at [docs.rs](https://docs.rs/triplox-client/latest/triplox_client/).

## Installation

Add the following dependencies to your `Cargo.toml`:

```toml
[dependencies]
triplox-client = "0.1.0-alpha.8"
edn = { package = "triplox-edn", version = "0.1.0-alpha.8" }
anyhow = "1.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The client is async, so you'll also need an async runtime such as [`tokio`](https://crates.io/crates/tokio).

## Example

The following program defines a small schema, inserts two entities, and runs a Datalog query against a db value.
You will need a running Triplox server. See the [quick start](/getting-started/quick-start/) for how to launch one.

```rust
use anyhow::Result;
use edn::kw;
use edn::Keyword;
use triplox_client::client::ClientNode;
use triplox_client::node::{Database, QueryNode, SubmitNode};
use triplox_client::ops::{DataType, TxOp};
use triplox_client::transaction::TransactionResult;

/// Build a schema attribute definition as a Put document.
fn schema_attribute(name: &str, value_type: &str) -> TxOp {
    TxOp::put([
        (kw!(:db/ident), DataType::Keyword(Keyword::plain(name))),
        (kw!(:db/valueType), DataType::Keyword(Keyword::namespaced("db.type", value_type))),
        (kw!(:db/cardinality), DataType::Keyword(kw!(:db.cardinality/one))),
    ])
}

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "http://127.0.0.1:5490";
    println!("Connecting to {addr}...");
    let node = ClientNode::connect(addr).await?;
    println!("Connected.");

    // 1. Define schema attributes
    let schema_ops = vec![
        schema_attribute("name", "string"),
        schema_attribute("age", "long"),
    ];
    match node.execute_tx(schema_ops).await? {
        TransactionResult::TxCommitted(tx_key) => {
            println!("Schema defined (tx_id={}).", tx_key.tx_id);
        }
        TransactionResult::TxAborted(_, err) => {
            anyhow::bail!("Schema transaction aborted: {err}");
        }
    }

    // 2. Insert some data
    let data_ops = vec![
        TxOp::put([
            (kw!(:name), "alice".into()),
            (kw!(:age), 30_i64.into()),
        ]),
        TxOp::put([
            (kw!(:name), "bob".into()),
            (kw!(:age), 25_i64.into()),
        ]),
    ];
    match node.execute_tx(data_ops).await? {
        TransactionResult::TxCommitted(tx_key) => {
            println!("Data inserted (tx_id={}).", tx_key.tx_id);
        }
        TransactionResult::TxAborted(_, err) => {
            anyhow::bail!("Data transaction aborted: {err}");
        }
    }

    // 3. Open a DB value and query
    let db = node.db().await?;
    println!("Opened DB value (tx_id={}).", db.tx_key().tx_id);

    let rows = db
        .query(r#"{:find [?e ?name ?age]
                   :where [[?e :name ?name]
                           [?e :age ?age]]}"#)
        .await?;

    println!("Query returned {} row(s):", rows.len());
    for row in &rows {
        println!("  {:?}", row);
    }

    println!("Done.");

    Ok(())
}
```
