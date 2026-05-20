//! Simple example using static EDN strings: connect to a running Triplox server,
//! define schema attributes, insert data, and query it back.
//!
//! Start the server first:
//!   cargo run                             (from project root, uses config/triplox.toml)
//!
//! Then run this example:
//!   cargo run --bin simple-example-edn  (from examples/rust/)

use anyhow::Result;
use triplox::client::ClientNode;
use triplox::node::{Database, QueryNode, SubmitNode, TransactionResult};

const SCHEMA_OPS: &[&str] = &[
    "{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}",
    "{:db/ident :age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}",
];

const DATA_OPS: &[&str] = &["{:name \"alice\" :age 30}", "{:name \"bob\" :age 25}"];

const QUERY: &str = "{:find [?e ?name ?age] :where [[?e :name ?name] [?e :age ?age]]}";

fn require_committed(label: &str, result: &TransactionResult) -> Result<()> {
    match result {
        TransactionResult::TxCommited(_) => Ok(()),
        TransactionResult::TxAborted(_, err) => anyhow::bail!("{label} transaction aborted: {err}"),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "http://127.0.0.1:5490";
    println!("Connecting to {addr}...");
    let node = ClientNode::connect(addr).await?;
    println!("Connected.");

    // 1. Define schema attributes
    let result = node.execute_tx(SCHEMA_OPS.to_vec()).await?;
    require_committed("Schema", &result)?;
    if let TransactionResult::TxCommited(basis) = &result {
        println!("Schema defined (tx_id={}).", basis.tx_key.tx_id);
    }

    // 2. Insert some data
    let result = node.execute_tx(DATA_OPS.to_vec()).await?;
    require_committed("Data", &result)?;
    if let TransactionResult::TxCommited(basis) = &result {
        println!("Data inserted (tx_id={}).", basis.tx_key.tx_id);
    }

    // 3. Open a DB snapshot and query
    let db = node.db().await?;
    println!("Opened DB snapshot (tx_eid={}).", db.tx_eid());

    let rows = db.query(QUERY).await?;

    println!("Query returned {} row(s):", rows.len());
    for row in &rows {
        println!("  {:?}", row);
    }

    // 4. Clean up
    db.close().await?;
    println!("Done.");

    Ok(())
}
