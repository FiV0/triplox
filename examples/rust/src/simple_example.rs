//! Simple example: connect to a running Triplox server, define a schema
//! attribute, insert data, and query it back.
//!
//! Start the server first:
//!   cargo run                     (from project root, uses config/triplox.toml)
//!
//! Then run this example:
//!   cargo run --bin simple-example  (from examples/rust/)

use anyhow::Result;
use edn::kw;
use edn::Keyword;
use triplox::client::ClientNode;
use triplox::node::{Database, QueryNode, SubmitNode, TransactionResult};
use triplox::ops::{DataType, TxOp};

/// Build a schema attribute definition as a Put document.
/// This mirrors the internal `plain_schema_attribute` helper.
fn schema_attribute(name: &str, value_type: &str) -> TxOp {
    TxOp::put(vec![
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
    let result = node.execute_tx(schema_ops).await?;
    match &result {
        TransactionResult::TxCommited(tx_key) => {
            println!("Schema defined (tx_id={}).", tx_key.tx_id);
        }
        TransactionResult::TxAborted(_, err) => {
            anyhow::bail!("Schema transaction aborted: {err}");
        }
    }

    // 2. Insert some data
    let data_ops = vec![
        TxOp::put(vec![
            (kw!(:name), "alice".into()),
            (kw!(:age), 30_i64.into()),
        ]),
        TxOp::put(vec![
            (kw!(:name), "bob".into()),
            (kw!(:age), 25_i64.into()),
        ]),
    ];
    let result = node.execute_tx(data_ops).await?;
    match &result {
        TransactionResult::TxCommited(tx_key) => {
            println!("Data inserted (tx_id={}).", tx_key.tx_id);
        }
        TransactionResult::TxAborted(_, err) => {
            anyhow::bail!("Data transaction aborted: {err}");
        }
    }

    // 3. Open a DB value and query
    let db = node.db().await?;
    println!("Opened DB value (tx_eid={}).", db.tx_eid());

    let rows = db
        .query(r#"{:find [?e ?name ?age] :where [[?e :name ?name] [?e :age ?age]]}"#)
        .await?;

    println!("Query returned {} row(s):", rows.len());
    for row in &rows {
        println!("  {:?}", row);
    }

    println!("Done.");

    Ok(())
}
