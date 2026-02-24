//! Simple example: connect to a running Triplox server, define a schema
//! attribute, insert data, and query it back.
//!
//! Start the server first:
//!   cargo run                     (from project root, uses config/triplox.toml)
//!
//! Then run this example:
//!   cargo run --bin simple-example  (from examples/)

use std::collections::BTreeMap;

use anyhow::Result;
use edn::Keyword;
use triplox::client::ClientNode;
use triplox::node::{QueryNode, SubmitNode, TransactionResult};
use triplox::ops::{DataType, Document, TxOp};

/// Build a schema attribute definition as a Put document.
/// This mirrors the internal `plain_schema_attribute` helper.
fn schema_attribute(id: i64, name: &str, value_type: &str) -> TxOp {
    let mut doc = BTreeMap::new();
    doc.insert("db/id".to_string(), DataType::Long(id));
    doc.insert(
        "db/ident".to_string(),
        DataType::Keyword(Keyword::plain(name)),
    );
    doc.insert(
        "db/valueType".to_string(),
        DataType::Keyword(Keyword::namespaced("db.type", value_type)),
    );
    TxOp::Put(Document(doc))
}

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:5490";
    println!("Connecting to {addr}...");
    let node = ClientNode::connect(addr).await?;
    println!("Connected.");

    // 1. Define schema attributes (entity IDs 50-51, above bootstrap range 1-31)
    let schema_ops = vec![
        schema_attribute(50, "name", "string"),
        schema_attribute(51, "age", "long"),
    ];
    let result = node.execute_tx(schema_ops).await?;
    match &result {
        TransactionResult::TxCommited(basis) => {
            println!("Schema defined (tx_id={}).", basis.tx_key.tx_id);
        }
        TransactionResult::TxAborted(_, err) => {
            anyhow::bail!("Schema transaction aborted: {err}");
        }
    }

    // 2. Insert some data
    let mut alice = BTreeMap::new();
    alice.insert("db/id".to_string(), DataType::Long(100));
    alice.insert("name".to_string(), DataType::String("alice".to_string()));
    alice.insert("age".to_string(), DataType::Long(30));

    let mut bob = BTreeMap::new();
    bob.insert("db/id".to_string(), DataType::Long(101));
    bob.insert("name".to_string(), DataType::String("bob".to_string()));
    bob.insert("age".to_string(), DataType::Long(25));

    let data_ops = vec![TxOp::Put(Document(alice)), TxOp::Put(Document(bob))];
    let result = node.execute_tx(data_ops).await?;
    match &result {
        TransactionResult::TxCommited(basis) => {
            println!("Data inserted (tx_id={}).", basis.tx_key.tx_id);
        }
        TransactionResult::TxAborted(_, err) => {
            anyhow::bail!("Data transaction aborted: {err}");
        }
    }

    // 3. Open a DB snapshot and query
    let db = node.db().await?;
    println!("Opened DB snapshot (tx_id={}).", db.tx_id());

    let edn_query = r#"{:find [?e ?name ?age] :where [[?e :name ?name] [?e :age ?age]]}"#;
    let rows = db.query_edn(edn_query).await?;

    println!("Query returned {} row(s):", rows.len());
    for row in &rows {
        println!("  {:?}", row);
    }

    // 4. Clean up
    db.close().await?;
    node.close().await?;
    println!("Done.");

    Ok(())
}
