//! Streaming example: subscribe to an incremental query and print result deltas
//! as transactions arrive.
//!
//! Start the server first (from the project root):
//!   cargo run                       (uses config/triplox.toml — in-memory storage)
//!
//! Then run this example (from examples/rust/):
//!   cargo run --bin streaming-example

use anyhow::Result;
use edn::kw;
use edn::Keyword;
use futures::StreamExt;
use triplox::client::ClientNode;
use triplox::node::{SubmitNode, TransactionResult};
use triplox::ops::{DataType, TxOp};

/// Build a schema attribute definition as a Put document.
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

    // Define a :name attribute so we can subscribe to a query over it.
    match node
        .execute_tx(vec![schema_attribute("name", "string")])
        .await?
    {
        TransactionResult::TxCommitted(_) => println!("Schema defined."),
        TransactionResult::TxAborted(_, err) => anyhow::bail!("Schema aborted: {err}"),
    }

    // Subscribe at the latest indexed basis. The subscription is a `Stream` that
    // yields one delta per transaction affecting the query; dropping it unsubscribes.
    let mut sub = node
        .subscribe("[:find ?name :where [?e :name ?name]]", &[])
        .await?;
    println!("Subscribed at tx_id={}.", sub.tx_key().tx_id);

    // Transact three names; each produces a delta on the subscription.
    for name in ["alice", "bob", "carol"] {
        node.execute_tx(vec![TxOp::put(vec![(kw!(:name), name.into())])])
            .await?;
    }

    // Print the next three deltas, then drop the subscription (unsubscribe).
    println!("Waiting for deltas...");
    for _ in 0..3 {
        match sub.next().await {
            Some(Ok(delta)) => {
                for (row, weight) in delta.rows {
                    println!("  {row:?}  (weight {weight})");
                }
            }
            Some(Err(err)) => anyhow::bail!("subscription error: {err}"),
            None => {
                println!("Stream ended.");
                break;
            }
        }
    }

    println!("Done.");
    Ok(())
}
