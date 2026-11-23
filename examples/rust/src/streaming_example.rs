//! Streaming example: subscribe to an incremental query and print result deltas
//! as transactions arrive.
//!
//! Start the server first (from the project root):
//!   cargo run                       (uses config/triplox.toml — in-memory storage)
//!
//! Then run this example (from examples/rust/):
//!   cargo run --bin streaming-example

use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use tokio::time::timeout;
use triplox_client::edn::{kw, Keyword};
use triplox_client::{
    ClientNode, DataType, Delta, SubmitNode, Subscription, TransactionResult, TxOp,
};

/// Build a schema attribute definition as a Put document.
fn schema_attribute(name: &str, value_type: &str) -> TxOp {
    TxOp::put([
        (kw!(:db/ident), DataType::Keyword(Keyword::plain(name))),
        (
            kw!(:db/valueType),
            DataType::Keyword(Keyword::namespaced("db.type", value_type)),
        ),
        (
            kw!(:db/cardinality),
            DataType::Keyword(kw!(:db.cardinality/one)),
        ),
    ])
}

/// Wait up to `wait` for the next delta; `None` means no delta arrived in time.
async fn next_delta(sub: &mut Subscription, wait: Duration) -> Result<Option<Delta>> {
    match timeout(wait, sub.next()).await {
        Err(_) => Ok(None),
        Ok(Some(Ok(delta))) => Ok(Some(delta)),
        Ok(Some(Err(err))) => anyhow::bail!("subscription error: {err}"),
        Ok(None) => anyhow::bail!("subscription stream ended"),
    }
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
        .subscribe("[:find ?name :where [?e :name ?name]]")
        .await?;
    println!("Subscribed at tx_id={}.", sub.registration_tx_key().tx_id);

    // The first delta is the priming result at the registration basis, also when it is empty.
    let wait = Duration::from_secs(5);
    let priming = next_delta(&mut sub, wait)
        .await?
        .context("no priming delta")?;
    println!("Priming delta: {:?}", priming.rows);

    // Transact three names; each produces a delta on the subscription.
    for name in ["alice", "bob", "carol"] {
        node.execute_tx(vec![TxOp::put([(kw!(:name), name.into())])])
            .await?;
    }

    // Print the next three deltas, then drop the subscription (unsubscribe).
    println!("Waiting for deltas...");
    for _ in 0..3 {
        let Some(delta) = next_delta(&mut sub, wait).await? else {
            println!("No more deltas.");
            break;
        };
        println!("Consumed through {:?}.", sub.tx_key().unwrap());
        for (row, weight) in delta.rows {
            println!("  {row:?}  (weight {weight})");
        }
    }

    println!("Done.");
    Ok(())
}
