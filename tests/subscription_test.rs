use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use edn::kw;
use triplox::client::ClientNode;
use triplox::node::{Database, Node, QueryNode, SubmitNode};
use triplox::ops::{DataType, EntityRef, TxOp};
use triplox::schema::test_schema_tx;
use triplox::server::Server;
use triplox::TransactionResult;

const NAMES_QUERY: &str = "[:find ?name :where [?e :name ?name]]";

async fn next_delta(sub: &mut triplox::subscription::Subscription) -> triplox::subscription::Delta {
    tokio::time::timeout(Duration::from_secs(10), sub.next())
        .await
        .expect("a delta within 10s")
        .expect("the stream yields a delta")
        .expect("the delta is Ok")
}

fn add_name(entity: &'static str, name: &'static str) -> Vec<TxOp> {
    vec![TxOp::Add {
        entity: entity.into(),
        attribute: kw!(:name),
        value: name.into(),
    }]
}

async fn start_test_server_with_handle() -> (String, CancellationToken, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}", addr);

    let node = Arc::new(Node::memory_node().await);
    let server = Server::new(node);

    let token = CancellationToken::new();
    let server_token = token.clone();
    let handle = tokio::spawn(async move {
        server
            .listen_on(listener, server_token)
            .await
            .expect("server should shut down cleanly");
    });

    (url, token, handle)
}

async fn start_test_server() -> (String, CancellationToken) {
    let (url, token, _handle) = start_test_server_with_handle().await;
    (url, token)
}

/// Smoke test: a subscription delivers a delta for a transaction made after it
/// was opened, end-to-end over HTTP.
#[tokio::test(flavor = "multi_thread")]
async fn subscribe_receives_transaction_delta() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    client.execute_tx(test_schema_tx()).await.unwrap();

    let mut sub = client.subscribe(NAMES_QUERY, &[]).await.unwrap();
    client.execute_tx(add_name("alice", "Alice")).await.unwrap();

    let delta = next_delta(&mut sub).await;
    assert_eq!(
        delta.rows,
        vec![(vec![DataType::String("Alice".to_string())], 1)]
    );

    token.cancel();
}

/// A slow consumer (transactions made before the client reads) loses no deltas:
/// every transaction's change is delivered once the client drains.
#[tokio::test(flavor = "multi_thread")]
async fn subscription_loses_no_deltas_under_slow_consumer() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    client.execute_tx(test_schema_tx()).await.unwrap();

    let mut sub = client.subscribe(NAMES_QUERY, &[]).await.unwrap();

    // Transact several times without reading the subscription.
    let names = ["a", "b", "c", "d", "e", "f", "g", "h"];
    for n in names {
        client.execute_tx(add_name(n, n)).await.unwrap();
    }

    // Drain: one delta per transaction, none dropped.
    let mut seen: BTreeSet<String> = BTreeSet::new();
    while seen.len() < names.len() {
        let delta = next_delta(&mut sub).await;
        for (row, weight) in delta.rows {
            assert_eq!(weight, 1, "each name is added once");
            if let [DataType::String(name)] = row.as_slice() {
                seen.insert(name.clone());
            }
        }
    }
    let expected: BTreeSet<String> = names.iter().map(|s| s.to_string()).collect();
    assert_eq!(seen, expected);

    token.cancel();
}

/// The accumulated deltas (including a retraction) reconstruct the same result
/// set as a standard query at the final basis.
#[tokio::test(flavor = "multi_thread")]
async fn subscription_deltas_match_standard_query() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    client.execute_tx(test_schema_tx()).await.unwrap();

    let mut sub = client.subscribe(NAMES_QUERY, &[]).await.unwrap();

    client.execute_tx(add_name("a", "Ann")).await.unwrap();
    client.execute_tx(add_name("b", "Bob")).await.unwrap();

    // Retract by resolved entity id (a string entity ref is a per-tx tempid).
    let rows = client
        .db()
        .await
        .unwrap()
        .query("[:find ?e :where [?e :name \"Ann\"]]")
        .await
        .unwrap();
    let ann_id = match &rows[0][0] {
        DataType::Long(id) => *id,
        other => panic!("expected entity id, got {other:?}"),
    };
    let last = client
        .execute_tx(vec![TxOp::Retract {
            entity: EntityRef::Id(ann_id),
            attribute: kw!(:name),
            value: "Ann".into(),
        }])
        .await
        .unwrap();
    let last_basis = match last {
        TransactionResult::TxCommitted(basis) => basis,
        TransactionResult::TxAborted(_, err) => panic!("transaction aborted: {err}"),
    };

    // Three query-affecting transactions -> three deltas; accumulate signed weights.
    let mut counts: BTreeMap<String, i64> = BTreeMap::new();
    for _ in 0..3 {
        let delta = next_delta(&mut sub).await;
        for (row, weight) in &delta.rows {
            *counts.entry(format!("{row:?}")).or_default() += *weight;
        }
    }
    let reconstructed: BTreeSet<String> = counts
        .into_iter()
        .filter(|(_, weight)| *weight != 0)
        .map(|(row, _)| row)
        .collect();

    let standard = client
        .db_as_of(last_basis)
        .await
        .unwrap()
        .query(NAMES_QUERY)
        .await
        .unwrap();
    let expected: BTreeSet<String> = standard.iter().map(|row| format!("{row:?}")).collect();

    assert_eq!(reconstructed, expected);

    token.cancel();
}

/// Dropping a subscription cancels its HTTP/2 stream; the server keeps serving
/// and a fresh subscription still receives deltas (the engine is not wedged).
#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_subscription_keeps_the_server_serving() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    client.execute_tx(test_schema_tx()).await.unwrap();

    // Open and immediately drop a subscription.
    {
        let _sub = client.subscribe(NAMES_QUERY, &[]).await.unwrap();
    }

    // A transaction drives the CDC apply that reaps the dropped subscription.
    client.execute_tx(add_name("a", "Ann")).await.unwrap();

    // A fresh subscription still works.
    let mut sub = client.subscribe(NAMES_QUERY, &[]).await.unwrap();
    client.execute_tx(add_name("b", "Bob")).await.unwrap();
    let delta = next_delta(&mut sub).await;
    assert_eq!(
        delta.rows,
        vec![(vec![DataType::String("Bob".to_string())], 1)]
    );

    token.cancel();
}

/// Server shutdown cancels live subscription bodies so graceful connection drain
/// is not held open by an otherwise idle streaming response.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_ends_live_subscription_and_drains_server() {
    let (addr, token, server_task) = start_test_server_with_handle().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    client.execute_tx(test_schema_tx()).await.unwrap();

    let mut sub = client.subscribe(NAMES_QUERY, &[]).await.unwrap();

    token.cancel();

    let next = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("subscription should end promptly on server shutdown");
    assert!(
        next.is_none(),
        "shutdown should close the subscription stream, got {next:?}"
    );

    tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("server should drain promptly with a live subscription")
        .expect("server task should not panic");
}
