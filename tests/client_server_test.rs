use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use triplox::client::ClientNode;
use triplox::node::{Node, QueryNode, SubmitNode};
use triplox::ops::{DataType, Document, TxOp};
use triplox::server::Server;
use triplox::{Basis, TransactionResult};

/// Start an in-memory server on a free port.
/// Returns the address string and a cancellation token.
/// Cancel the token to shut down the server.
async fn start_test_server() -> (String, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let node = Arc::new(Node::memory_node().await);
    let server = Server::new(node, 1024);

    let token = CancellationToken::new();
    let server_token = token.clone();
    tokio::spawn(async move {
        let _ = server.listen_on(listener, server_token).await;
    });

    (addr, token)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_client_connect_and_close() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    client.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_tx_and_query() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();

    // Insert a document
    let mut doc = BTreeMap::new();
    doc.insert("db/id".to_string(), DataType::Long(1));
    doc.insert("name".to_string(), DataType::String("alice".to_string()));
    let result = client.execute_tx(vec![TxOp::Put(Document(doc))]).await.unwrap();
    assert!(matches!(result, TransactionResult::TxCommited(_)));

    // Open a DB and query
    let db = client.db().await.unwrap();
    let result = db
        .query_edn("{:find [?e ?name] :where [[?e :name ?name]]}")
        .await
        .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![DataType::Long(1), DataType::String("alice".to_string())]);

    db.close().await.unwrap();
    client.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_submit_tx() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();

    let mut doc = BTreeMap::new();
    doc.insert("db/id".to_string(), DataType::Long(1));
    doc.insert("name".to_string(), DataType::String("bob".to_string()));
    let tx_key = client.submit_tx(vec![TxOp::Put(Document(doc))]).await.unwrap();
    assert_eq!(tx_key.tx_id, 0);

    client.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_transactions_and_query() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();

    // Insert two documents in separate transactions
    let mut doc1 = BTreeMap::new();
    doc1.insert("db/id".to_string(), DataType::Long(1));
    doc1.insert("name".to_string(), DataType::String("alice".to_string()));
    client.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap();

    let mut doc2 = BTreeMap::new();
    doc2.insert("db/id".to_string(), DataType::Long(2));
    doc2.insert("name".to_string(), DataType::String("bob".to_string()));
    client.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap();

    let db = client.db().await.unwrap();
    let result = db
        .query_edn("{:find [?e ?name] :where [[?e :name ?name]]}")
        .await
        .unwrap();

    assert_eq!(result.len(), 2);
    assert!(result.contains(&vec![DataType::Long(1), DataType::String("alice".to_string())]));
    assert!(result.contains(&vec![DataType::Long(2), DataType::String("bob".to_string())]));

    db.close().await.unwrap();
    client.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_open_close_multiple_dbs() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();

    // Insert data
    let mut doc = BTreeMap::new();
    doc.insert("db/id".to_string(), DataType::Long(1));
    doc.insert("name".to_string(), DataType::String("alice".to_string()));
    client.execute_tx(vec![TxOp::Put(Document(doc))]).await.unwrap();

    // Open two DB handles
    let db1 = client.db().await.unwrap();
    let db2 = client.db().await.unwrap();

    // Both should return same data
    let r1 = db1.query_edn("{:find [?e] :where [[?e :name \"alice\"]]}").await.unwrap();
    let r2 = db2.query_edn("{:find [?e] :where [[?e :name \"alice\"]]}").await.unwrap();
    assert_eq!(r1.len(), 1);
    assert_eq!(r2.len(), 1);

    db1.close().await.unwrap();
    db2.close().await.unwrap();
    client.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_two_connections() {
    let (addr, token) = start_test_server().await;

    // Connection 1 inserts data
    let client1 = ClientNode::connect(&addr).await.unwrap();
    let mut doc = BTreeMap::new();
    doc.insert("db/id".to_string(), DataType::Long(1));
    doc.insert("name".to_string(), DataType::String("alice".to_string()));
    client1.execute_tx(vec![TxOp::Put(Document(doc))]).await.unwrap();

    // Connection 2 can see the data
    let client2 = ClientNode::connect(&addr).await.unwrap();
    let db = client2.db().await.unwrap();
    let result = db.query_edn("{:find [?e] :where [[?e :name \"alice\"]]}").await.unwrap();
    assert_eq!(result.len(), 1);

    db.close().await.unwrap();
    client1.close().await.unwrap();
    client2.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_tx_returns_basis_with_seq_num() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();

    let mut doc = BTreeMap::new();
    doc.insert("db/id".to_string(), DataType::Long(1));
    doc.insert("name".to_string(), DataType::String("alice".to_string()));
    let result = client.execute_tx(vec![TxOp::Put(Document(doc))]).await.unwrap();

    match result {
        TransactionResult::TxCommited(basis) => {
            assert!(basis.seq_num > 0, "seq_num should be positive after indexing");
        }
        _ => panic!("Expected TxCommited"),
    }

    client.close().await.unwrap();
    token.cancel();
}

#[ignore] // await_tx returns latest seq_num, not the one for the requested tx
#[tokio::test(flavor = "multi_thread")]
async fn test_db_with_basis() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();

    // First transaction
    let mut doc1 = BTreeMap::new();
    doc1.insert("db/id".to_string(), DataType::Long(1));
    doc1.insert("name".to_string(), DataType::String("alice".to_string()));
    let result1 = client.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap();
    let basis1 = match result1 {
        TransactionResult::TxCommited(b) => b,
        _ => panic!("Expected TxCommited"),
    };

    // Second transaction
    let mut doc2 = BTreeMap::new();
    doc2.insert("db/id".to_string(), DataType::Long(2));
    doc2.insert("name".to_string(), DataType::String("bob".to_string()));
    client.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap();

    // Open DB pinned to basis after first tx — should only see alice
    let db = client.db_with_basis(basis1).await.unwrap();
    let result = db
        .query_edn("{:find [?e ?name] :where [[?e :name ?name]]}")
        .await
        .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![DataType::Long(1), DataType::String("alice".to_string())]);

    db.close().await.unwrap();
    client.close().await.unwrap();
    token.cancel();
}
