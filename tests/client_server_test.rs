use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use triplox::client::ClientNode;
use triplox::node::{Node, QueryNode, SubmitNode};
use triplox::ops::{DataType, Document, TxOp};
use edn::symbols::Keyword;
use triplox::schema::test_schema_tx;
use triplox::server::{DevServer, Server};
use triplox::TransactionResult;

async fn define_test_schema(client: &ClientNode) {
    let result = client.execute_tx(test_schema_tx()).await.unwrap();
    assert!(matches!(result, TransactionResult::TxCommited(_)));
}

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
    define_test_schema(&client).await;

    // Insert a document
    let mut doc = BTreeMap::new();
    doc.insert("db/id".to_string(), DataType::Long(100));
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
    assert_eq!(result[0], vec![DataType::Long(100), DataType::String("alice".to_string())]);

    db.close().await.unwrap();
    client.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_submit_tx() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_test_schema(&client).await;

    let mut doc = BTreeMap::new();
    doc.insert("db/id".to_string(), DataType::Long(100));
    doc.insert("name".to_string(), DataType::String("bob".to_string()));
    let tx_key = client.submit_tx(vec![TxOp::Put(Document(doc))]).await.unwrap();
    assert!(tx_key.tx_id >= 0);

    client.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_transactions_and_query() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_test_schema(&client).await;

    // Insert two documents in separate transactions
    let mut doc1 = BTreeMap::new();
    doc1.insert("db/id".to_string(), DataType::Long(100));
    doc1.insert("name".to_string(), DataType::String("alice".to_string()));
    client.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap();

    let mut doc2 = BTreeMap::new();
    doc2.insert("db/id".to_string(), DataType::Long(200));
    doc2.insert("name".to_string(), DataType::String("bob".to_string()));
    client.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap();

    let db = client.db().await.unwrap();
    let result = db
        .query_edn("{:find [?e ?name] :where [[?e :name ?name]]}")
        .await
        .unwrap();

    assert_eq!(result.len(), 2);
    assert!(result.contains(&vec![DataType::Long(100), DataType::String("alice".to_string())]));
    assert!(result.contains(&vec![DataType::Long(200), DataType::String("bob".to_string())]));

    db.close().await.unwrap();
    client.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_open_close_multiple_dbs() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_test_schema(&client).await;

    // Insert data
    let mut doc = BTreeMap::new();
    doc.insert("db/id".to_string(), DataType::Long(100));
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
    define_test_schema(&client1).await;
    let mut doc = BTreeMap::new();
    doc.insert("db/id".to_string(), DataType::Long(100));
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
async fn test_execute_tx_returns_tx_key() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_test_schema(&client).await;

    let mut doc = BTreeMap::new();
    doc.insert("db/id".to_string(), DataType::Long(100));
    doc.insert("name".to_string(), DataType::String("alice".to_string()));
    let result = client.execute_tx(vec![TxOp::Put(Document(doc))]).await.unwrap();

    match result {
        TransactionResult::TxCommited(tx_key) => {
            assert!(tx_key.tx_id > 0, "tx_id should be positive after indexing");
        }
        _ => panic!("Expected TxCommited"),
    }

    client.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_as_of() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_test_schema(&client).await;

    // First transaction
    let mut doc1 = BTreeMap::new();
    doc1.insert("db/id".to_string(), DataType::Long(100));
    doc1.insert("name".to_string(), DataType::String("alice".to_string()));
    let result1 = client.execute_tx(vec![TxOp::Put(Document(doc1))]).await.unwrap();
    let tx_key1 = match result1 {
        TransactionResult::TxCommited(tk) => tk,
        _ => panic!("Expected TxCommited"),
    };

    // Second transaction
    let mut doc2 = BTreeMap::new();
    doc2.insert("db/id".to_string(), DataType::Long(200));
    doc2.insert("name".to_string(), DataType::String("bob".to_string()));
    client.execute_tx(vec![TxOp::Put(Document(doc2))]).await.unwrap();

    // Open DB pinned to tx_key after first tx — should only see alice
    let db = client.db_as_of(tx_key1).await.unwrap();
    let result = db
        .query_edn("{:find [?e ?name] :where [[?e :name ?name]]}")
        .await
        .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![DataType::Long(100), DataType::String("alice".to_string())]);

    db.close().await.unwrap();
    client.close().await.unwrap();
    token.cancel();
}

// ---------------------------------------------------------------------------
// DevServer tests
// ---------------------------------------------------------------------------

async fn start_dev_server() -> (String, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let server = DevServer::new(1024);

    let token = CancellationToken::new();
    let server_token = token.clone();
    tokio::spawn(async move {
        let _ = server.listen_on(listener, server_token).await;
    });

    (addr, token)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dev_server_connections_are_isolated() {
    let (addr, token) = start_dev_server().await;

    // Connection 1: define schema and insert data
    let client1 = ClientNode::connect(&addr).await.unwrap();
    define_test_schema(&client1).await;

    let mut doc = BTreeMap::new();
    doc.insert("db/id".to_string(), DataType::Long(100));
    doc.insert("name".to_string(), DataType::String("alice".to_string()));
    client1.execute_tx(vec![TxOp::Put(Document(doc))]).await.unwrap();

    let db1 = client1.db().await.unwrap();
    let result1 = db1
        .query_edn("{:find [?e ?name] :where [[?e :name ?name]]}")
        .await
        .unwrap();
    assert_eq!(result1.len(), 1, "client1 should see its own data");

    // Connection 2: gets a fresh node, should see nothing
    let client2 = ClientNode::connect(&addr).await.unwrap();
    define_test_schema(&client2).await;

    let db2 = client2.db().await.unwrap();
    let result2 = db2
        .query_edn("{:find [?e ?name] :where [[?e :name ?name]]}")
        .await
        .unwrap();
    assert_eq!(result2.len(), 0, "client2 should not see client1's data");

    db1.close().await.unwrap();
    db2.close().await.unwrap();
    client1.close().await.unwrap();
    client2.close().await.unwrap();
    token.cancel();
}

/// Mirror of Clojure test-query-using-keywords, exercised through the full
/// client-server wire protocol via query_edn.
#[tokio::test(flavor = "multi_thread")]
async fn test_query_keyword_value_comparison_via_wire() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_test_schema(&client).await;

    // Add sex attribute (keyword type) at ID 54
    let mut sex_attr = BTreeMap::new();
    sex_attr.insert("db/id".to_string(), DataType::Long(54));
    sex_attr.insert("db/ident".to_string(), DataType::Keyword(Keyword::plain("sex")));
    sex_attr.insert("db/valueType".to_string(), DataType::Keyword(Keyword::namespaced("db.type", "keyword")));
    client.execute_tx(vec![TxOp::Put(Document(sex_attr))]).await.unwrap();

    // Insert 4 people with keyword sex values
    for (id, name, sex) in [(100, "Ivan", "male"), (101, "Petr", "male"),
                             (102, "Doris", "female"), (103, "Jane", "female")] {
        let mut doc = BTreeMap::new();
        doc.insert("db/id".to_string(), DataType::Long(id));
        doc.insert("name".to_string(), DataType::String(name.to_string()));
        doc.insert("sex".to_string(), DataType::Keyword(Keyword::plain(sex)));
        client.execute_tx(vec![TxOp::Put(Document(doc))]).await.unwrap();
    }

    let db = client.db().await.unwrap();

    // Same clause order as Clojure: name first (binds ?e), sex filter second
    let result = db
        .query_edn("{:find [?name] :where [[?e :name ?name] [?e :sex :male]]}")
        .await
        .unwrap();

    assert_eq!(result.len(), 2, "expected only Ivan and Petr, got {:?}", result);
    assert!(result.contains(&vec![DataType::String("Ivan".to_string())]));
    assert!(result.contains(&vec![DataType::String("Petr".to_string())]));

    db.close().await.unwrap();
    client.close().await.unwrap();
    token.cancel();
}
