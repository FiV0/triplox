use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use triplox::client::ClientNode;
use triplox::node::{Node, QueryNode, SubmitNode};
use triplox::ops::{DataType, TxOp, TxValue};
use edn::kw;
use edn::symbols::Keyword;
use triplox::schema::test_schema_tx;
use triplox::server::{DevServer, Server};
use triplox::TransactionResult;

async fn define_base_schema(client: &ClientNode) {
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
    define_base_schema(&client).await;

    // Insert a document
    let result = client
        .execute_tx(vec![TxOp::put(vec![(kw!(:db/id), TxValue::Ref(100_i64.into())), (kw!(:name), "alice".into())])])
        .await
        .unwrap();
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
    define_base_schema(&client).await;

    let tx_key = client
        .submit_tx(vec![TxOp::put(vec![(kw!(:db/id), TxValue::Ref(100_i64.into())), (kw!(:name), "bob".into())])])
        .await
        .unwrap();
    assert!(tx_key.tx_id >= 0);

    client.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_transactions_and_query() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;

    // Insert two documents in separate transactions
    client
        .execute_tx(vec![TxOp::put(vec![(kw!(:db/id), TxValue::Ref(100_i64.into())), (kw!(:name), "alice".into())])])
        .await
        .unwrap();

    client
        .execute_tx(vec![TxOp::put(vec![(kw!(:db/id), TxValue::Ref(200_i64.into())), (kw!(:name), "bob".into())])])
        .await
        .unwrap();

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
    define_base_schema(&client).await;

    // Insert data
    client
        .execute_tx(vec![TxOp::put(vec![(kw!(:db/id), TxValue::Ref(100_i64.into())), (kw!(:name), "alice".into())])])
        .await
        .unwrap();

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
    define_base_schema(&client1).await;
    client1
        .execute_tx(vec![TxOp::put(vec![(kw!(:db/id), TxValue::Ref(100_i64.into())), (kw!(:name), "alice".into())])])
        .await
        .unwrap();

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
    define_base_schema(&client).await;

    let result = client
        .execute_tx(vec![TxOp::put(vec![(kw!(:db/id), TxValue::Ref(100_i64.into())), (kw!(:name), "alice".into())])])
        .await
        .unwrap();

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
    define_base_schema(&client).await;

    // First transaction
    let result1 = client
        .execute_tx(vec![TxOp::put(vec![(kw!(:db/id), TxValue::Ref(100_i64.into())), (kw!(:name), "alice".into())])])
        .await
        .unwrap();
    let tx_key1 = match result1 {
        TransactionResult::TxCommited(tk) => tk,
        _ => panic!("Expected TxCommited"),
    };

    // Second transaction
    client
        .execute_tx(vec![TxOp::put(vec![(kw!(:db/id), TxValue::Ref(200_i64.into())), (kw!(:name), "bob".into())])])
        .await
        .unwrap();

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
    define_base_schema(&client1).await;

    client1
        .execute_tx(vec![TxOp::put(vec![(kw!(:db/id), TxValue::Ref(100_i64.into())), (kw!(:name), "alice".into())])])
        .await
        .unwrap();

    let db1 = client1.db().await.unwrap();
    let result1 = db1
        .query_edn("{:find [?e ?name] :where [[?e :name ?name]]}")
        .await
        .unwrap();
    assert_eq!(result1.len(), 1, "client1 should see its own data");

    // Connection 2: gets a fresh node, should see nothing
    let client2 = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client2).await;

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

async fn define_schema_attr(client: &ClientNode, id: i64, name: &str, vtype: &str) {
    client
        .execute_tx(vec![TxOp::put(vec![(kw!(:db/id), TxValue::Ref(id.into())), 
            (kw!(:db/ident), DataType::Keyword(Keyword::plain(name)).into()),
            (kw!(:db/valueType), DataType::Keyword(Keyword::namespaced("db.type", vtype)).into()),
            (kw!(:db/cardinality), DataType::Keyword(Keyword::namespaced("db.cardinality", "one")).into()),
        ])])
        .await
        .unwrap();
}

async fn define_people_schema(client: &ClientNode) {
    define_schema_attr(client, 54, "last-name", "string").await;
    define_schema_attr(client, 55, "sex", "keyword").await;
    define_schema_attr(client, 56, "salary", "long").await;
    define_schema_attr(client, 57, "city", "string").await;
}

async fn define_heads_schema(client: &ClientNode) {
    define_schema_attr(client, 58, "heads", "long").await;
}

/// Mirror of Clojure test-query-using-keywords, exercised through the full
/// client-server wire protocol via query_edn.
#[tokio::test(flavor = "multi_thread")]
async fn test_query_keyword_value_comparison_via_wire() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;
    define_people_schema(&client).await;

    // Insert 4 people with keyword sex values
    for (id, name, sex) in [(100, "Ivan", "male"), (101, "Petr", "male"),
                             (102, "Doris", "female"), (103, "Jane", "female")] {
        client
            .execute_tx(vec![TxOp::put(vec![(kw!(:db/id), TxValue::Ref(id.into())), 
                (kw!(:name), name.into()),
                (kw!(:sex), DataType::Keyword(Keyword::plain(sex)).into()),
            ])])
            .await
            .unwrap();
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

// ---------------------------------------------------------------------------
// Aggregate tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_aggregates_and_or() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;
    define_people_schema(&client).await;

    // Insert Ada, Alan, Adam
    for (id, name, last, sex, age) in [
        (100, "Ada", "Lovelace", "female", 21),
        (101, "Alan", "Turing", "male", 22),
        (102, "Adam", "Smith", "male", 23),
    ] {
        client
            .execute_tx(vec![TxOp::put(vec![(kw!(:db/id), TxValue::Ref(id.into())), 
                (kw!(:name), name.into()),
                (kw!(:last-name), last.into()),
                (kw!(:sex), DataType::Keyword(Keyword::plain(sex)).into()),
                (kw!(:age), (age as i64).into()),
            ])])
            .await
            .unwrap();
    }

    let db = client.db().await.unwrap();

    // count with OR: Lovelace AND (name=Ada OR sex=male) → 1 (only Ada)
    let result = db
        .query_edn("{:find [(count ?p)] :where [[?p :last-name \"Lovelace\"] (or [?p :name \"Ada\"] [?p :sex :male])]}")
        .await
        .unwrap();
    assert_eq!(result, vec![vec![DataType::Long(1)]]);

    // count with OR: Lovelace AND (name=Ada OR sex=female) → 1
    let result = db
        .query_edn("{:find [(count ?p)] :where [[?p :last-name \"Lovelace\"] (or [?p :name \"Ada\"] [?p :sex :female])]}")
        .await
        .unwrap();
    assert_eq!(result, vec![vec![DataType::Long(1)]]);

    // count with top-level OR: Lovelace OR male → 3
    let result = db
        .query_edn("{:find [(count ?p)] :where [(or [?p :last-name \"Lovelace\"] [?p :sex :male])]}")
        .await
        .unwrap();
    assert_eq!(result, vec![vec![DataType::Long(3)]]);

    // Grouped: gender, count, sum
    let result = db
        .query_edn("{:find [?gender (count ?p) (sum ?age)] :where [[?p :sex ?gender] [?p :age ?age]]}")
        .await
        .unwrap();
    assert_eq!(result.len(), 2, "expected 2 groups, got {:?}", result);
    assert!(result.contains(&vec![
        DataType::Keyword(kw!(:male)),
        DataType::Long(2),
        DataType::Long(45),
    ]));
    assert!(result.contains(&vec![
        DataType::Keyword(kw!(:female)),
        DataType::Long(1),
        DataType::Long(21),
    ]));

    db.close().await.unwrap();
    client.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_aggregate_set_semantics() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;
    define_people_schema(&client).await;

    for (id, name, city) in [(100, "Alice", "NYC"), (101, "Bob", "NYC"), (102, "Carol", "LA")] {
        client
            .execute_tx(vec![TxOp::put(vec![(kw!(:db/id), TxValue::Ref(id.into())), 
                (kw!(:name), name.into()),
                (kw!(:city), city.into()),
            ])])
            .await
            .unwrap();
    }

    let db = client.db().await.unwrap();

    // TODO: do we want Datomic (set → 2) or XTDB (bag → 3) semantics here?
    let result = db
        .query_edn("{:find [(count ?city)] :where [[?p :city ?city]]}")
        .await
        .unwrap();
    assert_eq!(result, vec![vec![DataType::Long(3)]]);

    db.close().await.unwrap();
    client.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_datascript_aggregates() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;
    define_heads_schema(&client).await;

    // Insert monsters with heads
    for (id, heads) in [(100, 3), (101, 1), (102, 1), (103, 1)] {
        client
            .execute_tx(vec![TxOp::put(vec![(kw!(:db/id), TxValue::Ref(id.into())), 
                (kw!(:heads), (heads as i64).into()),
            ])])
            .await
            .unwrap();
    }

    let db = client.db().await.unwrap();

    // All aggregate functions at once
    let result = db
        .query_edn("{:find [(sum ?heads) (min ?heads) (max ?heads) (count ?heads) (count-distinct ?heads)] :where [[?monster :heads ?heads]]}")
        .await
        .unwrap();
    assert_eq!(result.len(), 1, "expected single row, got {:?}", result);
    let row = &result[0];
    assert_eq!(row[0], DataType::Long(6), "sum");
    assert_eq!(row[1], DataType::Long(1), "min");
    assert_eq!(row[2], DataType::Long(3), "max");
    assert_eq!(row[3], DataType::Long(4), "count");
    assert_eq!(row[4], DataType::Long(2), "count-distinct");

    db.close().await.unwrap();
    client.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_aggregate_avg() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;

    for (id, age) in [(100, 21), (101, 22), (102, 23)] {
        client
            .execute_tx(vec![TxOp::put(vec![(kw!(:db/id), TxValue::Ref(id.into())), 
                (kw!(:age), (age as i64).into()),
            ])])
            .await
            .unwrap();
    }

    let db = client.db().await.unwrap();

    let result = db
        .query_edn("{:find [(avg ?age)] :where [[?e :age ?age]]}")
        .await
        .unwrap();
    assert_eq!(result, vec![vec![DataType::Double(22.0)]]);

    db.close().await.unwrap();
    client.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_aggregate_min_max_strings() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;

    for (id, name) in [(100, "Charlie"), (101, "Alice"), (102, "Bob")] {
        client
            .execute_tx(vec![TxOp::put(vec![(kw!(:db/id), TxValue::Ref(id.into())), 
                (kw!(:name), name.into()),
            ])])
            .await
            .unwrap();
    }

    let db = client.db().await.unwrap();

    let result = db
        .query_edn("{:find [(min ?name) (max ?name)] :where [[?e :name ?name]]}")
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][0], DataType::String("Alice".to_string()));
    assert_eq!(result[0][1], DataType::String("Charlie".to_string()));

    db.close().await.unwrap();
    client.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_aggregate_empty_result() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;

    // No data inserted — count over empty result should return [[0]]
    let db = client.db().await.unwrap();

    let result = db
        .query_edn("{:find [(count ?e)] :where [[?e :name \"nobody\"]]}")
        .await
        .unwrap();
    assert_eq!(result, vec![vec![DataType::Long(0)]]);

    db.close().await.unwrap();
    client.close().await.unwrap();
    token.cancel();
}

// ---------------------------------------------------------------------------
// Order + Limit tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_order_and_limit() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;

    // Insert 5 people with different ages
    for (id, name, age) in [
        (100, "Alice", 30),
        (101, "Bob", 20),
        (102, "Carol", 40),
        (103, "Dave", 10),
        (104, "Eve", 50),
    ] {
        client
            .execute_tx(vec![TxOp::put(vec![(kw!(:db/id), TxValue::Ref(id.into())), 
                (kw!(:name), name.into()),
                (kw!(:age), (age as i64).into()),
            ])])
            .await
            .unwrap();
    }

    let db = client.db().await.unwrap();

    // ORDER BY age ascending, LIMIT 3 → youngest 3
    let result = db
        .query_edn("{:find [?name ?age] :where [[?e :name ?name] [?e :age ?age]] :order [[?age :asc]] :limit 3}")
        .await
        .unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0], vec![DataType::String("Dave".to_string()), DataType::Long(10)]);
    assert_eq!(result[1], vec![DataType::String("Bob".to_string()), DataType::Long(20)]);
    assert_eq!(result[2], vec![DataType::String("Alice".to_string()), DataType::Long(30)]);

    // ORDER BY age descending, LIMIT 2 → oldest 2
    let result = db
        .query_edn("{:find [?name ?age] :where [[?e :name ?name] [?e :age ?age]] :order [[?age :desc]] :limit 2}")
        .await
        .unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], vec![DataType::String("Eve".to_string()), DataType::Long(50)]);
    assert_eq!(result[1], vec![DataType::String("Carol".to_string()), DataType::Long(40)]);

    // LIMIT only (no order) — should return exactly 2 rows
    let result = db
        .query_edn("{:find [?name ?age] :where [[?e :name ?name] [?e :age ?age]] :limit 2}")
        .await
        .unwrap();
    assert_eq!(result.len(), 2);

    // ORDER BY only (no limit) — all 5 rows, sorted
    let result = db
        .query_edn("{:find [?name ?age] :where [[?e :name ?name] [?e :age ?age]] :order [[?age :asc]]}")
        .await
        .unwrap();
    assert_eq!(result.len(), 5);
    assert_eq!(result[0], vec![DataType::String("Dave".to_string()), DataType::Long(10)]);
    assert_eq!(result[4], vec![DataType::String("Eve".to_string()), DataType::Long(50)]);

    db.close().await.unwrap();
    client.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_aggregate_min_incompatible_types() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;

    // Insert entity with both name (string) and age (long)
    client
        .execute_tx(vec![TxOp::put(vec![(kw!(:db/id), TxValue::Ref(100_i64.into())), 
            (kw!(:name), "Alice".into()),
            (kw!(:age), 30_i64.into()),
        ])])
        .await
        .unwrap();

    let db = client.db().await.unwrap();

    // OR binds ?v to both string and long values → min should error on incomparable types
    let result = db
        .query_edn("{:find [(min ?v)] :where [(or [?e :name ?v] [?e :age ?v])]}")
        .await;
    assert!(result.is_err(), "min over incompatible types should error");

    db.close().await.unwrap();
    client.close().await.unwrap();
    token.cancel();
}
