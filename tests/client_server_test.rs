use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use chrono::TimeZone;
use edn::kw;
use edn::symbols::Keyword;
use triplox::client::ClientNode;
use triplox::node::{Database, Node, QueryNode, SubmitNode};
use triplox::ops::{DataType, EntityRef, TxOp};
use triplox::schema::test_schema_tx;
use triplox::server::{DevServer, Server};
use triplox::TransactionResult;

async fn define_base_schema(client: &ClientNode) {
    let result = client.execute_tx(test_schema_tx()).await.unwrap();
    assert!(matches!(result, TransactionResult::TxCommited(_)));
}

/// Start an in-memory HTTP server on a free port.
/// Returns the base URL and a cancellation token.
async fn start_test_server() -> (String, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}", addr);

    let node = Arc::new(Node::memory_node().await);
    let server = Server::new(node);

    let token = CancellationToken::new();
    let server_token = token.clone();
    tokio::spawn(async move {
        let _ = server.listen_on(listener, server_token).await;
    });

    (url, token)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_client_connect() {
    let (addr, token) = start_test_server().await;
    let _client = ClientNode::connect(&addr).await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_tx_and_query() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;

    // Insert a document
    let result = client
        .execute_tx(vec![TxOp::Add {
            entity: "alice".into(),
            attribute: kw!(:name),
            value: "alice".into(),
        }])
        .await
        .unwrap();
    assert!(matches!(result, TransactionResult::TxCommited(_)));

    // Open a DB and query
    let db = client.db().await.unwrap();
    let result = db
        .query("{:find [?name] :where [[?e :name ?name]]}")
        .await
        .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![DataType::String("alice".to_string())]);

    db.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_tx_with_string_ops() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;

    // Mix [:db/add ...] form and {:db/id ...} map form, both as &str.
    let result = client
        .execute_tx(vec![
            "[:db/add \"alice\" :name \"Alice\"]",
            "[:db/add \"alice\" :age 30]",
            "{:db/id \"bob\" :name \"Bob\" :age 42}",
        ])
        .await
        .unwrap();
    assert!(matches!(result, TransactionResult::TxCommited(_)));

    let db = client.db().await.unwrap();
    let result = db
        .query("{:find [?name ?age] :where [[?e :name ?name] [?e :age ?age]]}")
        .await
        .unwrap();

    let mut rows: Vec<(String, i64)> = result
        .into_iter()
        .map(|row| match row.as_slice() {
            [DataType::String(n), DataType::Long(a)] => (n.clone(), *a),
            other => panic!("unexpected row {:?}", other),
        })
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![("Alice".to_string(), 30), ("Bob".to_string(), 42)]
    );

    db.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_submit_tx() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;

    let tx_key = client
        .submit_tx(vec![TxOp::Add {
            entity: "bob".into(),
            attribute: kw!(:name),
            value: "bob".into(),
        }])
        .await
        .unwrap();
    assert!(tx_key.tx_id >= 0);

    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_transactions_and_query() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;

    // Insert two documents in separate transactions
    client
        .execute_tx(vec![TxOp::Add {
            entity: "alice".into(),
            attribute: kw!(:name),
            value: "alice".into(),
        }])
        .await
        .unwrap();

    client
        .execute_tx(vec![TxOp::Add {
            entity: "bob".into(),
            attribute: kw!(:name),
            value: "bob".into(),
        }])
        .await
        .unwrap();

    let db = client.db().await.unwrap();
    let result = db
        .query("{:find [?name] :where [[?e :name ?name]]}")
        .await
        .unwrap();

    assert_eq!(result.len(), 2);
    assert!(result.contains(&vec![DataType::String("alice".to_string())]));
    assert!(result.contains(&vec![DataType::String("bob".to_string())]));

    db.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_open_close_multiple_dbs() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;

    // Insert data
    client
        .execute_tx(vec![TxOp::Add {
            entity: "alice".into(),
            attribute: kw!(:name),
            value: "alice".into(),
        }])
        .await
        .unwrap();

    // Open two DB handles
    let db1 = client.db().await.unwrap();
    let db2 = client.db().await.unwrap();

    // Both should return same data
    let r1 = db1
        .query(r#"{:find [?e] :where [[?e :name "alice"]]}"#)
        .await
        .unwrap();
    let r2 = db2
        .query(r#"{:find [?e] :where [[?e :name "alice"]]}"#)
        .await
        .unwrap();
    assert_eq!(r1.len(), 1);
    assert_eq!(r2.len(), 1);

    db1.close().await.unwrap();
    db2.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_two_connections() {
    let (addr, token) = start_test_server().await;

    // Connection 1 inserts data
    let client1 = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client1).await;
    client1
        .execute_tx(vec![TxOp::Add {
            entity: "alice".into(),
            attribute: kw!(:name),
            value: "alice".into(),
        }])
        .await
        .unwrap();

    // Connection 2 can see the data
    let client2 = ClientNode::connect(&addr).await.unwrap();
    let db = client2.db().await.unwrap();
    let result = db
        .query(r#"{:find [?e] :where [[?e :name "alice"]]}"#)
        .await
        .unwrap();
    assert_eq!(result.len(), 1);

    db.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_tx_returns_tx_key() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;

    let result = client
        .execute_tx(vec![TxOp::Add {
            entity: "alice".into(),
            attribute: kw!(:name),
            value: "alice".into(),
        }])
        .await
        .unwrap();

    match result {
        TransactionResult::TxCommited(tx_key) => {
            assert!(tx_key.tx_id > 0, "tx_id should be positive after indexing");
        }
        _ => panic!("Expected TxCommited"),
    }

    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_as_of() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;

    // First transaction
    let result1 = client
        .execute_tx(vec![TxOp::Add {
            entity: "alice".into(),
            attribute: kw!(:name),
            value: "alice".into(),
        }])
        .await
        .unwrap();
    let tx_key1 = match result1 {
        TransactionResult::TxCommited(tk) => tk,
        _ => panic!("Expected TxCommited"),
    };

    // Second transaction
    client
        .execute_tx(vec![TxOp::Add {
            entity: "bob".into(),
            attribute: kw!(:name),
            value: "bob".into(),
        }])
        .await
        .unwrap();

    // Open DB pinned to tx_key after first tx — should only see alice
    let db = client.db_as_of(tx_key1).await.unwrap();
    let result = db
        .query("{:find [?name] :where [[?e :name ?name]]}")
        .await
        .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![DataType::String("alice".to_string())]);

    db.close().await.unwrap();
    token.cancel();
}

// ---------------------------------------------------------------------------
// DevServer tests
// ---------------------------------------------------------------------------

async fn start_dev_server() -> (String, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}", addr);

    let server = DevServer::new();

    let token = CancellationToken::new();
    let server_token = token.clone();
    tokio::spawn(async move {
        let _ = server.listen_on(listener, server_token).await;
    });

    (url, token)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dev_server_connections_are_isolated() {
    let (addr, token) = start_dev_server().await;

    // Connection 1: define schema and insert data
    let client1 = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client1).await;

    client1
        .execute_tx(vec![TxOp::Add {
            entity: "alice".into(),
            attribute: kw!(:name),
            value: "alice".into(),
        }])
        .await
        .unwrap();

    let db1 = client1.db().await.unwrap();
    let result1 = db1
        .query("{:find [?name] :where [[?e :name ?name]]}")
        .await
        .unwrap();
    assert_eq!(result1.len(), 1, "client1 should see its own data");

    // Connection 2: gets a fresh node, should see nothing
    let client2 = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client2).await;

    let db2 = client2.db().await.unwrap();
    let result2 = db2
        .query("{:find [?name] :where [[?e :name ?name]]}")
        .await
        .unwrap();
    assert_eq!(result2.len(), 0, "client2 should not see client1's data");

    db1.close().await.unwrap();
    db2.close().await.unwrap();
    token.cancel();
}

async fn define_schema_attr(client: &ClientNode, name: &str, vtype: &str) {
    client
        .execute_tx(vec![TxOp::put(vec![
            (
                kw!(:db/ident),
                DataType::Keyword(Keyword::plain(name)).into(),
            ),
            (
                kw!(:db/valueType),
                DataType::Keyword(Keyword::namespaced("db.type", vtype)).into(),
            ),
            (
                kw!(:db/cardinality),
                DataType::Keyword(Keyword::namespaced("db.cardinality", "one")).into(),
            ),
        ])])
        .await
        .unwrap();
}

async fn define_people_schema(client: &ClientNode) {
    define_schema_attr(client, "last-name", "string").await;
    define_schema_attr(client, "sex", "keyword").await;
    define_schema_attr(client, "salary", "long").await;
    define_schema_attr(client, "city", "string").await;
}

async fn define_heads_schema(client: &ClientNode) {
    define_schema_attr(client, "heads", "long").await;
}

/// Mirror of Clojure test-query-using-keywords, exercised through the full
/// client-server wire protocol.
#[tokio::test(flavor = "multi_thread")]
async fn test_query_keyword_value_comparison_via_wire() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;
    define_people_schema(&client).await;

    // Insert 4 people with keyword sex values
    for (name, sex) in [
        ("Ivan", "male"),
        ("Petr", "male"),
        ("Doris", "female"),
        ("Jane", "female"),
    ] {
        client
            .execute_tx(vec![TxOp::put(vec![
                (kw!(:name), name.into()),
                (kw!(:sex), DataType::Keyword(Keyword::plain(sex)).into()),
            ])])
            .await
            .unwrap();
    }

    let db = client.db().await.unwrap();

    // Same clause order as Clojure: name first (binds ?e), sex filter second
    let result = db
        .query("{:find [?name] :where [[?e :name ?name] [?e :sex :male]]}")
        .await
        .unwrap();

    assert_eq!(
        result.len(),
        2,
        "expected only Ivan and Petr, got {:?}",
        result
    );
    assert!(result.contains(&vec![DataType::String("Ivan".to_string())]));
    assert!(result.contains(&vec![DataType::String("Petr".to_string())]));

    db.close().await.unwrap();
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
    for (name, last, sex, age) in [
        ("Ada", "Lovelace", "female", 21),
        ("Alan", "Turing", "male", 22),
        ("Adam", "Smith", "male", 23),
    ] {
        client
            .execute_tx(vec![TxOp::put(vec![
                (kw!(:name), name.into()),
                (kw!(:last-name), last.into()),
                (kw!(:sex), DataType::Keyword(Keyword::plain(sex)).into()),
                (kw!(:age), (age as i64).into()),
            ])])
            .await
            .unwrap();
    }

    let db = client.db().await.unwrap();

    // count with OR: Lovelace AND (name=Ada OR sex=male) -> 1 (only Ada)
    let result = db
        .query(r#"{:find [(count ?p)] :where [[?p :last-name "Lovelace"] (or [?p :name "Ada"] [?p :sex :male])]}"#)
        .await
        .unwrap();
    assert_eq!(result, vec![vec![DataType::Long(1)]]);

    // count with OR: Lovelace AND (name=Ada OR sex=female) -> 1
    let result = db
        .query(r#"{:find [(count ?p)] :where [[?p :last-name "Lovelace"] (or [?p :name "Ada"] [?p :sex :female])]}"#)
        .await
        .unwrap();
    assert_eq!(result, vec![vec![DataType::Long(1)]]);

    // count with top-level OR: Lovelace OR male -> 3
    let result = db
        .query(r#"{:find [(count ?p)] :where [(or [?p :last-name "Lovelace"] [?p :sex :male])]}"#)
        .await
        .unwrap();
    assert_eq!(result, vec![vec![DataType::Long(3)]]);

    // Grouped: gender, count, sum
    let result = db
        .query("{:find [?gender (count ?p) (sum ?age)] :where [[?p :sex ?gender] [?p :age ?age]]}")
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
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_aggregate_set_semantics() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;
    define_people_schema(&client).await;

    for (name, city) in [("Alice", "NYC"), ("Bob", "NYC"), ("Carol", "LA")] {
        client
            .execute_tx(vec![TxOp::put(vec![
                (kw!(:name), name.into()),
                (kw!(:city), city.into()),
            ])])
            .await
            .unwrap();
    }

    let db = client.db().await.unwrap();

    // TODO: do we want Datomic (set -> 2) or XTDB (bag -> 3) semantics here?
    let result = db
        .query("{:find [(count ?city)] :where [[?p :city ?city]]}")
        .await
        .unwrap();
    assert_eq!(result, vec![vec![DataType::Long(3)]]);

    db.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_datascript_aggregates() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;
    define_heads_schema(&client).await;

    // Insert monsters with heads
    for heads in [3, 1, 1, 1] {
        client
            .execute_tx(vec![TxOp::Add {
                entity: "monster".into(),
                attribute: kw!(:heads),
                value: (heads as i64).into(),
            }])
            .await
            .unwrap();
    }

    let db = client.db().await.unwrap();

    // All aggregate functions at once
    let result = db
        .query("{:find [(sum ?heads) (min ?heads) (max ?heads) (count ?heads) (count-distinct ?heads)] :where [[?monster :heads ?heads]]}")
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
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_aggregate_avg() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;

    for age in [21, 22, 23] {
        client
            .execute_tx(vec![TxOp::Add {
                entity: "person".into(),
                attribute: kw!(:age),
                value: (age as i64).into(),
            }])
            .await
            .unwrap();
    }

    let db = client.db().await.unwrap();

    let result = db
        .query("{:find [(avg ?age)] :where [[?e :age ?age]]}")
        .await
        .unwrap();
    assert_eq!(result, vec![vec![DataType::Double(22.0)]]);

    db.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_aggregate_min_max_strings() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;

    for name in ["Charlie", "Alice", "Bob"] {
        client
            .execute_tx(vec![TxOp::Add {
                entity: name.into(),
                attribute: kw!(:name),
                value: name.into(),
            }])
            .await
            .unwrap();
    }

    let db = client.db().await.unwrap();

    let result = db
        .query("{:find [(min ?name) (max ?name)] :where [[?e :name ?name]]}")
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][0], DataType::String("Alice".to_string()));
    assert_eq!(result[0][1], DataType::String("Charlie".to_string()));

    db.close().await.unwrap();
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
        .query(r#"{:find [(count ?e)] :where [[?e :name "nobody"]]}"#)
        .await
        .unwrap();
    assert_eq!(result, vec![vec![DataType::Long(0)]]);

    db.close().await.unwrap();
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
    for (name, age) in [
        ("Alice", 30),
        ("Bob", 20),
        ("Carol", 40),
        ("Dave", 10),
        ("Eve", 50),
    ] {
        client
            .execute_tx(vec![TxOp::put(vec![
                (kw!(:name), name.into()),
                (kw!(:age), (age as i64).into()),
            ])])
            .await
            .unwrap();
    }

    let db = client.db().await.unwrap();

    // ORDER BY age ascending, LIMIT 3 -> youngest 3
    let result = db
        .query("{:find [?name ?age] :where [[?e :name ?name] [?e :age ?age]] :order [[?age :asc]] :limit 3}")
        .await
        .unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(
        result[0],
        vec![DataType::String("Dave".to_string()), DataType::Long(10)]
    );
    assert_eq!(
        result[1],
        vec![DataType::String("Bob".to_string()), DataType::Long(20)]
    );
    assert_eq!(
        result[2],
        vec![DataType::String("Alice".to_string()), DataType::Long(30)]
    );

    // ORDER BY age descending, LIMIT 2 -> oldest 2
    let result = db
        .query("{:find [?name ?age] :where [[?e :name ?name] [?e :age ?age]] :order [[?age :desc]] :limit 2}")
        .await
        .unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(
        result[0],
        vec![DataType::String("Eve".to_string()), DataType::Long(50)]
    );
    assert_eq!(
        result[1],
        vec![DataType::String("Carol".to_string()), DataType::Long(40)]
    );

    // LIMIT only (no order) — should return exactly 2 rows
    let result = db
        .query("{:find [?name ?age] :where [[?e :name ?name] [?e :age ?age]] :limit 2}")
        .await
        .unwrap();
    assert_eq!(result.len(), 2);

    // ORDER BY only (no limit) — all 5 rows, sorted
    let result = db
        .query("{:find [?name ?age] :where [[?e :name ?name] [?e :age ?age]] :order [[?age :asc]]}")
        .await
        .unwrap();
    assert_eq!(result.len(), 5);
    assert_eq!(
        result[0],
        vec![DataType::String("Dave".to_string()), DataType::Long(10)]
    );
    assert_eq!(
        result[4],
        vec![DataType::String("Eve".to_string()), DataType::Long(50)]
    );

    db.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_aggregate_min_incompatible_types() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;

    // Insert entity with both name (string) and age (long)
    client
        .execute_tx(vec![TxOp::put(vec![
            (kw!(:name), "Alice".into()),
            (kw!(:age), 30_i64.into()),
        ])])
        .await
        .unwrap();

    let db = client.db().await.unwrap();

    // OR binds ?v to both string and long values -> min should error on incomparable types
    let result = db
        .query("{:find [(min ?v)] :where [(or [?e :name ?v] [?e :age ?v])]}")
        .await;
    assert!(result.is_err(), "min over incompatible types should error");

    db.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_join_ref_with_plain_long() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;

    // Insert Bob
    client
        .execute_tx(vec![TxOp::put(vec![(kw!(:name), "Bob".into())])])
        .await
        .unwrap();

    // Query for Bob's entity id
    let db = client.db().await.unwrap();
    let result = db
        .query(r#"{:find [?e] :where [[?e :name "Bob"]]}"#)
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    let bob_id = match &result[0][0] {
        DataType::Long(id) => *id,
        other => panic!("Expected Long entity ID, got {:?}", other),
    };
    db.close().await.unwrap();

    // Insert Alice with :follows pointing to Bob's entity id
    client
        .execute_tx(vec![TxOp::put(vec![
            (kw!(:name), "Alice".into()),
            (kw!(:follows), DataType::Long(bob_id)),
        ])])
        .await
        .unwrap();

    // Join through the ref: find the name of who Alice follows
    let db = client.db().await.unwrap();
    let result = db
        .query(r#"{:find [?friend-name] :where [[?e :name "Alice"] [?e :follows ?f] [?f :name ?friend-name]]}"#)
        .await
        .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![DataType::String("Bob".to_string())]);

    db.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_upsert_with_resolved_entity_id() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;

    // Insert entity with auto-assigned ID
    client
        .execute_tx(vec![TxOp::put(vec![
            (kw!(:name), "alice".into()),
            (kw!(:age), 30_i64.into()),
        ])])
        .await
        .unwrap();

    // Discover the auto-assigned entity ID
    let db = client.db().await.unwrap();
    let result = db
        .query("{:find [?e] :where [[?e :name \"alice\"]]}")
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    let entity_id = match &result[0][0] {
        DataType::Long(id) => *id,
        other => panic!("Expected Long entity ID, got {:?}", other),
    };
    db.close().await.unwrap();

    // Upsert: update age using the discovered entity ID
    client
        .execute_tx(vec![TxOp::Add {
            entity: EntityRef::Id(entity_id),
            attribute: kw!(:age),
            value: 31_i64.into(),
        }])
        .await
        .unwrap();

    // Verify: alice should now have age 31 (cardinality-one retracted 30)
    let db = client.db().await.unwrap();
    let result = db
        .query("{:find [?name ?age] :where [[?e :name ?name] [?e :age ?age]]}")
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0],
        vec![DataType::String("alice".to_string()), DataType::Long(31)]
    );

    db.close().await.unwrap();
    token.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_year_and_month_extraction() {
    let (addr, token) = start_test_server().await;
    let client = ClientNode::connect(&addr).await.unwrap();
    define_base_schema(&client).await;
    define_schema_attr(&client, "birthday", "instant").await;

    let dt = chrono::Utc.with_ymd_and_hms(2024, 6, 15, 12, 0, 0).unwrap();
    client
        .execute_tx(vec![
            TxOp::Add {
                entity: "alice".into(),
                attribute: kw!(:name),
                value: "alice".into(),
            },
            TxOp::Add {
                entity: "alice".into(),
                attribute: kw!(:birthday),
                value: DataType::Instant(dt),
            },
        ])
        .await
        .unwrap();

    let db = client.db().await.unwrap();

    // Test year extraction
    let result = db
        .query("{:find [?y] :where [[?e :birthday ?d] [(year ?d) ?y]]}")
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![DataType::Long(2024)]);

    // Test month extraction
    let result = db
        .query("{:find [?m] :where [[?e :birthday ?d] [(month ?d) ?m]]}")
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![DataType::Long(6)]);

    db.close().await.unwrap();
    token.cancel();
}
