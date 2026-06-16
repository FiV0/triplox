#![cfg(feature = "remote-test")]

use tempfile::tempdir;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{runners::AsyncRunner, GenericImage, ImageExt};

use edn::kw;
use triplox::config::RemoteStorageConfig;
use triplox::node::{Database, Node, QueryNode, SubmitNode};
use triplox::ops::{DataType, TxOp};
use triplox::schema::test_schema_tx;
use triplox::TransactionResult;

#[tokio::test(flavor = "multi_thread")]
async fn test_remote_node_with_s3_storage() {
    triplox::logging::init();

    // Start RustFS container
    let container = GenericImage::new("rustfs/rustfs", "1.0.0-alpha.93")
        .with_exposed_port(9000.tcp())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/health/ready").with_expected_status_code(200u16),
        ))
        .with_env_var("RUSTFS_ROOT_USER", "minioadmin")
        .with_env_var("RUSTFS_ROOT_PASSWORD", "minioadmin")
        .with_cmd(vec!["server", "/data"])
        .with_startup_timeout(std::time::Duration::from_secs(30))
        .start()
        .await
        .unwrap();

    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let endpoint = format!("http://{}:{}", host, port);

    // Create bucket via aws-sdk-s3
    let config = aws_sdk_s3::config::Builder::new()
        .endpoint_url(&endpoint)
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            "minioadmin",
            "minioadmin",
            None,
            None,
            "test",
        ))
        .force_path_style(true)
        .build();

    let s3_client = aws_sdk_s3::Client::from_conf(config);
    s3_client
        .create_bucket()
        .bucket("triplox")
        .send()
        .await
        .expect("Failed to create bucket");

    // Create remote node with temp dirs for the FileLog and local disk storage.
    let log_dir = tempdir().unwrap();
    let disk_dir = tempdir().unwrap();
    let remote_config = RemoteStorageConfig {
        endpoint,
        bucket: "triplox".to_string(),
        access_key: "minioadmin".to_string(),
        secret_key: "minioadmin".to_string(),
        region: "us-east-1".to_string(),
        cache_path: disk_dir.path().to_path_buf(),
    };
    let node = Node::remote_node(&remote_config, &log_dir.path().join("log"))
        .await
        .unwrap();

    // Define schema
    let result = node.execute_tx(test_schema_tx()).await.unwrap();
    assert!(matches!(result, TransactionResult::TxCommitted(_)));

    // Insert data
    let result = node
        .execute_tx(vec![
            TxOp::Add {
                entity: "alice".into(),
                attribute: kw!(:name),
                value: "alice".into(),
            },
            TxOp::Add {
                entity: "alice".into(),
                attribute: kw!(:age),
                value: 30i64.into(),
            },
        ])
        .await
        .unwrap();
    assert!(matches!(result, TransactionResult::TxCommitted(_)));

    // Query
    let db = node.db().await.unwrap();
    let result = db
        .query("[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]")
        .await
        .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0],
        vec![DataType::String("alice".to_string()), DataType::Long(30),]
    );

    node.close().await.unwrap();
}
