#![cfg(feature = "remote-test")]

use slatedb::object_store::aws::AmazonS3Builder;
use slatedb::object_store::path::Path;
use slatedb::object_store::{ObjectStoreExt, WriteMultipart};
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

async fn check_multipart_round_trip(endpoint: &str) {
    let store = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_bucket_name("triplox")
        .with_access_key_id("rustfsadmin")
        .with_secret_access_key("rustfsadmin")
        .with_region("us-east-1")
        .with_allow_http(true)
        .build()
        .unwrap();
    // Exercise SlateDB's S3 client with two full parts and a short final part.
    let part_size = 5 * 1024 * 1024;
    let data: Vec<u8> = (0..2 * part_size + 12345)
        .map(|i| ((i / part_size + i % 251) % 256) as u8)
        .collect();
    let path = Path::from("multipart-regression");
    let mut upload = WriteMultipart::new(store.put_multipart(&path).await.unwrap());
    upload.write(&data);
    upload.finish().await.unwrap();

    let downloaded = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(downloaded.len(), data.len());
    assert!(downloaded.as_ref() == data.as_slice());
    let range = part_size - 16..part_size + 16;
    let downloaded = store
        .get_range(&path, range.start as u64..range.end as u64)
        .await
        .unwrap();
    assert_eq!(downloaded.as_ref(), &data[range]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_remote_node_with_s3_storage() {
    triplox::logging::init();

    // Start RustFS container
    let container = GenericImage::new("rustfs/rustfs", "1.0.0")
        .with_exposed_port(9000.tcp())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/health/ready").with_expected_status_code(200u16),
        ))
        .with_env_var("RUSTFS_ACCESS_KEY", "rustfsadmin")
        .with_env_var("RUSTFS_SECRET_KEY", "rustfsadmin")
        .with_cmd(vec!["/data"])
        .with_startup_timeout(std::time::Duration::from_secs(60))
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
            "rustfsadmin",
            "rustfsadmin",
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

    check_multipart_round_trip(&endpoint).await;

    // Create remote node with temp dirs for the FileLog and local disk storage.
    let log_dir = tempdir().unwrap();
    let disk_dir = tempdir().unwrap();
    let remote_config = RemoteStorageConfig {
        endpoint,
        bucket: "triplox".to_string(),
        access_key: "rustfsadmin".to_string(),
        secret_key: "rustfsadmin".to_string(),
        region: "us-east-1".to_string(),
        cache_path: disk_dir.path().to_path_buf(),
        wal_flush_interval_us: std::num::NonZeroU64::new(25_000).unwrap(),
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
        .query(
            "{:find [?name ?age]
              :where [[?e :name ?name]
                      [?e :age ?age]]}",
        )
        .await
        .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0],
        vec![DataType::String("alice".to_string()), DataType::Long(30),]
    );

    node.close().await.unwrap();

    // Reopen with an empty cache so reads must recover persisted objects.
    let cold_cache = tempdir().unwrap();
    let remote_config = RemoteStorageConfig {
        cache_path: cold_cache.path().to_path_buf(),
        ..remote_config
    };
    let node = Node::remote_node(&remote_config, &log_dir.path().join("log"))
        .await
        .unwrap();
    let reopened = node
        .db()
        .await
        .unwrap()
        .query(
            "{:find [?name ?age]
              :where [[?e :name ?name]
                      [?e :age ?age]]}",
        )
        .await
        .unwrap();
    assert_eq!(reopened, result);
    node.close().await.unwrap();
}
