use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use log::{error, warn};
use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::future_producer::Delivery;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::Offset;
use rdkafka::util::Timeout;
use rdkafka::Message;
use rdkafka::Timestamp;
use rdkafka::TopicPartitionList;
use tokio::sync::broadcast;

use crate::log::{Record, TxId, TxLog, TxLogReader, TxLogWriter, BOOTSTRAP_RECORD};
use crate::transaction::TxKey;

const PARTITION: i32 = 0;
const MESSAGE_TIMESTAMP_TYPE_CONFIG: &str = "message.timestamp.type";
const LOG_APPEND_TIME: &str = "LogAppendTime";
const RETENTION_MS_CONFIG: &str = "retention.ms";
const RETENTION_BYTES_CONFIG: &str = "retention.bytes";
// Per-message stall budget for reads below the high watermark; generous to cover AutoMQ S3 reads.
const READ_POLL_STALL_TIMEOUT: Duration = Duration::from_secs(10);

fn validate_log_append_time_config(topic: &str, timestamp_type: Option<&str>) -> Result<()> {
    match timestamp_type {
        Some(LOG_APPEND_TIME) => Ok(()),
        Some(other) => bail!(
            "Kafka topic {} must set {}={}, got {}",
            topic,
            MESSAGE_TIMESTAMP_TYPE_CONFIG,
            LOG_APPEND_TIME,
            other
        ),
        None => bail!(
            "Kafka topic {} must expose {}={}",
            topic,
            MESSAGE_TIMESTAMP_TYPE_CONFIG,
            LOG_APPEND_TIME
        ),
    }
}

// Kafka's default 7-day retention would silently delete tx log history.
fn validate_infinite_retention_config(topic: &str, key: &str, value: Option<&str>) -> Result<()> {
    match value {
        Some("-1") => Ok(()),
        Some(other) => bail!(
            "Kafka topic {} must set {}=-1 to retain the full tx log, got {}",
            topic,
            key,
            other
        ),
        None => bail!(
            "Kafka topic {} must expose {}=-1 to retain the full tx log",
            topic,
            key
        ),
    }
}

async fn ensure_tx_log_topic(bootstrap_servers: &str, topic: &str) -> Result<()> {
    let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()
        .context("Failed to create Kafka admin client")?;

    let resources = [ResourceSpecifier::Topic(topic)];
    let mut configs = admin_client
        .describe_configs(&resources, &AdminOptions::new())
        .await
        .context("Failed to describe Kafka topic config")?;
    let config = configs
        .pop()
        .context("Kafka describe configs returned no topic config")?
        .map_err(|e| {
            anyhow::anyhow!("Failed to describe Kafka topic config for {}: {}", topic, e)
        })?;
    let timestamp_type = config
        .get(MESSAGE_TIMESTAMP_TYPE_CONFIG)
        .and_then(|entry| entry.value.as_deref());
    validate_log_append_time_config(topic, timestamp_type)?;
    for key in [RETENTION_MS_CONFIG, RETENTION_BYTES_CONFIG] {
        let value = config.get(key).and_then(|entry| entry.value.as_deref());
        validate_infinite_retention_config(topic, key, value)?;
    }

    let metadata = admin_client
        .inner()
        .fetch_metadata(Some(topic), Duration::from_secs(5))
        .context("Failed to fetch Kafka topic metadata")?;
    let partition_count = metadata
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .with_context(|| format!("Kafka metadata missing topic {}", topic))?
        .partitions()
        .len();
    // tx_ids are offsets of partition 0; extra partitions would break total ordering.
    if partition_count != 1 {
        bail!(
            "Kafka topic {} must have exactly 1 partition, got {}",
            topic,
            partition_count
        );
    }
    Ok(())
}

fn timestamp_to_system_time(offset: i64, timestamp: Timestamp) -> Result<DateTime<Utc>> {
    let timestamp_ms = match timestamp {
        Timestamp::LogAppendTime(ms) => ms,
        _ => {
            // Transaction time must be broker append time
            bail!(
                "Triplox Kafka log requires LogAppendTime at offset {}",
                offset
            );
        }
    };

    // Never fall back to wall-clock time: system_time must be identical on every replay.
    DateTime::from_timestamp_millis(timestamp_ms).with_context(|| {
        format!(
            "Kafka timestamp {} out of range at offset {}",
            timestamp_ms, offset
        )
    })
}

fn message_to_record(msg: &BorrowedMessage<'_>) -> Result<Record> {
    let offset = msg.offset();
    let system_time = timestamp_to_system_time(offset, msg.timestamp())?;
    let payload = msg.payload().unwrap_or_default().to_vec();

    Ok(Record {
        tx_key: TxKey {
            tx_id: offset,
            system_time,
        },
        record: payload,
    })
}

fn delivery_to_tx_key(delivery: &Delivery) -> Result<TxKey> {
    Ok(TxKey {
        tx_id: delivery.offset,
        system_time: timestamp_to_system_time(delivery.offset, delivery.timestamp)?,
    })
}

fn is_kafka_bootstrap_record(record: &Record) -> bool {
    record.tx_key.tx_id == BOOTSTRAP_RECORD.tx_key.tx_id && record.record == BOOTSTRAP_RECORD.record
}

struct LiveConsumer {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Drop for LiveConsumer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            if handle.join().is_err() {
                error!("Kafka live consumer thread panicked while stopping");
            }
        }
    }
}

fn spawn_live_consumer(
    consumer_config: ClientConfig,
    topic: String,
    tx_sender: broadcast::Sender<Record>,
    next_offset: Arc<AtomicI64>,
    start_offset: i64,
) -> Result<LiveConsumer> {
    let thread_name = String::from("triplox-kafka-live");
    let consumer: BaseConsumer = consumer_config
        .clone()
        .set("group.id", format!("triplox-live-{}", std::process::id()))
        .create()
        .context("Failed to create Kafka consumer for live updates")?;

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic, PARTITION, Offset::Offset(start_offset))
        .context("Failed to set Kafka live consumer offset")?;
    consumer
        .assign(&tpl)
        .context("Failed to assign Kafka live consumer partition")?;

    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = stop.clone();
    let handle = std::thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                match consumer.poll(Timeout::After(Duration::from_millis(100))) {
                    Some(Ok(msg)) => match message_to_record(&msg) {
                        Ok(record) => {
                            next_offset.fetch_max(record.tx_key.tx_id + 1, Ordering::Release);
                            if tx_sender.receiver_count() > 0 {
                                if let Err(e) = tx_sender.send(record) {
                                    warn!(
                                        "Failed to send record from kafka log to subscribers: {}",
                                        e
                                    );
                                }
                            }
                        }
                        // Skipping the record would hand subscribers a silent gap;
                        // stalling live updates is the lesser failure.
                        Err(e) => {
                            error!("Kafka live consumer stopping on unreadable record: {}", e);
                            break;
                        }
                    },
                    Some(Err(e)) => error!("Kafka live consumer error: {}", e),
                    None => {}
                }
            }
        })
        .context("Failed to spawn Kafka live consumer thread")?;

    Ok(LiveConsumer {
        stop,
        handle: Some(handle),
    })
}

fn read_records_blocking(
    consumer_config: &ClientConfig,
    topic: &str,
    start_offset: i64,
    limit: u16,
    next_offset: &AtomicI64,
) -> Result<Vec<Record>> {
    // Create a temporary consumer for this read
    let consumer: BaseConsumer = consumer_config
        .clone()
        .set("group.id", format!("triplox-read-{}", start_offset))
        .create()
        .context("Failed to create Kafka consumer for read")?;

    let (_low, high) = consumer
        .fetch_watermarks(topic, PARTITION, Duration::from_secs(5))
        .context("Failed to fetch watermarks")?;
    next_offset.fetch_max(high, Ordering::Release);
    if start_offset >= high {
        return Ok(vec![]);
    }

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, PARTITION, Offset::Offset(start_offset))
        .context("Failed to set partition offset")?;
    consumer
        .assign(&tpl)
        .context("Failed to assign partition")?;

    let end_offset = std::cmp::min(start_offset.saturating_add(limit as i64), high);
    let mut records = Vec::with_capacity((end_offset - start_offset) as usize);

    // Offsets below `high` are known to exist, so a poll timeout is a stalled fetch,
    // not end-of-log. Callers treat a short batch as having reached the end of the
    // log, so a partial result here would make subscribers silently skip records.
    while (records.len() as i64) < (end_offset - start_offset) {
        let expected_offset = start_offset + records.len() as i64;
        match consumer.poll(Timeout::After(READ_POLL_STALL_TIMEOUT)) {
            Some(Ok(msg)) => {
                if msg.offset() != expected_offset {
                    bail!(
                        "Kafka read got offset {}, expected {}",
                        msg.offset(),
                        expected_offset
                    );
                }
                records.push(message_to_record(&msg)?);
            }
            Some(Err(e)) => {
                bail!("Kafka consume error: {}", e);
            }
            None => {
                bail!(
                    "Kafka read stalled at offset {} (high watermark {})",
                    expected_offset,
                    high
                );
            }
        }
    }

    Ok(records)
}

pub struct KafkaLog {
    producer: FutureProducer,
    consumer_config: ClientConfig,
    topic: String,
    tx_sender: broadcast::Sender<Record>,
    next_offset: Arc<AtomicI64>,
    _live_consumer: LiveConsumer,
}

impl KafkaLog {
    pub async fn new(bootstrap_servers: &str, topic: String) -> Result<Self> {
        // Idempotence stops librdkafka's internal retries from duplicating
        // or reordering tx records; it implies acks=all, set here explicitly.
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .create()
            .context("Failed to create Kafka producer")?;

        // An out-of-range offset (e.g. a retention-truncated topic) must surface
        // as an error; the default reset to "latest" silently skips records.
        let mut consumer_config = ClientConfig::new();
        consumer_config
            .set("bootstrap.servers", bootstrap_servers)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "error");

        ensure_tx_log_topic(bootstrap_servers, &topic).await?;

        // Fetch current high watermark to initialize next_offset
        let consumer: BaseConsumer = consumer_config
            .clone()
            .set("group.id", "triplox-init")
            .create()
            .context("Failed to create Kafka consumer for watermark query")?;

        let (_low, high) = consumer
            .fetch_watermarks(&topic, PARTITION, Duration::from_secs(5))
            .context("Failed to fetch watermarks")?;

        let (tx_sender, _) = broadcast::channel(1024);
        let next_offset = Arc::new(AtomicI64::new(high));

        let live_consumer = spawn_live_consumer(
            consumer_config.clone(),
            topic.clone(),
            tx_sender.clone(),
            next_offset.clone(),
            high,
        )?;

        Ok(KafkaLog {
            producer,
            consumer_config,
            topic,
            tx_sender,
            next_offset,
            _live_consumer: live_consumer,
        })
    }
}

impl TxLogReader for KafkaLog {
    async fn read_txs_after(&self, after_tx_id: Option<TxId>, limit: u16) -> Result<Vec<Record>> {
        let start_offset = match after_tx_id {
            None => 0i64,
            Some(id) => id + 1,
        };

        let consumer_config = self.consumer_config.clone();
        let topic = self.topic.clone();
        let next_offset = self.next_offset.clone();

        // BaseConsumer::poll blocks, so run the read off the async runtime.
        tokio::task::spawn_blocking(move || {
            read_records_blocking(&consumer_config, &topic, start_offset, limit, &next_offset)
        })
        .await
        .context("Kafka read task failed")?
    }

    async fn subscribe_txs(&self) -> broadcast::Receiver<Record> {
        self.tx_sender.subscribe()
    }
}

impl TxLogWriter for KafkaLog {
    async fn append_tx(&self, record: Vec<u8>) -> Result<TxKey> {
        let delivery_result = self
            .producer
            .send(
                FutureRecord::<str, _>::to(&self.topic)
                    .partition(PARTITION)
                    .payload(&record),
                Timeout::After(Duration::from_secs(5)),
            )
            .await;

        let delivery = delivery_result
            .map_err(|(e, _message)| e)
            .context("Kafka produce failed")?;
        if delivery.partition != PARTITION {
            bail!(
                "Kafka produced to unexpected partition: expected {}, got {}",
                PARTITION,
                delivery.partition
            );
        }
        let tx_key =
            delivery_to_tx_key(&delivery).context("Failed to read Kafka delivery timestamp")?;
        self.next_offset
            .fetch_max(tx_key.tx_id + 1, Ordering::Release);

        Ok(tx_key)
    }
}

impl TxLog for KafkaLog {
    async fn ensure_bootstrap_record(&self) -> Result<()> {
        let bootstrap_record = BOOTSTRAP_RECORD.clone();

        match self.read_txs_after(None, 1).await?.first() {
            Some(record) if is_kafka_bootstrap_record(record) => return Ok(()),
            Some(record) => {
                bail!(
                    "kafka log starts with non-bootstrap record {:?}",
                    record.tx_key
                );
            }
            None => {}
        }

        // Best-effort early-out; the delivery offset check below is the real arbiter.
        if self.next_offset.load(Ordering::Acquire) != 0 {
            bail!("kafka log has offsets but no readable bootstrap record");
        }

        let delivery_result = self
            .producer
            .send(
                FutureRecord::<str, _>::to(&self.topic)
                    .partition(PARTITION)
                    .payload(&bootstrap_record.record),
                Timeout::After(Duration::from_secs(5)),
            )
            .await;

        let delivery = delivery_result
            .map_err(|(e, _message)| e)
            .context("Kafka bootstrap produce failed")?;
        if delivery.partition != PARTITION {
            bail!(
                "Kafka bootstrap produced to unexpected partition: expected {}, got {}",
                PARTITION,
                delivery.partition
            );
        }
        let offset = delivery.offset;
        self.next_offset.fetch_max(offset + 1, Ordering::Release);
        if offset != bootstrap_record.tx_key.tx_id {
            // Lost a bootstrap race: another writer claimed offset 0 and our empty
            // record is orphaned at `offset` (indexed later as a failed tx).
            return match self.read_txs_after(None, 1).await?.first() {
                Some(record) if is_kafka_bootstrap_record(record) => {
                    warn!(
                        "Kafka bootstrap raced another writer; orphan empty record at offset {}",
                        offset
                    );
                    Ok(())
                }
                first => bail!(
                    "Kafka bootstrap produced to offset {} and offset 0 holds no bootstrap record: {:?}",
                    offset,
                    first.map(|record| record.tx_key)
                ),
            };
        }

        // Offset and payload are checked above; the delivery just needs broker append time.
        delivery_to_tx_key(&delivery)
            .context("Failed to read Kafka bootstrap delivery timestamp")?;
        Ok(())
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::clock::st_from_unix_epoch;

    #[test]
    fn test_kafka_bootstrap_record_allows_broker_timestamp() {
        let mut record = BOOTSTRAP_RECORD.clone();
        record.tx_key.system_time = st_from_unix_epoch(1_780_931_760_255_000);

        assert!(is_kafka_bootstrap_record(&record));
    }

    #[test]
    fn test_kafka_bootstrap_record_rejects_non_bootstrap_payload() {
        let mut record = BOOTSTRAP_RECORD.clone();
        record.record = vec![1];

        assert!(!is_kafka_bootstrap_record(&record));
    }

    #[test]
    fn test_kafka_bootstrap_record_rejects_non_bootstrap_offset() {
        let mut record = BOOTSTRAP_RECORD.clone();
        record.tx_key.tx_id = 1;

        assert!(!is_kafka_bootstrap_record(&record));
    }

    #[test]
    fn test_timestamp_to_system_time_rejects_create_time() {
        let err = timestamp_to_system_time(7, Timestamp::CreateTime(1_000))
            .unwrap_err()
            .to_string();
        assert!(err.contains("requires LogAppendTime"));
        assert!(err.contains("offset 7"));
    }

    #[test]
    fn test_timestamp_to_system_time_rejects_not_available() {
        let err = timestamp_to_system_time(7, Timestamp::NotAvailable)
            .unwrap_err()
            .to_string();

        assert!(err.contains("requires LogAppendTime"));
        assert!(err.contains("offset 7"));
    }

    #[test]
    fn test_timestamp_to_system_time_rejects_out_of_range_millis() {
        let err = timestamp_to_system_time(7, Timestamp::LogAppendTime(i64::MAX))
            .unwrap_err()
            .to_string();

        assert!(err.contains("out of range"));
        assert!(err.contains("offset 7"));
    }

    #[test]
    fn test_delivery_to_tx_key_uses_delivery_offset_and_log_append_time() {
        let delivery = Delivery {
            partition: PARTITION,
            offset: 7,
            timestamp: Timestamp::LogAppendTime(1_780_931_760_255),
        };

        let tx_key = delivery_to_tx_key(&delivery).unwrap();

        assert_eq!(tx_key.tx_id, 7);
        assert_eq!(tx_key.system_time.timestamp_millis(), 1_780_931_760_255);
    }

    #[test]
    fn test_delivery_to_tx_key_rejects_non_log_append_time() {
        let delivery = Delivery {
            partition: PARTITION,
            offset: 7,
            timestamp: Timestamp::CreateTime(1_000),
        };

        let err = delivery_to_tx_key(&delivery).unwrap_err().to_string();

        assert!(err.contains("requires LogAppendTime"));
        assert!(err.contains("offset 7"));
    }

    #[test]
    fn test_validate_log_append_time_config_accepts_log_append_time() {
        validate_log_append_time_config("topic", Some("LogAppendTime")).unwrap();
    }

    #[test]
    fn test_validate_log_append_time_config_rejects_create_time() {
        let err = validate_log_append_time_config("topic", Some("CreateTime"))
            .unwrap_err()
            .to_string();

        assert!(err.contains("message.timestamp.type=LogAppendTime"));
        assert!(err.contains("got CreateTime"));
    }

    #[test]
    fn test_validate_log_append_time_config_rejects_missing_config() {
        let err = validate_log_append_time_config("topic", None)
            .unwrap_err()
            .to_string();

        assert!(err.contains("message.timestamp.type=LogAppendTime"));
    }

    #[test]
    fn test_validate_infinite_retention_config_accepts_minus_one() {
        validate_infinite_retention_config("topic", RETENTION_MS_CONFIG, Some("-1")).unwrap();
    }

    #[test]
    fn test_validate_infinite_retention_config_rejects_finite_retention() {
        let err =
            validate_infinite_retention_config("topic", RETENTION_MS_CONFIG, Some("604800000"))
                .unwrap_err()
                .to_string();

        assert!(err.contains("retention.ms=-1"));
        assert!(err.contains("got 604800000"));
    }

    #[test]
    fn test_validate_infinite_retention_config_rejects_missing_config() {
        let err = validate_infinite_retention_config("topic", RETENTION_BYTES_CONFIG, None)
            .unwrap_err()
            .to_string();

        assert!(err.contains("retention.bytes=-1"));
    }
}

#[cfg(feature = "kafka-integration-test")]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::{subscribe, MockSubscriber};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::RwLock;

    use crate::logging::init;

    fn test_bootstrap_servers() -> Option<String> {
        std::env::var("KAFKA_BOOTSTRAP_SERVERS").ok()
    }

    fn unique_topic() -> String {
        format!("triplox-test-{}", uuid::Uuid::new_v4())
    }

    async fn create_topic(bootstrap: &str, topic: &str) {
        let admin_client: rdkafka::admin::AdminClient<rdkafka::client::DefaultClientContext> =
            ClientConfig::new()
                .set("bootstrap.servers", bootstrap)
                .create()
                .expect("Failed to create admin client");

        use rdkafka::admin::{AdminOptions, NewTopic, TopicReplication};
        let topic_config = NewTopic::new(topic, 1, TopicReplication::Fixed(1))
            .set("message.timestamp.type", "LogAppendTime")
            .set("retention.ms", "-1")
            .set("retention.bytes", "-1");

        admin_client
            .create_topics(&[topic_config], &AdminOptions::new())
            .await
            .expect("Failed to create topic");

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_kafka_log() {
        init();
        let Some(bootstrap) = test_bootstrap_servers() else {
            eprintln!("Skipping: KAFKA_BOOTSTRAP_SERVERS not set");
            return;
        };
        let topic = unique_topic();

        create_topic(&bootstrap, &topic).await;

        let subscriber = Arc::new(RwLock::new(MockSubscriber::new()));

        let log = Arc::new(KafkaLog::new(&bootstrap, topic.clone()).await.unwrap());
        let token = subscribe(log.clone(), None, subscriber.clone()).await;

        let tx_key_0 = log.append_tx(vec![1, 2, 3]).await.unwrap();
        let tx_key_1 = log.append_tx(vec![4, 5, 6]).await.unwrap();
        let tx_key_2 = log.append_tx(vec![7, 8, 9]).await.unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;

        token.cancel();

        let subscriber = subscriber.read().await;

        assert_eq!(subscriber.records.len(), 3);
        assert_eq!(subscriber.records[0].record, vec![1, 2, 3]);
        assert_eq!(subscriber.records[1].record, vec![4, 5, 6]);
        assert_eq!(subscriber.records[2].record, vec![7, 8, 9]);
        assert_eq!(subscriber.records[0].tx_key, tx_key_0);
        assert_eq!(subscriber.records[1].tx_key, tx_key_1);
        assert_eq!(subscriber.records[2].tx_key, tx_key_2);

        let tx_id_1 = subscriber.records[1].tx_key.tx_id;
        drop(subscriber);

        // Subscribe after second transaction
        let subscriber2 = Arc::new(RwLock::new(MockSubscriber::new()));
        let token2 = subscribe(log.clone(), Some(tx_id_1), subscriber2.clone()).await;

        log.append_tx(vec![10, 11, 12]).await.unwrap();
        log.append_tx(vec![13, 14, 15]).await.unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;

        token2.cancel();

        let subscriber2 = subscriber2.read().await;

        assert_eq!(subscriber2.records.len(), 3);
        assert_eq!(subscriber2.records[0].record, vec![7, 8, 9]);
        assert_eq!(subscriber2.records[1].record, vec![10, 11, 12]);
        assert_eq!(subscriber2.records[2].record, vec![13, 14, 15]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_kafka_log_single_record() {
        init();
        let Some(bootstrap) = test_bootstrap_servers() else {
            eprintln!("Skipping: KAFKA_BOOTSTRAP_SERVERS not set");
            return;
        };
        let topic = unique_topic();

        create_topic(&bootstrap, &topic).await;

        let log = Arc::new(KafkaLog::new(&bootstrap, topic.clone()).await.unwrap());

        // Write one transaction
        log.append_tx(vec![1, 2, 3]).await.unwrap();

        // Subscribe from the beginning
        let subscriber = Arc::new(RwLock::new(MockSubscriber::new()));
        let token = subscribe(log.clone(), None, subscriber.clone()).await;

        tokio::time::sleep(Duration::from_millis(500)).await;

        token.cancel();

        let subscriber = subscriber.read().await;
        assert_eq!(
            subscriber.records.len(),
            1,
            "Should process transaction exactly once"
        );
        assert_eq!(subscriber.records[0].record, vec![1, 2, 3]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_kafka_log_bootstrap_record_is_idempotent() {
        init();
        let Some(bootstrap) = test_bootstrap_servers() else {
            eprintln!("Skipping: KAFKA_BOOTSTRAP_SERVERS not set");
            return;
        };
        let topic = unique_topic();
        create_topic(&bootstrap, &topic).await;

        let log = KafkaLog::new(&bootstrap, topic.clone()).await.unwrap();
        log.ensure_bootstrap_record().await.unwrap();
        // Second call takes the fast path: offset 0 already holds the bootstrap record.
        log.ensure_bootstrap_record().await.unwrap();

        let records = log.read_txs_after(None, 10).await.unwrap();
        assert_eq!(records.len(), 1);
        assert!(is_kafka_bootstrap_record(&records[0]));

        // Appends after bootstrap start at offset 1.
        let tx_key = log.append_tx(vec![1, 2, 3]).await.unwrap();
        assert_eq!(tx_key.tx_id, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_kafka_log_observes_broker_appended_records() {
        init();
        let Some(bootstrap) = test_bootstrap_servers() else {
            eprintln!("Skipping: KAFKA_BOOTSTRAP_SERVERS not set");
            return;
        };
        let topic = unique_topic();
        create_topic(&bootstrap, &topic).await;

        let log = Arc::new(KafkaLog::new(&bootstrap, topic.clone()).await.unwrap());
        let subscriber = Arc::new(RwLock::new(MockSubscriber::new()));
        let token = subscribe(log.clone(), None, subscriber.clone()).await;

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Failed to create Kafka producer");
        let payload = vec![42, 43, 44];
        producer
            .send(
                // LogAppendTime topics ignore producer timestamps and use broker append time.
                FutureRecord::<str, _>::to(&topic)
                    .partition(PARTITION)
                    .payload(&payload)
                    .timestamp(1_000),
                Timeout::After(Duration::from_secs(5)),
            )
            .await
            .expect("Kafka produce failed");

        let replayed = log.read_txs_after(None, 10).await.unwrap();
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].record, payload);

        for _ in 0..20 {
            if subscriber.read().await.records.len() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        token.cancel();

        let subscriber = subscriber.read().await;
        assert_eq!(subscriber.records.len(), 1);
        assert_eq!(subscriber.records[0].record, vec![42, 43, 44]);
    }
}
