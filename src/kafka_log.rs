use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use log::{error, warn};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::Offset;
use rdkafka::util::Timeout;
use rdkafka::Message;
use rdkafka::Timestamp;
use rdkafka::TopicPartitionList;
use tokio::sync::{broadcast, Mutex};

use crate::log::{Record, TxId, TxLog, TxLogReader, TxLogWriter, BOOTSTRAP_RECORD};
use crate::transaction::TxKey;

const PARTITION: i32 = 0;
static NEXT_CONSUMER_ID: AtomicU64 = AtomicU64::new(0);

pub struct KafkaLog {
    producer: FutureProducer,
    consumer_config: ClientConfig,
    topic: String,
    tx_sender: broadcast::Sender<Record>,
    append_lock: Mutex<()>,
    next_offset: Arc<AtomicI64>,
    _live_consumer: LiveConsumer,
}

impl KafkaLog {
    pub async fn new(bootstrap_servers: &str, topic: String) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .create()
            .context("Failed to create Kafka producer")?;

        let mut consumer_config = ClientConfig::new();
        consumer_config
            .set("bootstrap.servers", bootstrap_servers)
            .set("enable.auto.commit", "false");

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
            append_lock: Mutex::new(()),
            next_offset,
            _live_consumer: live_consumer,
        })
    }
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

fn timestamp_to_system_time(offset: i64, timestamp: Timestamp) -> Result<DateTime<Utc>> {
    let timestamp_ms = match timestamp {
        Timestamp::LogAppendTime(ms) => ms,
        _ => {
            // Transaction time must be broker append time
            bail!("Triplox Kafka log requires LogAppendTime at offset {}", offset);
        }
    };

    Ok(DateTime::from_timestamp_millis(timestamp_ms).unwrap_or_else(Utc::now))
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

fn is_kafka_bootstrap_record(record: &Record) -> bool {
    record.tx_key.tx_id == BOOTSTRAP_RECORD.tx_key.tx_id && record.record == BOOTSTRAP_RECORD.record
}

fn spawn_live_consumer(
    consumer_config: ClientConfig,
    topic: String,
    tx_sender: broadcast::Sender<Record>,
    next_offset: Arc<AtomicI64>,
    start_offset: i64,
) -> Result<LiveConsumer> {
    let consumer_id = NEXT_CONSUMER_ID.fetch_add(1, Ordering::Relaxed);
    let thread_name = format!("triplox-kafka-live-{}", consumer_id);
    let consumer: BaseConsumer = consumer_config
        .clone()
        .set(
            "group.id",
            format!("triplox-live-{}-{}", std::process::id(), consumer_id),
        )
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
                        Err(e) => error!("Kafka live consumer record error: {}", e),
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

impl TxLogReader for KafkaLog {
    async fn read_txs_after(&self, after_tx_id: Option<TxId>, limit: u16) -> Result<Vec<Record>> {
        let start_offset = match after_tx_id {
            None => 0i64,
            Some(id) => id + 1,
        };

        // Create a temporary consumer for this read
        let consumer: BaseConsumer = self
            .consumer_config
            .clone()
            .set("group.id", format!("triplox-read-{}", start_offset))
            .create()
            .context("Failed to create Kafka consumer for read")?;

        let (_low, high) = consumer
            .fetch_watermarks(&self.topic, PARTITION, Duration::from_secs(5))
            .context("Failed to fetch watermarks")?;
        self.next_offset.fetch_max(high, Ordering::Release);
        if start_offset >= high {
            return Ok(vec![]);
        }

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&self.topic, PARTITION, Offset::Offset(start_offset))
            .context("Failed to set partition offset")?;
        consumer
            .assign(&tpl)
            .context("Failed to assign partition")?;

        let mut records = Vec::new();
        let end_offset = std::cmp::min(start_offset + limit as i64, high);

        while (records.len() as i64) < (end_offset - start_offset) {
            match consumer.poll(Timeout::After(Duration::from_secs(2))) {
                Some(Ok(msg)) => {
                    records.push(message_to_record(&msg)?);
                }
                Some(Err(e)) => {
                    bail!("Kafka consume error: {}", e);
                }
                None => {
                    // Poll timeout — no more messages available
                    break;
                }
            }
        }

        Ok(records)
    }

    async fn subscribe_txs(&self) -> broadcast::Receiver<Record> {
        self.tx_sender.subscribe()
    }
}

fn read_record_at_offset(
    consumer_config: &ClientConfig,
    topic: &str,
    offset: i64,
    timeout: Duration,
) -> Result<Record> {
    let consumer_id = NEXT_CONSUMER_ID.fetch_add(1, Ordering::Relaxed);
    let consumer: BaseConsumer = consumer_config
        .clone()
        .set(
            "group.id",
            format!(
                "triplox-append-confirm-{}-{}",
                std::process::id(),
                consumer_id
            ),
        )
        .create()
        .context("Failed to create Kafka consumer for append confirmation")?;

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, PARTITION, Offset::Offset(offset))
        .context("Failed to set append confirmation offset")?;
    consumer
        .assign(&tpl)
        .context("Failed to assign append confirmation partition")?;

    match consumer.poll(Timeout::After(timeout)) {
        Some(Ok(msg)) if msg.offset() == offset => message_to_record(&msg),
        Some(Ok(msg)) => bail!(
            "Kafka append confirmation read unexpected offset: expected {}, got {}",
            offset,
            msg.offset()
        ),
        Some(Err(e)) => Err(e).context("Kafka append confirmation consume failed"),
        None => bail!(
            "Timed out reading Kafka append confirmation at offset {}",
            offset
        ),
    }
}

impl TxLogWriter for KafkaLog {
    async fn append_tx(&self, record: Vec<u8>) -> Result<TxKey> {
        let _append_guard = self.append_lock.lock().await;

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
        let offset = delivery.offset;

        let consumer_config = self.consumer_config.clone();
        let topic = self.topic.clone();
        let tx_key = tokio::task::spawn_blocking(move || {
            read_record_at_offset(&consumer_config, &topic, offset, Duration::from_secs(5))
        })
        .await
        .context("Kafka append confirmation task failed")?
        .context("Failed to read Kafka append timestamp")?
        .tx_key;
        self.next_offset.fetch_max(offset + 1, Ordering::Release);

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

        let _append_guard = self.append_lock.lock().await;
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
        if offset != bootstrap_record.tx_key.tx_id {
            bail!(
                "Kafka bootstrap produced to unexpected offset: expected {}, got {}",
                bootstrap_record.tx_key.tx_id,
                offset
            );
        }

        let record = read_record_at_offset(
            &self.consumer_config,
            &self.topic,
            offset,
            Duration::from_secs(5),
        )
        .context("Failed to read Kafka bootstrap record")?;
        if !is_kafka_bootstrap_record(&record) {
            bail!("Kafka bootstrap record did not match reserved bootstrap record");
        }
        self.next_offset.fetch_max(offset + 1, Ordering::Release);
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
        assert!(err.contains("require LogAppendTime"));
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
            .set("message.timestamp.type", "LogAppendTime");

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
