use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Timelike, Utc};
use log::warn;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::Offset;
use rdkafka::util::Timeout;
use rdkafka::Message;
use rdkafka::TopicPartitionList;
use tokio::sync::{broadcast, Mutex};

use crate::clock::SystemTimeSource;
use crate::log::{Record, TxId, TxLog, TxLogReader, TxLogWriter};
use crate::transaction::TxKey;

const PARTITION: i32 = 0;

pub struct KafkaLog {
    producer: FutureProducer,
    consumer_config: ClientConfig,
    topic: String,
    tx_sender: broadcast::Sender<Record>,
    clock: Mutex<Box<dyn SystemTimeSource>>,
    next_offset: AtomicI64,
}

impl KafkaLog {
    pub async fn new(
        bootstrap_servers: &str,
        topic: String,
        clock: Box<dyn SystemTimeSource>,
    ) -> Result<Self> {
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

        Ok(KafkaLog {
            producer,
            consumer_config,
            topic,
            tx_sender,
            clock: Mutex::new(clock),
            next_offset: AtomicI64::new(high),
        })
    }
}

impl TxLogReader for KafkaLog {
    async fn read_txs_after(&self, after_tx_id: Option<TxId>, limit: u16) -> Result<Vec<Record>> {
        let start_offset = match after_tx_id {
            None => 0i64,
            Some(id) => id + 1,
        };

        let high = self.next_offset.load(Ordering::Acquire);
        if start_offset >= high {
            return Ok(vec![]);
        }

        // Create a temporary consumer for this read
        let consumer: BaseConsumer = self
            .consumer_config
            .clone()
            .set("group.id", &format!("triplox-read-{}", start_offset))
            .create()
            .context("Failed to create Kafka consumer for read")?;

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
                    let offset = msg.offset();
                    let timestamp_ms = match msg.timestamp() {
                        rdkafka::Timestamp::CreateTime(ms) => ms,
                        rdkafka::Timestamp::LogAppendTime(ms) => ms,
                        rdkafka::Timestamp::NotAvailable => {
                            bail!("Kafka message at offset {} has no timestamp", offset);
                        }
                    };
                    let system_time =
                        DateTime::from_timestamp_millis(timestamp_ms).unwrap_or_else(Utc::now);

                    let payload = msg.payload().unwrap_or_default().to_vec();

                    records.push(Record {
                        tx_key: TxKey {
                            tx_id: offset,
                            system_time,
                        },
                        record: payload,
                    });
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

impl TxLogWriter for KafkaLog {
    async fn append_tx(&self, record: Vec<u8>) -> TxKey {
        // Truncate to millisecond precision to match Kafka timestamp resolution
        let mut clock = self.clock.lock().await;
        let mut system_time = clock.now();
        drop(clock);
        system_time = system_time
            .with_nanosecond((system_time.timestamp_subsec_nanos() / 1_000_000) * 1_000_000)
            .expect("truncated nanoseconds should always be valid");

        let timestamp_ms = system_time.timestamp_millis();

        let delivery_result = self
            .producer
            .send(
                FutureRecord::<str, _>::to(&self.topic)
                    .payload(&record)
                    .timestamp(timestamp_ms),
                Timeout::After(Duration::from_secs(5)),
            )
            .await;

        let (partition, offset) = delivery_result.expect("Kafka produce failed");
        assert_eq!(partition, PARTITION, "Unexpected partition assignment");

        let tx_key = TxKey {
            tx_id: offset,
            system_time,
        };

        let log_record = Record { tx_key, record };

        self.next_offset.store(offset + 1, Ordering::Release);

        if let Err(e) = self.tx_sender.send(log_record) {
            warn!("Failed to send record from kafka log to subscribers: {}", e);
        }

        tx_key
    }
}

impl TxLog for KafkaLog {}

#[cfg(feature = "kafka-integration-test")]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{st_from_unix_epoch, MockClock};
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_kafka_log() {
        init();
        let Some(bootstrap) = test_bootstrap_servers() else {
            eprintln!("Skipping: KAFKA_BOOTSTRAP_SERVERS not set");
            return;
        };
        let topic = unique_topic();

        // Create the topic with a single partition
        let admin_client: rdkafka::admin::AdminClient<rdkafka::client::DefaultClientContext> =
            ClientConfig::new()
                .set("bootstrap.servers", &bootstrap)
                .create()
                .expect("Failed to create admin client");

        use rdkafka::admin::{AdminOptions, NewTopic, TopicReplication};
        admin_client
            .create_topics(
                &[NewTopic::new(&topic, 1, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            )
            .await
            .expect("Failed to create topic");

        // Wait for topic to be ready
        tokio::time::sleep(Duration::from_secs(1)).await;

        let subscriber = Arc::new(RwLock::new(MockSubscriber::new()));
        let clock = MockClock::new(vec![
            st_from_unix_epoch(1_000_000), // 1 second (ms-aligned)
            st_from_unix_epoch(2_000_000), // 2 seconds
            st_from_unix_epoch(3_000_000), // 3 seconds
            st_from_unix_epoch(4_000_000),
            st_from_unix_epoch(5_000_000),
        ]);

        let log = Arc::new(
            KafkaLog::new(&bootstrap, topic.clone(), Box::new(clock))
                .await
                .unwrap(),
        );
        let token = subscribe(log.clone(), None, subscriber.clone()).await;

        log.append_tx(vec![1, 2, 3]).await;
        log.append_tx(vec![4, 5, 6]).await;
        log.append_tx(vec![7, 8, 9]).await;

        tokio::time::sleep(Duration::from_millis(500)).await;

        token.cancel();

        let subscriber = subscriber.read().await;

        assert_eq!(subscriber.records.len(), 3);
        assert_eq!(subscriber.records[0].record, vec![1, 2, 3]);
        assert_eq!(subscriber.records[1].record, vec![4, 5, 6]);
        assert_eq!(subscriber.records[2].record, vec![7, 8, 9]);

        let tx_id_1 = subscriber.records[1].tx_key.tx_id;
        drop(subscriber);

        // Subscribe after second transaction
        let subscriber2 = Arc::new(RwLock::new(MockSubscriber::new()));
        let token2 = subscribe(log.clone(), Some(tx_id_1), subscriber2.clone()).await;

        log.append_tx(vec![10, 11, 12]).await;
        log.append_tx(vec![13, 14, 15]).await;

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

        let admin_client: rdkafka::admin::AdminClient<rdkafka::client::DefaultClientContext> =
            ClientConfig::new()
                .set("bootstrap.servers", &bootstrap)
                .create()
                .expect("Failed to create admin client");

        use rdkafka::admin::{AdminOptions, NewTopic, TopicReplication};
        admin_client
            .create_topics(
                &[NewTopic::new(&topic, 1, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            )
            .await
            .expect("Failed to create topic");

        tokio::time::sleep(Duration::from_secs(1)).await;

        let clock = MockClock::new(vec![
            st_from_unix_epoch(1_000_000),
            st_from_unix_epoch(2_000_000),
        ]);

        let log = Arc::new(
            KafkaLog::new(&bootstrap, topic.clone(), Box::new(clock))
                .await
                .unwrap(),
        );

        // Write one transaction
        log.append_tx(vec![1, 2, 3]).await;

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
}
