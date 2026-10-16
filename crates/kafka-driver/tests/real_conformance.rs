//! Mock/Real behavioural-alignment harness (T077).
//!
//! The mock driver is the reference the UI is built against, so every difference
//! between it and a real cluster must be a written-down decision, not an accident.
//! This runs one scripted pass over the driver surface against both drivers and
//! demands the same answers, then pins the intended differences (R30, R33).
//!
//! Needs a live broker: `KAFKA_CONFORMANCE_BOOTSTRAP=localhost:9092 cargo test
//! -p kafka-driver --features real -- --ignored`.

use kafka_core::cluster::ClusterConfig;
use kafka_core::error::DriverError;
use kafka_core::group::{CommitOffsetsReq, ListGroupOffsetsReq, ResetOffsetsReq};
use kafka_core::message::{OffsetStart, ProduceMessage, ProduceReq, SubscribeReq, SubscriptionId};
use kafka_core::topic::{
    CreateTopicReq, DeleteTopicsReq, DescribeTopicsReq, ListOffsetsReq, OffsetSpec, TopicDetail, TopicPartition,
    UpdateTopicConfigReq,
};
use kafka_driver::driver::{KafkaDriver, driver_for};

/// Holds the real bootstrap server; unset means "this harness does not run".
const ENV_BOOTSTRAP: &str = "KAFKA_CONFORMANCE_BOOTSTRAP";
/// Why the ignored tests refuse to run.
const SKIP: &str = "needs a live broker; set KAFKA_CONFORMANCE_BOOTSTRAP and run with --ignored";

const PARTITIONS: u32 = 3;
/// The topic the scripted pass creates and drops again.
const TOPIC: &str = "t077-conformance";
/// A name no broker in either driver should know.
const GHOST: &str = "t077-ghost";
/// The topic the drift test creates, kept apart from `TOPIC` and `GHOST`.
const DRIFT: &str = "t077-drift";

/// The real driver's config, or `None` when the harness is not enabled.
fn real_config() -> Option<ClusterConfig> {
    let server = std::env::var(ENV_BOOTSTRAP).ok()?;
    let server = server.trim();
    (!server.is_empty()).then(|| ClusterConfig::real("t077-real", [server]))
}

/// `ok`, or the error's kind — the granularity both drivers can promise; the
/// message text and the offending field can legitimately differ.
fn label<T>(result: &Result<T, DriverError>) -> String {
    match result {
        Ok(_) => "ok".to_string(),
        Err(error) => format!("{:?}", error.kind()),
    }
}

fn create(topic: &str) -> CreateTopicReq {
    CreateTopicReq {
        name: topic.to_string(),
        num_partitions: PARTITIONS,
        replication_factor: 1,
        configs: [("retention.ms".to_string(), "60000".to_string())].into_iter().collect(),
        ..CreateTopicReq::default()
    }
}

fn describe(topic: &str) -> DescribeTopicsReq {
    DescribeTopicsReq { names: vec![topic.to_string()], include_internal: false }
}

fn delete(topic: &str) -> DeleteTopicsReq {
    DeleteTopicsReq { names: vec![topic.to_string()] }
}

fn config_set(topic: &str, set: &[(&str, &str)], unset: &[&str]) -> UpdateTopicConfigReq {
    UpdateTopicConfigReq {
        topic: topic.to_string(),
        set: set.iter().map(|(key, value)| (key.to_string(), value.to_string())).collect(),
        unset: unset.iter().map(|key| key.to_string()).collect(),
    }
}

/// `describe_topics` for the single topic this harness works with.
async fn one(driver: &dyn KafkaDriver) -> TopicDetail {
    driver.describe_topics(&describe(TOPIC)).await.expect("describe_topics should succeed").remove(0)
}

/// What `describe_topics` reports for one configuration key.
fn setting(detail: &TopicDetail, key: &str) -> String {
    detail.configs.get(key).cloned().unwrap_or_else(|| "absent".to_string())
}

/// The `resolved_offset` of every partition, in partition order.
async fn marks(driver: &dyn KafkaDriver, spec: OffsetSpec) -> Vec<i64> {
    let partitions = (0..PARTITIONS).map(|partition| TopicPartition::new(TOPIC, partition)).collect();
    let mut ranges = driver.list_offsets(&ListOffsetsReq { partitions, spec }).await.expect("list_offsets");
    ranges.sort_by_key(|range| range.partition);
    ranges.into_iter().map(|range| range.resolved_offset.unwrap_or(-1)).collect()
}

/// Everything a bounded read hands back, sorted so partition order cannot matter.
async fn read_back(driver: &dyn KafkaDriver) -> Vec<String> {
    let req =
        SubscribeReq { topic: TOPIC.to_string(), start: OffsetStart::Earliest, follow: false, ..Default::default() };
    let mut stream = driver.subscribe(req).await.expect("subscribe");
    let mut values = Vec::new();
    while let Some(item) = stream.next().await {
        match item {
            Ok(event) => values.push(String::from_utf8_lossy(&event.value).to_string()),
            Err(_) => break,
        }
    }
    values.sort();
    values
}

/// A subscription that starts past the end of partition 0.
fn past_the_end() -> SubscribeReq {
    SubscribeReq {
        subscription_id: SubscriptionId::new(),
        topic: TOPIC.to_string(),
        partitions: vec![0],
        start: OffsetStart::Offset(i64::MAX),
        follow: false,
        ..Default::default()
    }
}

/// One scripted pass, reduced to the facts both drivers must agree on. Anything a
/// real broker cannot know (cluster id, controller, leader epoch, commit time) is
/// left out, and the partition a message lands on is forced, so the round-robin
/// mock and the sticky real partitioner cannot disagree by accident.
async fn transcript(driver: &dyn KafkaDriver) -> Vec<String> {
    let mut log = Vec::new();

    // A topic left behind by an earlier run must not colour this one.
    let _ = driver.delete_topics(&delete(TOPIC)).await;

    let info = driver.cluster_info().await.expect("cluster_info");
    log.push(format!("brokers present: {}", !info.brokers.is_empty()));

    log.push(format!("create: {}", label(&driver.create_topic(&create(TOPIC)).await)));
    log.push(format!("create twice: {}", label(&driver.create_topic(&create(TOPIC)).await)));

    let detail = one(driver).await;
    log.push(format!("partitions: {}", detail.partitions.len()));
    log.push(format!("replicas: {}", detail.partitions[0].replica_ids.len()));
    log.push(format!("retention.ms: {}", setting(&detail, "retention.ms")));

    let messages: Vec<ProduceMessage> = (0..PARTITIONS)
        .map(|partition| ProduceMessage { partition: Some(partition), ..ProduceMessage::text(format!("p{partition}")) })
        .collect();
    let produced = driver.produce(ProduceReq { topic: TOPIC.to_string(), messages }).await.expect("produce");
    log.push(format!("produced: {}", produced.len()));
    log.push(format!("delivery errors: {}", produced.iter().filter(|result| result.error.is_some()).count()));

    log.push(format!("earliest: {:?}", marks(driver, OffsetSpec::Earliest).await));
    log.push(format!("latest: {:?}", marks(driver, OffsetSpec::Latest).await));
    log.push(format!("read back: {:?}", read_back(driver).await));

    let write = driver.update_topic_config(&config_set(TOPIC, &[("retention.ms", "120000")], &[])).await;
    log.push(format!("write: {}", label(&write)));
    log.push(format!("retention.ms now: {}", setting(&one(driver).await, "retention.ms")));
    let unset = driver.update_topic_config(&config_set(TOPIC, &[], &["retention.ms"])).await;
    log.push(format!("unset: {}", label(&unset)));
    log.push(format!("retention.ms gone: {}", setting(&one(driver).await, "retention.ms")));

    // Rejections both drivers must produce, for the same reason.
    log.push(format!("blank topic: {}", label(&driver.create_topic(&create("")).await)));
    let empty = ProduceReq { topic: TOPIC.to_string(), messages: Vec::new() };
    log.push(format!("empty batch: {}", label(&driver.produce(empty).await)));
    log.push(format!("ghost topic: {}", label(&driver.describe_topics(&describe(GHOST)).await)));
    let ghost_offsets = ListGroupOffsetsReq { group_id: GHOST.to_string(), with_lag: false, ..Default::default() };
    log.push(format!("ghost group: {}", label(&driver.list_group_offsets(&ghost_offsets).await)));
    let ghost_reset = ResetOffsetsReq { group_id: GHOST.to_string(), ..ResetOffsetsReq::default() };
    log.push(format!("ghost reset: {}", label(&driver.reset_offsets(&ghost_reset).await)));
    let no_group = CommitOffsetsReq { group_id: None, offsets: Vec::new() };
    log.push(format!("no group id: {}", label(&driver.commit_offsets(no_group).await)));
    log.push(format!("past the end: {}", label(&driver.subscribe(past_the_end()).await)));

    let _ = driver.delete_topics(&delete(TOPIC)).await;
    log.push(format!("after delete: {}", label(&driver.describe_topics(&describe(TOPIC)).await)));
    log
}

/// The configs `list_topics` reports for one topic, as a stable sorted list.
async fn configs_of(driver: &dyn KafkaDriver, topic: &str) -> Vec<(String, String)> {
    driver
        .list_topics()
        .await
        .expect("list_topics")
        .into_iter()
        .find(|entry| entry.name == topic)
        .expect("the topic was just created")
        .configs
        .into_iter()
        .collect()
}

/// Both drivers must answer the scripted pass identically.
#[tokio::test]
#[ignore = "needs a live broker; set KAFKA_CONFORMANCE_BOOTSTRAP and run with --ignored"]
async fn mock_and_real_agree_on_the_shared_contract() {
    let Some(cfg) = real_config() else {
        eprintln!("skipping T077: {SKIP}");
        return;
    };

    let mock = driver_for(&ClusterConfig::mock("t077-mock"));
    mock.connect(&ClusterConfig::mock("t077-mock")).await.expect("mock connect");
    let real = driver_for(&cfg);
    real.connect(&cfg).await.expect("real connect");

    let expected = transcript(&*mock).await;
    let actual = transcript(&*real).await;
    assert_eq!(actual, expected, "the real driver drifted from the mock contract");

    mock.disconnect().await.expect("mock disconnect");
    real.disconnect().await.expect("real disconnect");
}

/// The differences that are known and intended. Pinning them here means a change
/// that quietly removes one has to say so out loud.
#[tokio::test]
#[ignore = "needs a live broker; set KAFKA_CONFORMANCE_BOOTSTRAP and run with --ignored"]
async fn documented_drifts_between_mock_and_real_hold() {
    let Some(cfg) = real_config() else {
        eprintln!("skipping T077: {SKIP}");
        return;
    };

    let mock = driver_for(&ClusterConfig::mock("t077-drift"));
    mock.connect(&ClusterConfig::mock("t077-drift")).await.expect("mock connect");
    let real = driver_for(&cfg);
    real.connect(&cfg).await.expect("real connect");

    // R30: a mock cluster knows its own id and controller; librdkafka exposes neither.
    let mock_info = mock.cluster_info().await.expect("mock cluster_info");
    assert!(mock_info.cluster_id.is_some(), "R30: the mock still reports a cluster id");
    assert!(mock_info.controller_id.is_some(), "R30: the mock still reports a controller");
    let real_info = real.cluster_info().await.expect("real cluster_info");
    assert!(real_info.cluster_id.is_none(), "R30: the real cluster id stays unknown");
    assert!(real_info.controller_id.is_none(), "R30: the real controller stays unknown");

    // R33: the topic list carries configs on the mock and never on the real driver.
    mock.create_topic(&create(DRIFT)).await.expect("mock create");
    real.create_topic(&create(DRIFT)).await.expect("real create");
    let mock_configs = configs_of(&*mock, DRIFT).await;
    let real_configs = configs_of(&*real, DRIFT).await;
    assert!(!mock_configs.is_empty(), "R33: the mock fills list_topics configs, got {mock_configs:?}");
    assert!(real_configs.is_empty(), "R33: the real list_topics leaves configs empty, got {real_configs:?}");

    let _ = real.delete_topics(&delete(DRIFT)).await;
    mock.disconnect().await.expect("mock disconnect");
    real.disconnect().await.expect("real disconnect");
}
