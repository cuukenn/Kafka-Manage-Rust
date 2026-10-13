//! Topic-management contract for `MockKafkaDriver` (T011): create → list, duplicate
//! and invalid-input rejection, the `describe_topics` leader/replica/ISR report,
//! per-item `delete_topics` results, config writes visible through `describe`,
//! empty-topic offset watermarks, and the `NotConnected` guard every topic op goes
//! through — all driven through the public `KafkaDriver` trait.

use kafka_core::cluster::ClusterConfig;
use kafka_core::error::{DriverError, ErrorKind};
use kafka_core::topic::{
    CreateTopicReq, DeleteTopicsReq, DescribeTopicsReq, ListOffsetsReq, OffsetSpec, TopicDetail, TopicPartition,
    UpdateTopicConfigReq,
};
use kafka_driver::driver::KafkaDriver;
use kafka_driver::mock::MockKafkaDriver;

/// Three bootstrap servers, so a `replication_factor` up to 3 stays legal.
const BROKERS: [&str; 3] = ["mock://b1:9092", "mock://b2:9092", "mock://b3:9092"];

/// Connected mock cluster backed by the first `count` brokers of [`BROKERS`].
async fn connected_driver(count: usize) -> MockKafkaDriver {
    let mut cfg = ClusterConfig::mock("topics-test");
    cfg.bootstrap_servers = BROKERS.iter().take(count).map(|s| (*s).to_string()).collect();
    let driver = MockKafkaDriver::new();
    driver.connect(&cfg).await.expect("connect on a fresh mock driver should succeed");
    driver
}

fn names(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| (*s).to_string()).collect()
}

fn describe_req(list: &[&str]) -> DescribeTopicsReq {
    DescribeTopicsReq { names: names(list), include_internal: false }
}

fn offsets_req(partitions: Vec<TopicPartition>, spec: OffsetSpec) -> ListOffsetsReq {
    ListOffsetsReq { partitions, spec }
}

fn config_req(topic: &str, set: &[(&str, &str)], unset: &[&str]) -> UpdateTopicConfigReq {
    UpdateTopicConfigReq {
        topic: topic.to_string(),
        set: set.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect(),
        unset: unset.iter().map(|k| (*k).to_string()).collect(),
    }
}

async fn topic_names(driver: &MockKafkaDriver) -> Vec<String> {
    driver.list_topics().await.expect("list_topics should succeed").into_iter().map(|t| t.name).collect()
}

/// `describe_topics` for exactly one topic.
async fn describe_one(driver: &MockKafkaDriver, topic: &str) -> TopicDetail {
    let mut details = driver.describe_topics(&describe_req(&[topic])).await.expect("describe_topics should succeed");
    assert_eq!(details.len(), 1, "exactly one topic was requested");
    details.remove(0)
}

/// Runs `create_topic` expecting failure, naming the offending request on panic.
async fn create_error(driver: &MockKafkaDriver, req: &CreateTopicReq, label: &str) -> DriverError {
    match driver.create_topic(req).await {
        Ok(()) => panic!("create_topic must fail for {label}"),
        Err(err) => err,
    }
}

fn assert_not_connected(label: &str, err: &DriverError) {
    assert!(matches!(err, DriverError::NotConnected(_)), "{label} should be NotConnected, got {err:?}");
}

/// A created topic is listed with the requested shape and create-time configs.
#[tokio::test]
async fn created_topic_is_listed_with_partitions_replicas_and_configs() {
    let driver = connected_driver(3).await;
    let mut req = CreateTopicReq::new("orders.v2");
    req.num_partitions = 3;
    req.replication_factor = 2;
    req.configs.insert("cleanup.policy".to_string(), "compact".to_string());
    driver.create_topic(&req).await.expect("create_topic should succeed");

    let topics = driver.list_topics().await.expect("list_topics should succeed");
    let info = topics.into_iter().find(|t| t.name == "orders.v2").expect("the created topic must be listed");
    assert!(!info.internal, "a name without the __ prefix is not internal");
    assert_eq!((info.partition_count, info.replication_factor), (3, 2), "shape must round-trip");
    assert_eq!(info.configs.get("cleanup.policy").map(String::as_str), Some("compact"), "create configs are kept");
}

/// Creating the same name twice is rejected, without duplicating the entry.
#[tokio::test]
async fn duplicate_create_fails_with_topic_already_exists() {
    let driver = connected_driver(3).await;
    let req = CreateTopicReq::new("dup.topic");
    driver.create_topic(&req).await.expect("the first create should succeed");

    let err = create_error(&driver, &req, "a second create of the same name").await;
    assert!(
        matches!(err, DriverError::TopicAlreadyExists(ref name) if name == "dup.topic"),
        "expected TopicAlreadyExists, got {err:?}"
    );
    let listed = topic_names(&driver).await;
    assert_eq!(listed.iter().filter(|n| n.as_str() == "dup.topic").count(), 1, "no duplicate entry, got {listed:?}");
}

/// Illegal, over-long and structurally impossible create requests are
/// `InvalidConfig`, and none of them leaves a topic behind.
#[tokio::test]
async fn invalid_create_requests_are_rejected() {
    let driver = connected_driver(3).await;
    let mut zero_partitions = CreateTopicReq::new("zero.partitions");
    zero_partitions.num_partitions = 0;
    let mut zero_replicas = CreateTopicReq::new("zero.replicas");
    zero_replicas.replication_factor = 0;
    let mut too_wide = CreateTopicReq::new("too.wide");
    too_wide.replication_factor = 4;
    let cases: Vec<(&str, CreateTopicReq, &str)> = vec![
        ("empty name", CreateTopicReq::new(""), "name"),
        ("space in name", CreateTopicReq::new("bad name"), "name"),
        ("slash in name", CreateTopicReq::new("bad/name"), "name"),
        ("over-long name", CreateTopicReq::new("t".repeat(250)), "name"),
        ("numPartitions=0", zero_partitions, "numPartitions"),
        ("replicationFactor=0", zero_replicas, "replicationFactor"),
        ("replicationFactor above the broker count", too_wide, "replicationFactor"),
    ];
    for (label, req, field) in &cases {
        let err = create_error(&driver, req, label).await;
        let rejected = match &err {
            DriverError::InvalidConfig { field, .. } => field.as_str(),
            other => panic!("create with {label} should be InvalidConfig, got {other:?}"),
        };
        assert_eq!((err.kind(), rejected), (ErrorKind::InvalidConfig, *field), "wrong rejection for {label}");
    }
    assert!(topic_names(&driver).await.is_empty(), "no rejected create may leave a topic behind");
}

/// `describe_topics` reports a dense partition list whose leader is a live broker
/// and whose ISR equals the replica set.
#[tokio::test]
async fn describe_topics_reports_leader_replicas_and_isr() {
    let driver = connected_driver(3).await;
    let mut req = CreateTopicReq::new("orders.described");
    req.num_partitions = 3;
    req.replication_factor = 2;
    driver.create_topic(&req).await.expect("create_topic should succeed");

    let detail = describe_one(&driver, "orders.described").await;
    assert_eq!(detail.name, "orders.described");
    assert_eq!(detail.partitions.len(), 3, "one entry per partition");
    let brokers: Vec<i32> = driver.cluster_info().await.expect("cluster_info").brokers.iter().map(|b| b.node_id).collect();
    for (index, partition) in detail.partitions.iter().enumerate() {
        assert_eq!(partition.id, index as u32, "partition ids are dense from 0");
        assert_eq!(partition.replica_ids.len(), 2, "replicas honour the replication factor");
        assert_eq!(partition.leader_id, partition.replica_ids[0], "the leader is the first replica");
        assert!(brokers.contains(&partition.leader_id), "leader {} is a live broker", partition.leader_id);
        assert_eq!(partition.isr_ids, partition.replica_ids, "a healthy partition has isr == replicas");
        assert!(!partition.is_under_replicated(), "the partition is fully replicated");
        assert_eq!(partition.message_count(), 0, "a fresh partition holds no messages");
    }
}

/// `delete_topics` answers per requested name, in request order, and only the
/// topics it accepted leave the listing.
#[tokio::test]
async fn delete_topics_reports_each_item_and_removes_what_it_accepted() {
    let driver = connected_driver(3).await;
    for name in ["gone.topic", "kept.topic", "__internal.secret"] {
        driver.create_topic(&CreateTopicReq::new(name)).await.expect("create_topic should succeed");
    }

    let requested = names(&["gone.topic", "missing.topic", "__internal.secret"]);
    let results = driver.delete_topics(&DeleteTopicsReq { names: requested.clone() }).await.expect("delete should succeed");
    assert_eq!(results.len(), 3, "one result per requested topic");
    let echoed: Vec<String> = results.iter().map(|r| r.name.clone()).collect();
    assert_eq!(echoed, requested, "results follow the request order");
    assert!(results[0].error.is_none(), "an existing topic deletes cleanly, got {:?}", results[0].error);
    let unknown = results[1].error.as_deref().unwrap_or_default();
    assert!(unknown.contains("missing.topic"), "unknown topics are reported per item, got {unknown:?}");
    let refused = results[2].error.as_deref().unwrap_or_default();
    assert!(refused.contains("internal"), "internal topics are refused, got {refused:?}");

    let listed = topic_names(&driver).await;
    assert!(!listed.contains(&"gone.topic".to_string()), "deleted topics leave the listing, got {listed:?}");
    assert!(listed.contains(&"kept.topic".to_string()), "untouched topics survive, got {listed:?}");
    assert!(listed.contains(&"__internal.secret".to_string()), "a refused delete keeps the topic");
}

/// Config writes are visible through `describe_topics`, and `unset` drops only the
/// keys it names.
#[tokio::test]
async fn updated_config_is_visible_through_describe() {
    let driver = connected_driver(2).await;
    let mut req = CreateTopicReq::new("orders.config");
    req.configs.insert("retention.ms".to_string(), "604800000".to_string());
    driver.create_topic(&req).await.expect("create_topic should succeed");

    let update = config_req("orders.config", &[("retention.ms", "3600000"), ("max.message.bytes", "1048576")], &[]);
    driver.update_topic_config(&update).await.expect("update_topic_config should succeed");
    let configs = describe_one(&driver, "orders.config").await.configs;
    assert_eq!(configs.get("retention.ms").map(String::as_str), Some("3600000"), "describe shows the new value");

    driver.update_topic_config(&config_req("orders.config", &[], &["retention.ms"])).await.expect("unset should succeed");
    let configs = describe_one(&driver, "orders.config").await.configs;
    assert!(!configs.contains_key("retention.ms"), "unset drops the key, got {configs:?}");
    assert_eq!(configs.get("max.message.bytes").map(String::as_str), Some("1048576"), "unset keeps other keys");
}

/// On an empty topic every partition reports `low == high == 0`, so earliest and
/// latest resolve to the same offset.
#[tokio::test]
async fn list_offsets_on_an_empty_topic_has_equal_watermarks() {
    let driver = connected_driver(1).await;
    let mut req = CreateTopicReq::new("empty.topic");
    req.num_partitions = 2;
    driver.create_topic(&req).await.expect("create_topic should succeed");

    let partitions = vec![TopicPartition::new("empty.topic", 0), TopicPartition::new("empty.topic", 1)];
    let earliest = driver.list_offsets(&offsets_req(partitions.clone(), OffsetSpec::Earliest)).await.expect("earliest");
    let latest = driver.list_offsets(&offsets_req(partitions, OffsetSpec::Latest)).await.expect("latest");
    assert_eq!(earliest.len(), 2, "one range per requested partition");
    for (index, range) in earliest.iter().enumerate() {
        assert_eq!((range.topic.as_str(), range.partition), ("empty.topic", index as u32));
        assert_eq!((range.low, range.high, range.count()), (0, 0, 0), "an empty log is empty");
        assert!(!range.contains(0), "there is no offset 0 to consume yet");
        assert_eq!(range.resolved_offset, Some(range.low), "Earliest resolves to the low watermark");
    }
    for range in &latest {
        assert_eq!(range.resolved_offset, Some(range.high), "Latest resolves to the high watermark");
    }
    let earliest_offsets: Vec<Option<i64>> = earliest.iter().map(|r| r.resolved_offset).collect();
    let latest_offsets: Vec<Option<i64>> = latest.iter().map(|r| r.resolved_offset).collect();
    assert_eq!(earliest_offsets, latest_offsets, "earliest == latest while the topic is empty");
}

/// T069: an explicitly requested unknown topic fails with `TopicNotFound`
/// (matching `describe_topics`), while an empty partitions list resolves every
/// partition of EVERY topic in the cluster (see `list_offsets` in `mock/driver.rs`).
#[tokio::test]
async fn list_offsets_rejects_unknown_topic_but_keeps_known_topic_empty_partition_list() {
    let driver = connected_driver(1).await;
    let mut req = CreateTopicReq::new("known.topic");
    req.num_partitions = 2;
    driver.create_topic(&req).await.expect("create_topic should succeed");
    let mut second = CreateTopicReq::new("other.topic");
    second.num_partitions = 3;
    driver.create_topic(&second).await.expect("second create_topic should succeed");

    let unknown = vec![TopicPartition::new("missing.topic", 0)];
    let err = driver
        .list_offsets(&offsets_req(unknown, OffsetSpec::Latest))
        .await
        .expect_err("an unknown topic must fail, not silently resolve to an empty Ok");
    assert!(
        matches!(err, DriverError::TopicNotFound(ref name) if name == "missing.topic"),
        "expected TopicNotFound, got {err:?}"
    );
    assert_eq!(err.kind(), ErrorKind::NotFound, "TopicNotFound maps to NotFound, got {:?}", err.kind());

    let all = driver
        .list_offsets(&offsets_req(Vec::new(), OffsetSpec::Earliest))
        .await
        .expect("an empty partitions list resolves across the whole cluster");
    let mut resolved: Vec<(String, u32)> = all.iter().map(|r| (r.topic.clone(), r.partition)).collect();
    resolved.sort();
    assert_eq!(
        resolved,
        vec![
            ("known.topic".to_string(), 0),
            ("known.topic".to_string(), 1),
            ("other.topic".to_string(), 0),
            ("other.topic".to_string(), 1),
            ("other.topic".to_string(), 2),
        ],
        "an empty partitions list returns every partition of every topic in the cluster"
    );
}

/// Topic ops on a driver that never connected fail with `NotConnected` instead of
/// succeeding against an empty state.
#[tokio::test]
async fn topic_management_requires_a_connection() {
    let driver = MockKafkaDriver::new();
    let name = "orders.late";

    let create = driver.create_topic(&CreateTopicReq::new(name)).await.expect_err("create must fail");
    assert_not_connected("create_topic", &create);
    let describe = driver.describe_topics(&describe_req(&[name])).await.expect_err("describe must fail");
    assert_not_connected("describe_topics", &describe);
    let delete = driver.delete_topics(&DeleteTopicsReq { names: names(&[name]) }).await.expect_err("delete must fail");
    assert_not_connected("delete_topics", &delete);
    let update = driver.update_topic_config(&config_req(name, &[], &[])).await.expect_err("update must fail");
    assert_not_connected("update_topic_config", &update);
    let offsets = driver.list_offsets(&ListOffsetsReq::default()).await.expect_err("list_offsets must fail");
    assert_not_connected("list_offsets", &offsets);
}
