//! Consumer-group contract for `MockKafkaDriver` (T014): initial group listing,
//! `describe` with members/assignments/state, committed offsets joined with lag,
//! all four `reset_offsets` strategies, the dry-run preview that computes without
//! writing, commits visible through `list_group_offsets`, and `GroupNotFound` for unknown groups.

use kafka_core::cluster::ClusterConfig;
use kafka_core::error::{DriverError, ErrorKind};
use kafka_core::group::{CommitOffsetsReq, GroupOffsetEntry, GroupState, ListGroupOffsetsReq, ResetOffsetsReq, ResetSpec};
use kafka_core::message::{ProduceMessage, ProduceReq};
use kafka_core::topic::{CreateTopicReq, TopicPartition, TopicPartitionOffset};
use kafka_driver::driver::KafkaDriver;
use kafka_driver::mock::MockKafkaDriver;

/// A connected, empty cluster: no topics, no groups.
async fn connected() -> MockKafkaDriver {
    let driver = MockKafkaDriver::new();
    driver.connect(&ClusterConfig::mock("groups-test")).await.expect("connect should succeed");
    driver
}

/// A connected cluster pre-loaded with the mock fixtures: topic `orders` (3
/// partitions holding 3/2/1 messages), `audit-log` (2 messages) and groups
/// `checkout-service` (Stable, 2 members) / `analytics-etl` (Empty).
async fn seeded() -> MockKafkaDriver {
    let driver = MockKafkaDriver::seeded();
    driver.connect(&ClusterConfig::mock("groups-test")).await.expect("connect should succeed");
    driver
}

/// A connected cluster with one single-partition topic holding exactly `stamps.len()` messages.
async fn driver_with_log(topic: &str, stamps: &[i64]) -> MockKafkaDriver {
    let driver = connected().await;
    let mut req = CreateTopicReq::new(topic);
    req.num_partitions = 1;
    driver.create_topic(&req).await.expect("create_topic should succeed");
    let messages: Vec<ProduceMessage> =
        stamps.iter().enumerate().map(|(i, &ts)| ProduceMessage { timestamp: Some(ts), ..ProduceMessage::text(format!("m{i}")) }).collect();
    driver.produce(ProduceReq { topic: topic.to_string(), messages }).await.expect("produce should succeed");
    driver
}

async fn commit(driver: &MockKafkaDriver, group: &str, offsets: Vec<TopicPartitionOffset>) {
    driver.commit_offsets(CommitOffsetsReq { group_id: Some(group.to_string()), offsets }).await.expect("commit should succeed");
}

fn commit_entry(topic: &str, partition: u32, offset: i64) -> TopicPartitionOffset {
    TopicPartitionOffset { topic: topic.to_string(), partition, offset, metadata: String::new(), timestamp: None, leader_epoch: None }
}

fn offsets_req(group: &str, with_lag: bool) -> ListGroupOffsetsReq {
    ListGroupOffsetsReq { group_id: group.to_string(), topics: Vec::new(), with_lag }
}

fn reset_req(group: &str, spec: ResetSpec) -> ResetOffsetsReq {
    ResetOffsetsReq { group_id: group.to_string(), topics: Vec::new(), partitions: Vec::new(), spec, dry_run: false }
}

/// The committed-offset row for one partition, panicking with the listing when absent.
fn entry<'a>(entries: &'a [GroupOffsetEntry], topic: &str, partition: u32) -> &'a GroupOffsetEntry {
    entries.iter().find(|e| e.topic == topic && e.partition == partition).unwrap_or_else(|| panic!("no offset entry for {topic}-{partition} in {entries:?}"))
}

/// The offset a reset planned for one partition, panicking with the plan when absent.
fn applied_at(applied: &[TopicPartitionOffset], topic: &str, partition: u32) -> i64 {
    applied.iter().find(|e| e.topic == topic && e.partition == partition).unwrap_or_else(|| panic!("no applied entry for {topic}-{partition} in {applied:?}")).offset
}

/// A cluster nobody consumed from owns no group; the fixture cluster lists its two
/// seeded groups with state, member count and assignor.
#[tokio::test(start_paused = true)]
async fn a_fresh_cluster_lists_no_groups_while_the_fixture_cluster_lists_two() {
    let fresh = connected().await;
    assert!(fresh.list_consumer_groups().await.expect("list on an empty cluster should succeed").is_empty(), "no commits, no groups");

    let groups = seeded().await.list_consumer_groups().await.expect("list on the fixture cluster should succeed");
    let ids: Vec<&str> = groups.iter().map(|g| g.group_id.as_str()).collect();
    assert_eq!(ids, vec!["analytics-etl", "checkout-service"], "groups are listed sorted by id");
    let checkout = groups.iter().find(|g| g.group_id == "checkout-service").expect("checkout-service must be listed");
    assert_eq!((checkout.state, checkout.member_count), (GroupState::Stable, 2), "checkout-service is Stable with two members");
    assert_eq!((checkout.protocol_type.as_str(), checkout.partition_assignor.as_str()), ("consumer", "range"));
    assert_eq!(checkout.coordinator_id, Some(1), "the single bootstrap server is broker 1");
    assert!(!checkout.is_local, "a seeded group is not driven by this application");
    let etl = groups.iter().find(|g| g.group_id == "analytics-etl").expect("analytics-etl must be listed");
    assert_eq!((etl.state, etl.member_count), (GroupState::Empty, 0), "analytics-etl is idle with no members");
}

/// `describe_consumer_groups` echoes state and every member with its client
/// identity and partition assignments; an empty id list describes every group.
#[tokio::test(start_paused = true)]
async fn describe_consumer_groups_reports_members_assignments_and_state() {
    let driver = seeded().await;
    let mut details = driver.describe_consumer_groups(&["checkout-service".to_string()]).await.expect("describe should succeed");
    assert_eq!(details.len(), 1, "one detail per requested id");
    let detail = details.remove(0);
    assert_eq!((detail.info.group_id.as_str(), detail.info.state), ("checkout-service", GroupState::Stable), "state is a real field, not Unknown");
    assert_eq!((detail.info.member_count, detail.members.len()), (2, 2), "member_count mirrors the member list");
    let first = &detail.members[0];
    assert_eq!((first.member_id.as_str(), first.client_id.as_str(), first.client_host.as_str()), ("checkout-1", "checkout-pod-7d9f", "/10.1.4.21"));
    assert_eq!(first.assignments, vec![TopicPartition::new("orders", 0), TopicPartition::new("orders", 1)], "the first member owns two partitions");
    assert_eq!(detail.members[1].assignments, vec![TopicPartition::new("orders", 2)], "the second member owns the rest");
    let etl = driver.describe_consumer_groups(&["analytics-etl".to_string()]).await.expect("describe should succeed").remove(0);
    assert_eq!(etl.info.state, GroupState::Empty);
    assert!(etl.members.is_empty(), "an Empty group has no members");
    assert_eq!(etl.info.partition_assignor, "roundrobin");

    let all = driver.describe_consumer_groups(&[]).await.expect("describe all should succeed");
    let ids: Vec<&str> = all.iter().map(|d| d.info.group_id.as_str()).collect();
    assert_eq!(ids, vec!["analytics-etl", "checkout-service"], "an empty id list means every group");
}

/// Committed offsets join with the log end: `lag = end - committed`; the topic
/// filter and `with_lag=false` behave as documented.
#[tokio::test(start_paused = true)]
async fn list_group_offsets_joins_committed_offsets_with_lag() {
    let driver = seeded().await;
    let entries = driver.list_group_offsets(&offsets_req("checkout-service", true)).await.expect("offsets should succeed");
    assert_eq!(entries.len(), 2, "two committed partitions");
    let p0 = entry(&entries, "orders", 0);
    assert_eq!((p0.offset, p0.lag), (2, Some(1)), "lag = end(3) - committed(2)");
    let p1 = entry(&entries, "orders", 1);
    assert_eq!((p1.offset, p1.lag), (1, Some(1)), "lag = end(2) - committed(1)");
    assert!(p0.committed_at.is_some(), "a commit carries its commit time");
    let etl = driver.list_group_offsets(&offsets_req("analytics-etl", true)).await.expect("offsets should succeed");
    let audit = entry(&etl, "audit-log", 0);
    assert_eq!((audit.offset, audit.lag, audit.metadata.as_str()), (1, Some(1), "v1"), "commit metadata is kept");

    let no_lag = driver.list_group_offsets(&offsets_req("checkout-service", false)).await.expect("offsets should succeed");
    assert!(no_lag.iter().all(|e| e.lag.is_none()), "with_lag=false skips the end-offset join");

    let mut filtered = offsets_req("checkout-service", true);
    filtered.topics = vec!["audit-log".to_string()];
    let empty = driver.list_group_offsets(&filtered).await.expect("a topic filter should succeed");
    assert!(empty.is_empty(), "a topic the group holds no offsets for filters everything out");
}

/// Earliest rewinds every committed partition to 0, latest jumps to the log end,
/// and both rewrites are visible through `list_group_offsets`.
#[tokio::test(start_paused = true)]
async fn reset_offsets_to_earliest_and_latest_rewrites_committed_offsets() {
    let driver = seeded().await;
    let earliest = driver.reset_offsets(&reset_req("checkout-service", ResetSpec::Earliest)).await.expect("earliest reset should succeed");
    assert_eq!(earliest.group_id, "checkout-service");
    assert!(!earliest.dry_run && earliest.error.is_none(), "an applied reset is not a dry run");
    assert_eq!(earliest.applied.len(), 2, "an empty partition list resets every committed partition");
    assert_eq!((applied_at(&earliest.applied, "orders", 0), applied_at(&earliest.applied, "orders", 1)), (0, 0), "earliest lands on the beginning offset");
    let entries = driver.list_group_offsets(&offsets_req("checkout-service", true)).await.expect("offsets should succeed");
    let rewound = entry(&entries, "orders", 0);
    assert_eq!((rewound.offset, rewound.lag), (0, Some(3)), "the rewind is committed, so the lag grows to the full log");
    let latest = driver.reset_offsets(&reset_req("checkout-service", ResetSpec::Latest)).await.expect("latest reset should succeed");
    assert_eq!((applied_at(&latest.applied, "orders", 0), applied_at(&latest.applied, "orders", 1)), (3, 2), "latest lands on the log end");
    let entries = driver.list_group_offsets(&offsets_req("checkout-service", true)).await.expect("offsets should succeed");
    assert_eq!(entry(&entries, "orders", 0).lag, Some(0), "at the end offset the lag is zero");
}

/// An explicit offset lands verbatim (out-of-range refused, not clamped); a
/// timestamp resolves to the first message at or after it, else the end offset.
#[tokio::test(start_paused = true)]
async fn reset_offsets_to_an_explicit_offset_or_timestamp_lands_on_the_right_message() {
    let driver = driver_with_log("reset.log", &[1_000, 2_000, 3_000, 4_000, 5_000]).await;
    commit(&driver, "resetters", vec![commit_entry("reset.log", 0, 0)]).await;
    let target = || vec![TopicPartition::new("reset.log", 0)];
    let mut req = reset_req("resetters", ResetSpec::ToOffset(3));
    req.partitions = target();
    assert_eq!(applied_at(&driver.reset_offsets(&req).await.expect("in-range reset should succeed").applied, "reset.log", 0), 3, "the requested offset is applied verbatim");
    let mut start = reset_req("resetters", ResetSpec::ToOffset(0));
    start.partitions = target();
    assert_eq!(applied_at(&driver.reset_offsets(&start).await.expect("offset 0 is in range").applied, "reset.log", 0), 0, "the beginning offset is a legal target");
    let mut over = reset_req("resetters", ResetSpec::ToOffset(99));
    over.partitions = target();
    let err = driver.reset_offsets(&over).await.expect_err("an offset past the log must be refused");
    assert!(matches!(err, DriverError::OffsetOutOfRange { requested, low, high, .. } if requested == 99 && low == 0 && high == 5), "expected OffsetOutOfRange, got {err:?}");
    assert_eq!(err.kind(), ErrorKind::OffsetOutOfRange);
    let mut mid = reset_req("resetters", ResetSpec::ToTimestamp(2_500));
    mid.partitions = target();
    assert_eq!(applied_at(&driver.reset_offsets(&mid).await.expect("timestamp reset should succeed").applied, "reset.log", 0), 2, "the first message at or after 2500 is offset 2");
    let mut late = reset_req("resetters", ResetSpec::ToTimestamp(99_999));
    late.partitions = target();
    assert_eq!(applied_at(&driver.reset_offsets(&late).await.expect("late timestamp should succeed").applied, "reset.log", 0), 5, "a timestamp past the log lands on the end offset");
    let entries = driver.list_group_offsets(&offsets_req("resetters", true)).await.expect("offsets should succeed");
    assert_eq!(entry(&entries, "reset.log", 0).offset, 5, "the applied reset is visible through the committed offsets");
}

/// A dry run reports the plan without writing it: the result echoes `dry_run`,
/// the applied entries show what latest would do, and the committed offsets stay put.
#[tokio::test(start_paused = true)]
async fn reset_offsets_dry_run_computes_the_plan_without_writing_it() {
    let driver = seeded().await;
    let before = driver.list_group_offsets(&offsets_req("checkout-service", true)).await.expect("offsets should succeed");
    assert_eq!(entry(&before, "orders", 0).offset, 2, "the seed committed orders-0 at offset 2");

    let mut req = reset_req("checkout-service", ResetSpec::Latest);
    req.dry_run = true;
    let preview = driver.reset_offsets(&req).await.expect("a dry run of a known group should succeed");
    assert!(preview.dry_run, "the result echoes the dry-run flag");
    assert!(preview.error.is_none(), "a dry run reports no error");
    assert_eq!((applied_at(&preview.applied, "orders", 0), applied_at(&preview.applied, "orders", 1)), (3, 2), "the preview still shows what latest would apply");

    let after = driver.list_group_offsets(&offsets_req("checkout-service", true)).await.expect("offsets should succeed");
    assert_eq!(entry(&after, "orders", 0).offset, 2, "a dry run must not touch committed offsets");
    assert_eq!(entry(&after, "orders", 1).offset, 1, "a dry run must not touch committed offsets");
}

/// A commit creates/updates committed offsets that `list_group_offsets` reports
/// with a fresh lag; out-of-range and group-less commits are refused.
#[tokio::test(start_paused = true)]
async fn commit_offsets_makes_the_new_offsets_visible_through_list_group_offsets() {
    let driver = driver_with_log("commits.log", &[1_000; 10]).await;
    commit(&driver, "committers", vec![commit_entry("commits.log", 0, 7)]).await;
    let entries = driver.list_group_offsets(&offsets_req("committers", true)).await.expect("offsets should succeed");
    assert_eq!(entries.len(), 1, "one committed partition");
    assert_eq!((entries[0].topic.as_str(), entries[0].partition, entries[0].offset), ("commits.log", 0, 7));
    assert_eq!(entries[0].lag, Some(3), "lag = end(10) - committed(7)");

    let mut bumped = commit_entry("commits.log", 0, 9);
    bumped.metadata = "v7".to_string();
    commit(&driver, "committers", vec![bumped]).await;
    let entries = driver.list_group_offsets(&offsets_req("committers", true)).await.expect("offsets should succeed");
    let latest = &entries[0];
    assert_eq!((latest.offset, latest.metadata.as_str(), latest.lag), (9, "v7", Some(1)), "a later commit replaces the offset and shrinks the lag");

    let err = driver.commit_offsets(CommitOffsetsReq { group_id: Some("committers".to_string()), offsets: vec![commit_entry("commits.log", 0, 99)] }).await.expect_err("a commit past the log end must be refused");
    assert!(matches!(err, DriverError::OffsetOutOfRange { requested, low, high, .. } if requested == 99 && low == 0 && high == 10), "expected OffsetOutOfRange, got {err:?}");

    let err = driver.commit_offsets(CommitOffsetsReq { group_id: None, offsets: vec![commit_entry("commits.log", 0, 1)] }).await.expect_err("the mock needs an explicit group to commit for");
    assert_eq!(err.kind(), ErrorKind::Unsupported);
}

/// Every read-only group op on an unknown group answers `GroupNotFound` — a
/// `NotFound`-kind error carrying the group id, never a panic.
#[tokio::test(start_paused = true)]
async fn group_operations_on_a_missing_group_fail_with_group_not_found() {
    let driver = connected().await;
    let ghost = "ghost-group".to_string();

    let describe = driver.describe_consumer_groups(std::slice::from_ref(&ghost)).await.expect_err("describe must fail");
    assert!(matches!(&describe, DriverError::GroupNotFound(id) if id == "ghost-group"), "expected GroupNotFound, got {describe:?}");
    assert_eq!(describe.kind(), ErrorKind::NotFound);

    let offsets = driver.list_group_offsets(&offsets_req(&ghost, true)).await.expect_err("offsets must fail");
    assert!(matches!(&offsets, DriverError::GroupNotFound(id) if id == "ghost-group"), "expected GroupNotFound, got {offsets:?}");
    assert_eq!(offsets.kind(), ErrorKind::NotFound);

    let reset = driver.reset_offsets(&reset_req(&ghost, ResetSpec::Earliest)).await.expect_err("reset must fail");
    assert!(matches!(&reset, DriverError::GroupNotFound(id) if id == "ghost-group"), "expected GroupNotFound, got {reset:?}");
    assert_eq!(reset.kind(), ErrorKind::NotFound);
}
