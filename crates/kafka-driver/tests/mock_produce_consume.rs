//! Produce + subscribe contract for `MockKafkaDriver` (T012): what a write costs in
//! partition/offset terms, how keys, headers and raw bytes survive the round trip,
//! where a subscription starts reading, and that a stream opened before a write still
//! receives it, all driven through the public `KafkaDriver` trait. Timers are paused
//! and every read is bounded, so the suite is fast and cannot hang.

use std::collections::BTreeMap;
use std::time::Duration;

use kafka_core::cluster::ClusterConfig;
use kafka_core::error::{DriverError, ErrorKind};
use kafka_core::message::{
    MessageEvent, MessageHeader, MessageStream, OffsetStart, ProduceMessage, ProduceReq, ProduceResult, SearchFilter,
    SubscribeReq,
};
use kafka_core::topic::CreateTopicReq;
use kafka_driver::driver::KafkaDriver;
use kafka_driver::mock::MockKafkaDriver;

/// Widest topic the tests build, so partition routing can be observed.
const WIDE: u32 = 4;

/// A connected driver with one freshly created topic.
async fn driver_with(topic: &str, partitions: u32) -> MockKafkaDriver {
    let driver = MockKafkaDriver::new();
    driver.connect(&ClusterConfig::mock("produce-consume-test")).await.expect("connect should succeed");
    let mut req = CreateTopicReq::new(topic);
    req.num_partitions = partitions;
    driver.create_topic(&req).await.expect("create_topic should succeed");
    driver
}

async fn produce(driver: &MockKafkaDriver, topic: &str, messages: Vec<ProduceMessage>) -> Vec<ProduceResult> {
    let results = driver
        .produce(ProduceReq { topic: topic.to_string(), messages })
        .await
        .expect("produce should succeed");
    assert!(results.iter().all(|r| r.error.is_none()), "produce reported a per-message failure: {results:?}");
    results
}

fn text(count: usize, prefix: &str) -> Vec<ProduceMessage> {
    (0..count).map(|i| ProduceMessage::text(format!("{prefix}{i}"))).collect()
}

/// `count` messages that all carry the same append timestamp.
fn stamped(count: usize, prefix: &str, timestamp: i64) -> Vec<ProduceMessage> {
    text(count, prefix).into_iter().map(|mut m| { m.timestamp = Some(timestamp); m }).collect()
}

fn offsets<T>(items: &[T], at: impl Fn(&T) -> i64) -> Vec<i64> {
    items.iter().map(at).collect()
}

fn bodies(events: &[MessageEvent]) -> Vec<String> {
    events.iter().map(|e| String::from_utf8_lossy(&e.value).into_owned()).collect()
}

/// A case-insensitive substring filter over message bodies.
fn filter(query: &str) -> SearchFilter {
    SearchFilter { query: query.to_string(), match_value: true, ..Default::default() }
}

/// A subscription that hands over at most `max` messages, then ends by itself.
fn live(topic: &str, start: OffsetStart, max: u64) -> SubscribeReq {
    SubscribeReq { topic: topic.to_string(), start, follow: true, max_messages: Some(max), ..Default::default() }
}

async fn subscribe(driver: &MockKafkaDriver, req: SubscribeReq) -> MessageStream {
    driver.subscribe(req).await.expect("subscribe should succeed")
}

/// Reads up to `count` events, stopping early when the stream ends on its own.
async fn read(stream: &mut MessageStream, count: usize) -> Vec<MessageEvent> {
    let mut out = Vec::with_capacity(count);
    while out.len() < count {
        match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
            Ok(Some(Ok(event))) => out.push(event),
            Ok(None) => break,
            other => panic!("the stream failed or stalled: {other:?}"),
        }
    }
    out
}

/// A single write answers with the topic, a partition and the offset it took.
#[tokio::test(start_paused = true)]
async fn a_single_produce_reports_topic_partition_and_first_offset() {
    let driver = driver_with("single.topic", 1).await;
    let results = produce(&driver, "single.topic", vec![ProduceMessage::text("hello")]).await;
    assert_eq!(results.len(), 1, "one result per produced message");
    assert_eq!((results[0].topic.as_str(), results[0].partition, results[0].offset), ("single.topic", 0, 0));
    assert!(results[0].timestamp > 0, "an append carries a create-time stamp, got {:?}", results[0]);
}

/// A batch answers once per message with contiguous offsets, and the next batch
/// continues the same log instead of restarting it.
#[tokio::test(start_paused = true)]
async fn a_batch_returns_one_contiguous_offset_per_message() {
    let driver = driver_with("batch.topic", 1).await;
    let first = produce(&driver, "batch.topic", text(5, "m")).await;
    let second = produce(&driver, "batch.topic", text(3, "m")).await;
    assert_eq!(first.len(), 5, "one result per request entry");
    assert_eq!(offsets(&first, |r| r.offset), vec![0, 1, 2, 3, 4], "offsets grow without a gap");
    assert_eq!(offsets(&second, |r| r.offset), vec![5, 6, 7], "the log continues where the last batch stopped");
    assert!(first.iter().chain(&second).all(|r| r.partition == 0), "a one-partition topic never routes elsewhere");
}

/// Without an explicit partition the mock round-robins (what `ProduceMessage.partition`
/// documents), so a key does not pin a partition; `partition: Some(..)` does.
#[tokio::test(start_paused = true)]
async fn routing_round_robins_by_default_and_honours_an_explicit_partition() {
    let driver = driver_with("routing.topic", WIDE).await;
    let keyed: Vec<ProduceMessage> = (0..6).map(|i| ProduceMessage::text(format!("v{i}")).with_key("order-7")).collect();
    let routed = produce(&driver, "routing.topic", keyed).await;
    assert_eq!(routed.iter().map(|r| r.partition).collect::<Vec<_>>(), vec![0, 1, 2, 3, 0, 1], "round-robin");
    let mut pinned_req = CreateTopicReq::new("pinned.topic");
    pinned_req.num_partitions = WIDE;
    driver.create_topic(&pinned_req).await.expect("create_topic should succeed");
    let mut pinned = text(2, "pinned-");
    for message in &mut pinned {
        message.partition = Some(2);
    }
    let results = produce(&driver, "pinned.topic", pinned).await;

    assert_eq!(results.iter().map(|r| r.partition).collect::<Vec<_>>(), vec![2, 2], "the partition must be honoured");
    assert_eq!(offsets(&results, |r| r.offset), vec![0, 1], "a fresh partition starts at offset 0");
}

/// Keys, text values and binary (non-UTF-8) headers come back exactly as written.
#[tokio::test(start_paused = true)]
async fn keys_values_and_binary_headers_round_trip_through_the_stream() {
    let driver = driver_with("headers.topic", 1).await;
    let headers = vec![
        MessageHeader::text("trace", "abc-123"),
        MessageHeader::new("blob", vec![0u8, 1, 2, 250, 255, 0]),
    ];
    let mut message = ProduceMessage::text("payload");
    message.key = Some(b"order-key".to_vec());
    message.headers = headers.clone();
    produce(&driver, "headers.topic", vec![message]).await;
    let mut stream = subscribe(&driver, live("headers.topic", OffsetStart::Earliest, 1)).await;
    let events = read(&mut stream, 1).await;

    let event = events.first().expect("the written message must be streamed back");
    assert_eq!(event.value, b"payload".to_vec(), "raw value bytes are preserved");
    assert_eq!(event.key.as_deref(), Some(b"order-key".as_slice()), "the key round-trips");
    assert_eq!(event.headers, headers, "headers round-trip verbatim");
}

/// Earliest reads every message already in the log, each partition from its own
/// beginning offset.
#[tokio::test(start_paused = true)]
async fn subscribing_from_earliest_reads_every_message() {
    let driver = driver_with("earliest.topic", WIDE).await;
    produce(&driver, "earliest.topic", text(6, "m-")).await;
    let mut stream = subscribe(&driver, live("earliest.topic", OffsetStart::Earliest, 6)).await;
    let events = read(&mut stream, 6).await;

    let mut per: BTreeMap<u32, Vec<i64>> = BTreeMap::new();
    for event in &events {
        per.entry(event.partition).or_default().push(event.offset);
    }
    assert_eq!(events.len(), 6, "everything written must be readable from earliest, got {per:?}");
    assert_eq!(per.len(), WIDE as usize, "every partition was written to, got {per:?}");
    for (partition, seen) in per {
        assert_eq!(seen, (0..seen.len() as i64).collect::<Vec<_>>(), "partition {partition} reads from its beginning");
    }
}

/// Latest skips the backlog: only messages written after the subscription are
/// delivered.
#[tokio::test(start_paused = true)]
async fn subscribing_from_latest_sees_only_new_messages() {
    let driver = driver_with("latest.topic", 1).await;
    produce(&driver, "latest.topic", text(3, "old-")).await;
    let mut stream = subscribe(&driver, live("latest.topic", OffsetStart::Latest, 2)).await;
    produce(&driver, "latest.topic", text(2, "new-")).await;
    let events = read(&mut stream, 2).await;

    assert_eq!(bodies(&events), vec!["new-0", "new-1"], "the pre-existing backlog must be skipped");
    assert_eq!(offsets(&events, |e| e.offset), vec![3, 4], "latest resumes at the high watermark");
}

/// An explicit start offset replays from there, not from the beginning; one past the
/// end of the log is refused instead of clamped.
#[tokio::test(start_paused = true)]
async fn an_explicit_offset_start_replays_or_is_refused_past_the_end() {
    let driver = driver_with("replay.topic", 1).await;
    produce(&driver, "replay.topic", text(5, "m-")).await;
    let mut stream = subscribe(&driver, live("replay.topic", OffsetStart::Offset(2), 3)).await;
    let events = read(&mut stream, 3).await;
    assert_eq!(offsets(&events, |e| e.offset), vec![2, 3, 4], "reading resumes at the requested offset");
    assert_eq!(bodies(&events), vec!["m-2", "m-3", "m-4"], "the right messages are replayed");

    let req = live("replay.topic", OffsetStart::Offset(99), 1);
    let err = driver.subscribe(req).await.expect_err("an offset past the log must be refused");
    assert!(
        matches!(err, DriverError::OffsetOutOfRange { requested, high, .. } if requested == 99 && high == 5),
        "expected OffsetOutOfRange for offset 99 of a 5-message log, got {err:?}"
    );
    assert_eq!(err.kind(), ErrorKind::OffsetOutOfRange);
}

/// A timestamp start replays only the messages appended at or after it.
#[tokio::test(start_paused = true)]
async fn subscribing_from_a_timestamp_skips_older_messages() {
    let driver = driver_with("replay-by-time.topic", 1).await;
    produce(&driver, "replay-by-time.topic", stamped(2, "old-", 1_000)).await;
    produce(&driver, "replay-by-time.topic", stamped(3, "new-", 9_000)).await;
    let mut stream = subscribe(&driver, live("replay-by-time.topic", OffsetStart::Timestamp(5_000), 3)).await;
    let events = read(&mut stream, 3).await;

    assert_eq!(offsets(&events, |e| e.offset), vec![2, 3, 4], "only messages at or after the timestamp are replayed");
    assert_eq!(bodies(&events), vec!["new-0", "new-1", "new-2"]);
}

/// A subscription opened before the write still receives the new messages.
#[tokio::test(start_paused = true)]
async fn a_subscription_opened_before_the_write_receives_them() {
    let driver = driver_with("live.topic", 1).await;
    let mut stream = subscribe(&driver, live("live.topic", OffsetStart::Earliest, 2)).await;
    produce(&driver, "live.topic", text(2, "after-")).await;
    let events = read(&mut stream, 2).await;
    assert_eq!(bodies(&events), vec!["after-0", "after-1"], "messages written after the subscription are pushed");
    assert_eq!(offsets(&events, |e| e.offset), vec![0, 1], "the live log started empty");
}

/// The server-side filter drops non-matching messages before they reach the UI, and
/// a filter that matches nothing streams nothing at all.
#[tokio::test(start_paused = true)]
async fn a_server_side_filter_streams_only_matching_messages() {
    let driver = driver_with("filtered.topic", 1).await;
    produce(&driver, "filtered.topic", text(5, "noise-")).await;
    produce(&driver, "filtered.topic", text(3, "keep-")).await;

    let mut hit = live("filtered.topic", OffsetStart::Earliest, 3);
    hit.filter = Some(filter("KEEP"));
    let mut stream = subscribe(&driver, hit).await;
    let events = read(&mut stream, 3).await;
    assert_eq!(bodies(&events), vec!["keep-0", "keep-1", "keep-2"], "only matching messages are streamed");

    let mut miss = live("filtered.topic", OffsetStart::Earliest, 9);
    miss.follow = false;
    miss.filter = Some(filter("not-present-anywhere"));
    let mut stream = subscribe(&driver, miss).await;
    let silent = read(&mut stream, 9).await;
    assert!(silent.is_empty(), "unmatched messages must never reach the UI, got {:?}", bodies(&silent));
}
