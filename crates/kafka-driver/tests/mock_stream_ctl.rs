//! Stream-control contract for `MockKafkaDriver` (T013): what pause, resume, seek and
//! stop do to one live subscription — the semantics behind the UI's 暂停/恢复/停止
//! buttons — all driven through the public `KafkaDriver` trait and its control handle.
//!
//! Timers are paused and every read is bounded by a timeout, so the clock auto-advances
//! through the mock pump's poll interval: the suite is fast and cannot hang. Pump-task
//! exit itself is owned by T068; here only the observable stream contract is locked —
//! the stream ends and nothing new arrives after a stop.

use std::time::Duration;

use futures::StreamExt;
use kafka_core::cluster::ClusterConfig;
use kafka_core::error::DriverError;
use kafka_core::message::{MessageEvent, MessageStream, OffsetStart, ProduceMessage, ProduceReq, ProduceResult, SubscribeReq};
use kafka_core::topic::CreateTopicReq;
use kafka_driver::driver::KafkaDriver;
use kafka_driver::mock::MockKafkaDriver;

/// Generous against the mock pump's 500ms poll interval: many ticks fit inside it on a
/// paused clock, so a working pause and a working stop both settle well within the bound.
const TIMEOUT: Duration = Duration::from_secs(5);

/// A connected driver with one freshly created topic.
async fn driver_with(topic: &str, partitions: u32) -> MockKafkaDriver {
    let driver = MockKafkaDriver::new();
    driver.connect(&ClusterConfig::mock("stream-control-test")).await.expect("connect should succeed");
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

fn offsets(events: &[MessageEvent]) -> Vec<i64> {
    events.iter().map(|e| e.offset).collect()
}

fn bodies(events: &[MessageEvent]) -> Vec<String> {
    events.iter().map(|e| String::from_utf8_lossy(&e.value).into_owned()).collect()
}

/// A subscription pinned to `partitions`, handing over at most `max` messages.
fn live_on(topic: &str, partitions: Vec<u32>, start: OffsetStart, max: u64) -> SubscribeReq {
    SubscribeReq {
        topic: topic.to_string(),
        partitions,
        start,
        follow: true,
        max_messages: Some(max),
        ..Default::default()
    }
}

async fn subscribe(driver: &MockKafkaDriver, req: SubscribeReq) -> MessageStream {
    driver.subscribe(req).await.expect("subscribe should succeed")
}

/// Reads up to `count` events, stopping early when the stream ends on its own.
async fn read(stream: &mut MessageStream, count: usize) -> Vec<MessageEvent> {
    let mut out = Vec::with_capacity(count);
    while out.len() < count {
        match tokio::time::timeout(TIMEOUT, stream.next()).await {
            Ok(Some(Ok(event))) => out.push(event),
            Ok(None) => break,
            other => panic!("the stream failed or stalled: {other:?}"),
        }
    }
    out
}

/// While paused, nothing arrives and the subscription must not end: a pause suspends
/// delivery, it does not close the stream.
async fn expect_paused_silence<S>(stream: &mut S, why: &str)
where
    S: StreamExt<Item = Result<MessageEvent, DriverError>> + Unpin,
{
    match tokio::time::timeout(TIMEOUT, stream.next()).await {
        Err(_) => {}
        Ok(None) => panic!("{why}: the subscription ended while it was only paused"),
        Ok(Some(event)) => panic!("{why}: a message arrived while paused: {event:?}"),
    }
}

/// Drains whatever the stream still holds and asserts it then ends on its own, which
/// is how a stopped or dropped subscription presents itself. Panics if it stalls.
async fn drain_to_end<S>(stream: &mut S) -> usize
where
    S: StreamExt<Item = Result<MessageEvent, DriverError>> + Unpin,
{
    let mut count = 0;
    loop {
        match tokio::time::timeout(TIMEOUT, stream.next()).await {
            Ok(Some(Ok(_))) => count += 1,
            Ok(Some(Err(err))) => panic!("the stream failed while draining: {err:?}"),
            Ok(None) => return count,
            Err(_) => panic!("the stream stalled instead of ending"),
        }
    }
}

/// Pause holds back everything the log already has; a resume delivers exactly the
/// backlog, in order, and the subscription keeps running afterwards.
#[tokio::test(start_paused = true)]
async fn a_paused_subscription_holds_back_messages_until_resumed() {
    let driver = driver_with("pause.topic", 1).await;
    produce(&driver, "pause.topic", text(1, "m-")).await;
    let mut stream = subscribe(&driver, live_on("pause.topic", Vec::new(), OffsetStart::Earliest, 10)).await;
    let events = read(&mut stream, 1).await;
    assert_eq!(offsets(&events), vec![0], "the first backlog message is delivered before the pause");

    stream.controls().pause().expect("pause must reach the pump");
    produce(&driver, "pause.topic", text(3, "held-")).await;
    expect_paused_silence(&mut stream, "while paused").await;

    stream.controls().resume().expect("resume must reach the pump");
    let events = read(&mut stream, 3).await;
    assert_eq!(bodies(&events), vec!["held-0", "held-1", "held-2"], "the backlog is delivered once resumed");
    assert_eq!(offsets(&events), vec![1, 2, 3], "the pause hid messages, it did not drop them");

    produce(&driver, "pause.topic", text(1, "after-")).await;
    let events = read(&mut stream, 1).await;
    assert_eq!(offsets(&events), vec![4], "the subscription is still live after a resume");
}

/// Seek repositions a caught-up subscription's cursor: the stream replays the log
/// starting exactly at the requested offset.
#[tokio::test(start_paused = true)]
async fn a_seek_repositions_the_stream_to_the_requested_offset() {
    let driver = driver_with("seek.topic", 1).await;
    produce(&driver, "seek.topic", text(5, "m-")).await;
    let mut stream = subscribe(&driver, live_on("seek.topic", Vec::new(), OffsetStart::Earliest, 10)).await;
    let events = read(&mut stream, 5).await;
    assert_eq!(offsets(&events), vec![0, 1, 2, 3, 4], "the whole backlog is delivered before the seek");

    stream.controls().seek("seek.topic", 0, 2).expect("seek must reach the pump");
    let events = read(&mut stream, 3).await;
    assert_eq!(offsets(&events), vec![2, 3, 4], "the stream resumes exactly at the seek offset");
    assert_eq!(bodies(&events), vec!["m-2", "m-3", "m-4"], "seek replays the log, it does not skip or restart");
}

/// A seek naming a partition the subscription does not cover changes nothing: the
/// cursor of the covered partition stays where it was and delivery continues.
#[tokio::test(start_paused = true)]
async fn a_seek_for_an_unsubscribed_partition_is_ignored() {
    let driver = driver_with("seek-out.topic", 2).await;
    let mut on_zero = text(2, "p0-");
    for message in &mut on_zero {
        message.partition = Some(0);
    }
    let mut on_one = text(2, "p1-");
    for message in &mut on_one {
        message.partition = Some(1);
    }
    produce(&driver, "seek-out.topic", on_zero).await;
    produce(&driver, "seek-out.topic", on_one).await;

    let mut stream = subscribe(&driver, live_on("seek-out.topic", vec![0], OffsetStart::Earliest, 10)).await;
    let events = read(&mut stream, 2).await;
    assert_eq!(bodies(&events), vec!["p0-0", "p0-1"], "only the subscribed partition is streamed");

    stream.controls().seek("seek-out.topic", 1, 0).expect("seek must reach the pump");
    let mut more = text(2, "p0-");
    for message in &mut more {
        message.partition = Some(0);
    }
    produce(&driver, "seek-out.topic", more).await;
    let events = read(&mut stream, 2).await;
    assert_eq!(offsets(&events), vec![2, 3], "the out-of-scope seek left the covered partition's cursor untouched");
    assert_eq!(bodies(&events), vec!["p0-0", "p0-1"], "the cursor never rewound");
}

/// A stop ends the stream: it drains what it already holds, then yields nothing, and
/// messages produced afterwards never reach the closed subscription.
#[tokio::test(start_paused = true)]
async fn a_stop_command_ends_the_stream_and_delivers_nothing_afterwards() {
    let driver = driver_with("stop.topic", 1).await;
    produce(&driver, "stop.topic", text(4, "m-")).await;
    let mut stream = subscribe(&driver, live_on("stop.topic", Vec::new(), OffsetStart::Earliest, 10)).await;
    let events = read(&mut stream, 4).await;
    assert_eq!(offsets(&events), vec![0, 1, 2, 3], "the whole backlog arrives before the stop");

    stream.controls().stop().expect("stop must reach the pump");
    let drained = drain_to_end(&mut stream).await;
    assert!(drained <= 1, "a stop may drain a little already-buffered work, got {drained}");

    produce(&driver, "stop.topic", text(2, "post-")).await;
    let delivered = drain_to_end(&mut stream).await;
    assert_eq!(delivered, 0, "the stream stays ended and silent for messages produced after the stop");
}

/// Dropping a subscription without any command ends its stream too, and a subscription
/// boxed away forever must not poison the driver (no deadlock, log intact).
#[tokio::test(start_paused = true)]
async fn a_dropped_subscription_ends_its_stream_and_the_driver_survives_it() {
    let driver = driver_with("drop.topic", 1).await;
    produce(&driver, "drop.topic", text(3, "m-")).await;
    let mut inner = subscribe(&driver, live_on("drop.topic", Vec::new(), OffsetStart::Earliest, 2)).await.into_stream();
    let drained = drain_to_end(&mut inner).await;
    assert!(drained <= 2, "only the subscription's own quota could still be buffered, got {drained}");

    produce(&driver, "drop.topic", text(2, "post-")).await;
    let mut fresh = subscribe(&driver, live_on("drop.topic", Vec::new(), OffsetStart::Offset(3), 2)).await;
    let events = read(&mut fresh, 2).await;
    assert_eq!(bodies(&events), vec!["post-0", "post-1"], "produce after a drop still lands in the log");
    assert_eq!(offsets(&events), vec![3, 4], "a fresh subscription keeps the growing offsets");
}

/// Two subscriptions on one topic steer independently: pausing one never touches the
/// other's delivery, and each reads the shared log from its own cursor.
#[tokio::test(start_paused = true)]
async fn two_subscriptions_on_one_topic_steer_independently() {
    let driver = driver_with("shared.topic", 1).await;
    produce(&driver, "shared.topic", text(2, "m-")).await;

    let mut paused_one = subscribe(&driver, live_on("shared.topic", Vec::new(), OffsetStart::Earliest, 10)).await;
    let mut running_one = subscribe(&driver, live_on("shared.topic", Vec::new(), OffsetStart::Earliest, 10)).await;

    paused_one.controls().pause().expect("pause must reach the pump");
    let events = read(&mut running_one, 2).await;
    assert_eq!(offsets(&events), vec![0, 1], "the other subscription is unaffected by the pause");

    produce(&driver, "shared.topic", text(2, "post-")).await;
    let events = read(&mut running_one, 2).await;
    assert_eq!(offsets(&events), vec![2, 3], "live messages reach the subscription that was not paused");

    paused_one.controls().resume().expect("resume must reach the pump");
    let events = read(&mut paused_one, 4).await;
    assert_eq!(offsets(&events), vec![0, 1, 2, 3], "the paused one keeps its own cursor: backlog plus what it missed");
    assert_eq!(bodies(&events), vec!["m-0", "m-1", "post-0", "post-1"], "each subscription reads the same log from its own cursor");
}
