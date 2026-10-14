//! Fault injection and the `DriverEvent` stream for `MockKafkaDriver` (T015): the two
//! capabilities T029 (retry wrapper) and T062 (reconnect) are tested against, locked
//! here through the public `KafkaDriver` trait only.
//!
//! The clock is paused in every test and every read is bounded, so there are no
//! wall-clock sleeps and nothing can hang. Latency is measured with
//! `tokio::time::Instant`, which reports an injected sleep as virtual time; only lower
//! bounds are asserted, per the card. Probability rolls are exercised at their
//! deterministic extremes (`0.0` never fails, `1.0` always fails) or pinned through
//! `pin_rng_seed` (T078), so no test depends on the wall clock.

use std::time::Duration;

use kafka_core::cluster::ClusterConfig;
use kafka_core::error::{DriverError, ErrorKind};
use kafka_core::group::{ResetOffsetsReq, ResetSpec};
use kafka_core::message::{ProduceMessage, ProduceReq, ProduceResult, SubscribeReq};
use kafka_core::topic::{CreateTopicReq, DeleteTopicsReq};
use kafka_driver::driver::{DriverEvent, KafkaDriver};
use kafka_driver::mock::{FaultInjection, LatencyRange, MockKafkaDriver};
use tokio::sync::broadcast;

/// Bounds every read: events are sent synchronously, so this only turns a lost event
/// into a failure instead of a hang.
const TIMEOUT: Duration = Duration::from_secs(5);

/// The fixed injected delay of the latency test (`min == max`: no jitter).
const DELAY_MS: u64 = 400;

async fn driver(name: &str, seeded: bool) -> MockKafkaDriver {
    let driver = if seeded { MockKafkaDriver::seeded() } else { MockKafkaDriver::new() };
    driver.connect(&ClusterConfig::mock(name)).await.expect("connect should succeed");
    driver
}

async fn create(driver: &MockKafkaDriver, topic: &str, partitions: u32) {
    let mut req = CreateTopicReq::new(topic);
    req.num_partitions = partitions;
    driver.create_topic(&req).await.expect("create_topic should succeed");
}

/// Produces one message to `topic`; `index` only keeps the calls distinct.
async fn produce_text(driver: &MockKafkaDriver, topic: &str, index: u32) -> Result<Vec<ProduceResult>, DriverError> {
    driver.produce(ProduceReq::one(topic, ProduceMessage::text(format!("m{index}")))).await
}

fn on(partition: u32) -> ProduceMessage {
    ProduceMessage { partition: Some(partition), ..ProduceMessage::text("payload") }
}

/// Produces one message per listed partition; the call itself must succeed so the
/// per-message results carry the injected failure.
async fn produce_on(driver: &MockKafkaDriver, topic: &str, partitions: &[u32]) -> Vec<ProduceResult> {
    let messages = partitions.iter().copied().map(on).collect();
    driver.produce(ProduceReq { topic: topic.to_string(), messages }).await.expect("produce should succeed")
}

async fn next_event(events: &mut broadcast::Receiver<DriverEvent>) -> DriverEvent {
    match tokio::time::timeout(TIMEOUT, events.recv()).await {
        Ok(Ok(event)) => event,
        other => panic!("expected a driver event, got {other:?}"),
    }
}

/// Faults are runtime state on a live connection: the probability extremes behave
/// deterministically (`1.0` fails with the retryable `Connection` error, `0.0` never
/// fails), another kind can be swapped in, and clearing restores normal service.
#[tokio::test(start_paused = true)]
async fn failure_and_timeout_faults_are_reconfigurable_at_runtime() {
    let driver = driver("fault-rates", true).await;
    driver.set_faults(FaultInjection { fail_probability: 1.0, ..Default::default() }).await;
    for attempt in 0..5 {
        let err = produce_text(&driver, "orders", attempt).await.expect_err("probability 1.0 must fail");
        assert_eq!(err.kind(), ErrorKind::Connection, "attempt {attempt}");
        assert!(err.is_retryable(), "attempt {attempt}: {err:?}");
    }

    // Swapped in on the same handle: the timeout kind now wins over a plain failure.
    driver.set_faults(FaultInjection { timeout_probability: 1.0, timeout_ms: 120, ..Default::default() }).await;
    let err = produce_text(&driver, "orders", 9).await.expect_err("the timeout fault must fail");
    assert_eq!(err.kind(), ErrorKind::Timeout);
    assert!(matches!(&err, DriverError::Timeout { operation, elapsed_ms } if operation == "produce" && *elapsed_ms == 120));
    assert!(err.is_retryable());

    driver.set_faults(FaultInjection { fail_probability: 0.0, ..Default::default() }).await;
    for attempt in 0..4 {
        let results = produce_text(&driver, "orders", attempt).await.expect("probability 0.0 must not fail");
        assert!(results.iter().all(|r| r.error.is_none()), "attempt {attempt}: {results:?}");
    }
}

/// Injected latency stretches a call by at least the configured delay, measured in
/// virtual time against an unfaulted call that costs nothing.
#[tokio::test(start_paused = true)]
async fn injected_latency_delays_every_call() {
    let driver = driver("fault-latency", true).await;
    let started = tokio::time::Instant::now();
    driver.list_topics().await.expect("list_topics should succeed");
    let baseline = started.elapsed();
    assert!(baseline < Duration::from_millis(DELAY_MS), "an unfaulted call slept: {baseline:?}");

    driver
        .set_faults(FaultInjection {
            latency: LatencyRange { min_ms: DELAY_MS, max_ms: DELAY_MS },
            ..Default::default()
        })
        .await;
    for attempt in 0..3 {
        let started = tokio::time::Instant::now();
        driver.list_topics().await.expect("list_topics should succeed");
        let spent = started.elapsed();
        assert!(spent >= Duration::from_millis(DELAY_MS), "attempt {attempt} took only {spent:?}");
    }
}

/// `set_leader_down` is per-partition, and `clear()` reverts it: the leaderless partition
/// refuses produce (and subscribe) while its neighbours keep serving, and every partition
/// works again once the fault is cleared.
#[tokio::test(start_paused = true)]
async fn a_leaderless_partition_fails_produce_until_faults_are_cleared() {
    let driver = driver("fault-leader", false).await;
    create(&driver, "orders.leaders", 3).await;
    let mut faults = FaultInjection::default();
    faults.set_leader_down("orders.leaders", 1, true);
    driver.set_faults(faults).await;

    let results = produce_on(&driver, "orders.leaders", &[0, 1, 2]).await;
    let failed: Vec<_> = results.iter().filter(|r| r.error.is_some()).collect();
    assert_eq!(failed.len(), 1, "only the leaderless partition may fail: {results:?}");
    assert_eq!(failed[0].partition, 1);
    assert!(failed[0].error.as_deref().is_some_and(|e| e.contains("LEADER_NOT_AVAILABLE")));
    assert!(results[0].error.is_none() && results[2].error.is_none());

    // Consume side of the same injection: broker error 5 (LEADER_NOT_AVAILABLE).
    let err = driver
        .subscribe(SubscribeReq { topic: "orders.leaders".into(), partitions: vec![1], ..Default::default() })
        .await
        .expect_err("a leaderless partition cannot be subscribed");
    assert!(matches!(err, DriverError::Broker { code: 5, .. }), "{err:?}");

    let mut cleared = driver.faults().await;
    cleared.clear();
    driver.set_faults(cleared).await;
    let after = produce_on(&driver, "orders.leaders", &[0, 1, 2]).await;
    assert!(after.iter().all(|r| r.error.is_none()), "cleared faults must not fail produce: {after:?}");
}

/// The event stream is a broadcast: topic lifecycle and produce events reach every
/// subscriber, in order.
#[tokio::test(start_paused = true)]
async fn topic_and_message_events_reach_every_subscriber() {
    let driver = driver("fault-events", false).await;
    let mut first = driver.events().await.expect("the mock driver emits events");
    let mut second = driver.events().await.expect("the mock driver emits events");

    create(&driver, "orders.events", 1).await;
    driver.produce(ProduceReq::one("orders.events", ProduceMessage::text("one"))).await.expect("produce");
    driver.delete_topics(&DeleteTopicsReq { names: vec!["orders.events".into()] }).await.expect("delete");

    let expected = [
        DriverEvent::TopicCreated { topic: "orders.events".into() },
        DriverEvent::MessagesAppended { topic: "orders.events".into(), count: 1 },
        DriverEvent::TopicDeleted { topic: "orders.events".into() },
    ];
    for events in [&mut first, &mut second] {
        for want in &expected {
            assert_eq!(next_event(events).await, *want);
        }
    }
}

/// A group offset reset publishes one `GroupOffsetsReset` with the partition count it
/// touched; a dry run publishes nothing.
#[tokio::test(start_paused = true)]
async fn a_group_offset_reset_publishes_its_event() {
    let driver = driver("fault-group", true).await;
    let mut events = driver.events().await.expect("the mock driver emits events");
    let reset = |dry_run| ResetOffsetsReq {
        group_id: "checkout-service".into(),
        topics: vec!["orders".into()],
        spec: ResetSpec::Earliest,
        dry_run,
        ..Default::default()
    };
    driver.reset_offsets(&reset(false)).await.expect("reset should succeed");
    assert_eq!(
        next_event(&mut events).await,
        DriverEvent::GroupOffsetsReset { group: "checkout-service".into(), partitions: 3 }
    );

    driver.reset_offsets(&reset(true)).await.expect("a dry run still resolves");
    assert!(tokio::time::timeout(Duration::from_millis(50), events.recv()).await.is_err(), "a dry run emitted");
}

/// Dropping a subscriber is not an error for the driver: survivors still get events, and
/// a driver with no subscriber at all keeps working.
#[tokio::test(start_paused = true)]
async fn a_dropped_subscriber_does_not_disturb_the_driver() {
    let driver = driver("fault-drop", false).await;
    let mut kept = driver.events().await.expect("the mock driver emits events");
    drop(driver.events().await.expect("the mock driver emits events"));

    create(&driver, "orders.kept", 1).await;
    assert_eq!(next_event(&mut kept).await, DriverEvent::TopicCreated { topic: "orders.kept".into() });

    drop(kept);
    create(&driver, "orders.silent", 1).await;
    produce_text(&driver, "orders.silent", 1).await.expect("producing with no subscriber must still work");
    let topics = driver.list_topics().await.expect("list_topics should succeed");
    assert_eq!(topics.len(), 2, "both topics must exist with no subscriber: {topics:?}");
}

/// An injected connect refusal is a retryable `Connection` error that leaves the cluster
/// unconnected; clearing the fault lets the same config connect, and an explicit
/// disconnect clears the state again — calls are then refused with `NotConnected`
/// instead of serving stale fixture data.
#[tokio::test(start_paused = true)]
async fn connection_faults_leave_the_cluster_disconnected() {
    let driver = MockKafkaDriver::new();
    let faults =
        FaultInjection { fail_connect: true, connect_message: "bootstrap refused (injected)".into(), ..Default::default() };
    driver.set_faults(faults).await;

    let err = driver.connect(&ClusterConfig::mock("fault-connect")).await.expect_err("fail_connect must refuse");
    assert_eq!(err.kind(), ErrorKind::Connection);
    assert!(err.is_retryable());
    assert!(matches!(&err, DriverError::Connection { message, .. } if message.contains("bootstrap refused")));
    assert!(!driver.is_connected().await, "a refused connect must not leave the driver connected");

    driver.set_faults(FaultInjection::default()).await;
    driver.connect(&ClusterConfig::mock("fault-connect")).await.expect("clearing the fault must connect");
    assert!(driver.is_connected().await);

    driver.disconnect().await.expect("disconnect should succeed");
    assert!(!driver.is_connected().await);
    let info = driver.cluster_info().await.expect_err("no cluster while disconnected");
    assert_eq!(info.kind(), ErrorKind::NotConnected);
    driver.disconnect().await.expect("disconnecting twice is a no-op");
}

/// A fresh, seeded mock cluster with the fault RNG pinned to `seed` (T078).
async fn pinned_driver(name: &str, seed: u64) -> MockKafkaDriver {
    let driver = MockKafkaDriver::seeded();
    driver.pin_rng_seed(seed);
    driver.connect(&ClusterConfig::mock(name)).await.expect("connect should succeed");
    driver
}

/// T078: `pin_rng_seed` restarts the roll counter and drops the wall-clock mix, so two
/// fresh instances pinned to the same seed replay the exact same mid-range (`0 < p < 1`)
/// failure sequence — and the sequence genuinely exercises both outcomes, unlike the
/// `0.0`/`1.0` extremes the other tests rely on.
#[tokio::test(start_paused = true)]
async fn pinned_seed_replays_identical_midrange_failure_sequences() {
    const SEED: u64 = 0x0785_EED5;
    let mut patterns = Vec::new();
    for name in ["pinned-fail-a", "pinned-fail-b"] {
        let driver = pinned_driver(name, SEED).await;
        driver
            .set_faults(FaultInjection {
                fail_probability: 0.5,
                fail_operation: Some("produce".into()),
                ..Default::default()
            })
            .await;
        let mut pattern = Vec::with_capacity(40);
        for attempt in 0..40u32 {
            pattern.push(produce_text(&driver, "orders", attempt).await.is_ok());
        }
        patterns.push(pattern);
    }
    assert_eq!(patterns[0], patterns[1], "same pinned seed must replay the same failure pattern");
    let failures = patterns[0].iter().filter(|ok| !**ok).count();
    assert!(
        failures > 0 && failures < patterns[0].len(),
        "p=0.5 must roll both outcomes, got {failures}/40 failures: {:?}",
        patterns[0]
    );
}

/// T078: every faulted driver call rolls the RNG once for injected latency; with the
/// seed pinned, two fresh instances observe identical multi-roll sequences — the exact
/// rolled milliseconds, read off virtual time — and the sequence genuinely jitters.
#[tokio::test(start_paused = true)]
async fn pinned_seed_repeats_the_multi_roll_latency_sequence() {
    const SEED: u64 = 0x51EE_D00D;
    let mut sequences = Vec::new();
    for name in ["pinned-rolls-a", "pinned-rolls-b"] {
        let driver = pinned_driver(name, SEED).await;
        driver
            .set_faults(FaultInjection { latency: LatencyRange { min_ms: 1, max_ms: 49 }, ..Default::default() })
            .await;
        let mut rolled = Vec::with_capacity(12);
        for _ in 0..12 {
            let started = tokio::time::Instant::now();
            driver.list_topics().await.expect("list_topics should succeed");
            rolled.push(started.elapsed().as_millis() as u64);
        }
        sequences.push(rolled);
    }
    assert_eq!(sequences[0], sequences[1], "same pinned seed must replay the same roll sequence");
    assert!(sequences[0].iter().all(|ms| (1..=49).contains(ms)), "rolls must stay in range: {:?}", sequences[0]);
    assert!(
        sequences[0].iter().min() != sequences[0].iter().max(),
        "the sequence must jitter, not repeat one value: {:?}",
        sequences[0]
    );
}
