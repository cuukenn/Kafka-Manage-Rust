//! T063 — subscription teardown: graceful `stop`, natural end, and `App::shutdown` order.
//!
//! Everything runs against the mock driver. `MockState::active_pumps` (the pump's RAII
//! counter) is the leak detector: it proves the pump released, and every wait below is a
//! bounded poll with a real 2 s deadline, never a fixed sleep-and-hope.
//!
//! Coverage limit (review I1, accepted): the mock pump always breaks immediately on
//! `Stop` (`mock/pump.rs`), so these tests cannot distinguish R29's bounded graceful join
//! from a plain `task.abort()` — `STOP_GRACE` and the abort fallback branch are exercised
//! by no test here. They pin "`Stop` is honoured and the pump is released", not the grace
//! window itself; a mock knob that ignores `Stop` is the follow-up that would cover it.

use std::time::Duration;

use kafka_app::App;
use kafka_app::bus::{AppEvent, BusReceiver, EventBus, SubscriptionStatus};
use kafka_app::services::{ClusterService, ConsumeService};
use kafka_app::store::Store;
use kafka_core::ClusterId;
use kafka_core::cluster::{ClusterConfig, ConnectionState};
use kafka_core::error::ErrorKind;
use kafka_core::message::{
    MessageEvent, OffsetStart, ProduceMessage, ProduceReq, SubscribeReq, SubscriptionId,
};
use kafka_core::topic::CreateTopicReq;
use kafka_driver::mock::MockKafkaDriver;

/// Connected mock cluster with one topic and `count` messages already produced.
async fn harness(
    topic: &str,
    partitions: u32,
    count: usize,
) -> (ConsumeService, ClusterId, BusReceiver, MockKafkaDriver) {
    let store = Store::open_in_memory().expect("in-memory store");
    let bus = EventBus::new(256);
    let mut rx = bus.subscribe();
    let clusters = ClusterService::new(store, bus);

    let saved = clusters
        .save(&ClusterConfig::mock("teardown-demo"))
        .await
        .expect("save cluster");
    clusters.connect(&saved.id).await.expect("connect cluster");

    let consume = ConsumeService::new(clusters.clone());
    let mock = seed(&clusters, &saved.id, topic, partitions, count).await;

    // Drain the connect events so tests only see subscription traffic.
    while rx.try_recv().is_ok() {}
    (consume, saved.id, rx, mock)
}

/// Same as [`harness`] but wired through the full [`App`] so `shutdown()` is reachable.
async fn app_harness(topic: &str, count: usize) -> (App, ClusterId, BusReceiver, MockKafkaDriver) {
    let store = Store::open_in_memory().expect("in-memory store");
    let bus = EventBus::new(256);
    let mut rx = bus.subscribe();
    let app = App::new(store, bus);
    let clusters = app.clusters().clone();

    let saved = clusters
        .save(&ClusterConfig::mock("teardown-app"))
        .await
        .expect("save cluster");
    clusters.connect(&saved.id).await.expect("connect cluster");

    let mock = seed(&clusters, &saved.id, topic, 1, count).await;
    while rx.try_recv().is_ok() {}
    (app, saved.id, rx, mock)
}

/// Creates `topic`, produces `count` messages and returns the mock control handle
/// (downcast through the guard exactly like the other service-level tests).
async fn seed(
    clusters: &ClusterService,
    cluster: &ClusterId,
    topic: &str,
    partitions: u32,
    count: usize,
) -> MockKafkaDriver {
    let driver = clusters.driver(cluster).expect("driver");
    driver
        .create_topic(&CreateTopicReq {
            name: topic.to_string(),
            num_partitions: partitions,
            ..Default::default()
        })
        .await
        .expect("create topic");
    if count > 0 {
        let results = driver
            .produce(ProduceReq {
                topic: topic.to_string(),
                messages: (0..count)
                    .map(|i| ProduceMessage::text(format!("m{i}")))
                    .collect(),
            })
            .await
            .expect("produce");
        assert!(results.iter().all(|r| r.error.is_none()), "produce failed: {results:?}");
    }
    driver
        .as_any()
        .and_then(|any| any.downcast_ref::<MockKafkaDriver>())
        .expect("mock seam reachable through the guard")
        .clone()
}

fn live(topic: &str) -> SubscribeReq {
    SubscribeReq {
        topic: topic.to_string(),
        start: OffsetStart::Earliest,
        follow: true,
        ..Default::default()
    }
}

async fn pumps(mock: &MockKafkaDriver) -> u64 {
    mock.state_handle().read().await.active_pumps()
}

/// Bounded poll (real 2 s deadline) for the mock's live-pump counter: the leak detector
/// this card relies on — it witnesses the *pump's* release, not the forwarding task's exit
/// (see the module note). Fails loudly instead of hanging forever.
async fn wait_for_pumps(mock: &MockKafkaDriver, want: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let now = pumps(mock).await;
        if now == want {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "pump count stuck at {now}, want {want} within 2s"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn next_event(rx: &mut BusReceiver) -> AppEvent {
    tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("timed out waiting for an event")
        .expect("bus closed")
}

async fn wait_for<F>(rx: &mut BusReceiver, _what: &str, predicate: F) -> AppEvent
where
    F: Fn(&AppEvent) -> bool,
{
    loop {
        let event = next_event(rx).await;
        if predicate(&event) {
            return event;
        }
    }
}

fn message_for<'a>(event: &'a AppEvent, id: &SubscriptionId) -> Option<&'a MessageEvent> {
    match event {
        AppEvent::MessageReceived { subscription_id, message } if subscription_id == id => Some(message),
        _ => None,
    }
}

async fn wait_message(rx: &mut BusReceiver, id: &SubscriptionId) -> MessageEvent {
    let event = wait_for(rx, "message.received", |event| message_for(event, id).is_some()).await;
    message_for(&event, id).expect("matched above").clone()
}

fn drain(rx: &mut BusReceiver) -> Vec<AppEvent> {
    let mut out = Vec::new();
    while let Ok(event) = rx.try_recv() {
        out.push(event);
    }
    out
}

fn count_messages(events: &[AppEvent], id: &SubscriptionId) -> usize {
    events.iter().filter(|event| message_for(event, id).is_some()).count()
}

fn count_dones(events: &[AppEvent], id: &SubscriptionId) -> usize {
    events
        .iter()
        .filter(|event| {
            matches!(event, AppEvent::SubscriptionDone { subscription_id, .. } if subscription_id == id)
        })
        .count()
}

/// (a) `stop()` hands a `Stop` to the pump and the pump's RAII counter reaches zero.
/// NOTE (review I1): this pins "`Stop` is honoured and the pump released", NOT R29's
/// grace window — the mock pump breaks on `Stop` regardless of the join, so this test
/// also passes under a plain `task.abort()`. See the module note.
#[tokio::test]
async fn stop_hands_off_to_the_pump_and_releases_it() {
    let (consume, cluster, mut rx, mock) = harness("orders", 1, 3).await;
    let req = live("orders");
    let id = req.subscription_id.clone();
    consume.start(&cluster, req).await.expect("start");

    // The pump registers itself asynchronously: bounded wait, not a fixed sleep.
    wait_for_pumps(&mock, 1).await;
    assert_eq!(wait_message(&mut rx, &id).await.offset, 0);

    assert!(consume.stop(&id).await.expect("stop"));
    // `stop` returns only once the task ended (or the 2 s grace expired); the registry
    // entry is gone either way.
    assert_eq!(consume.state_of(&id), None, "stopped subscription must leave the registry");
    assert_eq!(consume.active_count(), 0);
    assert_eq!(consume.total_count(), 0);

    // The pump honoured `Stop` and exited, so its guard dropped the counter.
    wait_for_pumps(&mock, 0).await;

    // Take whatever the pump had already handed over before it stopped...
    let _inflight = drain(&mut rx);

    // ...then prove the forwarding really stopped: with the pump provably gone, a fresh
    // produce cannot reach the subscription any more (a still-live loop would deliver it).
    let driver = consume.clusters().driver(&cluster).expect("driver");
    let results = driver
        .produce(ProduceReq {
            topic: "orders".to_string(),
            messages: (0..2).map(|i| ProduceMessage::text(format!("after{i}"))).collect(),
        })
        .await
        .expect("produce");
    assert!(results.iter().all(|r| r.error.is_none()), "produce failed: {results:?}");

    tokio::time::sleep(Duration::from_millis(100)).await;
    let quiet = drain(&mut rx);
    assert_eq!(
        count_messages(&quiet, &id),
        0,
        "a stopped subscription must not receive messages produced after it stopped: {quiet:?}"
    );
}

/// (b) A bounded, non-follow subscription drains to a natural `Done`: the forwarding
/// task exits, the pump is released, and `prune_done` clears the registry.
#[tokio::test]
async fn bounded_subscription_ends_naturally_and_releases_the_pump() {
    let (consume, cluster, mut rx, mock) = harness("orders", 1, 5).await;
    let req = SubscribeReq {
        topic: "orders".to_string(),
        start: OffsetStart::Earliest,
        follow: false,
        max_messages: Some(2),
        ..Default::default()
    };
    let id = req.subscription_id.clone();
    consume.start(&cluster, req).await.expect("start");

    for expected in 0..2 {
        assert_eq!(wait_message(&mut rx, &id).await.offset, expected);
    }
    let event = wait_for(&mut rx, "subscription.done", |event| {
        matches!(event, AppEvent::SubscriptionDone { subscription_id, .. } if subscription_id == &id)
    })
    .await;
    match event {
        AppEvent::SubscriptionDone { delivered, reason, .. } => {
            assert_eq!(delivered, 2);
            assert_eq!(reason, None, "a clean end has no reason");
        }
        other => panic!("unexpected event: {other:?}"),
    }

    assert_eq!(consume.state_of(&id), Some(SubscriptionStatus::Done));
    assert_eq!(consume.active_count(), 0, "a finished subscription is no longer active");
    // The pump ended by itself after hitting `max_messages`.
    wait_for_pumps(&mock, 0).await;

    assert_eq!(consume.prune_done(), 1);
    assert_eq!(consume.total_count(), 0);
    assert_eq!(consume.list(), Vec::new());
}

/// (c) `App::shutdown()` is idempotent: a second call must not panic, and afterwards
/// nothing is left running (registry empty, no live pump).
#[tokio::test]
async fn shutdown_is_idempotent_and_releases_everything() {
    let (app, cluster, mut rx, mock) = app_harness("orders", 4).await;
    let req = live("orders");
    let id = req.subscription_id.clone();
    app.consume().start(&cluster, req).await.expect("start");
    wait_for_pumps(&mock, 1).await;
    assert_eq!(wait_message(&mut rx, &id).await.offset, 0);

    app.shutdown().await;
    app.shutdown().await; // must be a harmless no-op, not a panic

    assert_eq!(app.consume().active_count(), 0);
    assert_eq!(app.consume().total_count(), 0, "shutdown must empty the registry");
    wait_for_pumps(&mock, 0).await;
}

/// (d) `stop_all` alongside a single `stop` on the same id: the wire sees exactly one
/// `subscription.done`, and exactly one of the two calls reports "stopped".
/// NOTE (review M1): this is not a symmetric race — `stop` removes the registry entry
/// before its first await, so `stop_all` is in practice the caller that does the work and
/// the single `stop` returns early without touching `finished`. The invariant asserted
/// (exactly-once done; a double-count would break it) is real; the losing path is not
/// exercised.
#[tokio::test]
async fn stop_all_racing_a_single_stop_publishes_done_exactly_once() {
    let (consume, cluster, mut rx, mock) = harness("orders", 1, 3).await;
    let req = live("orders");
    let id = req.subscription_id.clone();
    consume.start(&cluster, req).await.expect("start");
    wait_for_pumps(&mock, 1).await;
    assert_eq!(consume.active_count(), 1);

    let all = consume.clone();
    let one = consume.clone();
    let id_one = id.clone();
    let (stopped_all, stopped_one) = tokio::join!(
        async move { all.stop_all().await },
        async move { one.stop(&id_one).await },
    );
    let one_reported = stopped_one.expect("single stop must not error") as usize;
    assert_eq!(
        stopped_all + one_reported,
        1,
        "exactly one of stop_all/stop must report the subscription as stopped"
    );

    // Both calls have returned, so every publish is complete: the done count is final.
    let events = drain(&mut rx);
    assert_eq!(
        count_dones(&events, &id),
        1,
        "subscription.done must be published exactly once: {events:?}"
    );
    assert_eq!(consume.total_count(), 0);
    assert_eq!(consume.active_count(), 0);
    wait_for_pumps(&mock, 0).await;
}

/// (e) Regression pin: `shutdown()` stops subscriptions BEFORE disconnecting clusters.
/// Observable order on the bus (`subscription.done` precedes `connection.state`
/// disconnected), plus the post-conditions: driver handle gone, registry empty.
/// Substitution (review M3): the card's literal "cluster disconnect → stop_all" linkage
/// does not exist — `stop_all`'s only production caller is `shutdown` — so this pins
/// `shutdown`'s internal order instead.
#[tokio::test]
async fn shutdown_stops_subscriptions_before_disconnecting_clusters() {
    let (app, cluster, mut rx, mock) = app_harness("orders", 3).await;
    let req = live("orders");
    let id = req.subscription_id.clone();
    app.consume().start(&cluster, req).await.expect("start");
    wait_for_pumps(&mock, 1).await;
    assert_eq!(wait_message(&mut rx, &id).await.offset, 0);

    app.shutdown().await;

    let events = drain(&mut rx);
    let done_at = events
        .iter()
        .position(|event| {
            matches!(event, AppEvent::SubscriptionDone { subscription_id, .. } if subscription_id == &id)
        })
        .expect("shutdown must finish the live subscription");
    let disconnected_at = events
        .iter()
        .position(|event| {
            matches!(
                event,
                AppEvent::ConnectionStateChanged { cluster_id, state: ConnectionState::Disconnected, .. }
                    if cluster_id == cluster.as_str()
            )
        })
        .expect("shutdown must disconnect the cluster");
    assert!(
        done_at < disconnected_at,
        "stop_all must run before disconnect_all: {events:?}"
    );
    match &events[done_at] {
        AppEvent::SubscriptionDone { delivered, .. } => {
            assert!(*delivered >= 1, "the subscription was live and delivering when stopped")
        }
        other => panic!("unexpected event: {other:?}"),
    }

    assert_eq!(app.consume().total_count(), 0, "registry must be empty after shutdown");
    let err = app
        .clusters()
        .driver(&cluster)
        .err()
        .expect("driver handle must be gone after shutdown");
    assert_eq!(err.kind(), ErrorKind::NotConnected);
    wait_for_pumps(&mock, 0).await;
}
