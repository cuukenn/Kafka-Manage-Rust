//! T060 — consume throttle / backpressure limits.
//!
//! Pins the contract the UI relies on under load:
//! - the bus is capacity-bounded (broadcast), so a burst never grows it without bound;
//! - a fast subscriber that keeps up receives every message, in order, with no loss;
//! - a slow subscriber loses events drop-OLDEST-first: `Lagged(n)` reports the loss,
//!   `EventBus::lag_count` grows, and only the newest events survive;
//! - after a `Lagged` gap the same receiver keeps working: no panic, no deadlock;
//! - R27: each subscription reports a `dropped` counter on `SubscriptionInfo`
//!   (wire field `dropped`, camelCase). Attribution: the forwarding task counts
//!   every `MessageReceived` publish that returned 0, i.e. the bus had no live
//!   receiver at that moment, so the event could reach nobody. Slow-receiver
//!   drop-oldest losses stay in the global `EventBus::lag_count` (the bus cannot
//!   attribute them per subscription without a redesign).

use std::time::Duration;

use kafka_app::bus::{AppEvent, BusRecvError, BusReceiver, EventBus, SubscriptionStatus};
use kafka_app::services::{ClusterService, ConsumeService};
use kafka_app::store::Store;
use kafka_core::ClusterId;
use kafka_core::cluster::ClusterConfig;
use kafka_core::message::{OffsetStart, ProduceMessage, ProduceReq, SubscribeReq, SubscriptionId};
use kafka_core::topic::CreateTopicReq;

struct Harness {
    bus: EventBus,
    consume: ConsumeService,
    clusters: ClusterService,
    cluster_id: ClusterId,
    topic: String,
}

/// Mock cluster (1 partition, 1 ms virtual pump ticks) with an empty topic.
/// No bus subscriber is created here: tests subscribe exactly when they want.
async fn harness(topic: &str, capacity: usize) -> Harness {
    let store = Store::open_in_memory().expect("in-memory store");
    let bus = EventBus::new(capacity);
    let clusters = ClusterService::new(store, bus.clone());

    let mut config = ClusterConfig::mock("throttle-demo");
    config.timeouts.poll_ms = 1;
    let saved = clusters.save(&config).await.expect("save cluster");
    clusters.connect(&saved.id).await.expect("connect cluster");

    let consume = ConsumeService::new(clusters.clone());
    let driver = clusters.driver(&saved.id).expect("driver");
    driver
        .create_topic(&CreateTopicReq {
            name: topic.to_string(),
            num_partitions: 1,
            ..Default::default()
        })
        .await
        .expect("create topic");

    Harness {
        bus,
        consume,
        clusters,
        cluster_id: saved.id,
        topic: topic.to_string(),
    }
}

impl Harness {
    /// Produce `count` text messages labelled `m{from}..m{from+count-1}`.
    async fn produce(&self, from: usize, count: usize) {
        let driver = self.clusters.driver(&self.cluster_id).expect("driver");
        let req = ProduceReq {
            topic: self.topic.clone(),
            messages: (from..from + count).map(|i| ProduceMessage::text(format!("m{i}"))).collect(),
        };
        let results = driver.produce(req).await.expect("produce");
        assert!(
            results.iter().all(|r| r.error.is_none()),
            "produce failed: {results:?}"
        );
    }

    /// Open an Earliest+follow subscription over the harness topic.
    async fn start_follow(&self) -> SubscriptionId {
        let info = self
            .consume
            .start(
                &self.cluster_id,
                SubscribeReq {
                    topic: self.topic.clone(),
                    start: OffsetStart::Earliest,
                    follow: true,
                    ..Default::default()
                },
            )
            .await
            .expect("start subscription");
        info.subscription_id
    }

    fn counters(&self, id: &SubscriptionId) -> (u64, u64) {
        let sub = self
            .consume
            .list()
            .into_iter()
            .find(|s| s.subscription_id == *id)
            .expect("subscription is registered");
        (sub.delivered, sub.dropped)
    }

    /// Wait until the forwarding task has published exactly `want` counters.
    async fn wait_counters(&self, id: &SubscriptionId, want: (u64, u64)) {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if self.counters(id) == want {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("counters never reached {want:?} for {id}"));
    }
}

fn message_offset(ev: &AppEvent) -> Option<i64> {
    match ev {
        AppEvent::MessageReceived { message, .. } => Some(message.offset),
        _ => None,
    }
}

/// Fast subscriber + 5000-message burst: everything arrives, in order, loss-free.
#[tokio::test]
async fn burst_fast_subscriber_loses_nothing() {
    let h = harness("burst", 8192).await;
    h.produce(0, 5000).await;
    let id = h.start_follow().await;

    let mut rx = h.bus.subscribe();
    let mut expected_offset = 0i64;
    while expected_offset < 5000 {
        let ev = tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .expect("fast subscriber deadlocked")
            .expect("fast subscriber must not lag or see a closed bus");
        match ev {
            AppEvent::MessageReceived { subscription_id, message } => {
                assert_eq!(subscription_id, id);
                assert_eq!(message.offset, expected_offset, "burst must arrive in order");
                assert_eq!(
                    message.value_utf8(),
                    Some(format!("m{expected_offset}").as_str()),
                    "payload must match the produced message"
                );
                expected_offset += 1;
            }
            other => panic!("unexpected event while draining burst: {other:?}"),
        }
    }
    assert_eq!(h.bus.lag_count(), 0, "a keeping-up subscriber loses nothing");
    h.wait_counters(&id, (5000, 0)).await;
}

/// Slow subscriber + 5000-message burst on a 64-slot bus: growth is capped at the
/// bus capacity, `lag_count` grows, and the survivors are the NEWEST events
/// (drop-oldest: the first losses are the earliest offsets).
#[tokio::test]
async fn slow_subscriber_growth_is_bounded_and_drop_is_oldest_first() {
    const CAP: usize = 64;
    const BURST: usize = 5000;
    let h = harness("slow", CAP).await;
    let mut rx = h.bus.subscribe();
    h.produce(0, BURST).await;
    let id = h.start_follow().await;

    // The subscriber never reads while the burst flows. The forwarding task still
    // finishes every message: broadcast publish never blocks, so `delivered`
    // reaches the full burst while the bus buffer stays capped at `CAP`.
    h.wait_counters(&id, (BURST as u64, 0)).await;

    let lost = match tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("deadlock: bus stopped feeding the subscriber")
    {
        Err(BusRecvError::Lagged(n)) => n,
        other => panic!("expected Lagged after an over-capacity burst, got {other:?}"),
    };
    assert!(
        lost >= (BURST - CAP) as u64 && lost < BURST as u64,
        "lagged count {lost} must cover the burst beyond capacity {CAP} but keep survivors"
    );

    let mut survivors = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(ev) => survivors.push(message_offset(&ev).expect("only message events are queued")),
            Err(BusRecvError::Empty) => break,
            Err(BusRecvError::Lagged(n)) => panic!("unexpected second Lagged while draining: {n}"),
            Err(BusRecvError::Closed) => break,
        }
    }
    assert!(
        !survivors.is_empty() && survivors.len() <= CAP,
        "survivors {} must be non-empty and bounded by bus capacity {CAP}",
        survivors.len()
    );
    assert!(survivors.windows(2).all(|w| w[0] < w[1]), "survivors must stay ordered");
    assert!(
        survivors[0] >= (BURST - CAP) as i64,
        "oldest survivor {} proves drop-OLDEST: early events were the ones lost",
        survivors[0]
    );
    assert_eq!(*survivors.last().expect("survivors are non-empty"), (BURST - 1) as i64, "newest event must survive");
    assert_eq!(h.bus.lag_count(), lost, "bus lag_count must reflect the drop-oldest losses");
}

/// After a Lagged gap the same receiver must keep working: new traffic flows,
/// the subscription stays Running, and stop still succeeds. No panic, no deadlock.
#[tokio::test]
async fn subscriber_recovers_after_lagged_without_panic_or_deadlock() {
    let h = harness("recover", 16).await;
    let mut rx = h.bus.subscribe();
    h.produce(0, 200).await;
    let id = h.start_follow().await;
    h.wait_counters(&id, (200, 0)).await;

    match tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("deadlock: bus stopped feeding the subscriber")
    {
        Err(BusRecvError::Lagged(_)) => {}
        other => panic!("expected Lagged after an over-capacity burst, got {other:?}"),
    }
    // Drain the surviving tail; a second Lagged here would mean the recovery path is broken.
    loop {
        match rx.try_recv() {
            Ok(_) => {}
            Err(BusRecvError::Empty) | Err(BusRecvError::Closed) => break,
            Err(BusRecvError::Lagged(n)) => panic!("unexpected second Lagged while draining: {n}"),
        }
    }

    // Recovery: fresh messages must flow through the same receiver.
    h.produce(200, 5).await;
    let mut recovered = 0;
    while recovered < 5 {
        let ev = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("deadlock after Lagged: recv must stay usable")
            .expect("bus closed after Lagged: recv must stay usable");
        if let AppEvent::MessageReceived { message, .. } = ev {
            assert!(message.offset >= 200, "recovered events must be the fresh tail");
            recovered += 1;
        }
    }
    assert_eq!(h.consume.state_of(&id), Some(SubscriptionStatus::Running));
    assert!(h.consume.stop(&id).await.expect("stop"), "stop must still work after Lagged");
}

/// R27: `dropped` counts forwards published while the bus had zero live
/// receivers; it stops growing once a subscriber attaches, and serializes as
/// the camelCase wire field `dropped`.
#[tokio::test]
async fn dropped_counts_publishes_with_no_bus_subscriber() {
    let h = harness("dropped", 256).await;
    h.produce(0, 100).await;
    let id = h.start_follow().await;

    // Nobody ever subscribes to this bus: every publish reaches 0 receivers.
    h.wait_counters(&id, (100, 100)).await;

    let json = serde_json::to_value(h.consume.list()).expect("SubscriptionInfo must serialize");
    assert_eq!(json[0]["dropped"].as_u64(), Some(100), "wire field must be camelCase `dropped`");
    assert_eq!(json[0]["delivered"].as_u64(), Some(100), "delivered stays the forwarded count");

    // A live receiver stops the counter: dropped is attribution, not a leak.
    let _rx: BusReceiver = h.bus.subscribe();
    h.produce(100, 10).await;
    h.wait_counters(&id, (110, 100)).await;
}
