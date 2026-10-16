//! T061 — consume backpressure at the mid-channel layer (the EventBus).
//!
//! Pins the contract the consume pipeline relies on when the UI stops draining:
//! - the bus is capacity-bounded and evicts drop-OLDEST: a starved subscriber
//!   receives exactly the NEWEST events, asserted by PAYLOAD IDENTITY, and the
//!   losses surface precisely once via `Lagged(n)` + `EventBus::lag_count`;
//! - lag is per-subscriber: a keeping-up subscriber loses nothing while another
//!   one lags, and `lag_count` counts only the starved subscriber's losses;
//! - `EventBus::new(0)` (and any capacity below the private MIN_CAPACITY floor)
//!   does not panic and behaves exactly like the floor;
//! - end-to-end: mock driver stream -> ConsumeService forwarding task -> small
//!   bus -> slow receiver: the forwarding task never stalls (`delivered` grows
//!   monotonically to the full burst, the subscription stays Running and
//!   stoppable). Per T060/R27 the per-subscription `dropped` counter counts
//!   ONLY publishes that had zero live receivers, so it must stay 0 here while
//!   the slow-receiver losses live in `EventBus::lag_count`;
//! - with every receiver dropped, `publish` returns 0 and never panics.

use std::time::Duration;

use kafka_app::bus::{AppEvent, BusRecvError, BusReceiver, EventBus, SubscriptionStatus};
use kafka_app::services::{ClusterService, ConsumeService};
use kafka_app::store::Store;
use kafka_core::ClusterId;
use kafka_core::cluster::ClusterConfig;
use kafka_core::message::{OffsetStart, ProduceMessage, ProduceReq, SubscribeReq, SubscriptionId};
use kafka_core::topic::CreateTopicReq;

/// The MIN_CAPACITY floor inside bus.rs (private there; pinned behaviourally here).
const FLOOR: usize = 16;

fn bp_line(i: usize) -> AppEvent {
    AppEvent::LogLine { level: "info".into(), target: "t061".into(), message: format!("bp-{i:03}") }
}

/// Recover the publish index from a drained event's payload identity.
fn bp_id(ev: &AppEvent) -> usize {
    match ev {
        AppEvent::LogLine { message, .. } => {
            message.strip_prefix("bp-").expect("identifiable payload").parse().expect("numeric id")
        }
        other => panic!("unexpected event on the bus: {other:?}"),
    }
}

struct Drain {
    lag: Option<u64>,
    ids: Vec<usize>,
}

/// Non-blocking full drain of everything a starved subscriber can still see.
fn drain(rx: &mut BusReceiver) -> Drain {
    let mut lag = None;
    let mut ids = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(ev) => ids.push(bp_id(&ev)),
            Err(BusRecvError::Lagged(n)) => {
                assert!(lag.is_none(), "a static burst must report Lagged exactly once, got a second Lagged({n})");
                lag = Some(n);
            }
            Err(BusRecvError::Empty) => break,
            Err(BusRecvError::Closed) => break,
        }
    }
    Drain { lag, ids }
}

/// Small capacity (8, below the floor) + 32 identifiable publishes to a
/// subscriber that never drains: it must receive exactly the NEWEST events
/// (drop-OLDEST), with exact accounting against `lag_count`.
#[tokio::test]
async fn drop_oldest_keeps_newest_by_payload_identity() {
    const TOTAL: usize = 32;
    // 8 is below MIN_CAPACITY on purpose: the bus clamps to FLOOR.
    let bus = EventBus::new(8);
    let mut slow = bus.subscribe();

    for i in 0..TOTAL {
        assert_eq!(bus.publish(bp_line(i)), 1, "the lone subscriber is live");
    }

    let d = drain(&mut slow);
    let lag = d.lag.expect("an over-capacity burst must surface as Lagged");
    assert_eq!(
        lag + d.ids.len() as u64,
        TOTAL as u64,
        "lost + survivors must account for every published event exactly"
    );
    assert_eq!(
        d.ids,
        (lag as usize..TOTAL).collect::<Vec<_>>(),
        "survivors must be exactly the newest events, in publish order (drop-OLDEST by payload identity)"
    );
    assert!(d.ids.len() <= FLOOR, "survivors {} must be capped by the clamped capacity {FLOOR}", d.ids.len());
    assert_eq!(bus.lag_count(), lag, "the bus's global overflow counter must equal the dropped count");
}

/// One slow + one fast (drained between every publish) subscriber: the fast one
/// loses nothing, the slow one lags, and `lag_count` counts only the slow one.
#[tokio::test]
async fn slow_and_fast_subscribers_lag_independently() {
    const TOTAL: usize = 40;
    let bus = EventBus::new(FLOOR);
    let mut slow = bus.subscribe();
    let mut fast = bus.subscribe();

    for i in 0..TOTAL {
        assert_eq!(bus.publish(bp_line(i)), 2, "both subscribers are live");
        // The fast subscriber keeps up: it drains between every publish.
        assert_eq!(fast.try_recv(), Ok(bp_line(i)), "the keeping-up subscriber must receive every event in order");
    }

    let d = drain(&mut slow);
    let lag = d.lag.expect("the starved subscriber must have lost events");
    assert_eq!(lag + d.ids.len() as u64, TOTAL as u64, "exact accounting for the slow subscriber");
    assert_eq!(
        d.ids,
        (lag as usize..TOTAL).collect::<Vec<_>>(),
        "the slow subscriber keeps exactly the newest events (drop-OLDEST)"
    );
    assert_eq!(bus.lag_count(), lag, "lag_count must count ONLY the slow subscriber's losses");
    // The fast subscriber is still current: nothing is queued for it anymore.
    assert_eq!(fast.try_recv(), Err(BusRecvError::Empty), "the fast subscriber must not have been evicted");
}

/// `new(0)` must not panic and must behave EXACTLY like `new(FLOOR)`:
/// identical survivors (by identity) and identical lag.
#[tokio::test]
async fn capacity_zero_clamps_to_the_floor_behaviourally() {
    const TOTAL: usize = 40;
    let zero = EventBus::new(0);
    let floored = EventBus::new(FLOOR);
    let mut rx_zero = zero.subscribe();
    let mut rx_floor = floored.subscribe();

    for i in 0..TOTAL {
        assert_eq!(zero.publish(bp_line(i)), 1);
        assert_eq!(floored.publish(bp_line(i)), 1);
    }

    let d_zero = drain(&mut rx_zero);
    let d_floor = drain(&mut rx_floor);
    assert!(d_floor.lag.is_some(), "the floor itself must still drop on an over-capacity burst");
    assert_eq!(d_zero.ids, d_floor.ids, "capacity 0 must keep the same survivors as the floor");
    assert_eq!(d_zero.lag, d_floor.lag, "capacity 0 must report the same lag as the floor");
    assert_eq!(zero.lag_count(), floored.lag_count(), "overflow counters must match the floor bus");
}

/// End-to-end: mock driver stream -> ConsumeService forwarding task -> small
/// bus -> slow receiver. The forwarding task must NOT stall: `delivered` grows
/// monotonically to the full burst, the subscription stays Running and
/// stoppable, and the slow receiver still ends up with the newest messages.
struct Harness {
    bus: EventBus,
    consume: ConsumeService,
    clusters: ClusterService,
    cluster_id: ClusterId,
    topic: String,
}

/// Mock cluster (1 partition, 1 ms virtual pump ticks) with an empty topic.
async fn harness(topic: &str, capacity: usize) -> Harness {
    let store = Store::open_in_memory().expect("in-memory store");
    let bus = EventBus::new(capacity);
    let clusters = ClusterService::new(store, bus.clone());

    let mut config = ClusterConfig::mock("bp-e2e");
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

    Harness { bus, consume, clusters, cluster_id: saved.id, topic: topic.to_string() }
}

impl Harness {
    async fn produce(&self, from: usize, count: usize) {
        let driver = self.clusters.driver(&self.cluster_id).expect("driver");
        let req = ProduceReq {
            topic: self.topic.clone(),
            messages: (from..from + count).map(|i| ProduceMessage::text(format!("m{i}"))).collect(),
        };
        let results = driver.produce(req).await.expect("produce");
        assert!(results.iter().all(|r| r.error.is_none()), "produce failed: {results:?}");
    }

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

    /// Wait until the forwarding task has delivered exactly `want` messages.
    async fn wait_delivered(&self, id: &SubscriptionId, want: u64) {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let (delivered, _) = self.counters(id);
                if delivered >= want {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("delivered never reached {want} for {id}"));
    }
}

#[tokio::test]
async fn forwarding_task_never_stalls_behind_a_slow_receiver() {
    const BURST: usize = 64;
    // 8 is below the floor on purpose: the effective bus buffer is FLOOR slots.
    let h = harness("bp-e2e", 8).await;
    let mut slow = h.bus.subscribe();

    // Produce BEFORE starting the subscription so Earliest reads the whole burst,
    // and keep `slow` starved the entire time.
    h.produce(0, BURST).await;
    let id = h.start_follow().await;

    // The forwarding task is never back-pressured by the slow receiver:
    // broadcast publish does not block, so `delivered` reaches the full burst.
    h.wait_delivered(&id, BURST as u64).await;

    // `delivered` stays monotonic across samples while the receiver is starved.
    let mut last = 0u64;
    for _ in 0..5 {
        let (delivered, _) = h.counters(&id);
        assert!(delivered >= last, "delivered must never go backwards: {delivered} < {last}");
        last = delivered;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(last, BURST as u64, "the task must have forwarded every message");

    // Drain the starved receiver: one Lagged + the newest messages by identity.
    let mut lag = None;
    let mut survivors: Vec<i64> = Vec::new();
    loop {
        match slow.try_recv() {
            Ok(AppEvent::MessageReceived { subscription_id, message }) => {
                assert_eq!(subscription_id, id);
                let value = message.value_utf8().expect("text payload");
                let n: i64 = value.strip_prefix('m').expect("identifiable payload").parse().expect("numeric id");
                assert_eq!(n, message.offset, "payload identity must match the produced offset");
                survivors.push(n);
            }
            Ok(other) => panic!("only message events should be queued here, got {other:?}"),
            Err(BusRecvError::Lagged(n)) => {
                assert!(lag.is_none(), "Lagged must be reported exactly once, got a second Lagged({n})");
                lag = Some(n);
            }
            Err(BusRecvError::Empty) => break,
            Err(BusRecvError::Closed) => break,
        }
    }
    let lost = lag.expect("an over-capacity end-to-end burst must surface as Lagged");
    assert_eq!(
        lost + survivors.len() as u64,
        BURST as u64,
        "lost + survivors must account for the whole burst"
    );
    assert_eq!(
        survivors,
        (lost as i64..BURST as i64).collect::<Vec<_>>(),
        "the slow receiver must keep exactly the newest messages (drop-OLDEST by payload identity)"
    );
    assert!(survivors.len() <= FLOOR, "survivors {} must be capped by the clamped capacity {FLOOR}", survivors.len());
    assert_eq!(h.bus.lag_count(), lost, "bus lag_count must reflect the end-to-end losses");

    // T060/R27 semantics: the per-subscription `dropped` counter counts ONLY
    // publishes that reached ZERO live receivers. One slow receiver was live
    // the whole time, so `dropped` must stay 0 even though the bus dropped
    // events for it — asserting dropped > 0 here would invent semantics the
    // product does not have. Those losses live in bus.lag_count (asserted above).
    assert_eq!(h.counters(&id).1, 0, "dropped counts zero-receiver publishes only, not slow-receiver evictions");

    // The subscription stayed responsive the whole time: still Running, stop succeeds.
    assert_eq!(h.consume.state_of(&id), Some(SubscriptionStatus::Running), "the task must not have stalled or died");
    assert!(h.consume.stop(&id).await.expect("stop must succeed"), "stop must succeed on a live subscription");
}

/// Regression pin: with every subscriber gone, publish returns 0 and never panics.
#[tokio::test]
async fn publish_returns_zero_when_every_receiver_is_dropped() {
    let bus = EventBus::new(FLOOR);
    let rx = bus.subscribe();
    drop(rx);
    assert_eq!(bus.subscriber_count(), 0);
    assert_eq!(bus.publish(bp_line(0)), 0, "publish with no receivers must return 0");
    assert_eq!(bus.publish(bp_line(1)), 0, "and keep returning 0, never panicking");
    assert_eq!(bus.lag_count(), 0, "dropping receivers must not fabricate lag");
}
