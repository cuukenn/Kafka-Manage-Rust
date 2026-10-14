//! T037 event pump: batching/threshold/shutdown behaviour.
//!
//! All timing is driven by the paused virtual clock (`start_paused`): tokio auto-advances
//! to the earliest timer, so the 50ms window is pinned exactly and no test ever waits on
//! the wall clock. Waits are bounded (`tokio::time::timeout`) so a stuck pump fails fast.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::runtime::Handle;

use kafka_app::bus::{AppEvent, EventBus};
use kafka_core::protocol::event;
use webview_bridge::pump::{EventPump, EventSink, MAX_BATCH_DELAY, MAX_BATCH_EVENTS};

/// Fake outlet: keeps every batch string for inspection (T038 swaps in wry).
#[derive(Clone, Default)]
struct FakeSink(Arc<Mutex<Vec<String>>>);

impl FakeSink {
    /// Parsed batches: one `Vec<Value>` (the JSON array) per `push_batch` call.
    fn batches(&self) -> Vec<Vec<serde_json::Value>> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|raw| serde_json::from_str(raw).expect("batch must be a JSON array"))
            .collect()
    }

    fn sizes(&self) -> Vec<usize> {
        self.batches().iter().map(Vec::len).collect()
    }

    fn total(&self) -> usize {
        self.sizes().iter().sum()
    }
}

impl EventSink for FakeSink {
    fn push_batch(&self, batch_json: &str) {
        self.0.lock().unwrap().push(batch_json.to_string());
    }
}

fn log_line(n: usize) -> AppEvent {
    AppEvent::LogLine {
        level: "info".into(),
        target: "test".into(),
        message: format!("m{n}"),
    }
}

/// Just past the window, so a time flush has definitely fired.
fn after_window() -> Duration {
    MAX_BATCH_DELAY + Duration::from_millis(1)
}

#[tokio::test(start_paused = true)]
async fn envelope_shape_matches_wire_contract() {
    let bus = EventBus::default();
    let sink = FakeSink::default();
    let pump = EventPump::spawn(&bus, Arc::new(sink.clone()));

    bus.publish(log_line(7));
    tokio::time::sleep(after_window()).await;

    let batches = sink.batches();
    assert_eq!(batches.len(), 1, "one event must produce exactly one batch");
    assert_eq!(batches[0].len(), 1);
    let sent = &batches[0][0];
    assert_eq!(sent["event"], event::LOG_LINE);
    assert_eq!(sent["payload"]["message"], "m7");
    assert_eq!(sent["payload"]["level"], "info");
    assert!(sent["ts"].is_i64(), "ts must be a millisecond number");
    pump.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn time_flush_fires_at_50ms_not_before() {
    let bus = EventBus::default();
    let sink = FakeSink::default();
    let pump = EventPump::spawn(&bus, Arc::new(sink.clone()));

    for i in 0..3 {
        bus.publish(log_line(i));
    }
    // Half the window: the clock cannot reach the deadline, so nothing may be pushed.
    tokio::time::sleep(MAX_BATCH_DELAY / 2).await;
    assert!(
        sink.0.lock().unwrap().is_empty(),
        "batch must wait for the 50ms window"
    );

    // Auto-advance crosses the 50ms deadline and the pump flushes the whole batch.
    tokio::time::sleep(after_window()).await;
    assert_eq!(sink.sizes(), vec![3], "one batch holding all 3 events");
    pump.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn capacity_flush_at_200_without_clock_advance() {
    let bus = EventBus::new(1024);
    let sink = FakeSink::default();
    let pump = EventPump::spawn(&bus, Arc::new(sink.clone()));

    for i in 0..(MAX_BATCH_EVENTS + 50) {
        bus.publish(log_line(i));
    }
    // 1ms is far inside the window: the only flush that can have happened is the 200 cap.
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert_eq!(
        sink.sizes(),
        vec![MAX_BATCH_EVENTS],
        "first batch must be exactly 200"
    );

    // The 50 leftovers wait for the next window.
    tokio::time::sleep(after_window()).await;
    assert_eq!(sink.sizes(), vec![MAX_BATCH_EVENTS, 50]);
    pump.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_flushes_pending_and_ends_task() {
    let bus = EventBus::default();
    let sink = FakeSink::default();
    let metrics = Handle::current().metrics();
    let before = metrics.num_alive_tasks();

    let pump = EventPump::spawn(&bus, Arc::new(sink.clone()));
    assert!(
        metrics.num_alive_tasks() > before,
        "spawn must leave a live task"
    );

    for i in 0..5 {
        bus.publish(log_line(i));
    }
    // No clock advance at all: shutdown must drain the queue and flush it.
    tokio::time::timeout(Duration::from_secs(5), pump.shutdown())
        .await
        .expect("pump task must finish on shutdown");

    assert_eq!(sink.sizes(), vec![5], "no event may be lost on shutdown");
    assert_eq!(metrics.num_alive_tasks(), before, "pump task must be gone");
}

#[tokio::test(start_paused = true)]
async fn closed_bus_flushes_remainder_and_exits() {
    let bus = EventBus::default();
    let sink = FakeSink::default();
    let pump = EventPump::spawn(&bus, Arc::new(sink.clone()));

    for i in 0..3 {
        bus.publish(log_line(i));
    }
    drop(bus); // last sender gone -> recv() reports Closed once the buffer drains

    tokio::time::timeout(Duration::from_secs(5), pump.shutdown())
        .await
        .expect("pump must exit by itself when the bus closes");
    assert_eq!(sink.total(), 3, "remainder must be flushed before exiting");
}

#[tokio::test(start_paused = true)]
async fn two_pumps_each_receive_every_event() {
    let bus = EventBus::default();
    let a = FakeSink::default();
    let b = FakeSink::default();
    let pump_a = EventPump::spawn(&bus, Arc::new(a.clone()));
    let pump_b = EventPump::spawn(&bus, Arc::new(b.clone()));

    for i in 0..3 {
        bus.publish(log_line(i));
    }
    tokio::time::sleep(after_window()).await;

    assert_eq!(a.sizes(), vec![3]);
    assert_eq!(
        b.sizes(),
        vec![3],
        "broadcast fan-out: both subscriptions get the full stream"
    );
    pump_a.shutdown().await;
    pump_b.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn lagged_subscription_warns_and_keeps_pumping() {
    // Smallest legal capacity: the pump is not scheduled while the test publishes,
    // so the oldest events are evicted and recv() reports Lagged(4).
    let bus = EventBus::new(16);
    let sink = FakeSink::default();
    let pump = EventPump::spawn(&bus, Arc::new(sink.clone()));

    for i in 0..20 {
        bus.publish(log_line(i));
    }
    tokio::time::sleep(after_window()).await;

    assert_eq!(bus.lag_count(), 4, "4 events lost to the slow subscriber");
    assert_eq!(
        sink.sizes(),
        vec![16],
        "the surviving 16 must still be delivered"
    );

    // The pump must still be running afterwards.
    bus.publish(log_line(99));
    tokio::time::sleep(after_window()).await;
    assert_eq!(sink.sizes(), vec![16, 1]);
    pump.shutdown().await;
}
