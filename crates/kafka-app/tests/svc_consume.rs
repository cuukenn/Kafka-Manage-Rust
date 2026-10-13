//! ConsumeService: subscription lifecycle, control commands, and bus events.
//!
//! Every test drives the mock driver through the service and asserts on the bus, so the
//! wire contract the frontend will see is pinned here. Timers are virtual (`#[tokio::test]`),
//! so the mock pump's 500 ms poll interval costs nothing.

use std::time::Duration;

use kafka_app::bus::{AppEvent, BusReceiver, EventBus, SubscriptionStatus};
use kafka_app::services::{ClusterService, ConsumeService, ServiceError};
use kafka_app::store::Store;
use kafka_core::ClusterId;
use kafka_core::cluster::ClusterConfig;
use kafka_core::error::ErrorKind;
use kafka_core::message::{MessageEvent, OffsetStart, ProduceMessage, ProduceReq, SubscribeReq, SubscriptionId};
use kafka_core::topic::{CreateTopicReq, DeleteTopicsReq};

/// Connected mock cluster with one topic and `count` messages already produced.
async fn harness(topic: &str, partitions: u32, count: usize) -> (ConsumeService, ClusterId, BusReceiver) {
    let store = Store::open_in_memory().expect("in-memory store");
    let bus = EventBus::new(256);
    let mut rx = bus.subscribe();
    let clusters = ClusterService::new(store, bus);

    let saved = clusters
        .save(&ClusterConfig::mock("consume-demo"))
        .await
        .expect("save cluster");
    clusters.connect(&saved.id).await.expect("connect cluster");

    let consume = ConsumeService::new(clusters.clone());
    let driver = clusters.driver(&saved.id).expect("driver");
    driver
        .create_topic(&CreateTopicReq {
            name: topic.to_string(),
            num_partitions: partitions,
            ..Default::default()
        })
        .await
        .expect("create topic");
    if count > 0 {
        let req = ProduceReq {
            topic: topic.to_string(),
            messages: (0..count).map(|i| ProduceMessage::text(format!("m{i}"))).collect(),
        };
        let results = driver.produce(req).await.expect("produce");
        assert!(
            results.iter().all(|r| r.error.is_none()),
            "produce failed: {results:?}"
        );
    }

    // Drain the connect events so tests only see consume traffic.
    while rx.try_recv().is_ok() {}
    (consume, saved.id, rx)
}

fn live(topic: &str) -> SubscribeReq {
    SubscribeReq {
        topic: topic.to_string(),
        start: OffsetStart::Earliest,
        follow: true,
        ..Default::default()
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
    let event = wait_for(rx, "message.received", |event| {
        message_for(event, id).is_some()
    })
    .await;
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

async fn produce_more(consume: &ConsumeService, cluster: &ClusterId, topic: &str, count: usize) {
    let driver = consume.clusters().driver(cluster).expect("driver");
    let req = ProduceReq {
        topic: topic.to_string(),
        messages: (0..count).map(|i| ProduceMessage::text(format!("later{i}"))).collect(),
    };
    let results = driver.produce(req).await.expect("produce");
    assert!(results.iter().all(|r| r.error.is_none()), "{results:?}");
}

#[tokio::test]
async fn delivers_then_pauses_resumes_and_stops() {
    let (consume, cluster, mut rx) = harness("orders", 1, 3).await;
    let req = live("orders");
    let id = req.subscription_id.clone();

    let info = consume.start(&cluster, req).await.expect("start");
    assert_eq!(info.state, SubscriptionStatus::Running);
    assert_eq!(info.delivered, 0);
    assert_eq!(info.topic, "orders");
    assert_eq!(consume.state_of(&id), Some(SubscriptionStatus::Running));

    for expected in 0..3 {
        let message = wait_message(&mut rx, &id).await;
        assert_eq!(message.offset, expected);
        assert_eq!(message.value_utf8(), Some(format!("m{expected}").as_str()));
    }

    consume.pause(&id).expect("pause");
    let event = wait_for(&mut rx, "subscription.state", |event| {
        matches!(event, AppEvent::SubscriptionState { subscription_id, state: SubscriptionStatus::Paused, .. }
            if subscription_id == &id)
    })
    .await;
    assert_eq!(event.event_name(), "subscription.state");
    assert_eq!(consume.state_of(&id), Some(SubscriptionStatus::Paused));

    produce_more(&consume, &cluster, "orders", 2).await;
    tokio::time::sleep(Duration::from_millis(2_000)).await;
    let quiet = drain(&mut rx);
    assert_eq!(count_messages(&quiet, &id), 0, "paused subscription kept delivering: {quiet:?}");

    consume.resume(&id).expect("resume");
    assert_eq!(consume.state_of(&id), Some(SubscriptionStatus::Running));
    let resumed = wait_message(&mut rx, &id).await;
    assert_eq!(resumed.offset, 3);
    let resumed = wait_message(&mut rx, &id).await;
    assert_eq!(resumed.offset, 4);

    assert!(consume.stop(&id).await.expect("stop"));
    let event = wait_for(&mut rx, "subscription.done", |event| {
        matches!(event, AppEvent::SubscriptionDone { subscription_id, .. } if subscription_id == &id)
    })
    .await;
    match event {
        AppEvent::SubscriptionDone { delivered, reason, .. } => {
            assert_eq!(delivered, 5);
            assert_eq!(reason.as_deref(), Some("stopped by client"));
        }
        other => panic!("unexpected event: {other:?}"),
    }
    assert_eq!(consume.state_of(&id), None, "stopped subscription must leave the registry");

    // Idempotent: a second stop is a no-op and emits nothing.
    assert!(!consume.stop(&id).await.expect("second stop"));
    assert!(drain(&mut rx).is_empty(), "second stop emitted events");
    assert_eq!(consume.active_count(), 0);
}

#[tokio::test]
async fn bounded_subscription_finishes_on_its_own() {
    let (consume, cluster, mut rx) = harness("orders", 1, 5).await;
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
    assert_eq!(consume.active_count(), 0);
    assert_eq!(consume.total_count(), 1, "finished entries stay until pruned");
    assert!(matches!(
        consume.pause(&id),
        Err(ServiceError::Driver(error)) if error.kind() == ErrorKind::Cancelled
    ));

    assert_eq!(consume.prune_done(), 1);
    assert_eq!(consume.total_count(), 0);
    assert_eq!(consume.list(), Vec::new());
}

#[tokio::test]
async fn driver_error_ends_the_subscription_as_done() {
    let (consume, cluster, mut rx) = harness("orders", 1, 3).await;
    let req = live("orders");
    let id = req.subscription_id.clone();
    consume.start(&cluster, req).await.expect("start");
    for expected in 0..3 {
        assert_eq!(wait_message(&mut rx, &id).await.offset, expected);
    }

    let driver = consume.clusters().driver(&cluster).expect("driver");
    driver
        .delete_topics(&DeleteTopicsReq {
            names: vec!["orders".to_string()],
        })
        .await
        .expect("delete topic");

    let error = wait_for(&mut rx, "error", |event| {
        matches!(event, AppEvent::Error { error, .. } if error.kind == "not_found")
    })
    .await;
    let AppEvent::Error { error, correlation_id } = error else {
        unreachable!("matched above");
    };
    assert!(error.message.contains("orders"));
    assert_eq!(correlation_id, None);

    let event = wait_for(&mut rx, "subscription.done", |event| {
        matches!(event, AppEvent::SubscriptionDone { subscription_id, .. } if subscription_id == &id)
    })
    .await;
    match event {
        AppEvent::SubscriptionDone { delivered, reason, .. } => {
            assert_eq!(delivered, 3);
            assert!(reason.as_deref().unwrap_or_default().contains("not found"));
        }
        other => panic!("unexpected event: {other:?}"),
    }
    assert_eq!(consume.state_of(&id), Some(SubscriptionStatus::Done));
    assert_eq!(consume.active_count(), 0);
}

#[tokio::test]
async fn seek_replays_from_the_requested_offset() {
    let (consume, cluster, mut rx) = harness("orders", 1, 3).await;
    let req = live("orders");
    let id = req.subscription_id.clone();
    consume.start(&cluster, req).await.expect("start");
    for expected in 0..3 {
        assert_eq!(wait_message(&mut rx, &id).await.offset, expected);
    }

    consume.seek(&id, "orders", 0, 2).expect("seek");
    let replayed = wait_message(&mut rx, &id).await;
    assert_eq!(replayed.offset, 2);
    assert_eq!(replayed.value_utf8(), Some("m2"));

    assert!(consume.stop(&id).await.expect("stop"));
    assert_eq!(consume.active_count(), 0);
}

#[tokio::test]
async fn list_reports_metadata_per_subscription() {
    let (consume, cluster, mut rx) = harness("orders", 2, 4).await;
    let driver = consume.clusters().driver(&cluster).expect("driver");
    driver
        .create_topic(&CreateTopicReq::new("payments"))
        .await
        .expect("create second topic");

    let first = live("orders");
    let first_id = first.subscription_id.clone();
    let second = SubscribeReq {
        topic: "payments".to_string(),
        partitions: vec![0],
        follow: true,
        ..Default::default()
    };
    let second_id = second.subscription_id.clone();

    consume.start(&cluster, first).await.expect("start orders");
    consume.start(&cluster, second).await.expect("start payments");

    for _ in 0..4 {
        wait_message(&mut rx, &first_id).await;
    }

    let mut list = consume.list();
    assert_eq!(list.len(), 2);
    // Sorted by subscription id, so pick by topic rather than by position.
    list.sort_by(|a, b| a.topic.cmp(&b.topic));
    assert_eq!(list[0].topic, "orders");
    assert_eq!(list[0].partitions, Vec::<u32>::new(), "empty means all partitions");
    assert_eq!(list[0].cluster_id, cluster);
    assert_eq!(list[0].delivered, 4);
    assert_eq!(list[1].topic, "payments");
    assert_eq!(list[1].partitions, vec![0]);
    // `start` keys the registry by the caller's id, so the second subscription must
    // surface the id the test generated — not one the service minted for it.
    assert_eq!(
        list[1].subscription_id, second_id,
        "start must keep the caller's subscription id"
    );

    let json = serde_json::to_value(&list[0]).expect("serialize");
    assert_eq!(json["subscriptionId"], serde_json::json!(first_id.as_str()));
    assert_eq!(json["clusterId"], serde_json::json!(cluster.as_str()));
    assert_eq!(json["state"], "running");
    assert_eq!(json["delivered"], 4);
    assert!(json.get("reason").is_none());

    assert_eq!(consume.active_count(), 2);
    assert_eq!(consume.stop_all().await, 2);
    assert_eq!(consume.active_count(), 0);
    assert_eq!(consume.total_count(), 0);
    let events = drain(&mut rx);
    let done = events
        .iter()
        .filter(|event| matches!(event, AppEvent::SubscriptionDone { .. }))
        .count();
    assert_eq!(done, 2, "stop_all must emit one done event per subscription");
}

#[tokio::test]
async fn rejects_bad_start_requests_without_events() {
    let (consume, cluster, mut rx) = harness("orders", 1, 1).await;

    // Same subscription id twice: the second one is refused.
    let req = live("orders");
    let id = req.subscription_id.clone();
    consume.start(&cluster, req).await.expect("first start");
    let duplicate = consume
        .start(&cluster, live_with_id(&id, "orders"))
        .await
        .expect_err("duplicate id must fail");
    assert_eq!(duplicate.kind(), ErrorKind::InvalidConfig);

    // Unknown topic: the driver refuses before any task is spawned.
    let missing = consume
        .start(&cluster, live("ghost"))
        .await
        .expect_err("unknown topic must fail");
    assert_eq!(missing.kind(), ErrorKind::NotFound);

    // Empty topic name: refused by validation, before touching the driver.
    let empty = consume
        .start(&cluster, live("   "))
        .await
        .expect_err("empty topic must fail");
    assert_eq!(empty.kind(), ErrorKind::InvalidConfig);

    assert_eq!(consume.active_count(), 1);
    assert_eq!(wait_message(&mut rx, &id).await.offset, 0);
    assert_eq!(
        count_messages(&drain(&mut rx), &id),
        0,
        "the failed starts must not have produced any traffic"
    );
    assert_eq!(consume.stop_all().await, 1);
    assert_eq!(consume.active_count(), 0);
}

fn live_with_id(id: &SubscriptionId, topic: &str) -> SubscribeReq {
    SubscribeReq {
        subscription_id: id.clone(),
        ..live(topic)
    }
}

#[tokio::test]
async fn offline_cluster_cannot_start_a_subscription() {
    let store = Store::open_in_memory().expect("in-memory store");
    let bus = EventBus::new(16);
    let mut rx = bus.subscribe();
    let clusters = ClusterService::new(store, bus);
    let saved = clusters
        .save(&ClusterConfig::mock("offline"))
        .await
        .expect("save");
    let consume = ConsumeService::new(clusters);

    let error = consume
        .start(&saved.id, live("orders"))
        .await
        .expect_err("offline cluster must fail");
    assert_eq!(error.kind(), ErrorKind::NotConnected);
    assert_eq!(consume.active_count(), 0);
    assert!(drain(&mut rx).is_empty());
}
