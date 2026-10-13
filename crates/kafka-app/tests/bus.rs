//! Contract tests for the application event bus (task T023).

use kafka_app::bus::{AppEvent, BusRecvError, EventBus, SubscriptionStatus, TopicChange};
use kafka_core::cluster::ConnectionState;
use kafka_core::error::ErrorKind;
use kafka_core::message::{MessageEvent, SubscriptionId, TimestampType};
use kafka_core::protocol::{ErrorDto, event};
use kafka_driver::DriverEvent;

fn log_line(msg: &str) -> AppEvent {
    AppEvent::LogLine { level: "info".into(), target: "test".into(), message: msg.into() }
}

fn sample_message(offset: i64) -> MessageEvent {
    MessageEvent {
        topic: "orders".into(),
        partition: 0,
        offset,
        key: Some(b"k1".to_vec()),
        value: b"{\"n\":1}".to_vec(),
        headers: Vec::new(),
        timestamp: 1_700_000_000_000,
        timestamp_type: TimestampType::default(),
        leader_epoch: None,
    }
}

#[tokio::test]
async fn publish_reaches_every_subscriber() {
    let bus = EventBus::default();
    let mut a = bus.subscribe();
    let mut b = bus.subscribe();

    assert_eq!(bus.publish(log_line("hello")), 2);

    assert_eq!(a.recv().await.expect("a gets it"), log_line("hello"));
    assert_eq!(b.recv().await.expect("b gets it"), log_line("hello"));
    assert_eq!(bus.subscriber_count(), 2);
}

#[tokio::test]
async fn publish_without_subscribers_is_not_an_error() {
    let bus = EventBus::default();
    assert_eq!(bus.publish(log_line("nobody listening")), 0);
    assert_eq!(bus.lag_count(), 0);
}

#[tokio::test]
async fn try_recv_reports_empty() {
    let bus = EventBus::default();
    let mut rx = bus.subscribe();
    assert_eq!(rx.try_recv(), Err(BusRecvError::Empty));
    bus.publish(log_line("now"));
    assert_eq!(rx.try_recv(), Ok(log_line("now")));
}

#[tokio::test]
async fn slow_subscriber_records_lag_instead_of_panicking() {
    // Below MIN_CAPACITY on purpose: the bus must clamp, not create a 4-slot channel.
    let bus = EventBus::new(4);
    let mut rx = bus.subscribe();
    for i in 0..40 {
        bus.publish(log_line(&format!("m{i}")));
    }

    let mut saw_lag = false;
    let mut recovered = None;
    for _ in 0..5 {
        match rx.recv().await {
            Ok(ev) => {
                recovered = Some(ev);
                break;
            }
            Err(BusRecvError::Lagged(n)) => {
                assert!(n > 0, "lag count must be positive");
                saw_lag = true;
            }
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }

    assert!(saw_lag, "a subscriber that misses the whole buffer must report Lagged");
    assert!(bus.lag_count() > 0, "the bus must expose the drop counter");
    assert!(recovered.is_some(), "the subscriber must still be usable afterwards");
}

#[tokio::test]
async fn receiver_sees_closed_once_the_bus_is_gone() {
    let bus = EventBus::default();
    let mut rx = bus.subscribe();
    bus.publish(log_line("bye"));
    drop(bus);

    assert_eq!(rx.recv().await.expect("queued event still delivered"), log_line("bye"));
    assert_eq!(rx.recv().await, Err(BusRecvError::Closed));
}

#[test]
fn event_names_come_from_the_shared_protocol_constants() {
    let cases = [
        (AppEvent::ConnectionStateChanged { cluster_id: "c1".into(), state: ConnectionState::Connected, reason: None }, event::CONNECTION_STATE),
        (
            AppEvent::MessageReceived { subscription_id: SubscriptionId("s1".into()), message: sample_message(0) },
            event::MESSAGE_RECEIVED,
        ),
        (
            AppEvent::SubscriptionState {
                subscription_id: SubscriptionId("s1".into()),
                state: SubscriptionStatus::Paused,
                reason: None,
            },
            event::SUBSCRIPTION_STATE,
        ),
        (AppEvent::SubscriptionDone { subscription_id: SubscriptionId("s1".into()), delivered: 7, reason: None }, event::SUBSCRIPTION_DONE),
        (AppEvent::TopicChanged { cluster_id: "c1".into(), topic: "orders".into(), change: TopicChange::Created }, event::TOPIC_CHANGED),
        (AppEvent::GroupChanged { cluster_id: "c1".into(), group_id: "g1".into(), partitions: 3 }, event::GROUP_CHANGED),
        (AppEvent::ProduceResult { cluster_id: "c1".into(), results: Vec::new(), ok_count: 0, error_count: 0 }, event::PRODUCE_RESULT),
        (log_line("x"), event::LOG_LINE),
        (
            AppEvent::Error { error: ErrorDto::new(ErrorKind::Timeout, "boom"), correlation_id: Some("r1".into()) },
            event::ERROR,
        ),
    ];
    for (ev, name) in cases {
        assert_eq!(ev.event_name(), name, "event name must match protocol::event");
        let tag = serde_json::to_value(&ev).expect("serialises")["type"].as_str().unwrap().to_string();
        assert_eq!(tag, name, "serde tag must equal event_name() for every variant");
    }
}

#[test]
fn json_shape_is_tag_plus_payload_and_payload_is_extractable() {
    let ev = AppEvent::TopicChanged { cluster_id: "c1".into(), topic: "orders".into(), change: TopicChange::Created };
    let json = serde_json::to_value(&ev).expect("serialises");
    assert_eq!(json["type"], event::TOPIC_CHANGED);
    assert_eq!(json["type"], ev.event_name(), "serde tag must not drift from protocol::event");
    assert_eq!(json["payload"]["change"], "created");
    assert_eq!(json["payload"]["clusterId"], "c1");

    let payload = ev.payload_json();
    assert_eq!(payload["topic"], "orders");
    assert!(payload.get("type").is_none(), "payload_json must not carry the tag");
}

#[test]
fn error_event_keeps_the_dto_shape_the_ui_expects() {
    let ev = AppEvent::Error { error: ErrorDto::new(ErrorKind::NotConnected, "offline"), correlation_id: None };
    let json = serde_json::to_value(&ev).expect("serialises");
    assert!(json["payload"]["error"]["kind"].is_string());
    assert_eq!(json["payload"]["error"]["retryable"], true);
    assert!(json["payload"]["error"]["hint"].is_string());
    assert!(json["payload"].get("correlationId").is_none(), "None fields must be omitted");
}

#[test]
fn driver_events_translate_into_bus_events() {
    let created = AppEvent::from_driver("c1", DriverEvent::TopicCreated { topic: "orders".into() });
    assert_eq!(
        created,
        AppEvent::TopicChanged { cluster_id: "c1".into(), topic: "orders".into(), change: TopicChange::Created }
    );

    let appended = AppEvent::from_driver("c1", DriverEvent::MessagesAppended { topic: "orders".into(), count: 4 });
    assert_eq!(
        appended,
        AppEvent::TopicChanged { cluster_id: "c1".into(), topic: "orders".into(), change: TopicChange::Messages }
    );

    let reset = AppEvent::from_driver("c1", DriverEvent::GroupOffsetsReset { group: "g1".into(), partitions: 2 });
    assert_eq!(reset, AppEvent::GroupChanged { cluster_id: "c1".into(), group_id: "g1".into(), partitions: 2 });

    let lost = AppEvent::from_driver("c1", DriverEvent::ConnectionLost { reason: "broker gone".into() });
    assert_eq!(
        lost,
        AppEvent::ConnectionStateChanged {
            cluster_id: "c1".into(),
            state: ConnectionState::Error,
            reason: Some("broker gone".into())
        }
    );
    assert_eq!(lost.event_name(), event::CONNECTION_STATE);

    // The context-free From leaves clusterId empty.
    let bare = AppEvent::from(DriverEvent::TopicDeleted { topic: "orders".into() });
    assert_eq!(bare, AppEvent::TopicChanged { cluster_id: String::new(), topic: "orders".into(), change: TopicChange::Deleted });
}
