//! T026 acceptance: ProduceService — 前置校验、逐条回执、部分失败不炸整批、事件计数。

use kafka_app::bus::{AppEvent, BusReceiver, EventBus};
use kafka_app::services::{ClusterService, ProduceService};
use kafka_app::store::Store;
use kafka_core::cluster::ClusterConfig;
use kafka_core::error::ErrorKind;
use kafka_core::message::{ProduceMessage, ProduceReq};
use kafka_core::topic::{CreateTopicReq, ListOffsetsReq, OffsetSpec};
use kafka_core::ClusterId;

async fn connected(max_value_bytes: usize) -> (ProduceService, ClusterId, BusReceiver) {
    let store = Store::open_in_memory().expect("in-memory store");
    let bus = EventBus::new(64);
    let rx = bus.subscribe();
    let svc = ProduceService::new(ClusterService::new(store, bus)).with_max_value_bytes(max_value_bytes);

    let saved = svc
        .clusters()
        .save(&ClusterConfig::mock("produce-demo"))
        .await
        .expect("save cluster");
    svc.clusters().connect(&saved.id).await.expect("connect");
    (svc, saved.id, rx)
}

async fn make_topic(svc: &ProduceService, id: &ClusterId, name: &str, partitions: u32) {
    let driver = svc.clusters().driver(id).expect("driver");
    driver
        .create_topic(&CreateTopicReq {
            name: name.into(),
            num_partitions: partitions,
            replication_factor: 1,
            ..Default::default()
        })
        .await
        .expect("create topic");
}

fn drain(rx: &mut BusReceiver) -> Vec<AppEvent> {
    let mut out = Vec::new();
    while let Ok(event) = rx.try_recv() {
        out.push(event);
    }
    out
}

/// `(clusterId, 条数, okCount, errorCount)`
fn produce_events(events: &[AppEvent]) -> Vec<(String, usize, usize, usize)> {
    events
        .iter()
        .filter_map(|event| match event {
            AppEvent::ProduceResult {
                cluster_id,
                results,
                ok_count,
                error_count,
            } => Some((
                cluster_id.clone(),
                results.len(),
                *ok_count,
                *error_count,
            )),
            _ => None,
        })
        .collect()
}

async fn high_watermark(svc: &ProduceService, id: &ClusterId, topic: &str) -> i64 {
    svc.clusters()
        .driver(id)
        .expect("driver")
        .list_offsets(&ListOffsetsReq {
            partitions: Vec::new(),
            spec: OffsetSpec::Latest,
        })
        .await
        .expect("offsets")
        .into_iter()
        .filter(|range| range.topic == topic)
        .map(|range| range.high)
        .sum()
}

#[tokio::test]
async fn send_one_appends_and_publishes_one_result_event() {
    let (svc, id, mut rx) = connected(usize::MAX).await;
    make_topic(&svc, &id, "orders", 1).await;
    drain(&mut rx);

    let report = svc
        .send_one(&id, ProduceReq::one("orders", ProduceMessage::text("hello")))
        .await
        .expect("send");

    assert!(report.is_ok());
    assert_eq!((report.ok_count, report.error_count, report.results.len()), (1, 0, 1));
    assert_eq!(report.results[0].topic, "orders");
    assert_eq!(report.results[0].partition, 0);
    assert_eq!(report.results[0].offset, 0);
    assert!(report.results[0].error.is_none());
    assert!(report.results[0].timestamp > 0);

    assert_eq!(high_watermark(&svc, &id, "orders").await, 1);
    assert_eq!(
        produce_events(&drain(&mut rx)),
        vec![(id.to_string(), 1, 1, 0)]
    );
}

#[tokio::test]
async fn partial_failure_is_per_message_not_whole_batch() {
    let (svc, id, mut rx) = connected(usize::MAX).await;
    make_topic(&svc, &id, "orders", 3).await;
    drain(&mut rx);

    let mut missing = ProduceMessage::text("b");
    missing.partition = Some(99);
    let req = ProduceReq {
        topic: "orders".into(),
        messages: vec![ProduceMessage::text("a"), missing, ProduceMessage::text("c")],
    };

    let report = svc.send_one(&id, req).await.expect("partial batch still Ok");
    assert!(!report.is_ok());
    assert_eq!((report.ok_count, report.error_count), (2, 1));
    assert_eq!(report.results.len(), 3);
    assert_eq!(report.results[1].partition, 99);
    assert!(report.results[1].error.as_deref().unwrap().contains("unknown topic"));
    assert_eq!(report.failures().len(), 1);
    assert!(report.failures()[0].starts_with("orders-99: "));

    // 两条成功的仍然落盘了。
    assert_eq!(high_watermark(&svc, &id, "orders").await, 2);
    assert_eq!(produce_events(&drain(&mut rx)), vec![(id.to_string(), 3, 2, 1)]);
}

#[tokio::test]
async fn oversized_value_is_rejected_before_touching_the_driver() {
    let (svc, id, mut rx) = connected(16).await;
    make_topic(&svc, &id, "orders", 1).await;
    drain(&mut rx);
    assert_eq!(svc.max_value_bytes(), 16);

    let err = svc
        .send_one(&id, ProduceReq::one("orders", ProduceMessage::text("0123456789abcdefghij")))
        .await
        .expect_err("oversized value");
    assert_eq!(err.kind(), ErrorKind::InvalidConfig);
    let dto = err.to_dto();
    assert!(dto.message.contains("limit is 16 bytes"), "{}", dto.message);
    assert!(dto.message.contains("20 bytes"), "{}", dto.message);

    assert_eq!(high_watermark(&svc, &id, "orders").await, 0);
    assert!(drain(&mut rx).is_empty());
}

#[tokio::test]
async fn empty_batches_are_invalid_and_publish_nothing() {
    let (svc, id, mut rx) = connected(usize::MAX).await;
    make_topic(&svc, &id, "orders", 1).await;
    drain(&mut rx);

    let err = svc.send_batch(&id, &[]).await.expect_err("empty batch");
    assert_eq!(err.kind(), ErrorKind::InvalidConfig);
    assert!(err.to_dto().message.contains("at least one"));

    let empty_messages = ProduceReq {
        topic: "orders".into(),
        messages: Vec::new(),
    };
    let err = svc.send_one(&id, empty_messages).await.expect_err("no messages");
    assert_eq!(err.kind(), ErrorKind::InvalidConfig);

    let no_topic = ProduceReq::one("", ProduceMessage::text("x"));
    let err = svc.send_one(&id, no_topic).await.expect_err("empty topic");
    assert_eq!(err.kind(), ErrorKind::InvalidConfig);

    assert!(drain(&mut rx).is_empty());
}

#[tokio::test]
async fn batch_continues_after_one_request_fails() {
    let (svc, id, mut rx) = connected(usize::MAX).await;
    make_topic(&svc, &id, "orders", 1).await;
    drain(&mut rx);

    let report = svc
        .send_batch(
            &id,
            &[
                ProduceReq::one("orders", ProduceMessage::text("ok")),
                ProduceReq::one("ghost", ProduceMessage::text("nope")),
                ProduceReq {
                    topic: "orders".into(),
                    messages: vec![ProduceMessage::text("one"), ProduceMessage::text("two")],
                },
            ],
        )
        .await
        .expect("batch stays Ok");

    assert_eq!((report.ok_count, report.error_count), (3, 1));
    assert_eq!(report.results.len(), 4);
    assert_eq!(report.failures().len(), 1);
    assert!(report.failures()[0].starts_with("ghost-0: "));
    assert!(report.failures()[0].contains("ghost"));

    assert_eq!(high_watermark(&svc, &id, "orders").await, 3);
    assert_eq!(produce_events(&drain(&mut rx)), vec![(id.to_string(), 4, 3, 1)]);
}

#[tokio::test]
async fn offline_cluster_reports_not_connected() {
    let store = Store::open_in_memory().expect("in-memory store");
    let bus = EventBus::new(64);
    let mut rx = bus.subscribe();
    let svc = ProduceService::new(ClusterService::new(store, bus));
    let saved = svc
        .clusters()
        .save(&ClusterConfig::mock("offline"))
        .await
        .expect("save");

    let err = svc
        .send_one(&saved.id, ProduceReq::one("orders", ProduceMessage::text("x")))
        .await
        .expect_err("offline");
    assert_eq!(err.kind(), ErrorKind::NotConnected);

    // 校验先于连接检查：离线 + 超长 → 仍然是 InvalidConfig。
    let svc = svc.with_max_value_bytes(1);
    let err = svc
        .send_one(&saved.id, ProduceReq::one("orders", ProduceMessage::text("too long")))
        .await
        .expect_err("oversized wins over offline");
    assert_eq!(err.kind(), ErrorKind::InvalidConfig);

    assert!(drain(&mut rx).is_empty());
}

#[tokio::test]
async fn result_event_matches_the_wire_shape() {
    let (svc, id, mut rx) = connected(usize::MAX).await;
    make_topic(&svc, &id, "orders", 1).await;
    drain(&mut rx);

    svc.send_one(&id, ProduceReq::one("orders", ProduceMessage::with_key(ProduceMessage::text("hello"), "k1")))
        .await
        .expect("send");

    let events = drain(&mut rx);
    assert_eq!(events.len(), 1);
    let value = serde_json::to_value(&events[0]).expect("serialize event");
    let payload = &value["payload"];
    assert_eq!(value["type"], serde_json::json!("produce.result"));
    assert_eq!(payload["clusterId"], serde_json::json!(id.to_string()));
    assert_eq!(payload["okCount"], serde_json::json!(1));
    assert_eq!(payload["errorCount"], serde_json::json!(0));
    assert_eq!(payload["results"][0]["topic"], serde_json::json!("orders"));
    assert_eq!(payload["results"][0]["offset"], serde_json::json!(0));
    assert!(payload["results"][0]["error"].is_null());
}
