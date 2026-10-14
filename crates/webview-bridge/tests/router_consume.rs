//! T035 end-to-end: the 6 `consume.*` commands routed through `Router::handle`
//! against a real `App` on in-memory SQLite + mock driver. The service task
//! forwards driver messages to the bus; these tests observe delivery by
//! polling `consume.list`'s `delivered` counter under `tokio::time::timeout`.

use std::time::Duration;

use kafka_app::App;
use kafka_core::cluster::ClusterConfig;
use kafka_core::message::{ProduceMessage, ProduceReq};
use kafka_core::protocol::cmd;
use serde_json::{Value, json};
use webview_bridge::handlers::register_cluster;
use webview_bridge::handlers_consume::register_consume;
use webview_bridge::handlers_topic::register_topic_and_produce;
use webview_bridge::router::Router;

fn wired() -> (App, Router) {
    let app = App::builder().build();
    let router = Router::new();
    register_cluster(&app, &router);
    register_topic_and_produce(&app, &router);
    register_consume(&app, &router);
    (app, router)
}

/// Route one request and return its response envelope as JSON.
async fn call(router: &Router, command: &str, payload: Value) -> Value {
    let raw = json!({ "id": "t1", "cmd": command, "payload": payload }).to_string();
    let resp = serde_json::to_value(router.handle(&raw).await).expect("response serialises");
    assert_eq!(resp["id"], "t1", "the request id must come back untouched");
    resp
}

fn flag(resp: &Value) -> bool {
    resp["ok"].as_bool().expect("ok flag")
}

fn error_kind(resp: &Value) -> &str {
    resp["error"]["kind"].as_str().expect("error kind")
}

/// Save and connect a mock cluster; returns its id as the wire string.
async fn connected(router: &Router, name: &str) -> String {
    let cfg = ClusterConfig::mock(name);
    let payload = serde_json::to_value(&cfg).expect("cfg serialises");
    assert!(flag(&call(router, cmd::CLUSTER_SAVE, payload).await));
    let id = cfg.id.to_string();
    let connect = call(router, cmd::CLUSTER_CONNECT, json!({ "clusterId": &id })).await;
    assert!(flag(&connect), "connect failed: {connect}");
    id
}

async fn create_topic(router: &Router, id: &str, name: &str, partitions: u32) {
    let resp = call(
        router,
        cmd::TOPIC_CREATE,
        json!({ "clusterId": id, "name": name, "numPartitions": partitions, "replicationFactor": 1 }),
    )
    .await;
    assert!(flag(&resp), "create {name} failed: {resp}");
}

/// Send N text messages to one topic through `produce.send`.
async fn produce_texts(router: &Router, id: &str, topic: &str, texts: &[&str]) {
    let req = ProduceReq {
        topic: topic.into(),
        messages: texts.iter().map(|t| ProduceMessage::text(*t)).collect(),
    };
    let payload = json!({ "clusterId": id, "requests": [serde_json::to_value(&req).expect("req serialises")] });
    let resp = call(router, cmd::PRODUCE_SEND, payload).await;
    assert!(flag(&resp), "produce failed: {resp}");
    assert_eq!(resp["data"]["okCount"], texts.len() as u64, "{resp}");
}

/// Subscribe and return the `data` body (a `SubscriptionInfo`), asserting ok.
async fn subscribe(router: &Router, payload: Value) -> Value {
    let resp = call(router, cmd::CONSUBSCRIBE_START, payload).await;
    assert!(flag(&resp), "subscribe failed: {resp}");
    resp["data"].clone()
}

/// The `state` string `consume.list` reports for one subscription.
async fn sub_state(router: &Router, sub_id: &str) -> String {
    let resp = call(router, cmd::CONSUME_LIST, json!({})).await;
    assert!(flag(&resp), "{resp}");
    resp["data"]
        .as_array()
        .expect("consume.list is an array")
        .iter()
        .find(|row| row["subscriptionId"] == sub_id)
        .and_then(|row| row["state"].as_str())
        .unwrap_or_else(|| panic!("subscription {sub_id} missing from consume.list"))
        .to_string()
}

/// Poll `consume.list` until the subscription has delivered `want` messages.
/// The whole wait is bounded by `timeout`, never a bare sleep-and-hope.
async fn wait_delivered(router: &Router, sub_id: &str, want: u64) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let resp = call(router, cmd::CONSUME_LIST, json!({})).await;
            assert!(flag(&resp), "{resp}");
            let hit = resp["data"].as_array().expect("array").iter().any(|row| {
                row["subscriptionId"] == sub_id && row["delivered"].as_u64().unwrap_or(0) >= want
            });
            if hit {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("{want} messages should reach subscription {sub_id} before the timeout")
    });
}

/// Item 2: the full lifecycle connect -> seed data -> subscribe (bounded,
/// `follow:false`, `start:"earliest"`) -> delivery -> unsubscribe `true` ->
/// second unsubscribe `false`, every step through `router.handle`.
#[tokio::test]
async fn consume_lifecycle_round_trip() {
    let (_app, router) = wired();
    let id = connected(&router, "consume-life").await;
    create_topic(&router, &id, "orders", 1).await;
    produce_texts(&router, &id, "orders", &["m1", "m2"]).await;

    let info = subscribe(
        &router,
        json!({ "clusterId": &id, "topic": "orders", "start": {"type": "earliest"}, "follow": false, "maxMessages": 5 }),
    )
    .await;
    let sub_id = info["subscriptionId"]
        .as_str()
        .expect("generated subscriptionId");
    assert!(
        !sub_id.is_empty(),
        "the bridge must surface a non-empty subscriptionId"
    );
    assert_eq!(info["state"], "running");
    assert_eq!(info["topic"], "orders");

    wait_delivered(&router, sub_id, 2).await;

    let stopped = call(
        &router,
        cmd::CONSUBSCRIBE_STOP,
        json!({ "subscriptionId": sub_id }),
    )
    .await;
    assert!(flag(&stopped), "{stopped}");
    assert_eq!(
        stopped["data"], true,
        "first unsubscribe stops the live entry"
    );

    let again = call(
        &router,
        cmd::CONSUBSCRIBE_STOP,
        json!({ "subscriptionId": sub_id }),
    )
    .await;
    assert!(flag(&again), "{again}");
    assert_eq!(
        again["data"], false,
        "unsubscribe is idempotent: a second call answers false"
    );
}

/// Item 3: `consume.pause` flips `state` to `paused`, `consume.resume` back to
/// `running`, and `consume.seek` on the running subscription answers `data:null`.
#[tokio::test]
async fn pause_resume_seek_drive_state_transitions() {
    let (_app, router) = wired();
    let id = connected(&router, "consume-steer").await;
    create_topic(&router, &id, "events", 1).await;

    let info = subscribe(
        &router,
        json!({ "clusterId": &id, "topic": "events", "start": {"type": "earliest"}, "follow": true }),
    )
    .await;
    let sub_id = info["subscriptionId"]
        .as_str()
        .expect("subscriptionId")
        .to_string();
    assert_eq!(sub_state(&router, &sub_id).await, "running");

    let paused = call(
        &router,
        cmd::CONSUME_PAUSE,
        json!({ "subscriptionId": &sub_id }),
    )
    .await;
    assert!(flag(&paused), "{paused}");
    assert_eq!(paused["data"], Value::Null);
    assert_eq!(sub_state(&router, &sub_id).await, "paused");

    let resumed = call(
        &router,
        cmd::CONSUME_RESUME,
        json!({ "subscriptionId": &sub_id }),
    )
    .await;
    assert!(flag(&resumed), "{resumed}");
    assert_eq!(resumed["data"], Value::Null);
    assert_eq!(sub_state(&router, &sub_id).await, "running");

    let seeked = call(
        &router,
        cmd::CONSUME_SEEK,
        json!({ "subscriptionId": &sub_id, "topic": "events", "partition": 0, "offset": 0 }),
    )
    .await;
    assert!(
        flag(&seeked),
        "seek on a running subscription must be ok: {seeked}"
    );
    assert_eq!(seeked["data"], Value::Null);

    let stopped = call(
        &router,
        cmd::CONSUBSCRIBE_STOP,
        json!({ "subscriptionId": &sub_id }),
    )
    .await;
    assert!(flag(&stopped), "{stopped}");
    assert_eq!(stopped["data"], true);
}

/// Item 4: pausing an unknown subscriptionId is an error response, not a
/// crash — the service's `running_handle` lookup fails (kind follows its
/// semantics, so only `ok:false` is asserted).
#[tokio::test]
async fn pause_unknown_subscription_is_error() {
    let (_app, router) = wired();
    let resp = call(
        &router,
        cmd::CONSUME_PAUSE,
        json!({ "subscriptionId": "no-such-sub" }),
    )
    .await;
    assert!(
        !flag(&resp),
        "unknown subscription must not answer ok:true: {resp}"
    );
    assert!(
        resp["error"]["message"]
            .as_str()
            .is_some_and(|m| !m.is_empty())
    );
    assert!(resp["error"]["retryable"].as_bool().is_some());
}

/// Item 4: subscribing on a saved but never connected cluster answers
/// `not_connected`.
#[tokio::test]
async fn subscribe_disconnected_cluster_is_not_connected() {
    let (_app, router) = wired();
    let cfg = ClusterConfig::mock("cold");
    let payload = serde_json::to_value(&cfg).expect("cfg serialises");
    assert!(flag(&call(&router, cmd::CLUSTER_SAVE, payload).await));

    let resp = call(
        &router,
        cmd::CONSUBSCRIBE_START,
        json!({ "clusterId": cfg.id.to_string(), "topic": "orders" }),
    )
    .await;
    assert!(!flag(&resp));
    assert_eq!(error_kind(&resp), "not_connected");
}

/// Item 4: the service's `SubscribeReq::validate()` rejects an empty topic as
/// `invalid_config` (the bridge decodes, the domain reports).
#[tokio::test]
async fn subscribe_empty_topic_is_invalid_config() {
    let (_app, router) = wired();
    let id = connected(&router, "consume-validate").await;

    let resp = call(
        &router,
        cmd::CONSUBSCRIBE_START,
        json!({ "clusterId": &id, "topic": "" }),
    )
    .await;
    assert!(!flag(&resp));
    assert_eq!(error_kind(&resp), "invalid_config");
    assert_eq!(resp["error"]["retryable"], false);
}

/// Ledger ruling R6: with no `subscriptionId` in the payload, the id comes
/// from `SubscribeReq::default()` (a generated UUID) — the bridge never
/// invents one — and an explicit id on the wire is honoured verbatim.
#[tokio::test]
async fn subscribe_id_default_generates_and_explicit_id_is_honoured() {
    let (_app, router) = wired();
    let id = connected(&router, "consume-id").await;
    create_topic(&router, &id, "auto", 1).await;
    create_topic(&router, &id, "fixed", 1).await;

    let generated = subscribe(&router, json!({ "clusterId": &id, "topic": "auto" })).await;
    let gen_id = generated["subscriptionId"]
        .as_str()
        .expect("subscriptionId");
    assert!(
        !gen_id.is_empty(),
        "SubscribeReq::default() must supply a UUID"
    );
    assert_eq!(gen_id.len(), 32, "a uuid simple form is 32 hex chars");

    let explicit = subscribe(
        &router,
        json!({ "clusterId": &id, "topic": "fixed", "subscriptionId": "sub-42" }),
    )
    .await;
    assert_eq!(
        explicit["subscriptionId"], "sub-42",
        "an explicit id round-trips untouched"
    );

    let list = call(&router, cmd::CONSUME_LIST, json!({})).await;
    assert!(flag(&list), "{list}");
    let rows = list["data"].as_array().expect("array");
    assert_eq!(rows.len(), 2, "both subscriptions are listed");
    // sort is by id string: every UUID simple-form char (0-9a-f) sorts before 's'.
    assert_eq!(rows[0]["subscriptionId"], gen_id, "list sorts by id");
    assert_eq!(rows[1]["subscriptionId"], "sub-42");
}
