//! T034 end-to-end: the 6 `topic.*` commands and `produce.send` routed through
//! `Router::handle` against a real `App` on in-memory SQLite + mock driver.

use kafka_app::App;
use kafka_core::ClusterId;
use kafka_core::cluster::ClusterConfig;
use kafka_core::message::{ProduceMessage, ProduceReq};
use kafka_core::protocol::cmd;
use serde_json::{Value, json};
use webview_bridge::handlers::register_cluster;
use webview_bridge::handlers_topic::register_topic_and_produce;
use webview_bridge::router::Router;

fn wired() -> (App, Router) {
    let app = App::builder().build();
    let router = Router::new();
    register_cluster(&app, &router);
    register_topic_and_produce(&app, &router);
    (app, router)
}

/// The mock fixture: passes `validate()`, so a connect needs no broker.
fn mock_cluster(name: &str) -> ClusterConfig {
    ClusterConfig {
        id: ClusterId::new(),
        name: name.into(),
        bootstrap_servers: vec!["mock".into()],
        mock: true,
        ..Default::default()
    }
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
    let cfg = mock_cluster(name);
    let payload = serde_json::to_value(&cfg).expect("cfg serialises");
    let saved = call(router, cmd::CLUSTER_SAVE, payload).await;
    assert!(flag(&saved), "save failed: {saved}");
    let id = cfg.id.to_string();
    let connect = call(router, cmd::CLUSTER_CONNECT, json!({ "clusterId": &id })).await;
    assert!(flag(&connect), "connect failed: {connect}");
    id
}

/// Wire payload for `topic.create` (camelCase, per the DTO serde attrs).
fn create_payload(id: &str, name: &str, partitions: u32) -> Value {
    json!({ "clusterId": id, "name": name, "numPartitions": partitions, "replicationFactor": 1 })
}

async fn create_topic(router: &Router, id: &str, name: &str, partitions: u32) {
    let resp = call(
        router,
        cmd::TOPIC_CREATE,
        create_payload(id, name, partitions),
    )
    .await;
    assert!(flag(&resp), "create {name} failed: {resp}");
    assert_eq!(resp["data"], Value::Null);
}

async fn topic_names(router: &Router, id: &str) -> Vec<String> {
    let resp = call(router, cmd::TOPIC_LIST, json!({ "clusterId": id })).await;
    assert!(flag(&resp), "{resp}");
    resp["data"]
        .as_array()
        .expect("topic list is an array")
        .iter()
        .map(|row| row["name"].as_str().expect("name").to_string())
        .collect()
}

/// Item 2: the full lifecycle create -> list -> describe -> updateConfig ->
/// offsets -> delete -> list, every step through `router.handle`.
#[tokio::test]
async fn topic_lifecycle_round_trip() {
    let (_app, router) = wired();
    let id = connected(&router, "lifecycle").await;

    create_topic(&router, &id, "orders", 3).await;
    assert_eq!(topic_names(&router, &id).await, ["orders"]);

    let describe = call(
        &router,
        cmd::TOPIC_DESCRIBE,
        json!({ "clusterId": &id, "names": ["orders"], "includeInternal": false }),
    )
    .await;
    assert!(flag(&describe), "{describe}");
    let details = describe["data"].as_array().expect("array");
    assert_eq!(details.len(), 1);
    assert_eq!(details[0]["name"], "orders");
    assert_eq!(
        details[0]["partitions"]
            .as_array()
            .expect("partitions")
            .len(),
        3
    );

    let updated = call(
        &router,
        cmd::TOPIC_UPDATE_CONFIG,
        json!({ "clusterId": &id, "topic": "orders", "set": { "retention.ms": "60000" }, "unset": [] }),
    )
    .await;
    assert!(flag(&updated), "{updated}");
    assert_eq!(updated["data"], Value::Null);

    let offsets = call(
        &router,
        cmd::TOPIC_OFFSETS,
        json!({ "clusterId": &id, "partitions": [] }),
    )
    .await;
    assert!(flag(&offsets), "{offsets}");
    let ranges = offsets["data"].as_array().expect("array");
    assert_eq!(ranges.len(), 3, "one OffsetRange per partition");
    for range in ranges {
        assert_eq!(range["topic"], "orders");
        assert_eq!(range["low"], 0);
        assert_eq!(range["high"], 0);
    }

    let deleted = call(
        &router,
        cmd::TOPIC_DELETE,
        json!({ "clusterId": &id, "names": ["orders"] }),
    )
    .await;
    assert!(flag(&deleted), "{deleted}");
    let rows = deleted["data"].as_array().expect("array");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["name"], "orders");
    assert!(
        rows[0]["error"].is_null(),
        "delete reported no per-topic error"
    );

    assert!(
        topic_names(&router, &id).await.is_empty(),
        "topic.list no longer contains the deleted topic"
    );
}

/// Item 4: a saved but never connected cluster answers `not_connected`.
#[tokio::test]
async fn topic_list_before_connect_is_not_connected() {
    let (_app, router) = wired();
    let cfg = mock_cluster("cold");
    let payload = serde_json::to_value(&cfg).expect("cfg serialises");
    assert!(flag(&call(&router, cmd::CLUSTER_SAVE, payload).await));

    let resp = call(
        &router,
        cmd::TOPIC_LIST,
        json!({ "clusterId": cfg.id.to_string() }),
    )
    .await;
    assert!(!flag(&resp));
    assert_eq!(error_kind(&resp), "not_connected");
    assert!(resp["error"]["retryable"].as_bool().is_some());
}

/// Item 4: the service's `validate()` rejects an empty topic name as
/// `invalid_config`, and the error is the wire-shaped triple.
#[tokio::test]
async fn topic_create_empty_name_is_invalid_config() {
    let (_app, router) = wired();
    let id = connected(&router, "validate").await;

    let resp = call(&router, cmd::TOPIC_CREATE, create_payload(&id, "", 1)).await;
    assert!(!flag(&resp));
    assert_eq!(error_kind(&resp), "invalid_config");
    assert_eq!(resp["error"]["retryable"], false);
    assert!(
        resp["error"]["message"]
            .as_str()
            .is_some_and(|m| !m.is_empty())
    );
}

/// Item 4: a mistyped payload field is caught by the decode, and the message
/// names the command (the T033 `parse_payload` house style).
#[tokio::test]
async fn mistyped_payload_is_invalid_config_naming_the_command() {
    let (_app, router) = wired();
    let id = connected(&router, "mistyped").await;

    let resp = call(
        &router,
        cmd::TOPIC_OFFSETS,
        json!({ "clusterId": &id, "partitions": "all" }),
    )
    .await;
    assert!(!flag(&resp));
    assert_eq!(error_kind(&resp), "invalid_config");
    assert!(
        resp["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("topic.offsets"))
    );
}

/// Item 3: a single-message send reports one located result, no errors.
#[tokio::test]
async fn produce_single_message_reports_one_located_result() {
    let (_app, router) = wired();
    let id = connected(&router, "produce-one").await;
    create_topic(&router, &id, "events", 2).await;

    let req = ProduceReq::one("events", ProduceMessage::text("hello"));
    let payload = json!({ "clusterId": &id, "requests": [serde_json::to_value(&req).expect("req serialises")] });
    let resp = call(&router, cmd::PRODUCE_SEND, payload).await;
    assert!(flag(&resp), "{resp}");

    let report = &resp["data"];
    assert_eq!(report["okCount"], 1);
    assert_eq!(report["errorCount"], 0);
    let results = report["results"].as_array().expect("results");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["topic"], "events");
    assert!(results[0]["offset"].as_i64().is_some());
    assert!(results[0]["error"].is_null());
}

/// Item 3: a multi-request batch keeps one result row per message, in request
/// order, and counts ok vs error across the whole batch.
#[tokio::test]
async fn produce_batch_counts_ok_and_error_per_message() {
    let (_app, router) = wired();
    let id = connected(&router, "produce-batch").await;
    create_topic(&router, &id, "first", 1).await;

    let requests = [
        ProduceReq::one("first", ProduceMessage::text("a")),
        ProduceReq::one("first", ProduceMessage::text("b")),
        ProduceReq::one("second", ProduceMessage::text("c")),
    ];
    let payload = json!({
        "clusterId": &id,
        "requests": requests.iter().map(|r| serde_json::to_value(r).expect("req serialises")).collect::<Vec<_>>(),
    });
    let resp = call(&router, cmd::PRODUCE_SEND, payload).await;
    assert!(flag(&resp), "{resp}");

    let report = &resp["data"];
    let results = report["results"].as_array().expect("results");
    assert_eq!(results.len(), requests.len(), "one result row per message");
    assert_eq!(report["okCount"], 2);
    assert_eq!(report["errorCount"], 1);
    assert_eq!(results[2]["topic"], "second");
    assert!(results[2]["error"].as_str().is_some_and(|e| !e.is_empty()));
}

/// Item 3: producing to a missing topic is a partial failure, not a call
/// failure: `ok:true`, with the per-message error inside the report.
#[tokio::test]
async fn produce_to_missing_topic_is_partial_failure_not_call_failure() {
    let (_app, router) = wired();
    let id = connected(&router, "produce-ghost").await;

    let req = ProduceReq::one("ghost", ProduceMessage::text("nobody home"));
    let payload = json!({ "clusterId": &id, "requests": [serde_json::to_value(&req).expect("req serialises")] });
    let resp = call(&router, cmd::PRODUCE_SEND, payload).await;
    assert!(
        flag(&resp),
        "partial failure must still answer ok:true: {resp}"
    );

    let report = &resp["data"];
    assert_eq!(report["okCount"], 0);
    assert_eq!(report["errorCount"], 1);
    let results = report["results"].as_array().expect("results");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["topic"], "ghost");
    assert!(results[0]["error"].as_str().is_some_and(|e| !e.is_empty()));
}

/// Item 3 (ledger ruling R6): an empty batch is rejected up front as
/// `invalid_config` rather than reported as an empty ok report.
#[tokio::test]
async fn produce_empty_batch_is_invalid_config() {
    let (_app, router) = wired();
    let id = connected(&router, "produce-empty").await;

    let resp = call(
        &router,
        cmd::PRODUCE_SEND,
        json!({ "clusterId": &id, "requests": [] }),
    )
    .await;
    assert!(!flag(&resp));
    assert_eq!(error_kind(&resp), "invalid_config");
    assert_eq!(resp["error"]["retryable"], false);
}
