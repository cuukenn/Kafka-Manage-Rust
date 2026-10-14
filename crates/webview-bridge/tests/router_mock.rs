//! T085 end-to-end: the 3 `mock.*` control commands routed through
//! `Router::handle` against a real `App` on in-memory SQLite + mock driver.
//! Every assertion is on the wire envelope only — the control plane itself is
//! exercised through its effects (faults read-back, injected produce errors).

use kafka_app::App;
use kafka_core::ClusterId;
use kafka_core::cluster::ClusterConfig;
use kafka_core::message::{ProduceMessage, ProduceReq};
use kafka_core::protocol::cmd;
use serde_json::{Value, json};
use webview_bridge::handlers::register_cluster;
use webview_bridge::handlers_mock::register_mock;
use webview_bridge::handlers_topic::register_topic_and_produce;
use webview_bridge::router::Router;

fn wired() -> (App, Router) {
    let app = App::builder().build();
    let router = Router::new();
    register_cluster(&app, &router);
    register_topic_and_produce(&app, &router);
    register_mock(&app, &router);
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
    assert!(
        flag(&call(router, cmd::CLUSTER_SAVE, payload).await),
        "save failed"
    );
    let id = cfg.id.to_string();
    let connect = call(router, cmd::CLUSTER_CONNECT, json!({ "clusterId": &id })).await;
    assert!(flag(&connect), "connect failed: {connect}");
    id
}

async fn create_topic(router: &Router, id: &str, name: &str) {
    let resp = call(
        router,
        cmd::TOPIC_CREATE,
        json!({ "clusterId": id, "name": name, "numPartitions": 1, "replicationFactor": 1 }),
    )
    .await;
    assert!(flag(&resp), "create {name} failed: {resp}");
}

/// Read the current fault injection via `mock.faults`.
async fn read_faults(router: &Router, id: &str) -> Value {
    let resp = call(router, cmd::MOCK_FAULTS, json!({ "clusterId": id })).await;
    assert!(flag(&resp), "{resp}");
    resp["data"].clone()
}

/// Item 2: the full control-plane story through `router.handle` only —
/// default faults -> partial write -> read-back -> injected produce failure
/// -> `mock.reset` -> defaults again.
#[tokio::test]
async fn faults_write_produce_injection_then_reset_restores_defaults() {
    let (_app, router) = wired();
    let id = connected(&router, "faults-story").await;
    create_topic(&router, &id, "orders").await;

    // Defaults: everything off, the stock fail message still present.
    let defaults = read_faults(&router, &id).await;
    assert_eq!(defaults["failConnect"], false);
    assert_eq!(defaults["failProbability"], 0.0);
    assert_eq!(defaults["failMessage"], "injected mock failure");

    // Partial write: the serde defaults fill every unmentioned field.
    let write = call(
        &router,
        cmd::MOCK_FAULTS,
        json!({ "clusterId": &id, "injection": { "failProbability": 1.0, "failMessage": "boom" } }),
    )
    .await;
    assert!(flag(&write), "{write}");
    assert_eq!(write["data"]["failProbability"], 1.0);
    assert_eq!(write["data"]["failMessage"], "boom");
    assert_eq!(
        write["data"]["failConnect"], false,
        "unmentioned fields kept defaults"
    );
    assert_eq!(
        read_faults(&router, &id).await["failMessage"],
        "boom",
        "read-back matches"
    );

    // The guardrail semantics: a per-request driver failure becomes a partial
    // produce report (ok:true), with the injected message inside each row.
    let req = ProduceReq::one("orders", ProduceMessage::text("doomed"));
    let payload = json!({ "clusterId": &id, "requests": [serde_json::to_value(&req).expect("req serialises")] });
    let produced = call(&router, cmd::PRODUCE_SEND, payload).await;
    assert!(
        flag(&produced),
        "injection is a partial failure, not a call failure: {produced}"
    );
    assert_eq!(produced["data"]["okCount"], 0);
    assert_eq!(produced["data"]["errorCount"], 1);
    let results = produced["data"]["results"].as_array().expect("results");
    assert_eq!(results.len(), 1);
    assert!(
        results[0]["error"]
            .as_str()
            .is_some_and(|e| e.contains("boom")),
        "injected message must reach the per-message error: {}",
        results[0]
    );

    // Reset: data AND faults clear, connection stays, data is null.
    let reset = call(&router, cmd::MOCK_RESET, json!({ "clusterId": &id })).await;
    assert!(flag(&reset), "{reset}");
    assert_eq!(reset["data"], Value::Null);
    let after = read_faults(&router, &id).await;
    assert_eq!(after["failProbability"], 0.0);
    assert_eq!(after["failMessage"], "injected mock failure");

    // The connection survived the reset: re-create the topic and produce works.
    create_topic(&router, &id, "orders").await;
    let payload = json!({ "clusterId": &id, "requests": [serde_json::to_value(&req).expect("req serialises")] });
    let produced = call(&router, cmd::PRODUCE_SEND, payload).await;
    assert!(flag(&produced), "{produced}");
    assert_eq!(produced["data"]["okCount"], 1, "faults are off after reset");
}

/// Item 3: `mock.seed` answers null, and re-seeding with a different value
/// simply overwrites the pin (idempotent).
#[tokio::test]
async fn seed_answers_null_and_reseeding_overwrites() {
    let (_app, router) = wired();
    let id = connected(&router, "seed-pin").await;

    let first = call(
        &router,
        cmd::MOCK_SEED,
        json!({ "clusterId": &id, "seed": 42 }),
    )
    .await;
    assert!(flag(&first), "{first}");
    assert_eq!(first["data"], Value::Null);

    let second = call(
        &router,
        cmd::MOCK_SEED,
        json!({ "clusterId": &id, "seed": 7 }),
    )
    .await;
    assert!(
        flag(&second),
        "re-seeding must overwrite, not clash: {second}"
    );
    assert_eq!(second["data"], Value::Null);
}

/// Item 4: a saved but never connected cluster answers `not_connected` for
/// every mock command (the service's driver lookup, not the downcast).
#[tokio::test]
async fn mock_commands_before_connect_are_not_connected() {
    let (_app, router) = wired();
    let cfg = mock_cluster("cold");
    let payload = serde_json::to_value(&cfg).expect("cfg serialises");
    assert!(flag(&call(&router, cmd::CLUSTER_SAVE, payload).await));
    let id = cfg.id.to_string();

    for command in [cmd::MOCK_RESET, cmd::MOCK_FAULTS] {
        let resp = call(&router, command, json!({ "clusterId": &id })).await;
        assert!(!flag(&resp), "{command} should fail: {resp}");
        assert_eq!(error_kind(&resp), "not_connected", "{command}");
    }
}

/// Item 4: a payload without `seed` is caught by the decode as
/// `invalid_config`, and the message names the command (T033 house style).
#[tokio::test]
async fn seed_without_seed_value_is_invalid_config_naming_the_command() {
    let (_app, router) = wired();
    let id = connected(&router, "seed-missing").await;

    let resp = call(&router, cmd::MOCK_SEED, json!({ "clusterId": &id })).await;
    assert!(!flag(&resp));
    assert_eq!(error_kind(&resp), "invalid_config");
    assert_eq!(resp["error"]["retryable"], false);
    assert!(
        resp["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("mock.seed")),
        "the message must name the offending command: {resp}"
    );
}
