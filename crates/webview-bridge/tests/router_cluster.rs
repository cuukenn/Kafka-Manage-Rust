//! T033 end-to-end: every `cluster.*` / `config.*` command routed through
//! `Router::handle` against a real `App` on in-memory SQLite.

use kafka_app::App;
use kafka_core::cluster::ClusterConfig;
use kafka_core::protocol::cmd;
use kafka_core::ClusterId;
use serde_json::{json, Value};
use webview_bridge::handlers::{register_cluster, register_config};
use webview_bridge::router::Router;

fn wired() -> (App, Router) {
    let app = App::builder().build();
    let router = Router::new();
    register_cluster(&app, &router);
    register_config(&app, &router);
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
async fn call(router: &Router, cmd: &str, payload: Value) -> Value {
    let raw = json!({ "id": "t1", "cmd": cmd, "payload": payload }).to_string();
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

async fn save(router: &Router, name: &str) -> ClusterConfig {
    let cfg = mock_cluster(name);
    let payload = serde_json::to_value(&cfg).expect("cfg serialises");
    let resp = call(router, cmd::CLUSTER_SAVE, payload).await;
    assert!(flag(&resp), "save failed: {resp}");
    cfg
}

async fn rows(router: &Router) -> Vec<Value> {
    call(router, cmd::CLUSTER_LIST, json!({})).await["data"].as_array().expect("array").clone()
}

/// Item 3: save echoes the validated config, and the list row is a camelCase
/// `ClusterView` carrying the live state.
#[tokio::test]
async fn save_then_list_renders_the_cluster_view() {
    let (_app, router) = wired();
    let cfg = save(&router, "mock-one").await;

    let rows = rows(&router).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["name"], "mock-one");
    assert_eq!(rows[0]["bootstrapServers"], json!(["mock"]));
    assert_eq!(rows[0]["state"], "disconnected");
    assert!(rows[0]["driver"].is_null());

    let resp = call(&router, cmd::CLUSTER_SAVE, serde_json::to_value(&cfg).expect("cfg serialises")).await;
    assert_eq!(&resp["data"]["name"], "mock-one");
}

/// Item 5: connect reports the live view, disconnect clears the driver instance.
#[tokio::test]
async fn connect_info_and_disconnect_round_trip() {
    let (_app, router) = wired();
    let cfg = save(&router, "live").await;
    let id = cfg.id.to_string();

    let connect = call(&router, cmd::CLUSTER_CONNECT, json!({ "clusterId": id })).await;
    assert!(flag(&connect), "{connect}");
    assert!(connect["data"]["brokers"].as_array().is_some_and(|b| !b.is_empty()));

    let info = call(&router, cmd::CLUSTER_INFO, json!({ "clusterId": id })).await;
    assert!(flag(&info), "{info}");
    assert_eq!(info["data"], connect["data"], "info and connect report the same live view");

    let row = &rows(&router).await[0];
    assert_eq!(row["state"], "connected");
    assert_eq!(row["driver"], "mock");

    let disconnect = call(&router, cmd::CLUSTER_DISCONNECT, json!({ "clusterId": id })).await;
    assert!(flag(&disconnect));
    assert_eq!(disconnect["data"], Value::Null);

    let row = &rows(&router).await[0];
    assert_eq!(row["state"], "disconnected");
    assert!(row["driver"].is_null());
}

/// Item 5: delete drops the row and says whether there was one to drop.
#[tokio::test]
async fn delete_removes_the_row() {
    let (_app, router) = wired();
    let cfg = save(&router, "doomed").await;
    let payload = json!({ "clusterId": cfg.id.to_string() });

    let deleted = call(&router, cmd::CLUSTER_DELETE, payload.clone()).await;
    assert_eq!(deleted["data"], true);
    assert_eq!(rows(&router).await.len(), 0);

    let again = call(&router, cmd::CLUSTER_DELETE, payload).await;
    assert!(flag(&again), "{again}");
    assert_eq!(again["data"], false, "deleting an absent row is a no-op, not an error");
}

/// Item 6: export / import move clusters as a JSON array; the report counts the
/// imported, overwritten and skipped rows.
#[tokio::test]
async fn export_and_import_move_clusters_as_a_json_array() {
    let (_app, router) = wired();
    let cfg = save(&router, "exported").await;

    let exported = call(&router, cmd::CLUSTER_EXPORT, json!({})).await;
    assert!(flag(&exported), "{exported}");
    let exported = exported["data"].as_array().expect("export is an array").clone();
    assert_eq!(exported.len(), 1);
    assert_eq!(exported[0]["name"], cfg.name);

    let document = serde_json::to_string(&[mock_cluster("imported")]).expect("serialises");
    let imported = call(&router, cmd::CLUSTER_IMPORT, json!({ "json": document })).await;
    assert_eq!(imported["data"], json!({ "imported": 1, "overwritten": 0, "skipped": 0 }));

    let again = call(&router, cmd::CLUSTER_IMPORT, json!({ "json": document, "onConflict": "skip" })).await;
    assert_eq!(again["data"], json!({ "imported": 0, "overwritten": 0, "skipped": 1 }));

    let names: Vec<String> = rows(&router).await.iter().map(|row| row["name"].as_str().expect("name").to_string()).collect();
    assert_eq!(names, ["exported", "imported"]);
}

/// Item 8: a saved but never connected cluster answers `not_connected`, with all
/// three error fields present.
#[tokio::test]
async fn info_before_connect_is_not_connected() {
    let (_app, router) = wired();
    let cfg = save(&router, "cold").await;

    let resp = call(&router, cmd::CLUSTER_INFO, json!({ "clusterId": cfg.id.to_string() })).await;
    assert!(!flag(&resp));
    assert_eq!(resp["error"]["kind"], "not_connected");
    assert!(resp["error"]["message"].as_str().is_some_and(|m| !m.is_empty()));
    assert!(resp["error"]["retryable"].as_bool().is_some());
}

/// Item 8: an unknown cluster id is `not_found`, not a panic.
#[tokio::test]
async fn connect_to_unknown_cluster_is_not_found() {
    let (_app, router) = wired();
    let resp = call(&router, cmd::CLUSTER_CONNECT, json!({ "clusterId": ClusterId::new().to_string() })).await;
    assert!(!flag(&resp));
    assert_eq!(error_kind(&resp), "not_found");
}

/// Item 8: every `{clusterId}` command rejects a payload that lacks it.
#[tokio::test]
async fn missing_cluster_id_is_invalid_config() {
    let (_app, router) = wired();

    for command in [cmd::CLUSTER_INFO, cmd::CLUSTER_CONNECT, cmd::CLUSTER_DISCONNECT, cmd::CLUSTER_DELETE] {
        let resp = call(&router, command, json!({})).await;
        assert_eq!(error_kind(&resp), "invalid_config", "{command}");
        assert_eq!(resp["error"]["retryable"], false, "{command} is not retryable");
        assert!(resp["error"]["message"].as_str().is_some_and(|m| m.contains("clusterId")), "{command}");
    }
}

/// Item 1: a payload the service cannot decode names the command, and an unknown
/// `onConflict` value is rejected the same way.
#[tokio::test]
async fn undecodable_payload_names_the_command() {
    let (_app, router) = wired();

    // A wrong type rather than a missing field: `{}` would fail `validate()`
    // instead of the decode, and that error names the config, not the command.
    let resp = call(&router, cmd::CLUSTER_SAVE, json!({ "name": 7 })).await;
    assert_eq!(error_kind(&resp), "invalid_config");
    assert_eq!(resp["error"]["retryable"], false);
    assert!(resp["error"]["message"].as_str().is_some_and(|m| m.contains("cluster.save")));

    let resp = call(&router, cmd::CLUSTER_IMPORT, json!({ "json": "[]", "onConflict": "bogus" })).await;
    assert_eq!(error_kind(&resp), "invalid_config");
}

/// Item 7: `config.set` / `config.get` round-trip through the KV table.
#[tokio::test]
async fn config_set_and_get_round_trip() {
    let (_app, router) = wired();

    let set = call(&router, cmd::CONFIG_SET, json!({ "key": "ui.theme", "value": { "mode": "dark" } })).await;
    assert!(flag(&set), "{set}");
    assert_eq!(set["data"], Value::Null);

    let get = call(&router, cmd::CONFIG_GET, json!({ "key": "ui.theme" })).await;
    assert_eq!(get["data"], json!({ "mode": "dark" }));

    let absent = call(&router, cmd::CONFIG_GET, json!({ "key": "absent.key" })).await;
    assert!(flag(&absent));
    assert_eq!(absent["data"], Value::Null);
}
