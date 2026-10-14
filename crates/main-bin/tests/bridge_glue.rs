//! T038b automated half: the RPC closure and the event-script wire shape, with
//! no window and no WebView.
//!
//! main-bin is a binary crate, so an integration test cannot `use` its modules
//! — `glue.rs` is compiled into this test crate through `#[path]` instead, which
//! keeps it the single source of truth shared with `main.rs`.
//!
//! The window-only half (`ready_hook`, `ShutdownGuard`, `WrySink`, the actual
//! `app://localhost/rpc` route) needs a live tao loop and a WebView, so it stays
//! on the manual checklist (🔶).

// `glue`'s window-only items are unreachable from this test crate (the module
// is private here), and that is expected — do not let `dead_code` complain.
#[allow(dead_code)]
#[path = "../src/glue.rs"]
mod glue;

use kafka_app::store::Store;
use kafka_core::ClusterId;
use kafka_core::cluster::ClusterConfig;
use kafka_core::message::{ProduceMessage, ProduceReq};
use kafka_core::protocol::cmd;
use serde_json::{Value, json};
use webview_bridge::shell::{RpcHandler, event_script};

use glue::Assembly;

/// Assembly over in-memory SQLite: no data directory, no window, no runtime
/// outside what `Assembly` owns itself.
fn assembly() -> Assembly {
    let store = Store::open_in_memory().expect("in-memory store");
    Assembly::new(store).expect("assembly")
}

/// One RPC round-trip exactly as the page sends it: JSON text in, JSON text out.
fn call(rpc: &RpcHandler, body: &str) -> Value {
    let text = rpc(body.to_owned());
    serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("the rpc closure must answer valid JSON, got {text:?}: {e}"))
}

/// The pinned request envelope: `{id, cmd, payload}`.
fn request(id: &str, command: &str, payload: Value) -> String {
    json!({ "id": id, "cmd": command, "payload": payload }).to_string()
}

/// The mock fixture used across the router cards: passes `validate()`, so a
/// connect needs no broker.
fn mock_config(name: &str) -> ClusterConfig {
    ClusterConfig {
        id: ClusterId::new(),
        name: name.into(),
        bootstrap_servers: vec!["mock".into()],
        mock: true,
        ..Default::default()
    }
}

/// Save + connect a mock cluster through the rpc closure; returns its id.
fn connect_mock(rpc: &RpcHandler, name: &str) -> String {
    let cfg = mock_config(name);
    let saved = call(
        rpc,
        &request(
            "s",
            cmd::CLUSTER_SAVE,
            serde_json::to_value(&cfg).expect("cfg"),
        ),
    );
    assert_eq!(saved["ok"], true, "cluster.save failed: {saved}");
    let id = cfg.id.to_string();
    let connected = call(
        rpc,
        &request("c", cmd::CLUSTER_CONNECT, json!({ "clusterId": &id })),
    );
    assert_eq!(connected["ok"], true, "cluster.connect failed: {connected}");
    id
}

/// Test 1: the assembled rpc closure answers `cluster.list` with `ok: true`.
#[test]
fn rpc_closure_answers_cluster_list_with_ok_true() {
    let asm = assembly();
    let rpc = asm.rpc();

    let resp = call(&rpc, &request("1", cmd::CLUSTER_LIST, json!({})));
    assert_eq!(resp["id"], "1", "the request id must come back untouched");
    assert_eq!(resp["ok"], true, "{resp}");
    assert_eq!(resp["data"], json!([]), "a fresh store holds no clusters");
    assert!(
        resp.get("error").is_none(),
        "no error on a successful call: {resp}"
    );
}

/// Test 2: broken input never escapes as a panic or a non-JSON body — it becomes
/// an error envelope (`invalid_config`, the router's malformed kind) and the `id`
/// contract still holds.
#[test]
fn rpc_closure_answers_malformed_input_with_an_error_envelope() {
    let asm = assembly();
    let rpc = asm.rpc();

    for (body, expected_id) in [
        ("not json at all", ""),
        ("", ""),
        (r#"{"id":"9","payload":{}}"#, "9"),
        (r#"{"id":"4","cmd":123}"#, "4"),
        ("[1,2,3]", ""),
    ] {
        let resp = call(&rpc, body);
        assert_eq!(resp["ok"], false, "{body:?} must fail: {resp}");
        assert_eq!(resp["error"]["kind"], "invalid_config", "{body:?}");
        assert_eq!(resp["error"]["retryable"], false, "{body:?}");
        assert_eq!(resp["id"], expected_id, "the id contract holds: {body:?}");
    }

    // a missing `payload` is not malformed: it defaults to null and runs
    let resp = call(&rpc, r#"{"id":"5","cmd":"cluster.list"}"#);
    assert_eq!(resp["ok"], true, "payload defaults to null: {resp}");
    assert_eq!(resp["id"], "5");

    // a well-formed request for an unregistered command is a handler miss
    let resp = call(&rpc, &request("8", "cluster.nope", json!({})));
    assert_eq!(resp["ok"], false);
    assert_eq!(resp["error"]["kind"], "internal", "{resp}");
    assert!(
        resp["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("cluster.nope")),
        "the message names the command: {resp}"
    );
}

/// Test 3: `event_script` is a guarded JS expression with the batch embedded
/// verbatim — the shape `window.__bridgeEvents` receives.
#[test]
fn event_script_embeds_the_batch_json_verbatim() {
    let batch = r#"[{"event":"produce.result","payload":{"okCount":1},"ts":1700000000000}]"#;
    let js = event_script(batch);
    let prefix = "window.__bridgeEvents && window.__bridgeEvents(";
    assert!(js.starts_with(prefix), "unexpected script: {js}");
    assert!(js.ends_with(')'), "unexpected script: {js}");

    let inner = &js[prefix.len()..js.len() - 1];
    assert_eq!(inner, batch, "the batch JSON must be embedded verbatim");
    let parsed: Value = serde_json::from_str(inner).expect("the embedded part is JSON");
    assert_eq!(parsed[0]["event"], "produce.result");
}

/// Test 4: `register_mock` is reachable through the glue assembly — a mock
/// cluster saved and connected over RPC answers `mock.reset` (and `mock.seed`)
/// with `ok: true`, which also proves the T038a `as_any`/snapshot wiring.
#[test]
fn mock_commands_work_through_the_glue_assembly() {
    let asm = assembly();
    let rpc = asm.rpc();
    let id = connect_mock(&rpc, "glue-mock");

    let reset = call(
        &rpc,
        &request("m1", cmd::MOCK_RESET, json!({ "clusterId": &id })),
    );
    assert_eq!(reset["ok"], true, "mock.reset: {reset}");
    assert_eq!(
        reset["data"],
        Value::Null,
        "void commands answer null: {reset}"
    );

    let seeded = call(
        &rpc,
        &request("m2", cmd::MOCK_SEED, json!({ "clusterId": &id, "seed": 7 })),
    );
    assert_eq!(seeded["ok"], true, "mock.seed: {seeded}");
}

/// All six register_* calls really did run: one command from each table answers
/// over the same rpc closure, against a connected mock cluster.
#[test]
fn every_command_table_is_registered_on_the_assembled_router() {
    let asm = assembly();
    let rpc = asm.rpc();
    let id = connect_mock(&rpc, "glue-tables");

    // config.* (handlers.rs)
    let set = call(
        &rpc,
        &request(
            "t1",
            cmd::CONFIG_SET,
            json!({ "key": "ui.theme", "value": "dark" }),
        ),
    );
    assert_eq!(set["ok"], true, "config.set: {set}");
    let get = call(
        &rpc,
        &request("t2", cmd::CONFIG_GET, json!({ "key": "ui.theme" })),
    );
    assert_eq!(get["ok"], true, "config.get: {get}");
    assert_eq!(get["data"], json!("dark"));

    // topic.* + produce.* (handlers_topic.rs)
    let created = call(
        &rpc,
        &request(
            "t3",
            cmd::TOPIC_CREATE,
            json!({ "clusterId": &id, "name": "orders", "numPartitions": 1, "replicationFactor": 1 }),
        ),
    );
    assert_eq!(created["ok"], true, "topic.create: {created}");
    let listed = call(
        &rpc,
        &request("t4", cmd::TOPIC_LIST, json!({ "clusterId": &id })),
    );
    assert_eq!(listed["ok"], true, "topic.list: {listed}");
    assert!(
        listed["data"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|r| r["name"] == "orders")),
        "the created topic must be listed: {}",
        listed["data"]
    );

    let req = ProduceReq::one("orders", ProduceMessage::text("hello"));
    let produced = call(
        &rpc,
        &request(
            "t5",
            cmd::PRODUCE_SEND,
            json!({ "clusterId": &id, "requests": [serde_json::to_value(&req).expect("req")] }),
        ),
    );
    assert_eq!(produced["ok"], true, "produce.send: {produced}");
    assert_eq!(
        produced["data"]["okCount"], 1,
        "the mock driver accepted it: {}",
        produced["data"]
    );

    // group.* (handlers_group.rs)
    let groups = call(
        &rpc,
        &request("t6", cmd::GROUP_LIST, json!({ "clusterId": &id })),
    );
    assert_eq!(groups["ok"], true, "group.list: {groups}");

    // consume.* (handlers_consume.rs) — no payload needed
    let subs = call(&rpc, &request("t7", cmd::CONSUME_LIST, Value::Null));
    assert_eq!(subs["ok"], true, "consume.list: {subs}");
    assert_eq!(subs["data"], json!([]), "no subscriptions yet: {subs}");

    // mock.* (handlers_mock.rs)
    let faults = call(
        &rpc,
        &request("t8", cmd::MOCK_FAULTS, json!({ "clusterId": &id })),
    );
    assert_eq!(faults["ok"], true, "mock.faults: {faults}");
    assert_eq!(faults["data"]["failProbability"], 0.0);
}

/// The rpc closure is `Send + Sync` and drives the runtime from a foreign
/// thread — which is exactly what wry's custom-protocol callback does — so the
/// `Handle::block_on` inside it must not panic there.
#[test]
fn rpc_closure_runs_on_a_foreign_thread_like_a_webview_callback() {
    let asm = assembly();
    let rpc = asm.rpc();
    let body = request("x", cmd::CLUSTER_LIST, json!({}));

    let answered = std::thread::spawn(move || rpc(body))
        .join()
        .expect("the rpc thread must not panic");
    let resp: Value = serde_json::from_str(&answered).expect("valid JSON");
    assert_eq!(resp["id"], "x");
    assert_eq!(resp["ok"], true, "{resp}");
}
