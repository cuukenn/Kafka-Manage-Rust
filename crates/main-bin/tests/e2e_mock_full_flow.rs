//! T100 / ruling R30: one scripted **full-flow** journey through the *assembled*
//! router — the exact path a WebView request takes:
//! `rpc closure → parse → command table → handler → service → event bus → serialized response`.
//!
//! Every *driving* step is one JSON request in and one JSON envelope out — no handler or
//! service method is called to make anything happen — so the test pins the cross-domain
//! contract (command names, payload shapes, response shapes, event names and ordering, and
//! that a failed command leaves global state untouched). Three read-only `App` accessors
//! (`consume().active_count()`, `clusters().driver()`) are used only for end-state
//! invariants that the wire does not expose.
//!
//! `main-bin` is the only crate that assembles all six command tables plus `App`, so the
//! test lives here and compiles `glue.rs` into this test crate through `#[path]` — the same
//! single-source-of-truth trick `bridge_glue.rs` uses.
//!
//! Every event wait is bounded (a real 2 s deadline), never an unbounded sleep, so a stuck
//! bus fails loudly instead of hanging the suite. The mock driver is deterministic, so the
//! journey is stable across runs.

// `glue`'s window-only items are unreachable from this test crate (the module is private
// here), and that is expected — do not let `dead_code` complain.
#[allow(dead_code)]
#[path = "../src/glue.rs"]
mod glue;

use std::time::Duration;

use kafka_app::bus::{BusReceiver, BusRecvError};
use kafka_app::store::Store;
use kafka_core::ClusterId;
use kafka_core::cluster::ClusterConfig;
use kafka_core::message::{ProduceMessage, ProduceReq};
use kafka_core::protocol::{cmd, event};
use kafka_driver::mock::MockKafkaDriver;
use serde_json::{Value, json};
use webview_bridge::shell::RpcHandler;

use glue::Assembly;

/// Per-event deadline. Bounded on purpose: a missing event must fail, not hang.
const WAIT: Duration = Duration::from_secs(2);

/// Assembly over in-memory SQLite: no data directory, no window, no runtime outside
/// what `Assembly` owns itself.
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

/// The mock fixture used across the router cards: passes `validate()`, so a connect
/// needs no broker.
fn mock_config(name: &str) -> ClusterConfig {
    ClusterConfig {
        id: ClusterId::new(),
        name: name.into(),
        bootstrap_servers: vec!["mock".into()],
        mock: true,
        ..Default::default()
    }
}

/// Next bus event as `(wire name, payload)`, waiting at most [`WAIT`]. The test's own
/// receiver is subscribed before every publishing RPC, so nothing is missed; a slow
/// subscriber (lagged) just skips forward instead of failing.
fn next_event(asm: &Assembly, rx: &mut BusReceiver) -> (String, Value) {
    loop {
        match asm
            .rt
            .block_on(async { tokio::time::timeout(WAIT, rx.recv()).await })
        {
            Ok(Ok(ev)) => return (ev.event_name().to_string(), ev.payload_json()),
            Ok(Err(BusRecvError::Lagged(skipped))) => {
                eprintln!("bus lagged: skipped {skipped} events while waiting");
            }
            Ok(Err(BusRecvError::Empty)) => {
                unreachable!("the blocking recv() never reports Empty")
            }
            Ok(Err(BusRecvError::Closed)) => {
                panic!("the event bus closed while the app is still alive")
            }
            Err(_elapsed) => panic!("no bus event arrived within {WAIT:?}"),
        }
    }
}

/// Drain events until one matches `name` and `pred`, skipping anything else (bounded by
/// [`WAIT`] per event via [`next_event`]).
fn wait_event<F>(asm: &Assembly, rx: &mut BusReceiver, name: &str, pred: F) -> Value
where
    F: Fn(&Value) -> bool,
{
    loop {
        let (got, payload) = next_event(asm, rx);
        if got == name && pred(&payload) {
            return payload;
        }
    }
}

/// Poll the bus for up to `quiet` with no event, collecting everything that arrives.
/// Used to prove an event was *not* published a second time (bounded quiet window,
/// never a fixed sleep-and-hope).
fn drain_for(asm: &Assembly, rx: &mut BusReceiver, quiet: Duration) -> Vec<(String, Value)> {
    asm.rt.block_on(async {
        let deadline = tokio::time::Instant::now() + quiet;
        let mut out = Vec::new();
        loop {
            match rx.try_recv() {
                Ok(ev) => out.push((ev.event_name().to_string(), ev.payload_json())),
                Err(BusRecvError::Lagged(_)) => {}
                Err(BusRecvError::Empty) => {
                    if tokio::time::Instant::now() >= deadline {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(BusRecvError::Closed) => break,
            }
        }
        out
    })
}

/// The scripted 11-step journey. One `#[test]` on purpose: it is a single user session, so
/// each step still sees the state the previous one left behind.
#[test]
fn scripted_mock_full_flow_pins_the_wire_contract() {
    let asm = assembly();
    let rpc = asm.rpc();
    // Subscribed before the first publishing RPC, so no event is missed (broadcast).
    let mut rx = asm.app.bus().subscribe();

    // 1. cluster.list (empty) → cluster.save (mock) → cluster.list has exactly one row,
    //    still disconnected.
    let empty = call(&rpc, &request("1", cmd::CLUSTER_LIST, json!({})));
    assert_eq!(empty["ok"], true, "cluster.list: {empty}");
    assert_eq!(
        empty["data"],
        json!([]),
        "a fresh store holds no clusters: {empty}"
    );

    let cfg = mock_config("t100-mock");
    let cluster = cfg.id.clone();
    let id = cluster.to_string();
    let saved = call(
        &rpc,
        &request(
            "2",
            cmd::CLUSTER_SAVE,
            serde_json::to_value(&cfg).expect("cfg"),
        ),
    );
    assert_eq!(saved["ok"], true, "cluster.save: {saved}");

    let listed = call(&rpc, &request("3", cmd::CLUSTER_LIST, json!({})));
    assert_eq!(listed["ok"], true, "cluster.list: {listed}");
    let rows = listed["data"]
        .as_array()
        .expect("cluster.list answers an array");
    assert_eq!(rows.len(), 1, "exactly the saved cluster: {listed}");
    assert_eq!(
        rows[0]["id"].as_str(),
        Some(id.as_str()),
        "the saved id round-trips: {listed}"
    );
    assert_eq!(
        rows[0]["state"], "disconnected",
        "saved but not connected yet: {listed}"
    );
    assert_eq!(rows[0]["mock"], true, "the mock flag round-trips: {listed}");

    // 2. cluster.connect → `connection.state` reaches `connected`.
    let connected = call(
        &rpc,
        &request("4", cmd::CLUSTER_CONNECT, json!({ "clusterId": &id })),
    );
    assert_eq!(connected["ok"], true, "cluster.connect: {connected}");
    let state = wait_event(&asm, &mut rx, event::CONNECTION_STATE, |p| {
        p["clusterId"].as_str() == Some(id.as_str()) && p["state"] == "connected"
    });
    assert_eq!(state["state"], "connected", "{state}");

    // 3. topic.create → topic.list contains it → `topic.changed` with change `created`.
    let topic = "t100-orders".to_string();
    let created = call(
        &rpc,
        &request(
            "5",
            cmd::TOPIC_CREATE,
            json!({ "clusterId": &id, "name": &topic, "numPartitions": 1, "replicationFactor": 1 }),
        ),
    );
    assert_eq!(created["ok"], true, "topic.create: {created}");
    let change = wait_event(&asm, &mut rx, event::TOPIC_CHANGED, |p| {
        p["topic"].as_str() == Some(topic.as_str()) && p["change"] == "created"
    });
    assert_eq!(change["change"], "created", "{change}");
    let topics = call(
        &rpc,
        &request("6", cmd::TOPIC_LIST, json!({ "clusterId": &id })),
    );
    assert_eq!(topics["ok"], true, "topic.list: {topics}");
    assert!(
        topics["data"].as_array().is_some_and(|all| all
            .iter()
            .any(|t| t["name"].as_str() == Some(topic.as_str()))),
        "the created topic must be listed: {topics}"
    );

    // 4. produce.send → `okCount == n` and every result carries offset/partition/timestamp.
    let inputs = vec![
        ProduceMessage::text("t100-alpha").with_key("k1"),
        ProduceMessage::text("t100-beta"),
    ];
    let input_values: Vec<Value> = inputs
        .iter()
        .map(|m| serde_json::to_value(&m.value).expect("message bytes serialize"))
        .collect();
    let produce_req = ProduceReq {
        topic: topic.clone(),
        messages: inputs,
    };
    let produced = call(
        &rpc,
        &request(
            "7",
            cmd::PRODUCE_SEND,
            json!({ "clusterId": &id, "requests": [serde_json::to_value(&produce_req).expect("req")] }),
        ),
    );
    assert_eq!(produced["ok"], true, "produce.send: {produced}");
    assert_eq!(
        produced["data"]["okCount"], 2,
        "both messages accepted: {produced}"
    );
    assert_eq!(produced["data"]["errorCount"], 0, "no failures: {produced}");
    let results = produced["data"]["results"]
        .as_array()
        .expect("results is an array");
    assert_eq!(results.len(), 2, "one result per message: {produced}");
    for result in results {
        assert!(result["offset"].is_number(), "result pins offset: {result}");
        assert!(
            result["partition"].is_number(),
            "result pins partition: {result}"
        );
        assert!(
            result["timestamp"].is_number(),
            "result pins timestamp: {result}"
        );
        assert!(result["error"].is_null(), "no per-message error: {result}");
    }

    // 5. consume.subscribe → `message.received` per message with byte-identical values.
    //    No `subscription.state == running` event exists on this path (the service only
    //    publishes state on pause/resume), so the running state is pinned from the
    //    subscribe *response* here and the event is pinned at pause→resume in step 6.
    let subscribed = call(
        &rpc,
        &request(
            "8",
            cmd::CONSUBSCRIBE_START,
            json!({ "clusterId": &id, "topic": &topic, "start": { "type": "earliest" }, "follow": true }),
        ),
    );
    assert_eq!(subscribed["ok"], true, "consume.subscribe: {subscribed}");
    assert_eq!(
        subscribed["data"]["state"], "running",
        "a fresh subscription is running: {subscribed}"
    );
    assert_eq!(
        subscribed["data"]["topic"].as_str(),
        Some(topic.as_str()),
        "{subscribed}"
    );
    let sub_id = subscribed["data"]["subscriptionId"]
        .as_str()
        .expect("the response carries subscriptionId")
        .to_string();
    assert!(!sub_id.is_empty(), "{subscribed}");

    let mut seen: Vec<Value> = Vec::new();
    while seen.len() < input_values.len() {
        let (name, payload) = next_event(&asm, &mut rx);
        if name == event::MESSAGE_RECEIVED
            && payload["subscriptionId"].as_str() == Some(sub_id.as_str())
        {
            assert_eq!(
                payload["message"]["topic"].as_str(),
                Some(topic.as_str()),
                "message.received names the topic: {payload}"
            );
            seen.push(payload["message"]["value"].clone());
        }
    }
    for want in &input_values {
        assert!(
            seen.contains(want),
            "every produced payload must arrive byte-identical; missing {want} in {seen:?}"
        );
    }

    // 6. consume.pause → state `paused`; consume.resume → state `running`;
    //    consume.seek → ok (void), never a panic.
    let paused = call(
        &rpc,
        &request(
            "9",
            cmd::CONSUME_PAUSE,
            json!({ "subscriptionId": &sub_id }),
        ),
    );
    assert_eq!(paused["ok"], true, "consume.pause: {paused}");
    let paused_state = wait_event(&asm, &mut rx, event::SUBSCRIPTION_STATE, |p| {
        p["subscriptionId"].as_str() == Some(sub_id.as_str()) && p["state"] == "paused"
    });
    assert_eq!(paused_state["state"], "paused", "{paused_state}");

    let resumed = call(
        &rpc,
        &request(
            "10",
            cmd::CONSUME_RESUME,
            json!({ "subscriptionId": &sub_id }),
        ),
    );
    assert_eq!(resumed["ok"], true, "consume.resume: {resumed}");
    let running_state = wait_event(&asm, &mut rx, event::SUBSCRIPTION_STATE, |p| {
        p["subscriptionId"].as_str() == Some(sub_id.as_str()) && p["state"] == "running"
    });
    assert_eq!(running_state["state"], "running", "{running_state}");

    let sought = call(
        &rpc,
        &request(
            "11",
            cmd::CONSUME_SEEK,
            json!({ "subscriptionId": &sub_id, "topic": &topic, "partition": 0, "offset": 0 }),
        ),
    );
    assert_eq!(sought["ok"], true, "consume.seek: {sought}");
    assert_eq!(
        sought["data"],
        Value::Null,
        "void commands answer null: {sought}"
    );

    // A failed command must not disturb global state: pausing an unknown subscription
    // errors, yet the live one stays active.
    let bogus = call(
        &rpc,
        &request(
            "12",
            cmd::CONSUME_PAUSE,
            json!({ "subscriptionId": "00000000-0000-0000-0000-000000000000" }),
        ),
    );
    assert_eq!(
        bogus["ok"], false,
        "pausing an unknown subscription must fail: {bogus}"
    );
    assert!(
        bogus.get("error").is_some(),
        "a failure carries an error body: {bogus}"
    );
    assert_eq!(
        asm.app.consume().active_count(),
        1,
        "the failed command must not touch the live subscription"
    );

    // 7. consume.unsubscribe → `subscription.done` exactly once.
    let unsub = call(
        &rpc,
        &request(
            "13",
            cmd::CONSUBSCRIBE_STOP,
            json!({ "subscriptionId": &sub_id }),
        ),
    );
    assert_eq!(unsub["ok"], true, "consume.unsubscribe: {unsub}");
    assert_eq!(
        unsub["data"], true,
        "stopping a live subscription answers true: {unsub}"
    );
    let done = wait_event(&asm, &mut rx, event::SUBSCRIPTION_DONE, |p| {
        p["subscriptionId"].as_str() == Some(sub_id.as_str())
    });
    assert_eq!(done["reason"], "stopped by client", "{done}");
    assert!(done["delivered"].is_number(), "{done}");

    // Idempotent second stop: a no-op, and it publishes no second done event.
    let again = call(
        &rpc,
        &request(
            "14",
            cmd::CONSUBSCRIBE_STOP,
            json!({ "subscriptionId": &sub_id }),
        ),
    );
    assert_eq!(
        again["ok"], true,
        "a second unsubscribe still succeeds: {again}"
    );
    assert_eq!(
        again["data"], false,
        "a second unsubscribe is a no-op: {again}"
    );
    let trailing = drain_for(&asm, &mut rx, Duration::from_millis(200));
    assert_eq!(
        trailing
            .iter()
            .filter(|(name, _)| name == event::SUBSCRIPTION_DONE)
            .count(),
        0,
        "subscription.done must be published exactly once: {trailing:?}"
    );

    // 11. The pump released: `active_pumps` back to zero, read from the still-connected
    //     driver (after `cluster.delete` the driver is gone) — the leak detector.
    let mock = {
        let driver = asm
            .app
            .clusters()
            .driver(&cluster)
            .expect("driver while connected");
        driver
            .as_any()
            .and_then(|any| any.downcast_ref::<MockKafkaDriver>())
            .expect("the connected driver is the mock")
            .clone()
    };
    let pump_deadline = std::time::Instant::now() + WAIT;
    loop {
        let active = asm
            .rt
            .block_on(async { mock.state_handle().read().await.active_pumps() });
        if active == 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < pump_deadline,
            "active_pumps stuck at {active} after unsubscribe"
        );
        asm.rt
            .block_on(async { tokio::time::sleep(Duration::from_millis(10)).await });
    }
    assert_eq!(
        asm.app.consume().active_count(),
        0,
        "no live subscription remains"
    );

    // 8. group.list → ok; the mock answers an array.
    let groups = call(
        &rpc,
        &request("15", cmd::GROUP_LIST, json!({ "clusterId": &id })),
    );
    assert_eq!(groups["ok"], true, "group.list: {groups}");
    assert!(
        groups["data"].is_array(),
        "group.list answers an array: {groups}"
    );

    // 9. topic.delete → `topic.changed` with change `deleted` → topic.list drops it.
    let deleted = call(
        &rpc,
        &request(
            "16",
            cmd::TOPIC_DELETE,
            json!({ "clusterId": &id, "names": [&topic] }),
        ),
    );
    assert_eq!(deleted["ok"], true, "topic.delete: {deleted}");
    let gone = wait_event(&asm, &mut rx, event::TOPIC_CHANGED, |p| {
        p["topic"].as_str() == Some(topic.as_str()) && p["change"] == "deleted"
    });
    assert_eq!(gone["change"], "deleted", "{gone}");
    let after = call(
        &rpc,
        &request("17", cmd::TOPIC_LIST, json!({ "clusterId": &id })),
    );
    assert_eq!(after["ok"], true, "topic.list: {after}");
    assert!(
        !after["data"].as_array().is_some_and(|all| all
            .iter()
            .any(|t| t["name"].as_str() == Some(topic.as_str()))),
        "the deleted topic must be gone: {after}"
    );

    // 10. cluster.disconnect → `connection.state` disconnected; cluster.delete → list empty.
    let disconnected = call(
        &rpc,
        &request("18", cmd::CLUSTER_DISCONNECT, json!({ "clusterId": &id })),
    );
    assert_eq!(
        disconnected["ok"], true,
        "cluster.disconnect: {disconnected}"
    );
    let off = wait_event(&asm, &mut rx, event::CONNECTION_STATE, |p| {
        p["clusterId"].as_str() == Some(id.as_str()) && p["state"] == "disconnected"
    });
    assert_eq!(off["state"], "disconnected", "{off}");

    let removed = call(
        &rpc,
        &request("19", cmd::CLUSTER_DELETE, json!({ "clusterId": &id })),
    );
    assert_eq!(removed["ok"], true, "cluster.delete: {removed}");
    let final_list = call(&rpc, &request("20", cmd::CLUSTER_LIST, json!({})));
    assert_eq!(final_list["ok"], true, "cluster.list: {final_list}");
    assert_eq!(
        final_list["data"],
        json!([]),
        "the journey leaves no cluster behind: {final_list}"
    );

    // 11 (final invariants): nothing is left running anywhere.
    assert_eq!(
        asm.app.consume().active_count(),
        0,
        "final invariant: no live subscription"
    );
}
