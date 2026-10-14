//! T036 end-to-end: the 4 `group.*` commands routed through `Router::handle`
//! against a real `App` on in-memory SQLite + the mock driver. The mock cluster
//! a router connects is empty by construction (`driver_for` builds
//! `MockKafkaDriver::new()`), so the fixture dataset is loaded through the T084
//! `as_any` seam — the preset groups then come from the driver itself, exactly
//! as the UI will see them once `mock.seed` (T085) exists.

use kafka_app::App;
use kafka_app::bus::{AppEvent, BusReceiver};
use kafka_core::ClusterId;
use kafka_core::cluster::ClusterConfig;
use kafka_core::protocol::cmd;
use kafka_driver::MockKafkaDriver;
use serde_json::{Value, json};
use webview_bridge::handlers::register_cluster;
use webview_bridge::handlers_group::register_group;
use webview_bridge::router::Router;

/// Fixture group with 2 members and committed offsets on `orders` 0/1.
const CHECKOUT: &str = "checkout-service";
/// Fixture group with one committed offset on `audit-log` 0.
const ETL: &str = "analytics-etl";

fn wired() -> (App, Router) {
    let app = App::builder().build();
    let router = Router::new();
    register_cluster(&app, &router);
    register_group(&app, &router);
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

/// The `data` body of an ok response, asserting `ok:true` first.
fn data(resp: &Value) -> Vec<Value> {
    assert!(flag(resp), "expected ok:true, got {resp}");
    resp["data"].as_array().expect("data is an array").clone()
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

/// Load the mock fixture dataset (topics, messages, the two preset groups) into
/// the live driver behind a connected cluster.
async fn seed(app: &App, id: &str) {
    let id = ClusterId(id.to_string());
    let driver = app.clusters().driver(&id).expect("live driver");
    let mock = driver
        .as_any()
        .and_then(|any| any.downcast_ref::<MockKafkaDriver>())
        .expect("a mock cluster is backed by MockKafkaDriver");
    mock.seed().await;
}

/// Events the app bus carries from here on; whatever connect/seed published is
/// dropped, so a later count measures only what the command under test emitted.
fn bus(app: &App) -> BusReceiver {
    let mut rx = app.bus().subscribe();
    while rx.try_recv().is_ok() {}
    rx
}

fn group_changes(rx: &mut BusReceiver) -> Vec<(String, String, usize)> {
    let mut out = Vec::new();
    while let Ok(event) = rx.try_recv() {
        if let AppEvent::GroupChanged {
            cluster_id,
            group_id,
            partitions,
        } = event
        {
            out.push((cluster_id, group_id, partitions));
        }
    }
    out
}

/// Committed-offset rows for one group, sorted by partition for a stable order.
async fn offset_rows(router: &Router, id: &str, group: &str, with_lag: bool) -> Vec<Value> {
    let mut rows = data(
        &call(
            router,
            cmd::GROUP_OFFSETS,
            json!({ "clusterId": id, "groupId": group, "withLag": with_lag }),
        )
        .await,
    );
    rows.sort_by_key(|row| row["partition"].as_u64().unwrap_or(0));
    rows
}

/// Item 2: the full read path over the fixture groups — `group.list` reports the
/// preset groups, `group.describe` (explicit `ids`) matches their fields,
/// `group.offsets` (`withLag:true`) carries lag, and a real
/// `group.resetOffsets` answers `dryRun:false` with a non-empty `applied`.
#[tokio::test]
async fn group_read_path_round_trips_the_fixture_groups() {
    let (_app, router) = wired();
    let id = connected(&router, "group-life").await;
    seed(&_app, &id).await;

    let listed = data(&call(&router, cmd::GROUP_LIST, json!({ "clusterId": &id })).await);
    let names: Vec<&str> = listed
        .iter()
        .filter_map(|g| g["groupId"].as_str())
        .collect();
    assert!(
        names.contains(&CHECKOUT),
        "preset group missing from {names:?}"
    );
    assert!(names.contains(&ETL), "preset group missing from {names:?}");
    let checkout = listed
        .iter()
        .find(|g| g["groupId"] == CHECKOUT)
        .expect(CHECKOUT);
    assert_eq!(
        checkout["state"], "stable",
        "GroupState serializes snake_case"
    );
    assert_eq!(checkout["memberCount"], 2);
    assert_eq!(checkout["protocolType"], "consumer");
    assert_eq!(checkout["partitionAssignor"], "range");
    assert!(checkout["coordinatorId"].is_number());
    assert_eq!(checkout["isLocal"], false);

    let detailed = data(
        &call(
            &router,
            cmd::GROUP_DESCRIBE,
            json!({ "clusterId": &id, "ids": [CHECKOUT] }),
        )
        .await,
    );
    assert_eq!(detailed.len(), 1, "one id in, one detail out");
    assert_eq!(detailed[0]["info"]["groupId"], CHECKOUT);
    assert_eq!(detailed[0]["info"]["memberCount"], 2);
    let members = detailed[0]["members"].as_array().expect("members");
    assert_eq!(members.len(), 2);
    assert_eq!(members[0]["memberId"], "checkout-1");
    assert_eq!(members[0]["clientId"], "checkout-pod-7d9f");
    assert_eq!(
        members[0]["assignments"]
            .as_array()
            .expect("assignments")
            .len(),
        2
    );

    let rows = offset_rows(&router, &id, CHECKOUT, true).await;
    assert_eq!(rows.len(), 2, "committed offsets on orders 0 and 1");
    assert_eq!(rows[0]["topic"], "orders");
    assert_eq!(rows[0]["offset"], 2);
    assert_eq!(rows[0]["lag"], 1, "orders/0 holds 3 messages");
    assert_eq!(rows[1]["partition"], 1);
    assert_eq!(rows[1]["offset"], 1);
    assert_eq!(rows[1]["lag"], 1, "orders/1 holds 2 messages");

    let reset = call(
        &router,
        cmd::GROUP_RESET_OFFSETS,
        json!({ "clusterId": &id, "groupId": CHECKOUT, "spec": {"type": "earliest"}, "dryRun": false }),
    )
    .await;
    assert!(flag(&reset), "{reset}");
    assert_eq!(reset["data"]["groupId"], CHECKOUT);
    assert_eq!(reset["data"]["dryRun"], false);
    assert_eq!(reset["data"]["error"], Value::Null);
    let applied = reset["data"]["applied"].as_array().expect("applied");
    assert!(
        !applied.is_empty(),
        "a real reset must report what it wrote"
    );
    assert_eq!(applied.len(), 2);
    assert!(applied.iter().all(|entry| entry["offset"] == 0), "{reset}");

    let after = offset_rows(&router, &id, CHECKOUT, true).await;
    assert_eq!(
        after[0]["offset"], 0,
        "the reset must be visible on re-read"
    );
    assert_eq!(after[1]["offset"], 0);
}

/// Ledger-facing shape: `ids` is optional on the wire, and an empty list means
/// every group — the same answer `group.list` gives.
#[tokio::test]
async fn describe_without_ids_covers_every_group() {
    let (_app, router) = wired();
    let id = connected(&router, "group-all").await;
    seed(&_app, &id).await;

    let all = data(&call(&router, cmd::GROUP_DESCRIBE, json!({ "clusterId": &id })).await);
    let names: Vec<&str> = all
        .iter()
        .filter_map(|d| d["info"]["groupId"].as_str())
        .collect();
    assert_eq!(names.len(), 2, "both fixture groups: {names:?}");
    assert!(names.contains(&CHECKOUT));
    assert!(names.contains(&ETL));

    let etl = all.iter().find(|d| d["info"]["groupId"] == ETL).expect(ETL);
    assert_eq!(etl["info"]["state"], "empty");
    assert_eq!(etl["info"]["memberCount"], 0);
    assert!(etl["members"].as_array().expect("members").is_empty());
}

/// Item 3: `dryRun:true` answers `dryRun:true` with the computed offsets, writes
/// nothing, and publishes no `GROUP_CHANGED` on the bus.
#[tokio::test]
async fn dry_run_reset_publishes_nothing_and_writes_nothing() {
    let (app, router) = wired();
    let id = connected(&router, "group-dry").await;
    seed(&app, &id).await;
    let mut rx = bus(&app);

    let resp = call(
        &router,
        cmd::GROUP_RESET_OFFSETS,
        json!({ "clusterId": &id, "groupId": CHECKOUT, "spec": {"type": "toOffset", "value": 1}, "dryRun": true }),
    )
    .await;
    assert!(flag(&resp), "{resp}");
    assert_eq!(resp["data"]["dryRun"], true);
    let applied = resp["data"]["applied"].as_array().expect("applied");
    assert_eq!(applied.len(), 2, "the dry run still computes both targets");
    assert!(applied.iter().all(|entry| entry["offset"] == 1), "{resp}");

    assert!(
        group_changes(&mut rx).is_empty(),
        "a dry run must not publish GROUP_CHANGED"
    );
    let rows = offset_rows(&router, &id, CHECKOUT, false).await;
    assert_eq!(rows[0]["offset"], 2, "a dry run must not write");
    assert_eq!(rows[1]["offset"], 1);
    assert_eq!(rows[0]["lag"], Value::Null, "withLag:false reports no lag");
}

/// Counterpart to the dry-run assertion: a written reset publishes exactly one
/// `GROUP_CHANGED`, so the empty bus above is evidence, not a missing wire.
#[tokio::test]
async fn written_reset_publishes_one_group_changed() {
    let (app, router) = wired();
    let id = connected(&router, "group-publish").await;
    seed(&app, &id).await;
    let mut rx = bus(&app);

    let resp = call(
        &router,
        cmd::GROUP_RESET_OFFSETS,
        json!({ "clusterId": &id, "groupId": ETL, "spec": {"type": "latest"}, "dryRun": false }),
    )
    .await;
    assert!(flag(&resp), "{resp}");
    assert_eq!(resp["data"]["dryRun"], false);
    assert_eq!(
        resp["data"]["applied"].as_array().expect("applied").len(),
        1
    );

    assert_eq!(
        group_changes(&mut rx),
        vec![(id.clone(), ETL.to_string(), 1)],
        "one event per written reset"
    );
    let rows = offset_rows(&router, &id, ETL, false).await;
    assert_eq!(
        rows[0]["offset"], 2,
        "audit-log holds 2 messages: latest is 2"
    );
}

/// Item 4: a saved but never connected cluster answers `not_connected`.
#[tokio::test]
async fn list_disconnected_cluster_is_not_connected() {
    let (_app, router) = wired();
    let cfg = ClusterConfig::mock("cold-groups");
    let payload = serde_json::to_value(&cfg).expect("cfg serialises");
    assert!(flag(&call(&router, cmd::CLUSTER_SAVE, payload).await));

    let resp = call(
        &router,
        cmd::GROUP_LIST,
        json!({ "clusterId": cfg.id.to_string() }),
    )
    .await;
    assert!(!flag(&resp));
    assert_eq!(error_kind(&resp), "not_connected");
}

/// Item 4: the service validates `groupId` before it looks for a driver, so a
/// blank id on a live cluster is still `invalid_config`, never `not_found`.
#[tokio::test]
async fn offsets_blank_group_id_is_invalid_config() {
    let (_app, router) = wired();
    let id = connected(&router, "group-validate").await;

    let resp = call(
        &router,
        cmd::GROUP_OFFSETS,
        json!({ "clusterId": &id, "groupId": "  " }),
    )
    .await;
    assert!(!flag(&resp), "{resp}");
    assert_eq!(error_kind(&resp), "invalid_config");
    assert_eq!(resp["error"]["retryable"], false);

    let offline = call(
        &router,
        cmd::GROUP_OFFSETS,
        json!({ "clusterId": "nope", "groupId": "" }),
    )
    .await;
    assert_eq!(
        error_kind(&offline),
        "invalid_config",
        "validation precedes the connection check"
    );
}

/// Item 4: an unknown groupId on `group.describe` is an error response whose kind
/// follows the driver (`GroupNotFound`), never a crash or an empty ok.
#[tokio::test]
async fn describe_unknown_group_is_error_response() {
    let (_app, router) = wired();
    let id = connected(&router, "group-ghost").await;
    seed(&_app, &id).await;

    let resp = call(
        &router,
        cmd::GROUP_DESCRIBE,
        json!({ "clusterId": &id, "ids": ["ghost"] }),
    )
    .await;
    assert!(
        !flag(&resp),
        "unknown group must not answer ok:true: {resp}"
    );
    assert!(
        resp["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("ghost")),
        "the message should name the group: {resp}"
    );
    assert!(resp["error"]["retryable"].as_bool().is_some());
}
