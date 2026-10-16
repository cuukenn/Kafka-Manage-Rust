//! T082b cancel-in-flight tests, one per 必须覆盖 item, driven through the
//! router's own wire contract.

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use kafka_core::protocol::{cmd, ErrorDto, ResponseEnvelope};
use kafka_core::ErrorKind;
use serde_json::{json, Value};
use webview_bridge::router::{Handler, Router};

type Fallible = Result<Value, ErrorDto>;

/// A handler that never finishes inside a test's lifetime.
fn slow(for_secs: u64) -> Handler {
    Arc::new(move |_: Value| -> BoxFuture<'static, Fallible> {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_secs(for_secs)).await;
            Ok(json!({ "finished": true }))
        })
    })
}

fn request(id: &str, cmd: &str) -> String {
    json!({ "id": id, "cmd": cmd, "payload": {} }).to_string()
}

fn cancel_request(id: &str) -> String {
    json!({ "id": format!("{id}-cancel"), "cmd": cmd::REQUEST_CANCEL, "payload": { "id": id } })
        .to_string()
}

fn parked(router: &Router, id: &'static str) -> tokio::task::JoinHandle<ResponseEnvelope> {
    let router = router.clone();
    tokio::spawn(async move { router.handle(&request(id, "slow.echo")).await })
}

/// Wait (bounded) until `count` requests are tracked: a `#[tokio::test]`
/// current-thread runtime tracks before any handler runs, but polling keeps the
/// assertion honest if that ever changes.
async fn tracked(router: &Router, count: usize) {
    for _ in 0..200 {
        if router.inflight() == count {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("router never reported {count} tracked request(s)");
}

fn kind_of(resp: &ResponseEnvelope) -> String {
    resp.error.as_ref().expect("error body").kind.clone()
}

/// Item 1: `request.cancel` drops a pending request instead of waiting it out.
#[tokio::test]
async fn a_pending_request_can_be_cancelled_from_the_ui() {
    let router = Router::new().with_timeout(Duration::from_secs(30));
    router.register("slow.echo", slow(30));

    let pending = parked(&router, "c-1");
    tracked(&router, 1).await;

    let cancel = router.handle(&cancel_request("c-1")).await;
    assert!(cancel.ok);
    assert_eq!(cancel.data, Some(json!({ "id": "c-1", "cancelled": true })));

    let resp = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .expect("a cancelled request answers at once instead of sleeping 30 s")
        .expect("task");
    assert!(!resp.ok);
    assert_eq!(resp.id, "c-1");
    assert_eq!(kind_of(&resp), ErrorKind::Cancelled.as_str());
    assert!(!resp.error.expect("error body").retryable, "the user asked for it");

    // Item 2: the neighbour is untouched and needs its own cancel.
    let neighbour = parked(&router, "c-2");
    tracked(&router, 1).await;
    let again = router.handle(&cancel_request("c-2")).await;
    assert_eq!(again.data, Some(json!({ "id": "c-2", "cancelled": true })));
    assert_eq!(kind_of(&neighbour.await.expect("task")), ErrorKind::Cancelled.as_str());
    assert_eq!(router.inflight(), 0);
}

/// Item 3: cancelling nothing is a `false`, not an error — for an id that never
/// existed, one that already answered, and one the deadline already collected.
#[tokio::test]
async fn only_a_live_request_can_be_cancelled() {
    let router = Router::new().with_timeout(Duration::from_millis(200));
    router.register("quick.echo", slow(0));
    router.register("slow.echo", slow(30));

    let ghost = router.handle(&cancel_request("ghost")).await;
    assert_eq!(ghost.data, Some(json!({ "id": "ghost", "cancelled": false })));

    assert!(router.handle(&request("q-1", "quick.echo")).await.ok);
    let late = router.handle(&cancel_request("q-1")).await;
    assert_eq!(late.data, Some(json!({ "id": "q-1", "cancelled": false })));

    let timed_out = router.handle(&request("q-2", "slow.echo")).await;
    assert_eq!(kind_of(&timed_out), ErrorKind::Timeout.as_str());
    let after = router.handle(&cancel_request("q-2")).await;
    assert_eq!(after.data, Some(json!({ "id": "q-2", "cancelled": false })));
    assert_eq!(router.inflight(), 0);

    // Item 4: without an id the command cannot work, and says which field is missing.
    let bare = router.handle(r#"{"id":"x-1","cmd":"request.cancel","payload":{}}"#).await;
    assert!(!bare.ok);
    assert_eq!(bare.id, "x-1");
    let error = bare.error.expect("error body");
    assert_eq!(error.kind, ErrorKind::InvalidConfig.as_str());
    assert!(!error.retryable);
    assert!(error.message.contains("id"), "names the missing field: {}", error.message);
}

/// Item 5: `ok`, `timeout` and `cancel` all empty the tracking table, so a long
/// session cannot grow it, and an unroutable envelope leaves no trace either.
#[tokio::test]
async fn every_ending_empties_the_tracking_table() {
    let router = Router::new().with_timeout(Duration::from_millis(50));
    router.register("quick.echo", slow(0));
    router.register("slow.echo", slow(30));

    for i in 0..5 {
        let ok = router.handle(&request(&format!("l-{i}"), "quick.echo")).await;
        assert!(ok.ok);
    }
    assert_eq!(router.inflight(), 0, "answered requests leave nothing behind");

    let unnamed = router.handle(r#"{"cmd":"slow.echo","payload":{}}"#).await;
    assert_eq!(unnamed.id, "");
    assert_eq!(router.inflight(), 0, "an id-less request is not addressable (R83)");

    let timed_out = router.handle(&request("l-timeout", "slow.echo")).await;
    assert_eq!(kind_of(&timed_out), ErrorKind::Timeout.as_str());

    let cancelled = parked(&router, "l-cancel");
    tracked(&router, 1).await;
    assert!(router.cancel("l-cancel"), "a live id is cancellable through the method too");
    let _ = cancelled.await.expect("task");

    assert_eq!(router.inflight(), 0, "no entry survives ok / timeout / cancel");
    assert!(!router.cancel("l-cancel"), "the entry is gone, so a second cancel is a no-op");
}
