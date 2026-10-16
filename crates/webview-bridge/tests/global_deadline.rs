//! T082a global-deadline tests: one per 必须覆盖 item of the card.
//! The deadline is a backstop, so both tests use a handler that never returns
//! on its own — the router must answer anyway.

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use kafka_core::protocol::ErrorDto;
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

/// Item 1: a handler that outlives the deadline is cut off with `timeout`.
#[tokio::test]
async fn a_slow_handler_is_cut_off_by_the_global_deadline() {
    let router = Router::new().with_timeout(Duration::from_millis(60));
    router.register("slow.echo", slow(30));

    let begun = tokio::time::Instant::now();
    let resp = router.handle(r#"{"id":"d-1","cmd":"slow.echo","payload":{}}"#).await;
    let waited = begun.elapsed();

    assert!(!resp.ok);
    assert_eq!(resp.id, "d-1");
    assert!(resp.data.is_none());
    let error = resp.error.expect("error body");
    assert_eq!(error.kind, ErrorKind::Timeout.as_str());
    assert!(error.retryable, "a timeout is worth retrying");
    assert!(error.message.contains("slow.echo"), "names the cmd: {}", error.message);
    assert!(error.message.contains("60"), "names the deadline: {}", error.message);
    assert!(waited < Duration::from_secs(5), "answered after {waited:?}, not after the 30 s sleep");
}

/// Items 2 and 3: a fast request is untouched by the deadline, and the router
/// (and the process) still routes normally after one was cut off.
#[tokio::test]
async fn the_deadline_leaves_fast_requests_and_the_router_alone() {
    let router = Router::new().with_timeout(Duration::from_millis(60));
    router.register("slow.echo", slow(30));
    router.register("quick.echo", Arc::new(|_: Value| -> BoxFuture<'static, Fallible> {
        Box::pin(async move { Ok(json!({ "finished": true })) })
    }));

    let quick = router.handle(r#"{"id":"f-1","cmd":"quick.echo","payload":{}}"#).await;
    assert!(quick.ok);
    assert_eq!(quick.id, "f-1");
    assert_eq!(quick.data, Some(json!({ "finished": true })));
    assert!(quick.error.is_none());

    let timed_out = router.handle(r#"{"id":"f-2","cmd":"slow.echo","payload":{}}"#).await;
    assert_eq!(timed_out.error.expect("error body").kind, ErrorKind::Timeout.as_str());

    let again = router.handle(r#"{"id":"f-3","cmd":"quick.echo","payload":{}}"#).await;
    assert!(again.ok, "an aborted handler must not take the router down with it");
    assert_eq!(again.id, "f-3");
}
