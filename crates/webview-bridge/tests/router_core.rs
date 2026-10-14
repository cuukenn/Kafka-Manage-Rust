//! T032 router-core tests: one test per 必须覆盖 item of the card.

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use kafka_core::protocol::{cmd, ErrorDto};
use kafka_core::ErrorKind;
use serde_json::{json, Value};
use tokio::sync::oneshot;
use webview_bridge::router::{Handler, Router};

type Fallible = Result<Value, ErrorDto>;

fn boxed(f: impl Fn(Value) -> Fallible + Send + Sync + 'static) -> Handler {
    Arc::new(move |payload: Value| -> BoxFuture<'static, Fallible> {
        let out = f(payload);
        Box::pin(async move { out })
    })
}

/// Item 1: legal envelope parses, hits the handler, echoes payload verbatim.
#[tokio::test]
async fn valid_envelope_hits_handler_and_echoes_payload() {
    let router = Router::new();
    router.register(cmd::TOPIC_LIST, boxed(Ok));

    let resp = router
        .handle(r#"{"id":"r1","cmd":"topic.list","payload":{"clusterId":"c1"}}"#)
        .await;

    assert!(resp.ok);
    assert_eq!(resp.id, "r1");
    assert_eq!(resp.data, Some(json!({"clusterId": "c1"})));
    assert!(resp.error.is_none());
}

/// Item 2: unknown cmd -> Internal, non-retryable, message names the cmd, id echoed.
#[tokio::test]
async fn unknown_cmd_is_non_retryable_internal_naming_the_cmd() {
    let router = Router::new();

    let resp = router.handle(r#"{"id":"r2","cmd":"nope.void"}"#).await;

    assert!(!resp.ok);
    assert_eq!(resp.id, "r2");
    let error = resp.error.expect("error body");
    assert_eq!(error.kind, ErrorKind::Internal.as_str());
    assert!(error.message.contains("nope.void"), "message names cmd: {}", error.message);
    assert!(!error.retryable);
}

/// Item 3: malformed envelopes never panic; id contract: "" when absent or
/// unextractable, echoed when present; missing payload becomes null.
#[tokio::test]
async fn malformed_envelopes_never_panic_and_keep_the_id_contract() {
    let router = Router::new();
    router.register(cmd::CLUSTER_LIST, boxed(Ok));

    // Non-JSON: error response, nothing extractable -> id is "".
    let resp = router.handle("definitely not json").await;
    assert!(!resp.ok);
    assert_eq!(resp.id, "");
    assert_eq!(resp.error.expect("error body").kind, ErrorKind::InvalidConfig.as_str());

    // Missing id and missing payload: routes normally; id "", payload null.
    let resp = router.handle(r#"{"cmd":"cluster.list"}"#).await;
    assert!(resp.ok);
    assert_eq!(resp.id, "");
    assert_eq!(resp.data, Some(Value::Null));

    // Missing id + unknown cmd: the error response also carries id "".
    let resp = router.handle(r#"{"cmd":"nope.void"}"#).await;
    assert!(!resp.ok);
    assert_eq!(resp.id, "");

    // Missing cmd: error echoing the id that was present.
    let resp = router.handle(r#"{"id":"r3","payload":{"a":1}}"#).await;
    assert!(!resp.ok);
    assert_eq!(resp.id, "r3");
    let error = resp.error.expect("error body");
    assert_eq!(error.kind, ErrorKind::InvalidConfig.as_str());
    assert!(!error.retryable);
}

/// Item 4: handler Err(ErrorDto) passes through verbatim (hand-built DTO, retryable=true).
#[tokio::test]
async fn handler_error_dto_passes_through_unchanged() {
    let router = Router::new();
    let dto =
        ErrorDto { kind: "storage".into(), message: "disk on fire".into(), retryable: true, hint: None };
    router.register(cmd::PRODUCE_SEND, boxed(move |_| Err(dto.clone())));

    let resp = router.handle(r#"{"id":"r4","cmd":"produce.send","payload":{}}"#).await;

    assert!(!resp.ok);
    assert_eq!(resp.id, "r4");
    assert!(resp.data.is_none());
    let error = resp.error.expect("error body");
    assert_eq!(error.kind, "storage");
    assert_eq!(error.message, "disk on fire");
    assert!(error.retryable, "Router must not re-derive retryable");
}

/// Item 5: duplicate registration on the same cmd = override, later wins.
#[tokio::test]
async fn duplicate_registration_is_overridden_by_the_later_one() {
    let router = Router::new();
    router.register(cmd::GROUP_LIST, boxed(Ok));
    let later = boxed(|payload| Ok(json!({"overridden": true, "seen": payload})));
    router.register(cmd::GROUP_LIST, later);

    let resp = router.handle(r#"{"id":"r5","cmd":"group.list","payload":"x"}"#).await;

    assert!(resp.ok);
    assert_eq!(resp.data, Some(json!({"overridden": true, "seen": "x"})));
}

/// Item 6: in-flight requests complete concurrently (reverse-order oneshot gates);
/// ids and data never cross-talk.
#[tokio::test]
async fn concurrent_requests_complete_independently_without_cross_talk() {
    fn gated(rx: oneshot::Receiver<()>, tag: &'static str) -> Handler {
        let slot = Arc::new(std::sync::Mutex::new(Some(rx)));
        Arc::new(move |_: Value| -> BoxFuture<'static, Fallible> {
            let slot = Arc::clone(&slot);
            Box::pin(async move {
                let rx = slot
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take()
                    .ok_or_else(|| ErrorDto::new(ErrorKind::Cancelled, "gate already consumed"))?;
                rx.await.map_err(|_| ErrorDto::new(ErrorKind::Cancelled, "gate dropped"))?;
                Ok(json!({"handler": tag}))
            })
        })
    }

    let (tx_a, rx_a) = oneshot::channel::<()>();
    let (tx_b, rx_b) = oneshot::channel::<()>();
    let router = Router::new();
    router.register(cmd::CLUSTER_CONNECT, gated(rx_a, "A"));
    router.register(cmd::CLUSTER_DISCONNECT, gated(rx_b, "B"));

    let spawn = |router: Router, raw: &'static str| tokio::spawn(async move { router.handle(raw).await });
    let fut_a = spawn(router.clone(), r#"{"id":"a-1","cmd":"cluster.connect","payload":{}}"#);
    let fut_b = spawn(router, r#"{"id":"b-2","cmd":"cluster.disconnect","payload":{}}"#);

    let (resp_a, resp_b) = tokio::time::timeout(Duration::from_secs(5), async {
        // Release B before A: only finishes if both handlers are in flight at
        // once (a serialized router would deadlock and trip the timeout).
        let _ = tx_b.send(());
        let resp_b = fut_b.await.expect("task b");
        let _ = tx_a.send(());
        let resp_a = fut_a.await.expect("task a");
        (resp_a, resp_b)
    })
    .await
    .expect("serialized requests would deadlock and trip this timeout");

    assert!(resp_a.ok && resp_b.ok);
    assert_eq!(resp_a.id, "a-1");
    assert_eq!(resp_a.data, Some(json!({"handler": "A"})));
    assert_eq!(resp_b.id, "b-2");
    assert_eq!(resp_b.data, Some(json!({"handler": "B"})));
}

/// Item 7 (final review I3): a panicking handler is contained by the router's
/// spawn boundary — the wire contract holds (`ok:false`, kind `internal`) and
/// the process survives: the router keeps routing afterwards.
#[tokio::test]
async fn panicking_handler_becomes_internal_error_and_process_survives() {
    let boom: Handler = Arc::new(|_: Value| -> BoxFuture<'static, Fallible> {
        Box::pin(async move { panic!("handler blew up on purpose") })
    });
    let router = Router::new();
    router.register("panic.test", boom);
    router.register(cmd::CLUSTER_LIST, boxed(Ok));

    let resp = router.handle(r#"{"id":"p1","cmd":"panic.test","payload":{}}"#).await;
    assert!(!resp.ok);
    assert_eq!(resp.id, "p1");
    let error = resp.error.expect("error body");
    assert_eq!(error.kind, ErrorKind::Internal.as_str());
    assert!(error.message.contains("handler panicked"), "message says panic: {}", error.message);

    // Process survives: routing still works after the contained panic.
    let resp = router.handle(r#"{"id":"p2","cmd":"cluster.list"}"#).await;
    assert!(resp.ok);
    assert_eq!(resp.id, "p2");
}
