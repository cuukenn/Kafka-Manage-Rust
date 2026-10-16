//! T062 — driver-agnostic reconnect supervision inside ClusterService (ruling R28).
//! Everything runs against the mock driver; virtual time (`start_paused` + `advance`)
//! pins the backoff table without any real waiting.

use std::time::Duration;

use kafka_app::bus::{AppEvent, BusReceiver, EventBus};
use kafka_app::services::clusters::{ClusterService, reconnect_backoff};
use kafka_app::store::Store;
use kafka_core::ClusterId;
use kafka_core::cluster::{ClusterConfig, ConnectionState};
use kafka_core::error::{DriverError, ErrorKind};
use kafka_driver::mock::{FaultInjection, MockKafkaDriver};

fn harness() -> (ClusterService, Store, EventBus) {
    let store = Store::open_in_memory().expect("in-memory store");
    let bus = EventBus::new(64);
    (ClusterService::new(store.clone(), bus.clone()), store, bus)
}

async fn save_mock(svc: &ClusterService, name: &str) -> ClusterId {
    svc.save(&ClusterConfig::mock(name)).await.expect("save cluster").id
}

fn drain(rx: &mut BusReceiver) -> Vec<AppEvent> {
    let mut out = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        out.push(ev);
    }
    out
}

fn connection_states(events: &[AppEvent]) -> Vec<ConnectionState> {
    events
        .iter()
        .filter_map(|ev| match ev {
            AppEvent::ConnectionStateChanged { state, .. } => Some(*state),
            _ => None,
        })
        .collect()
}

/// Let spawned tasks (the supervisor, store blocking jobs) make progress under
/// paused time, then return everything the bus published meanwhile.
async fn settle(rx: &mut BusReceiver) -> Vec<AppEvent> {
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    drain(rx)
}

/// Inject an always-failing connection error on `list_topics`, run one driver call
/// through the live handle, and report the failure to the service exactly like the
/// runtime call sites do. Returns the injected error's kind.
async fn inject_and_report(svc: &ClusterService, id: &ClusterId) -> ErrorKind {
    let driver = svc.driver(id).expect("live driver handle");
    let mock = driver
        .as_any()
        .and_then(|any| any.downcast_ref::<MockKafkaDriver>())
        .expect("mock seam reachable through the guard");
    mock.set_faults(FaultInjection {
        fail_probability: 1.0,
        fail_operation: Some("list_topics".into()),
        fail_message: "injected connection loss".into(),
        ..Default::default()
    })
    .await;
    let err = driver.list_topics().await.expect_err("injected failure must fail the call");
    let kind = err.kind();
    assert!(svc.report_driver_error(id, &err), "connection-class failure must start supervision");
    kind
}

/// Flip a saved (still mock-connected) cluster to a non-mock config: the supervisor's
/// fresh `connect` then builds the placeholder real driver (the `real` feature is off
/// in tests), whose connect always fails — a deterministic reconnect failure.
async fn make_reconnect_fail(svc: &ClusterService, id: &ClusterId) {
    let mut cfg = svc
        .list()
        .await
        .expect("list clusters")
        .into_iter()
        .find(|view| view.config.id == *id)
        .expect("saved cluster")
        .config;
    cfg.mock = false;
    cfg.bootstrap_servers = vec!["localhost:9092".into()];
    svc.save(&cfg).await.expect("re-save cluster");
}

#[test]
fn backoff_table_is_exponential_and_caps_at_30s() {
    for (attempt, secs) in [1u64, 2, 4, 8, 16, 30].into_iter().enumerate() {
        assert_eq!(reconnect_backoff(attempt), Duration::from_secs(secs), "attempt {attempt}");
    }
    // Past the table the cap repeats forever — no unbounded wait, no real 30s wait.
    assert_eq!(reconnect_backoff(6), Duration::from_secs(30));
    assert_eq!(reconnect_backoff(1000), Duration::from_secs(30));
}

#[tokio::test(start_paused = true)]
async fn connection_failure_degrades_then_reconnects_through_connecting_connected() {
    let (svc, _store, bus) = harness();
    let mut rx = bus.subscribe();
    let id = save_mock(&svc, "mock-reconn").await;
    svc.connect(&id).await.expect("connect");
    drain(&mut rx); // the manual connect's connecting+connected pair

    assert_eq!(inject_and_report(&svc, &id).await, ErrorKind::Connection);
    assert_eq!(svc.state_of(&id), ConnectionState::Degraded);

    let events = settle(&mut rx).await;
    assert_eq!(
        connection_states(&events),
        vec![
            ConnectionState::Degraded,
            ConnectionState::Disconnected,
            ConnectionState::Connecting,
            ConnectionState::Connected,
        ]
    );
    let reason = events.iter().find_map(|ev| match ev {
        AppEvent::ConnectionStateChanged { state: ConnectionState::Degraded, reason, .. } => reason.clone(),
        _ => None,
    });
    assert!(reason.as_deref().is_some_and(|r| r.contains("injected connection loss")), "degraded carries the reason");
    assert_eq!(svc.state_of(&id), ConnectionState::Connected);

    // The successful reconnect ended the task and reset the backoff: a fresh failure
    // spawns a fresh supervisor that reconnects just as cleanly.
    assert_eq!(inject_and_report(&svc, &id).await, ErrorKind::Connection);
    assert_eq!(
        connection_states(&settle(&mut rx).await),
        vec![
            ConnectionState::Degraded,
            ConnectionState::Disconnected,
            ConnectionState::Connecting,
            ConnectionState::Connected,
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn failed_reconnect_retries_on_the_backoff_table() {
    let (svc, _store, bus) = harness();
    let mut rx = bus.subscribe();
    let id = save_mock(&svc, "mock-fail").await;
    svc.connect(&id).await.expect("connect");
    drain(&mut rx);
    make_reconnect_fail(&svc, &id).await;

    assert_eq!(inject_and_report(&svc, &id).await, ErrorKind::Connection);

    // First attempt runs immediately: driver teardown, then a failed fresh connect.
    assert_eq!(
        connection_states(&settle(&mut rx).await),
        vec![
            ConnectionState::Degraded,
            ConnectionState::Disconnected,
            ConnectionState::Connecting,
            ConnectionState::Error,
        ]
    );

    // Backoff step 1 is 1s: 999ms in, nothing has been retried yet.
    tokio::time::advance(Duration::from_millis(999)).await;
    assert!(connection_states(&settle(&mut rx).await).is_empty(), "must wait the full 1s step");
    tokio::time::advance(Duration::from_millis(2)).await;
    assert_eq!(
        connection_states(&settle(&mut rx).await),
        vec![ConnectionState::Connecting, ConnectionState::Error],
        "attempt 2 after the 1s step"
    );

    // Backoff step 2 is 2s.
    tokio::time::advance(Duration::from_millis(1999)).await;
    assert!(connection_states(&settle(&mut rx).await).is_empty(), "must wait the full 2s step");
    tokio::time::advance(Duration::from_millis(2)).await;
    assert_eq!(
        connection_states(&settle(&mut rx).await),
        vec![ConnectionState::Connecting, ConnectionState::Error],
        "attempt 3 after the 2s step"
    );
}

#[tokio::test(start_paused = true)]
async fn manual_disconnect_cancels_the_reconnect_supervisor() {
    let (svc, _store, bus) = harness();
    let mut rx = bus.subscribe();
    let id = save_mock(&svc, "mock-cancel").await;
    svc.connect(&id).await.expect("connect");
    drain(&mut rx);
    // Reconnect would keep failing (and retrying forever) without the cancel.
    make_reconnect_fail(&svc, &id).await;

    assert_eq!(inject_and_report(&svc, &id).await, ErrorKind::Connection);
    assert_eq!(
        connection_states(&settle(&mut rx).await),
        vec![
            ConnectionState::Degraded,
            ConnectionState::Disconnected,
            ConnectionState::Connecting,
            ConnectionState::Error,
        ]
    );

    svc.disconnect(&id).await.expect("manual disconnect");
    assert_eq!(connection_states(&drain(&mut rx)), vec![ConnectionState::Disconnected]);

    // A cancelled supervisor stays dead no matter how much backoff time passes.
    tokio::time::advance(Duration::from_secs(3600)).await;
    assert!(
        connection_states(&settle(&mut rx).await).is_empty(),
        "no reconnect task may run after a manual disconnect"
    );
    assert_eq!(svc.state_of(&id), ConnectionState::Disconnected);
}

/// The connection-class filter is the design's load-bearing decision: a manual
/// disconnect reports `not_connected`, and such calls must never resurrect a
/// cancelled supervisor. Broker failures mean the connection works, and a bad
/// config will never connect; only `connection`/`timeout` may degrade the cluster.
#[tokio::test(start_paused = true)]
async fn only_connection_class_failures_start_supervision() {
    let (svc, _store, bus) = harness();
    let mut rx = bus.subscribe();
    let id = save_mock(&svc, "mock-filter").await;
    svc.connect(&id).await.expect("connect");
    drain(&mut rx);

    for err in [
        DriverError::NotConnected(id.to_string()),
        DriverError::Broker { code: 1, name: "b".into(), message: "refused".into() },
        DriverError::TopicNotFound("missing".into()),
        DriverError::InvalidConfig { field: "name".into(), message: "bad".into() },
    ] {
        assert!(
            !svc.report_driver_error(&id, &err),
            "{} must not start supervision",
            err.kind().as_str()
        );
    }
    assert_eq!(svc.state_of(&id), ConnectionState::Connected, "non-connection failures leave the cluster healthy");
    assert!(connection_states(&drain(&mut rx)).is_empty(), "no state event without a connection-class failure");

    // `timeout` is connection-class too (the card maps transport/timeout onto it).
    assert!(
        svc.report_driver_error(&id, &DriverError::Timeout { operation: "meta".into(), elapsed_ms: 5 }),
        "timeout must start supervision"
    );
    assert_eq!(svc.state_of(&id), ConnectionState::Degraded);
    svc.disconnect(&id).await.expect("cancel the supervisor");
}

/// The real `cluster.info` call path is wired to the same seam: a cluster with no
/// live driver fails `not_connected` before any driver call, so it must neither
/// degrade nor start a supervisor. (The connection-class branch — `info` reaching
/// the driver and reporting its error — is pinned by
/// `info_reports_a_connection_class_failure_into_supervision` below.)
#[tokio::test(start_paused = true)]
async fn info_without_a_live_driver_does_not_supervise() {
    let (svc, _store, bus) = harness();
    let mut rx = bus.subscribe();
    let id = save_mock(&svc, "mock-info").await;

    let err = svc.info(&id).await.expect_err("no live driver");
    assert_eq!(err.kind(), ErrorKind::NotConnected);
    assert!(connection_states(&settle(&mut rx).await).is_empty(), "no degrade without a live driver");
    assert_eq!(svc.state_of(&id), ConnectionState::Disconnected);
}

/// The connection-class branch of the same path: a live driver whose `cluster_info`
/// fails with a connection-class error must surface that error AND be reported
/// through the reconnect seam (degraded + supervision). This is the only production
/// wiring of the T062 feature, so it is pinned here rather than only via the public
/// `report_driver_error` seam.
#[tokio::test(start_paused = true)]
async fn info_reports_a_connection_class_failure_into_supervision() {
    let (svc, _store, bus) = harness();
    let mut rx = bus.subscribe();
    let id = save_mock(&svc, "mock-info-fault").await;
    svc.connect(&id).await.expect("connect");
    drain(&mut rx);

    let driver = svc.driver(&id).expect("live driver handle");
    let mock = driver
        .as_any()
        .and_then(|any| any.downcast_ref::<MockKafkaDriver>())
        .expect("mock seam reachable through the guard");
    mock.set_faults(FaultInjection {
        fail_probability: 1.0,
        fail_operation: Some("cluster_info".into()),
        fail_message: "injected cluster_info loss".into(),
        ..Default::default()
    })
    .await;

    let err = svc.info(&id).await.expect_err("the injected cluster_info failure must surface");
    assert!(
        matches!(err.kind(), ErrorKind::Connection | ErrorKind::Timeout),
        "info must surface a connection-class error, got {:?}",
        err.kind()
    );

    let states = connection_states(&settle(&mut rx).await);
    assert!(
        states.contains(&ConnectionState::Degraded),
        "a connection-class info failure must degrade the cluster: {states:?}"
    );

    svc.disconnect(&id).await.expect("cancel the supervisor");
}
