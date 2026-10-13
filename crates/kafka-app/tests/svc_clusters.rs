//! ClusterService: saved configs, live drivers, and the connection events the UI follows.
//! Every cluster here is a mock one, so no broker is required.

use std::sync::Arc;

use kafka_app::bus::{AppEvent, BusReceiver, EventBus};
use kafka_app::services::ServiceError;
use kafka_app::services::clusters::{ClusterService, ClusterView};
use kafka_app::store::Store;
use kafka_core::ClusterId;
use kafka_core::cluster::{ClusterConfig, ConnectionState};
use kafka_core::error::{DriverError, ErrorKind, StorageError};

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

#[tokio::test]
async fn connect_publishes_connecting_then_connected() {
    let (svc, _store, bus) = harness();
    let mut rx = bus.subscribe();
    let id = save_mock(&svc, "mock-a").await;

    let info = svc.connect(&id).await.expect("connect");
    assert!(info.cluster_id.is_some());
    assert_eq!(svc.state_of(&id), ConnectionState::Connected);

    // Saving a config publishes nothing; connecting publishes exactly two transitions.
    assert_eq!(connection_states(&drain(&mut rx)), vec![ConnectionState::Connecting, ConnectionState::Connected]);
}

#[tokio::test]
async fn concurrent_connects_share_one_driver_and_one_event_pair() {
    let (svc, _store, bus) = harness();
    let mut rx = bus.subscribe();
    let id = save_mock(&svc, "mock-race").await;

    let (a, b, c) = tokio::join!(svc.connect(&id), svc.connect(&id), svc.connect(&id));
    assert!(a.is_ok() && b.is_ok() && c.is_ok());

    let first = svc.driver(&id).expect("driver");
    let second = svc.driver(&id).expect("driver");
    assert!(Arc::ptr_eq(&first, &second), "second caller must reuse the same driver");
    assert_eq!(connection_states(&drain(&mut rx)), vec![ConnectionState::Connecting, ConnectionState::Connected]);
}

#[tokio::test]
async fn a_stale_driver_is_replaced_on_the_next_connect() {
    let (svc, _store, _bus) = harness();
    let id = save_mock(&svc, "mock-stale").await;

    svc.connect(&id).await.expect("first connect");
    let before = svc.driver(&id).expect("driver");
    before.disconnect().await.expect("driver drops underneath the service");

    svc.connect(&id).await.expect("reconnect");
    let after = svc.driver(&id).expect("driver");
    assert!(!Arc::ptr_eq(&before, &after), "a dead driver must not be handed out again");
    assert_eq!(svc.state_of(&id), ConnectionState::Connected);
}

#[tokio::test]
async fn disconnect_drops_the_driver_and_is_idempotent() {
    let (svc, _store, bus) = harness();
    let mut rx = bus.subscribe();
    let id = save_mock(&svc, "mock-off").await;
    svc.connect(&id).await.expect("connect");
    drain(&mut rx);

    svc.disconnect(&id).await.expect("disconnect");
    assert_eq!(svc.state_of(&id), ConnectionState::Disconnected);
    assert_eq!(connection_states(&drain(&mut rx)), vec![ConnectionState::Disconnected]);

    // `expect_err` would need `Ok(Arc<dyn KafkaDriver>): Debug`, which the trait does not
    // provide (and T051 rejected adding it) — destructure instead.
    let err = match svc.driver(&id) {
        Ok(_) => panic!("driver must be gone"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), ErrorKind::NotConnected);

    svc.disconnect(&id).await.expect("second disconnect is fine");
    assert_eq!(connection_states(&drain(&mut rx)), vec![ConnectionState::Disconnected]);
}

#[tokio::test]
async fn unknown_cluster_ids_are_reported() {
    let (svc, _store, _bus) = harness();
    let missing = ClusterId::new();

    let err = svc.connect(&missing).await.expect_err("no such config");
    assert!(matches!(err, ServiceError::Storage(StorageError::NotFound(_))));
    assert_eq!(err.kind(), ErrorKind::Storage);
    assert_eq!(svc.state_of(&missing), ConnectionState::Disconnected);

    assert!(
        matches!(svc.driver(&missing), Err(ServiceError::Driver(DriverError::NotConnected(_)))),
        "no live driver"
    );
}

#[tokio::test]
async fn save_rejects_an_invalid_config_before_writing_it() {
    let (svc, store, _bus) = harness();
    let mut cfg = ClusterConfig::mock("broken");
    cfg.bootstrap_servers.clear();

    let err = svc.save(&cfg).await.expect_err("rejected");
    assert_eq!(err.kind(), ErrorKind::InvalidConfig);
    assert_eq!(err.to_dto().kind, "invalid_config");
    assert_eq!(store.count_clusters().expect("count"), 0);
}

#[tokio::test]
async fn list_reports_state_and_driver_kind_for_every_cluster() {
    let (svc, _store, _bus) = harness();
    let _alpha = save_mock(&svc, "alpha").await;
    let beta = save_mock(&svc, "beta").await;

    let views: Vec<ClusterView> = svc.list().await.expect("list");
    assert_eq!(views.len(), 2);
    assert_eq!(views[0].config.name, "alpha", "store order is by name");
    assert_eq!(views[0].state, ConnectionState::Disconnected);
    assert_eq!(views[0].driver, None);

    svc.connect(&beta).await.expect("connect");
    let views = svc.list().await.expect("list again");
    let beta_view = views.iter().find(|v| v.config.id == beta).expect("beta row");
    assert_eq!(beta_view.state, ConnectionState::Connected);
    assert_eq!(beta_view.driver, Some("mock"));

    // Wire shape the frontend reads: camelCase config plus state and driver.
    let json = serde_json::to_value(beta_view).expect("serialize view");
    assert_eq!(json["bootstrapServers"], serde_json::json!(["mock://localhost:9092"]));
    assert_eq!(json["state"], "connected");
    assert_eq!(json["driver"], "mock");
    assert_eq!(json["mock"], true);
}

#[tokio::test]
async fn delete_disconnects_first_and_clears_the_recent_list() {
    let (svc, store, bus) = harness();
    let mut rx = bus.subscribe();
    let id = save_mock(&svc, "mock-del").await;
    svc.connect(&id).await.expect("connect");
    assert_eq!(store.recent_clusters().expect("recent"), vec![id.clone()]);

    assert!(svc.delete(&id).await.expect("delete"));
    assert!(!store.has_cluster(&id).expect("has_cluster"));
    assert!(store.recent_clusters().expect("recent").is_empty());
    assert_eq!(svc.state_of(&id), ConnectionState::Disconnected);

    let states = connection_states(&drain(&mut rx));
    assert_eq!(states.first(), Some(&ConnectionState::Connecting));
    assert_eq!(states.last(), Some(&ConnectionState::Disconnected));

    assert!(!svc.delete(&id).await.expect("second delete"), "row is already gone");
}

#[tokio::test]
async fn disconnect_all_closes_every_live_cluster() {
    let (svc, _store, _bus) = harness();
    let a = save_mock(&svc, "mock-1").await;
    let b = save_mock(&svc, "mock-2").await;
    svc.connect(&a).await.expect("connect a");
    svc.connect(&b).await.expect("connect b");
    assert_eq!(svc.connected_ids().len(), 2);

    svc.disconnect_all().await;
    assert!(svc.connected_ids().is_empty());
    assert_eq!(svc.state_of(&a), ConnectionState::Disconnected);
    assert_eq!(svc.state_of(&b), ConnectionState::Disconnected);
}

/// Without the `real` feature a non-mock cluster resolves to the placeholder driver, which
/// is the clearest way to check the failure path end to end.
#[cfg(not(feature = "real"))]
#[tokio::test]
async fn a_failed_connect_publishes_an_error_state() {
    let (svc, _store, bus) = harness();
    let mut rx = bus.subscribe();
    let saved = svc.save(&ClusterConfig::real("unreachable", ["127.0.0.1:9"])).await.expect("save");

    let err = svc.connect(&saved.id).await.expect_err("placeholder driver");
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert_eq!(svc.state_of(&saved.id), ConnectionState::Error);

    let events = drain(&mut rx);
    assert_eq!(
        connection_states(&events),
        vec![ConnectionState::Connecting, ConnectionState::Error]
    );
    match events.last().expect("last event") {
        AppEvent::ConnectionStateChanged { cluster_id, state, reason } => {
            assert_eq!(cluster_id.as_str(), saved.id.as_str());
            assert_eq!(*state, ConnectionState::Error);
            assert!(reason.as_deref().unwrap_or_default().contains("real Kafka driver"));
        }
        other => panic!("expected a connection state event, got {other:?}"),
    }
}
