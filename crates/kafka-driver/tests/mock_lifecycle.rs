//! Lifecycle contract for `MockKafkaDriver` (T010).
//!
//! Locks the connection semantics every later integration test relies on:
//! unconnected calls fail with `NotConnected`, `connect` publishes a consistent
//! `ClusterInfo`, `disconnect` returns the driver to the unconnected state, and
//! the capability / kind introspection matches the mock implementation.

use kafka_core::cluster::ClusterConfig;
use kafka_core::error::DriverError;
use kafka_driver::driver::{DriverKind, KafkaDriver};
use kafka_driver::mock::MockKafkaDriver;

fn mock_config() -> ClusterConfig {
    ClusterConfig::mock("lifecycle-test")
}

async fn connected_driver() -> MockKafkaDriver {
    let driver = MockKafkaDriver::new();
    driver
        .connect(&mock_config())
        .await
        .expect("connect on a fresh mock driver should succeed");
    driver
}

/// Calls made before `connect` must fail with `NotConnected` instead of
/// succeeding against an empty state or panicking.
#[tokio::test]
async fn unconnected_calls_return_not_connected() {
    let driver = MockKafkaDriver::new();

    assert!(!driver.is_connected().await, "fresh driver must not be connected");

    let list_err = driver.list_topics().await.expect_err("list_topics before connect must fail");
    assert!(
        matches!(list_err, DriverError::NotConnected(_)),
        "list_topics before connect should be NotConnected, got {list_err:?}"
    );

    let info_err = driver.cluster_info().await.expect_err("cluster_info before connect must fail");
    assert!(
        matches!(info_err, DriverError::NotConnected(_)),
        "cluster_info before connect should be NotConnected, got {info_err:?}"
    );
}

/// After a successful `connect`, `is_connected` flips to true and
/// `cluster_info()` reports the brokers from the config, with the controller
/// being one of that broker set.
#[tokio::test]
async fn connect_publishes_consistent_cluster_info() {
    let driver = connected_driver().await;

    assert!(driver.is_connected().await, "driver must be connected after connect");

    let info = driver.cluster_info().await.expect("cluster_info after connect should succeed");
    assert!(!info.brokers.is_empty(), "cluster_info must expose a non-empty broker list");

    let controller = info.controller_id.expect("cluster_info must carry a controller id");
    assert!(
        info.brokers.iter().any(|b| b.node_id == controller),
        "controller {controller} must be one of the brokers {:?}",
        info.brokers.iter().map(|b| b.node_id).collect::<Vec<_>>()
    );

    // The info returned by `connect` itself must agree with the live one.
    let connect_info = kafka_driver::driver::KafkaDriver::connect(&driver, &mock_config())
        .await
        .expect("re-connect should succeed");
    assert_eq!(connect_info.brokers, info.brokers, "connect and cluster_info must agree on brokers");
    assert_eq!(connect_info.controller_id, info.controller_id, "controller must be stable across calls");
}

/// `disconnect` flips the driver back to the unconnected state: calls that
/// required a connection start failing with `NotConnected` again.
#[tokio::test]
async fn disconnect_returns_to_not_connected() {
    let driver = connected_driver().await;

    driver.disconnect().await.expect("disconnect after connect should succeed");
    assert!(!driver.is_connected().await, "driver must not be connected after disconnect");

    let err = driver.list_topics().await.expect_err("list_topics after disconnect must fail");
    assert!(
        matches!(err, DriverError::NotConnected(_)),
        "list_topics after disconnect should be NotConnected, got {err:?}"
    );

    let info_err = driver.cluster_info().await.expect_err("cluster_info after disconnect must fail");
    assert!(
        matches!(info_err, DriverError::NotConnected(_)),
        "cluster_info after disconnect should be NotConnected, got {info_err:?}"
    );
}

/// The mock driver advertises its mock-only features so the UI can enable them.
#[tokio::test]
async fn capabilities_report_mock_features() {
    let caps = MockKafkaDriver::new().capabilities();

    assert!(caps.fault_injection, "mock driver must advertise fault_injection");
    assert!(caps.fixtures, "mock driver must advertise fixtures");
    assert!(caps.live_stream, "mock driver must advertise live_stream");
}

/// The introspection kind must identify the implementation.
#[tokio::test]
async fn kind_is_mock() {
    assert_eq!(MockKafkaDriver::new().kind(), DriverKind::Mock);
}

/// Re-connecting the same instance must not panic: the current implementation is
/// idempotent — it re-applies the config, stays connected and returns Ok again
/// (the one-shot fixture seed is consumed by the first connect).
#[tokio::test]
async fn repeated_connect_is_idempotent() {
    let driver = MockKafkaDriver::new();

    let first = driver.connect(&mock_config()).await.expect("first connect should succeed");
    let second = driver.connect(&mock_config()).await.expect("second connect should not panic or fail");

    assert!(driver.is_connected().await, "driver must stay connected after a repeated connect");
    assert_eq!(first.brokers, second.brokers, "repeated connect must yield the same broker list");
    assert_eq!(first.controller_id, second.controller_id, "controller must survive a repeated connect");
    assert!(!second.brokers.is_empty(), "repeated connect must still expose brokers");
}

/// Disconnecting before any connect is a no-op, not an error.
#[tokio::test]
async fn disconnect_without_connect_is_noop() {
    let driver = MockKafkaDriver::new();

    driver.disconnect().await.expect("disconnect on a fresh driver should succeed");
    assert!(!driver.is_connected().await, "driver must remain disconnected");
}
