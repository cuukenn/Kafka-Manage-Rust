//! Mock/Real conformance contract for the `KafkaDriver::as_any` downcast
//! seam (T084).
//!
//! This file is the home for contract tests that must hold across driver
//! implementations, so the mock driver cannot drift from the real one (or from
//! the bridge's expectations of both). It starts with the seam the `mock.*`
//! bridge commands (T085) rely on: only the mock driver offers itself for
//! downcasting to its control plane, a real driver answers `None`.

use kafka_core::cluster::ClusterConfig;
use kafka_driver::driver::{DriverKind, KafkaDriver, driver_for};
use kafka_driver::mock::MockKafkaDriver;

fn mock_config() -> ClusterConfig {
    ClusterConfig::mock("as-any-seam")
}

fn real_config() -> ClusterConfig {
    ClusterConfig::real("as-any-real", ["localhost:9092"])
}

/// A mock cluster's `DynDriver` must downcast to `MockKafkaDriver` through the
/// seam, otherwise the `mock.*` control commands could never reach it.
#[tokio::test]
async fn mock_driver_downcasts_through_as_any() {
    let driver = driver_for(&mock_config());

    let mock = driver
        .as_any()
        .and_then(|any| any.downcast_ref::<MockKafkaDriver>())
        .expect("mock driver must expose MockKafkaDriver through as_any");

    assert_eq!(
        mock.kind(),
        DriverKind::Mock,
        "downcast target must be the mock driver"
    );
}

/// A non-mock driver (`PlaceholderRealDriver` with `real` off,
/// `RealKafkaDriver` with it on) must not expose a concrete mock control plane.
#[tokio::test]
async fn real_driver_offers_no_as_any_seam() {
    let driver = driver_for(&real_config());

    assert!(
        driver.as_any().is_none(),
        "real drivers must keep the default as_any of None"
    );
}

/// The seam must really reach the mock control plane: after the downcast the
/// caller can take the fault-injection handle `mock.*` commands will drive.
#[tokio::test]
async fn downcast_reaches_the_mock_control_plane() {
    let driver = driver_for(&mock_config());

    let mock = driver
        .as_any()
        .and_then(|any| any.downcast_ref::<MockKafkaDriver>())
        .expect("mock driver must be downcastable");

    let faults = mock.faults_handle();
    assert!(
        faults.try_read().is_ok(),
        "fault-injection control plane must be reachable through the downcast seam"
    );
}
