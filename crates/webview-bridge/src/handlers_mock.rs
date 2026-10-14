//! T085: the 3 `mock.*` control commands (`reset` / `seed` / `faults`), wired
//! against a real [`App`]. Unlike the other command cards these three skip the
//! service layer: they take the live driver from `ClusterService::driver` and
//! reach the `MockKafkaDriver` control plane through the T084 `as_any` seam.
//! A cluster running a non-mock driver (seam `None` or downcast miss) answers
//! `unsupported` naming the clusterId — mock-only commands never pretend to
//! work against a real broker.

use kafka_app::App;
use kafka_core::protocol::{ErrorDto, cmd};
use kafka_core::{ClusterId, ErrorKind};
use kafka_driver::DynDriver;
use kafka_driver::mock::{FaultInjection, MockKafkaDriver};
use serde::Deserialize;
use serde_json::Value;

use crate::handlers::{cluster_id_of, parse_payload, register_with, service_dto, to_data};
use crate::router::Router;

/// `mock.seed`: `{clusterId, seed}`. A missing `seed` is a decode failure, so
/// the bridge answers `invalid_config` before the control plane is touched.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SeedPayload {
    cluster_id: ClusterId,
    seed: u64,
}

/// `mock.faults`: `{clusterId}` reads the current injection; `{clusterId,
/// injection}` installs one (partial objects fill the rest from
/// `FaultInjection`'s serde defaults) and answers the read-back.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct FaultsPayload {
    cluster_id: ClusterId,
    #[serde(default)]
    injection: Option<FaultInjection>,
}

/// Downcast the live driver handle to the mock control plane. The borrow is
/// tied to `driver`, which every caller keeps alive for the whole command.
/// `as_any` is `None` on real drivers (T084), so both failure shapes collapse
/// to one `unsupported` answer carrying the clusterId.
fn mock_of<'a>(
    driver: &'a DynDriver,
    id: &ClusterId,
    command: &str,
) -> Result<&'a MockKafkaDriver, ErrorDto> {
    driver
        .as_any()
        .and_then(|any| any.downcast_ref::<MockKafkaDriver>())
        .ok_or_else(|| {
            ErrorDto::new(
                ErrorKind::Unsupported,
                format!("cluster {id} is not running a mock driver, so '{command}' is unsupported"),
            )
        })
}

/// Register the 3 `mock.*` commands against a real [`App`].
///
/// `mock.reset` clears topics, groups and faults but keeps the connection, so
/// the driver handle stays valid afterwards; `data` is `null` for both
/// `reset` and `seed`, matching the other void commands on the wire.
pub fn register_mock(app: &App, router: &Router) {
    let clusters = app.clusters().clone();

    register_with(
        router,
        cmd::MOCK_RESET,
        clusters.clone(),
        |clusters, payload| async move {
            let id = cluster_id_of(&payload)?;
            let driver = clusters.driver(&id).map_err(service_dto)?;
            let mock = mock_of(&driver, &id, cmd::MOCK_RESET)?;
            mock.reset().await;
            Ok(Value::Null)
        },
    );

    register_with(
        router,
        cmd::MOCK_SEED,
        clusters.clone(),
        |clusters, payload| async move {
            let body: SeedPayload = parse_payload(&payload, cmd::MOCK_SEED)?;
            let driver = clusters.driver(&body.cluster_id).map_err(service_dto)?;
            let mock = mock_of(&driver, &body.cluster_id, cmd::MOCK_SEED)?;
            mock.pin_rng_seed(body.seed);
            Ok(Value::Null)
        },
    );

    register_with(
        router,
        cmd::MOCK_FAULTS,
        clusters,
        |clusters, payload| async move {
            let body: FaultsPayload = parse_payload(&payload, cmd::MOCK_FAULTS)?;
            let driver = clusters.driver(&body.cluster_id).map_err(service_dto)?;
            let mock = mock_of(&driver, &body.cluster_id, cmd::MOCK_FAULTS)?;
            if let Some(injection) = &body.injection {
                *mock.faults_handle().write().await = injection.clone();
            }
            to_data(mock.faults().await)
        },
    );
}
