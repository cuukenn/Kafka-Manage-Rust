//! Cluster lifecycle: the saved configs, plus the live driver instances they back.
//!
//! One [`ClusterService`] is shared by every command handler. It is the only place that
//! knows a cluster is "connected", so connection state reaches the UI from here and
//! nowhere else.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use serde::Serialize;
use tokio::sync::Mutex as AsyncMutex;

use kafka_core::cluster::{ClusterConfig, ClusterInfo, ConnectionState};
use kafka_core::error::{DriverError, StorageError};
use kafka_core::ClusterId;
use kafka_driver::driver::{DynDriver, driver_for};
use kafka_driver::mock::MockKafkaDriver;

use crate::bus::{AppEvent, EventBus};
use crate::driver_guard::GuardedDriver;
use crate::store::Store;

use super::{ServiceError, store_task};

/// Downcast a live driver handle to the mock control plane through the T084 `as_any`
/// seam. `GuardedDriver` forwards `as_any` to its inner driver (T038a), so this works
/// whether or not the guard wraps the driver; real drivers answer `None`.
fn mock_of(driver: &DynDriver) -> Option<&MockKafkaDriver> {
    driver.as_any().and_then(|any| any.downcast_ref::<MockKafkaDriver>())
}

/// One row of the cluster list: the saved config plus what the UI needs to render it.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterView {
    #[serde(flatten)]
    pub config: ClusterConfig,
    /// Live connection state; `disconnected` when no driver instance exists.
    pub state: ConnectionState,
    /// `mock` / `real` while a driver is live, `null` otherwise.
    pub driver: Option<&'static str>,
}

/// Live state of one cluster. `gate` gives connect/disconnect once-at-a-time semantics, so
/// two concurrent `cluster.connect` calls cannot build two drivers for the same cluster.
struct Slot {
    gate: AsyncMutex<()>,
    driver: Mutex<Option<DynDriver>>,
    state: Mutex<ConnectionState>,
}

/// `DynDriver` is `Arc<dyn KafkaDriver>` and `dyn KafkaDriver` is not `Debug`, so this cannot be
/// derived. Print what a slot means to the UI: which driver kind is live, and its state.
impl fmt::Debug for Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Slot")
            .field("driver", &self.driver_of().map(|driver| driver.kind().as_str()))
            .field("state", &self.state_of())
            .finish_non_exhaustive()
    }
}

impl Slot {
    fn new() -> Self {
        Self {
            gate: AsyncMutex::new(()),
            driver: Mutex::new(None),
            state: Mutex::new(ConnectionState::Disconnected),
        }
    }

    fn driver_of(&self) -> Option<DynDriver> {
        self.driver.lock().ok().and_then(|guard| guard.clone())
    }

    fn state_of(&self) -> ConnectionState {
        self.state.lock().map(|guard| *guard).unwrap_or(ConnectionState::Disconnected)
    }

    fn set(&self, driver: Option<DynDriver>, state: ConnectionState) {
        if let Ok(mut guard) = self.driver.lock() {
            *guard = driver;
        }
        if let Ok(mut guard) = self.state.lock() {
            *guard = state;
        }
    }
}

/// Registry of saved clusters and the drivers currently connected to them.
#[derive(Debug, Clone)]
pub struct ClusterService {
    store: Store,
    bus: EventBus,
    slots: Arc<Mutex<HashMap<ClusterId, Arc<Slot>>>>,
}

impl ClusterService {
    pub fn new(store: Store, bus: EventBus) -> Self {
        Self { store, bus, slots: Arc::new(Mutex::new(HashMap::new())) }
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn bus(&self) -> &EventBus {
        &self.bus
    }

    // ---- registry helpers ------------------------------------------------

    fn slot_of(&self, id: &ClusterId) -> Arc<Slot> {
        let mut slots = self.slots.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        slots.entry(id.clone()).or_insert_with(|| Arc::new(Slot::new())).clone()
    }

    fn peek_slot(&self, id: &ClusterId) -> Option<Arc<Slot>> {
        self.slots.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).get(id).cloned()
    }

    fn all_slots(&self) -> Vec<(ClusterId, Arc<Slot>)> {
        let slots = self.slots.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        slots.iter().map(|(id, slot)| (id.clone(), Arc::clone(slot))).collect()
    }

    /// Connection state of one cluster without touching the driver.
    pub fn state_of(&self, id: &ClusterId) -> ConnectionState {
        self.peek_slot(id).map(|slot| slot.state_of()).unwrap_or(ConnectionState::Disconnected)
    }

    /// Clusters that currently hold a driver instance.
    pub fn connected_ids(&self) -> Vec<ClusterId> {
        self.all_slots()
            .into_iter()
            .filter(|(_, slot)| slot.driver_of().is_some())
            .map(|(id, _)| id)
            .collect()
    }

    /// Live driver handle for other services. Errors with `not_connected` when the cluster
    /// was never connected (or was disconnected since).
    pub fn driver(&self, id: &ClusterId) -> Result<DynDriver, ServiceError> {
        self.peek_slot(id)
            .and_then(|slot| slot.driver_of())
            .ok_or_else(|| ServiceError::Driver(DriverError::NotConnected(id.to_string())))
    }

    // ---- configs ---------------------------------------------------------

    /// Saved clusters plus their live state, in the store's display order.
    pub async fn list(&self) -> Result<Vec<ClusterView>, ServiceError> {
        let configs = store_task(&self.store, |store| -> Result<_, ServiceError> {
            Ok(store.list_clusters()?)
        })
        .await?;

        Ok(configs
            .into_iter()
            .map(|config| {
                let slot = self.peek_slot(&config.id);
                ClusterView {
                    driver: slot.as_ref().and_then(|slot| slot.driver_of()).map(|d| d.kind().as_str()),
                    state: slot.as_ref().map(|slot| slot.state_of()).unwrap_or(ConnectionState::Disconnected),
                    config,
                }
            })
            .collect())
    }

    /// Validates before persisting, so the UI cannot save a cluster that would only fail
    /// when someone finally tries to connect to it.
    pub async fn save(&self, cfg: &ClusterConfig) -> Result<ClusterConfig, ServiceError> {
        cfg.validate()?;
        let cfg = cfg.clone();
        let saved = store_task(&self.store, move |store| -> Result<_, ServiceError> {
            Ok(store.save_cluster(&cfg)?)
        })
        .await?;
        Ok(saved)
    }

    /// Disconnects first, then removes the row and drops it from the recent list. A failed
    /// disconnect does not block the delete — the config is what the user asked to remove.
    pub async fn delete(&self, id: &ClusterId) -> Result<bool, ServiceError> {
        if let Err(err) = self.disconnect(id).await {
            tracing::warn!(cluster = %id, %err, "disconnect before delete failed");
        }
        let id = id.clone();
        let removed = store_task(&self.store, move |store| -> Result<_, ServiceError> {
            let removed = store.delete_cluster(&id)?;
            store.forget_recent(&id)?;
            Ok(removed)
        })
        .await?;
        Ok(removed)
    }

    // ---- connections -----------------------------------------------------

    /// Connects (or returns the existing connection). Serialized per cluster: concurrent
    /// callers share one driver instance and one pair of state events.
    pub async fn connect(&self, id: &ClusterId) -> Result<ClusterInfo, ServiceError> {
        let slot = self.slot_of(id);
        let _gate = slot.gate.lock().await;

        if let Some(driver) = slot.driver_of() {
            if driver.is_connected().await {
                return Ok(driver.cluster_info().await?);
            }
            // A driver that dropped underneath us must not be handed out again.
            slot.set(None, ConnectionState::Disconnected);
        }

        let cfg = store_task(&self.store, {
            let id = id.clone();
            move |store| -> Result<_, ServiceError> {
                store.get_cluster(&id)?.ok_or_else(|| {
                    ServiceError::Storage(StorageError::NotFound(format!("cluster {id}")))
                })
            }
        })
        .await?;
        cfg.validate()?;

        self.publish_state(id, ConnectionState::Connecting, None);
        // T038a: every driver handed out by `connect` is guarded (T029), and the
        // guard forwards `as_any`, so the mock seam below still reaches the mock.
        let driver: DynDriver = Arc::new(GuardedDriver::new(driver_for(&cfg)));
        self.restore_mock_snapshot(id, &driver).await;
        match driver.connect(&cfg).await {
            Ok(info) => {
                slot.set(Some(driver), ConnectionState::Connected);
                self.publish_state(id, ConnectionState::Connected, None);
                self.remember(id).await;
                Ok(info)
            }
            Err(err) => {
                slot.set(None, ConnectionState::Error);
                self.publish_state(id, ConnectionState::Error, Some(err.to_string()));
                Err(err.into())
            }
        }
    }

    /// Disconnects and forgets the driver instance. Idempotent: a cluster that is not
    /// connected still ends up reported as `disconnected`.
    pub async fn disconnect(&self, id: &ClusterId) -> Result<(), ServiceError> {
        let slot = self.slot_of(id);
        let _gate = slot.gate.lock().await;

        let Some(driver) = slot.driver_of() else {
            slot.set(None, ConnectionState::Disconnected);
            self.publish_state(id, ConnectionState::Disconnected, None);
            return Ok(());
        };

        // T079: persist the mock state BEFORE the disconnect clears the slot, so a
        // later connect can restore it; a snapshot failure must not fail disconnect.
        self.save_mock_snapshot(id, &driver).await;

        match driver.disconnect().await {
            Ok(()) => {
                slot.set(None, ConnectionState::Disconnected);
                self.publish_state(id, ConnectionState::Disconnected, None);
                Ok(())
            }
            Err(err) => {
                slot.set(None, ConnectionState::Error);
                self.publish_state(id, ConnectionState::Error, Some(err.to_string()));
                Err(err.into())
            }
        }
    }

    /// `cluster.info` — the live view of a connected cluster.
    pub async fn info(&self, id: &ClusterId) -> Result<ClusterInfo, ServiceError> {
        let driver = self.driver(id)?;
        Ok(driver.cluster_info().await?)
    }

    /// Shutdown path: every live driver gets a chance to close cleanly.
    pub async fn disconnect_all(&self) {
        for (id, _) in self.all_slots() {
            if let Err(err) = self.disconnect(&id).await {
                tracing::warn!(cluster = %id, %err, "disconnect during shutdown failed");
            }
        }
    }

    // ---- mock snapshots (T038a, T079 semantics) ---------------------------

    /// Reload the persisted mock state onto a freshly built driver, BEFORE `connect`
    /// runs: `restore_json` swaps data only (topics/groups/round-robin/version), so
    /// the connected flag and cluster identity always come from the live config.
    /// Any failure — storage read, or bad snapshot data — is a warning: the cluster
    /// simply connects empty.
    async fn restore_mock_snapshot(&self, id: &ClusterId, driver: &DynDriver) {
        let Some(mock) = mock_of(driver) else { return };
        let stored = store_task(&self.store, {
            let id = id.clone();
            move |store| -> Result<_, ServiceError> { Ok(store.load_mock_snapshot(&id)?) }
        })
        .await;
        let snapshot = match stored {
            Ok(snapshot) => snapshot,
            Err(err) => {
                tracing::warn!(cluster = %id, %err, "mock snapshot load failed, connecting empty");
                return;
            }
        };
        if let Some(json) = snapshot
            && let Err(err) = mock.restore_json(&json).await
        {
            tracing::warn!(cluster = %id, %err, "mock snapshot restore failed, connecting empty");
        }
    }

    /// Snapshot the mock driver into its SQLite slot before the driver is torn down.
    /// Best effort in both directions: snapshotting or saving may fail, disconnect
    /// must not.
    async fn save_mock_snapshot(&self, id: &ClusterId, driver: &DynDriver) {
        let Some(mock) = mock_of(driver) else { return };
        let json = match mock.snapshot_json().await {
            Ok(json) => json,
            Err(err) => {
                tracing::warn!(cluster = %id, %err, "mock snapshot failed, keeping previous slot");
                return;
            }
        };
        let slot_id = id.clone();
        if let Err(err) =
            store_task(&self.store, move |store| -> Result<(), ServiceError> {
                store.save_mock_snapshot(&slot_id, &json)?;
                Ok(())
            })
            .await
        {
            tracing::warn!(cluster = %id, %err, "mock snapshot save failed");
        }
    }

    // ---- events ----------------------------------------------------------

    fn publish_state(&self, id: &ClusterId, state: ConnectionState, reason: Option<String>) {
        self.bus.publish(AppEvent::ConnectionStateChanged {
            cluster_id: id.to_string(),
            state,
            reason,
        });
    }

    /// Recent-cluster bookkeeping is best effort: a failed write must not fail a connect.
    /// Awaited rather than spawned so the list is consistent by the time `connect` returns.
    async fn remember(&self, id: &ClusterId) {
        let id = id.clone();
        if let Err(err) = store_task(&self.store, move |store| -> Result<(), ServiceError> {
            store.push_recent(&id)?;
            Ok(())
        })
        .await
        {
            tracing::warn!(%err, "failed to record recent cluster");
        }
    }
}
