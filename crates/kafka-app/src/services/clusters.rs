//! Cluster lifecycle: the saved configs, plus the live driver instances they back.
//!
//! One [`ClusterService`] is shared by every command handler. It is the only place that
//! knows a cluster is "connected", so connection state reaches the UI from here and
//! nowhere else.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use serde::Serialize;
use tokio::sync::Mutex as AsyncMutex;

use kafka_core::cluster::{ClusterConfig, ClusterInfo, ConnectionState};
use kafka_core::error::{DriverError, ErrorKind, StorageError};
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

// ---- reconnect supervision (T062, ruling R28) ------------------------------

/// Exponential reconnect backoff steps in seconds; the last entry caps the wait.
pub const RECONNECT_BACKOFF_SECS: [u64; 6] = [1, 2, 4, 8, 16, 30];

/// Wait before reconnect attempt number `attempt` (0-based), capped at the last
/// table entry. A successful `connected` ends the supervisor task, so the next
/// failure restarts the table from 1s. Pure function: unit-testable without waiting.
pub fn reconnect_backoff(attempt: usize) -> Duration {
    let last = RECONNECT_BACKOFF_SECS.len() - 1;
    Duration::from_secs(RECONNECT_BACKOFF_SECS[attempt.min(last)])
}

/// Which failures mean "the connection itself is unhealthy" and must trigger the
/// reconnect supervisor. The card's timeout/transport/connection_lost map onto the
/// actual taxonomy as `ErrorKind::Timeout` and `ErrorKind::Connection`
/// (kafka-core `error.rs`). `NotConnected` is deliberately excluded: it is what a
/// manually disconnected cluster reports, and those calls must not resurrect a
/// cancelled supervisor. `Broker` is excluded too: the connection works, the broker
/// merely refused the request.
fn is_connection_class(kind: ErrorKind) -> bool {
    matches!(kind, ErrorKind::Connection | ErrorKind::Timeout)
}

/// Handle for one cluster's running reconnect supervisor task. `cancel` is polled by
/// the task between attempts; `abort` is the hard stop for a task parked in a sleep.
struct Supervisor {
    cancel: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
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
    /// Running reconnect supervisor for this cluster, if any (T062).
    supervisor: Mutex<Option<Supervisor>>,
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
            supervisor: Mutex::new(None),
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
            // R11: drop the mock snapshot slot too, so an orphan snapshot can never
            // resurrect stale state; its failure must not fail the delete (warn).
            if let Err(err) = store.clear_mock_snapshot(&id) {
                tracing::warn!(cluster = %id, %err, "failed to clear mock snapshot on delete");
            }
            Ok(removed)
        })
        .await?;
        Ok(removed)
    }

    // ---- connections -----------------------------------------------------

    /// Connects (or returns the existing connection). Serialized per cluster: concurrent
    /// callers share one driver instance and one pair of state events. A manual connect
    /// also supersedes any running reconnect supervisor (T062).
    pub async fn connect(&self, id: &ClusterId) -> Result<ClusterInfo, ServiceError> {
        self.cancel_supervisor(id);
        self.connect_inner(id).await
    }

    /// The connect machinery itself, shared with the reconnect supervisor so its
    /// attempts do not cancel the very task running them.
    async fn connect_inner(&self, id: &ClusterId) -> Result<ClusterInfo, ServiceError> {
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
    /// connected still ends up reported as `disconnected`. A manual disconnect (and the
    /// delete/shutdown paths that run it) cancels the cluster's reconnect supervisor.
    pub async fn disconnect(&self, id: &ClusterId) -> Result<(), ServiceError> {
        self.cancel_supervisor(id);
        self.disconnect_inner(id).await
    }

    /// The disconnect machinery itself, shared with the reconnect supervisor.
    async fn disconnect_inner(&self, id: &ClusterId) -> Result<(), ServiceError> {
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

    /// `cluster.info` — the live view of a connected cluster. A connection-class
    /// failure here degrades the cluster and starts its reconnect supervisor (T062).
    pub async fn info(&self, id: &ClusterId) -> Result<ClusterInfo, ServiceError> {
        let driver = self.driver(id)?;
        match driver.cluster_info().await {
            Ok(info) => Ok(info),
            Err(err) => {
                self.report_driver_error(id, &err);
                Err(err.into())
            }
        }
    }

    // ---- reconnect supervision (T062, ruling R28) --------------------------

    /// Marks the cluster `degraded` (one `connection.state` event with the reason) and
    /// starts its reconnect supervisor when a runtime driver call failed with a
    /// connection-class error. Other services' call sites can report their driver
    /// failures through this same seam. Returns whether supervision started.
    pub fn report_driver_error(&self, id: &ClusterId, err: &DriverError) -> bool {
        if !is_connection_class(err.kind()) {
            return false;
        }
        self.start_supervisor(id, err.to_string())
    }

    fn cancel_supervisor(&self, id: &ClusterId) {
        if let Some(slot) = self.peek_slot(id) {
            Self::cancel_slot(&slot);
        }
    }

    fn cancel_slot(slot: &Arc<Slot>) {
        if let Ok(mut guard) = slot.supervisor.lock() {
            if let Some(supervisor) = guard.take() {
                supervisor.cancel.store(true, Ordering::SeqCst);
                supervisor.task.abort();
            }
        }
    }

    /// Spawns the supervisor task unless one is already running, the cluster is not
    /// live, or its slot entry was cancelled while we raced with a manual disconnect.
    fn start_supervisor(&self, id: &ClusterId, reason: String) -> bool {
        let slot = self.slot_of(id);
        let mut guard = match slot.supervisor.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if guard.is_some() || !matches!(slot.state_of(), ConnectionState::Connected | ConnectionState::Degraded) {
            return false;
        }
        // Health degrades; the live driver instance stays in its slot.
        if let Ok(mut state) = slot.state.lock() {
            *state = ConnectionState::Degraded;
        }
        self.publish_state(id, ConnectionState::Degraded, Some(reason));
        let cancel = Arc::new(AtomicBool::new(false));
        let task = {
            let svc = self.clone();
            let slot = Arc::clone(&slot);
            let flag = Arc::clone(&cancel);
            let id = id.clone();
            tokio::spawn(async move { svc.reconnect_loop(id, slot, flag).await; })
        };
        guard.replace(Supervisor { cancel, task });
        true
    }

    /// One cluster's reconnect loop (R28): driver teardown + fresh connect, retrying on
    /// the backoff table. `connect_inner` publishes `connecting`/`connected`/`error`,
    /// so the UI sees the whole sequence without new wire events. A success ends the
    /// task (which resets the table for the next failure); a manual disconnect/delete
    /// cancels it through `cancel`, and a config that can never connect gives up.
    async fn reconnect_loop(self, id: ClusterId, slot: Arc<Slot>, cancel: Arc<AtomicBool>) {
        let mut attempt = 0usize;
        let mut needs_teardown = true;
        loop {
            if cancel.load(Ordering::SeqCst) {
                break;
            }
            if needs_teardown {
                // Best effort: a failed teardown still proceeds to the fresh connect.
                if let Err(err) = self.disconnect_inner(&id).await {
                    tracing::warn!(cluster = %id, %err, "reconnect: driver teardown failed");
                }
                needs_teardown = false;
            }
            match self.connect_inner(&id).await {
                Ok(_) => break,
                Err(err) => {
                    tracing::warn!(cluster = %id, %err, "reconnect attempt failed");
                    // Deleted or invalid configs will never connect: stop trying.
                    if matches!(err.kind(), ErrorKind::NotFound | ErrorKind::Storage | ErrorKind::InvalidConfig) {
                        break;
                    }
                    if cancel.load(Ordering::SeqCst) {
                        break;
                    }
                    let wait = reconnect_backoff(attempt);
                    attempt += 1;
                    tokio::time::sleep(wait).await;
                }
            }
        }
        // Drop our registry entry (if it is still ours) so a later failure can spawn
        // a fresh supervisor from the top of the backoff table.
        if let Ok(mut guard) = slot.supervisor.lock() {
            if guard.as_ref().is_some_and(|sup| Arc::ptr_eq(&sup.cancel, &cancel)) {
                guard.take();
            }
        }
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
        // Nested if-let, not a let-chain: let-chains need rustc ≥1.88 and the
        // workspace declares rust-version = "1.85".
        if let Some(json) = snapshot {
            if let Err(err) = mock.restore_json(&json).await {
                tracing::warn!(cluster = %id, %err, "mock snapshot restore failed, connecting empty");
            }
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
