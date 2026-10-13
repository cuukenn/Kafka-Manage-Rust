//! Registry of live subscriptions: start / pause / resume / seek / stop.
//!
//! One task per subscription pulls the driver's [`MessageStream`] and republishes every
//! message as [`AppEvent::MessageReceived`]. Control commands go straight to the driver's
//! [`StreamControls`] — the driver (mock pump or rdkafka loop) is what actually pauses the
//! poll loop, so the service never buffers messages itself.
//!
//! Throttling, back-pressure and reconnects are deliberately absent here (T060/T061/T062).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

use futures::StreamExt;
use tokio::task::JoinHandle;

use kafka_core::ClusterId;
use kafka_core::error::DriverError;
use kafka_core::message::{StreamCommand, StreamControls, SubscribeReq, SubscriptionId};
use kafka_core::protocol::ErrorDto;

use super::ServiceError;
use super::clusters::ClusterService;
use crate::bus::{AppEvent, EventBus, SubscriptionStatus};

/// Why a subscription stopped delivering.
const STOPPED_BY_CLIENT: &str = "stopped by client";

/// What the UI shows for one subscription.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionInfo {
    pub subscription_id: SubscriptionId,
    pub cluster_id: ClusterId,
    pub topic: String,
    /// Requested partitions; empty means "all partitions of the topic".
    pub partitions: Vec<u32>,
    pub state: SubscriptionStatus,
    pub delivered: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone)]
struct SubState {
    status: SubscriptionStatus,
    reason: Option<String>,
}

#[derive(Debug)]
struct SubHandle {
    cluster_id: ClusterId,
    topic: String,
    partitions: Vec<u32>,
    controls: StreamControls,
    state: Arc<Mutex<SubState>>,
    delivered: Arc<AtomicU64>,
    /// Flipped exactly once, by whoever publishes `subscription.done`.
    finished: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

/// Owns the live subscriptions of the whole app.
#[derive(Debug)]
pub struct ConsumeService {
    clusters: ClusterService,
    subs: Arc<Mutex<HashMap<SubscriptionId, SubHandle>>>,
}

impl Clone for ConsumeService {
    fn clone(&self) -> Self {
        Self {
            clusters: self.clusters.clone(),
            subs: Arc::clone(&self.subs),
        }
    }
}

impl ConsumeService {
    pub fn new(clusters: ClusterService) -> Self {
        Self {
            clusters,
            subs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn clusters(&self) -> &ClusterService {
        &self.clusters
    }

    pub fn bus(&self) -> &EventBus {
        self.clusters.bus()
    }

    /// Opens one subscription and starts forwarding its messages to the bus.
    pub async fn start(
        &self,
        cluster_id: &ClusterId,
        req: SubscribeReq,
    ) -> Result<SubscriptionInfo, ServiceError> {
        req.validate()?;
        let id = req.subscription_id.clone();
        if self.subs().contains_key(&id) {
            return Err(DriverError::invalid(
                "subscriptionId",
                format!("subscription {id} is already active"),
            )
            .into());
        }

        let driver = self.clusters.driver(cluster_id)?;
        let stream = driver.subscribe(req.clone()).await?;
        let controls = stream.controls();

        let state = Arc::new(Mutex::new(SubState {
            status: SubscriptionStatus::Running,
            reason: None,
        }));
        let delivered = Arc::new(AtomicU64::new(0));
        let finished = Arc::new(AtomicBool::new(false));

        let task = {
            let bus = self.bus().clone();
            let task_id = id.clone();
            let state = Arc::clone(&state);
            let delivered = Arc::clone(&delivered);
            let finished = Arc::clone(&finished);
            let mut messages = stream.into_stream();

            tokio::spawn(async move {
                let mut end_error: Option<ErrorDto> = None;
                while let Some(item) = messages.next().await {
                    match item {
                        Ok(message) => {
                            delivered.fetch_add(1, Ordering::Relaxed);
                            bus.publish(AppEvent::MessageReceived {
                                subscription_id: task_id.clone(),
                                message,
                            });
                        }
                        Err(err) => {
                            end_error = Some(ErrorDto::from_driver(&err));
                            break;
                        }
                    }
                }

                let reason = end_error.as_ref().map(|e| e.message.clone());
                set_state(&state, SubscriptionStatus::Done, reason.clone());
                if !finished.swap(true, Ordering::SeqCst) {
                    if let Some(err) = end_error {
                        bus.publish(AppEvent::Error {
                            error: err,
                            correlation_id: None,
                        });
                    }
                    bus.publish(AppEvent::SubscriptionDone {
                        subscription_id: task_id,
                        delivered: delivered.load(Ordering::Relaxed),
                        reason,
                    });
                }
            })
        };

        let info = SubscriptionInfo {
            subscription_id: id.clone(),
            cluster_id: cluster_id.clone(),
            topic: req.topic.clone(),
            partitions: req.partitions.clone(),
            state: SubscriptionStatus::Running,
            delivered: 0,
            reason: None,
        };

        self.subs().insert(
            id,
            SubHandle {
                cluster_id: cluster_id.clone(),
                topic: req.topic.clone(),
                partitions: req.partitions.clone(),
                controls,
                state,
                delivered,
                finished,
                task,
            },
        );
        Ok(info)
    }

    /// Stops one subscription. Idempotent: a second call returns `false`, never an error.
    pub async fn stop(&self, subscription_id: &SubscriptionId) -> Result<bool, ServiceError> {
        let handle = self.subs().remove(subscription_id);
        let Some(handle) = handle else {
            return Ok(false);
        };

        let SubHandle {
            controls,
            state,
            delivered,
            finished,
            task,
            ..
        } = handle;

        // Best effort: the pump may already be gone, which is exactly what we want.
        let _ = controls.send(StreamCommand::Stop);
        if !finished.swap(true, Ordering::SeqCst) {
            set_state(&state, SubscriptionStatus::Done, Some(STOPPED_BY_CLIENT.to_string()));
            self.bus().publish(AppEvent::SubscriptionDone {
                subscription_id: subscription_id.clone(),
                delivered: delivered.load(Ordering::Relaxed),
                reason: Some(STOPPED_BY_CLIENT.to_string()),
            });
        }
        task.abort();
        Ok(true)
    }

    pub fn pause(&self, subscription_id: &SubscriptionId) -> Result<(), ServiceError> {
        self.steer(
            subscription_id,
            StreamCommand::Pause,
            SubscriptionStatus::Paused,
        )
    }

    pub fn resume(&self, subscription_id: &SubscriptionId) -> Result<(), ServiceError> {
        self.steer(
            subscription_id,
            StreamCommand::Resume,
            SubscriptionStatus::Running,
        )
    }

    /// Repositions one partition cursor. No event: the next message tells the whole story.
    pub fn seek(
        &self,
        subscription_id: &SubscriptionId,
        topic: impl Into<String>,
        partition: u32,
        offset: i64,
    ) -> Result<(), ServiceError> {
        let topic = topic.into();
        let subs = self.subs();
        let handle = self.running_handle(&subs, subscription_id)?;
        handle
            .controls
            .send(StreamCommand::Seek {
                topic,
                partition,
                offset,
            })?;
        Ok(())
    }

    /// Every known subscription, including finished ones, sorted by id.
    pub fn list(&self) -> Vec<SubscriptionInfo> {
        let mut out = self
            .subs()
            .iter()
            .map(|(id, handle)| SubscriptionInfo {
                subscription_id: id.clone(),
                cluster_id: handle.cluster_id.clone(),
                topic: handle.topic.clone(),
                partitions: handle.partitions.clone(),
                state: state_of(&handle.state),
                delivered: handle.delivered.load(Ordering::Relaxed),
                reason: handle.state.lock().ok().and_then(|s| s.reason.clone()),
            })
            .collect::<Vec<_>>();
        out.sort_by(|a, b| a.subscription_id.cmp(&b.subscription_id));
        out
    }

    pub fn state_of(&self, subscription_id: &SubscriptionId) -> Option<SubscriptionStatus> {
        self.subs().get(subscription_id).map(|h| state_of(&h.state))
    }

    pub fn active_count(&self) -> usize {
        self.subs()
            .values()
            .filter(|h| !h.finished.load(Ordering::Relaxed))
            .count()
    }

    pub fn total_count(&self) -> usize {
        self.subs().len()
    }

    /// Drops finished entries so the registry does not grow without bound.
    pub fn prune_done(&self) -> usize {
        let before = self.subs().len();
        self.subs()
            .retain(|_, handle| !handle.finished.load(Ordering::Relaxed));
        before - self.subs().len()
    }

    /// Stops every subscription. Used on shutdown and on cluster disconnect (T030).
    pub async fn stop_all(&self) -> usize {
        let ids = self
            .subs()
            .keys()
            .cloned()
            .collect::<Vec<SubscriptionId>>();
        let mut stopped = 0usize;
        for id in ids {
            if self.stop(&id).await.unwrap_or(false) {
                stopped += 1;
            }
        }
        stopped
    }

    fn steer(
        &self,
        subscription_id: &SubscriptionId,
        command: StreamCommand,
        want: SubscriptionStatus,
    ) -> Result<(), ServiceError> {
        let subs = self.subs();
        let handle = self.running_handle(&subs, subscription_id)?;
        handle.controls.send(command)?;
        set_state(&handle.state, want, None);
        self.bus().publish(AppEvent::SubscriptionState {
            subscription_id: subscription_id.clone(),
            state: want,
            reason: None,
        });
        Ok(())
    }

    /// Looks up a subscription that is still delivering. The caller must hold the registry lock.
    fn running_handle<'a>(
        &self,
        subs: &'a HashMap<SubscriptionId, SubHandle>,
        subscription_id: &SubscriptionId,
    ) -> Result<&'a SubHandle, ServiceError> {
        let handle = subs.get(subscription_id).ok_or_else(|| {
            DriverError::other(format!("subscription {subscription_id} is not registered"))
        })?;
        if handle.finished.load(Ordering::Relaxed) {
            return Err(DriverError::Cancelled.into());
        }
        Ok(handle)
    }

    fn subs(&self) -> MutexGuard<'_, HashMap<SubscriptionId, SubHandle>> {
        self.subs.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn state_of(state: &Mutex<SubState>) -> SubscriptionStatus {
    state
        .lock()
        .map(|current| current.status)
        .unwrap_or(SubscriptionStatus::Done)
}

fn set_state(state: &Mutex<SubState>, status: SubscriptionStatus, reason: Option<String>) {
    match state.lock() {
        Ok(mut current) => {
            current.status = status;
            current.reason = reason;
        }
        Err(poisoned) => {
            let mut current = poisoned.into_inner();
            current.status = status;
            current.reason = reason;
        }
    }
}
