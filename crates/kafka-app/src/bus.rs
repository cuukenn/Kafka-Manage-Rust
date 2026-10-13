//! Global application event bus.
//!
//! One `broadcast` fan-out point: services publish [`AppEvent`]s, the WebView bridge
//! (phase 3) drains a subscription and forwards each event to the frontend as
//! `EventEnvelope { event, payload, ts }`.
//!
//! Deliberately free of throttling and batching — those belong to the consume pipeline
//! (T060/T061), not to the bus itself.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;
use tokio::sync::broadcast::{self, Receiver, error::RecvError, error::TryRecvError};

use kafka_core::cluster::ConnectionState;
use kafka_core::message::{MessageEvent, ProduceResult, SubscriptionId};
use kafka_core::protocol::{ErrorDto, event};
use kafka_driver::DriverEvent;

/// Capacity used by [`EventBus::default`]. Sized for a burst of consumed messages.
pub const DEFAULT_BUS_CAPACITY: usize = 1024;

/// Floor for the broadcast capacity: a tiny channel makes the UI lag on every click.
const MIN_CAPACITY: usize = 16;

/// Lifecycle of one active subscription, as shown to the UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionStatus {
    #[default]
    Running,
    Paused,
    Done,
}

/// Why a `topic.changed` event was emitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TopicChange {
    Created,
    Deleted,
    ConfigChanged,
    /// Messages were appended: the UI should refresh offsets / lag.
    Messages,
}

/// Everything the frontend may be told about.
///
/// Internally tagged so a raw bus dump is self-describing (`{"type":..,"payload":..}`);
/// the bridge uses [`AppEvent::event_name`] for the wire event name and
/// [`AppEvent::payload_json`] for the payload body.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", content = "payload")]
pub enum AppEvent {
    #[serde(rename = "connection.state", rename_all = "camelCase")]
    ConnectionStateChanged {
        cluster_id: String,
        state: ConnectionState,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    #[serde(rename = "message.received", rename_all = "camelCase")]
    MessageReceived {
        subscription_id: SubscriptionId,
        message: MessageEvent,
    },
    #[serde(rename = "subscription.state", rename_all = "camelCase")]
    SubscriptionState {
        subscription_id: SubscriptionId,
        state: SubscriptionStatus,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    #[serde(rename = "subscription.done", rename_all = "camelCase")]
    SubscriptionDone {
        subscription_id: SubscriptionId,
        delivered: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    #[serde(rename = "topic.changed", rename_all = "camelCase")]
    TopicChanged {
        cluster_id: String,
        topic: String,
        change: TopicChange,
    },
    #[serde(rename = "group.changed", rename_all = "camelCase")]
    GroupChanged {
        cluster_id: String,
        group_id: String,
        partitions: usize,
    },
    #[serde(rename = "produce.result", rename_all = "camelCase")]
    ProduceResult {
        cluster_id: String,
        results: Vec<ProduceResult>,
        ok_count: usize,
        error_count: usize,
    },
    #[serde(rename = "log.line", rename_all = "camelCase")]
    LogLine {
        level: String,
        target: String,
        message: String,
    },
    #[serde(rename = "error", rename_all = "camelCase")]
    Error {
        error: ErrorDto,
        #[serde(skip_serializing_if = "Option::is_none")]
        correlation_id: Option<String>,
    },
}

impl AppEvent {
    /// Wire event name, taken from the shared constants so Rust and JS cannot drift.
    pub fn event_name(&self) -> &'static str {
        match self {
            Self::ConnectionStateChanged { .. } => event::CONNECTION_STATE,
            Self::MessageReceived { .. } => event::MESSAGE_RECEIVED,
            Self::SubscriptionState { .. } => event::SUBSCRIPTION_STATE,
            Self::SubscriptionDone { .. } => event::SUBSCRIPTION_DONE,
            Self::TopicChanged { .. } => event::TOPIC_CHANGED,
            Self::GroupChanged { .. } => event::GROUP_CHANGED,
            Self::ProduceResult { .. } => event::PRODUCE_RESULT,
            Self::LogLine { .. } => event::LOG_LINE,
            Self::Error { .. } => event::ERROR,
        }
    }

    /// The variant's fields alone (the `payload` half of the tagged enum).
    pub fn payload_json(&self) -> serde_json::Value {
        serde_json::to_value(self)
            .ok()
            .and_then(|v| v.get("payload").cloned())
            .unwrap_or(serde_json::Value::Null)
    }

    /// Translate a driver domain event into a bus event, tagging it with the cluster
    /// it came from.
    pub fn from_driver(cluster_id: &str, ev: DriverEvent) -> Self {
        let cluster_id = cluster_id.to_string();
        match ev {
            DriverEvent::TopicCreated { topic } => Self::TopicChanged { cluster_id, topic, change: TopicChange::Created },
            DriverEvent::TopicDeleted { topic } => Self::TopicChanged { cluster_id, topic, change: TopicChange::Deleted },
            DriverEvent::TopicConfigChanged { topic } => {
                Self::TopicChanged { cluster_id, topic, change: TopicChange::ConfigChanged }
            }
            DriverEvent::MessagesAppended { topic, .. } => {
                Self::TopicChanged { cluster_id, topic, change: TopicChange::Messages }
            }
            DriverEvent::GroupOffsetsReset { group, partitions } | DriverEvent::OffsetsCommitted { group, partitions } => {
                Self::GroupChanged { cluster_id, group_id: group, partitions }
            }
            DriverEvent::ConnectionLost { reason } => {
                Self::ConnectionStateChanged { cluster_id, state: ConnectionState::Error, reason: Some(reason) }
            }
        }
    }
}

/// `From` without cluster context; prefer [`AppEvent::from_driver`], which fills
/// `clusterId` (this leaves it empty).
impl From<DriverEvent> for AppEvent {
    fn from(ev: DriverEvent) -> Self {
        Self::from_driver("", ev)
    }
}

/// Why a receive attempt failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BusRecvError {
    /// Every producer has been dropped.
    Closed,
    /// Nothing queued yet (non-blocking [`EventBus`] poll only).
    Empty,
    /// This subscriber was too slow and lost `n` events.
    Lagged(u64),
}

/// Shared bus state behind [`EventBus`]. `Debug` is required because `EventBus` derives it and
/// holds `Arc<Inner>`.
#[derive(Debug)]
struct Inner {
    tx: broadcast::Sender<AppEvent>,
    /// Total events dropped because some subscriber fell behind.
    lag: Arc<AtomicU64>,
}

/// Cloneable handle to the global bus.
#[derive(Debug, Clone)]
pub struct EventBus {
    inner: Arc<Inner>,
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new(DEFAULT_BUS_CAPACITY)
    }
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                tx: broadcast::Sender::new(capacity.max(MIN_CAPACITY)),
                lag: Arc::new(AtomicU64::new(0)),
            }),
        }
    }

    /// Sends to every live subscriber; returns how many received it.
    /// Publishing with no subscribers is normal (returns 0, never panics).
    pub fn publish(&self, ev: AppEvent) -> usize {
        self.inner.tx.send(ev).unwrap_or(0)
    }

    pub fn subscribe(&self) -> BusReceiver {
        BusReceiver { rx: self.inner.tx.subscribe(), lag: Arc::clone(&self.inner.lag) }
    }

    pub fn subscriber_count(&self) -> usize {
        self.inner.tx.receiver_count()
    }

    /// Events lost to slow subscribers, across all subscriptions.
    pub fn lag_count(&self) -> u64 {
        self.inner.lag.load(Ordering::Relaxed)
    }
}

/// One subscription. Each consumer owns exactly one of these.
pub struct BusReceiver {
    rx: Receiver<AppEvent>,
    lag: Arc<AtomicU64>,
}

impl BusReceiver {
    pub async fn recv(&mut self) -> Result<AppEvent, BusRecvError> {
        match self.rx.recv().await {
            Ok(ev) => Ok(ev),
            Err(RecvError::Lagged(n)) => {
                self.lag.fetch_add(n, Ordering::Relaxed);
                Err(BusRecvError::Lagged(n))
            }
            Err(RecvError::Closed) => Err(BusRecvError::Closed),
        }
    }

    pub fn try_recv(&mut self) -> Result<AppEvent, BusRecvError> {
        match self.rx.try_recv() {
            Ok(ev) => Ok(ev),
            Err(TryRecvError::Lagged(n)) => {
                self.lag.fetch_add(n, Ordering::Relaxed);
                Err(BusRecvError::Lagged(n))
            }
            Err(TryRecvError::Closed) => Err(BusRecvError::Closed),
            Err(TryRecvError::Empty) => Err(BusRecvError::Empty),
        }
    }
}
