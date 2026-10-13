//! The pump behind a mock subscription: a Tokio task that walks partition cursors,
//! honours pause/resume/seek/stop, applies the server-side filter, and pushes
//! messages into a bounded channel the UI stream reads from.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, RwLock};

use kafka_core::error::DriverError;
use kafka_core::message::{MessageEvent, SearchFilter, StreamCommand};

use super::state::{MockMessage, MockState};

/// Bounded so a fast producer cannot grow the queue without limit; `send().await`
/// applies real backpressure all the way to the pump.
const CHANNEL_DEPTH: usize = 256;
/// Upper bound on messages pulled from the log in one tick.
const MAX_PER_TICK: usize = 512;

#[derive(Debug, Clone)]
pub(crate) struct PumpOptions {
    pub topic: String,
    pub partitions: Vec<u32>,
    pub cursors: HashMap<u32, i64>,
    pub follow: bool,
    pub max_messages: Option<u64>,
    pub filter: Option<SearchFilter>,
    pub poll_ms: u64,
}

/// Handle for a running pump task.
///
/// Owns only the message receiver. It deliberately does **not** keep the
/// `JoinHandle`: `PumpHandle` is destructured and dropped at the end of
/// `MockKafkaDriver::subscribe` (`driver.rs:591-593`), so a `Drop` that aborted the
/// task would kill the stream before the caller ever received it. Dropping a
/// `JoinHandle` merely detaches the task, which is what the pump relies on: it
/// terminates by itself when the `StreamCommand` sender drops with the subscription
/// (`commands.recv()` → `None`) or when a send fails because the reader is gone.
/// The one case that does not terminate — `follow: true` with no new messages, which
/// never reaches a send and so never notices the closed channel — is tracked by T068.
pub(crate) struct PumpHandle {
    pub receiver: mpsc::Receiver<Result<MessageEvent, DriverError>>,
}

pub(crate) fn spawn(
    state: Arc<RwLock<MockState>>,
    options: PumpOptions,
    mut commands: mpsc::UnboundedReceiver<StreamCommand>,
) -> PumpHandle {
    let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);

    tokio::spawn(async move {
        let PumpOptions { topic, partitions, mut cursors, follow, max_messages, filter, poll_ms } = options;
        let mut paused = false;
        let mut sent: u64 = 0;

        let mut interval = tokio::time::interval(Duration::from_millis(poll_ms.max(1)));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            // Commands win over the tick so pause/stop take effect immediately.
            tokio::select! {
                biased;
                command = commands.recv() => match command {
                    None => break,
                    Some(StreamCommand::Stop) => break,
                    Some(StreamCommand::Pause) => paused = true,
                    Some(StreamCommand::Resume) => paused = false,
                    Some(StreamCommand::Seek { partition, offset, .. }) => {
                        if partitions.binary_search(&partition).is_ok() {
                            cursors.insert(partition, offset.max(0));
                        }
                    }
                },
                _ = interval.tick() => {}
            }
            if paused {
                continue;
            }

            // One read-lock pass, then release the lock before awaiting sends.
            let batch: Vec<(u32, i64, MockMessage)> = {
                let state = state.read().await;
                let Some(t) = state.topics.get(&topic) else {
                    let _ = tx.send(Err(DriverError::TopicNotFound(topic.clone()))).await;
                    return;
                };
                let mut out = Vec::new();
                for &pid in &partitions {
                    let Some(partition) = t.find_partition(pid) else { continue };
                    let end = partition.end_offset();
                    let mut cursor = cursors.get(&pid).copied().unwrap_or(0);
                    while cursor < end && out.len() < MAX_PER_TICK {
                        if let Some(message) = partition.messages.get(cursor as usize) {
                            out.push((pid, cursor, message.clone()));
                        }
                        cursor += 1;
                    }
                    // Tentative; only committed once the message is handed over.
                    cursors.insert(pid, cursor);
                }
                out
            };

            if batch.is_empty() {
                if !follow {
                    break;
                }
                continue;
            }

            for (partition, offset, message) in batch {
                let event = MessageEvent {
                    topic: topic.clone(),
                    partition,
                    offset,
                    key: message.key,
                    value: message.value,
                    headers: message.headers,
                    timestamp: message.timestamp,
                    timestamp_type: message.timestamp_type,
                    leader_epoch: None,
                };
                if filter.as_ref().is_some_and(|f| !f.matches(&event)) {
                    continue;
                }
                if tx.send(Ok(event)).await.is_err() {
                    // Reader gone (window closed / subscription dropped).
                    return;
                }
                sent += 1;
                if let Some(max) = max_messages {
                    if sent >= max {
                        return;
                    }
                }
            }
        }
    });

    PumpHandle { receiver: rx }
}
