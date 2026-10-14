//! Transport-agnostic event pump: one bus subscription -> batched [`EventSink`].
//!
//! Wire contract (agents.md): a batch is a JSON array of `EventEnvelope` and it is
//! flushed **immediately once it holds 200 events**, otherwise **50ms after its first
//! event**. The sink is a trait so this layer stays free of wry/tao — T038 supplies the
//! real `wry::evaluate` implementation, tests use a fake one.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use kafka_app::bus::{AppEvent, BusReceiver, BusRecvError, EventBus};
use kafka_core::protocol::{EventEnvelope, now_ms};

/// Hard cap on how many events one batch may hold before it is flushed early.
pub const MAX_BATCH_EVENTS: usize = 200;

/// How long the oldest pending event may wait before its batch is flushed.
pub const MAX_BATCH_DELAY: Duration = Duration::from_millis(50);

/// Event outlet. Implementations must not block for long: the pump calls this inline,
/// and the wry implementation only posts the script (it never waits for JS).
pub trait EventSink: Send + Sync + 'static {
    fn push_batch(&self, batch_json: &str);
}

/// Entry point; stateless, all state lives in the spawned task.
pub struct EventPump;

/// Live pump: its task handle plus the graceful-stop signal.
pub struct PumpHandle {
    task: JoinHandle<()>,
    stop: oneshot::Sender<()>,
}

impl PumpHandle {
    /// Stop receiving, flush whatever is still queued or pending, then await the task.
    pub async fn shutdown(self) {
        let Self { task, stop } = self;
        // Sending first makes the intent explicit; a dropped channel is treated the
        // same way, so leaking the handle still ends the task.
        let _ = stop.send(());
        if let Err(err) = task.await {
            tracing::warn!("event pump task ended abnormally: {err}");
        }
    }
}

impl EventPump {
    /// Subscribe to `bus` and pump events into `sink` on a Tokio task.
    pub fn spawn(bus: &EventBus, sink: Arc<dyn EventSink>) -> PumpHandle {
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(run(bus.subscribe(), sink, stopped));
        PumpHandle { task, stop }
    }
}

fn envelope(ev: &AppEvent) -> EventEnvelope {
    EventEnvelope {
        event: ev.event_name().to_string(),
        payload: Some(ev.payload_json()),
        ts: Some(now_ms()),
    }
}

fn flush(batch: &mut Vec<EventEnvelope>, sink: &dyn EventSink) {
    if batch.is_empty() {
        return;
    }
    let json = serde_json::to_string(batch.as_slice());
    batch.clear();
    match json {
        Ok(text) => sink.push_batch(&text),
        Err(err) => tracing::warn!("event batch could not be serialized: {err}"),
    }
}

/// Take everything already queued (used on graceful stop so nothing in flight is lost).
fn drain(rx: &mut BusReceiver, batch: &mut Vec<EventEnvelope>, sink: &dyn EventSink) {
    loop {
        match rx.try_recv() {
            Ok(ev) => {
                batch.push(envelope(&ev));
                if batch.len() >= MAX_BATCH_EVENTS {
                    flush(batch, sink);
                }
            }
            Err(BusRecvError::Empty) => return,
            Err(BusRecvError::Lagged(n)) => tracing::warn!("event pump lagged, {n} events dropped"),
            Err(BusRecvError::Closed) => return,
        }
    }
}

/// Waits for the pending batch's deadline; never resolves while nothing is pending.
async fn wait_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending::<()>().await,
    }
}

async fn run(mut rx: BusReceiver, sink: Arc<dyn EventSink>, mut stopped: oneshot::Receiver<()>) {
    let mut batch: Vec<EventEnvelope> = Vec::with_capacity(MAX_BATCH_EVENTS);
    let mut deadline: Option<tokio::time::Instant> = None;

    loop {
        tokio::select! {
            biased;
            // Stop requested: keep what is already queued, then exit via the flush below.
            _ = &mut stopped => {
                drain(&mut rx, &mut batch, sink.as_ref());
                break;
            }
            _ = wait_deadline(deadline) => {
                flush(&mut batch, sink.as_ref());
                deadline = None;
            }
            got = rx.recv() => {
                match got {
                    Ok(ev) => {
                        if batch.is_empty() {
                            deadline = Some(tokio::time::Instant::now() + MAX_BATCH_DELAY);
                        }
                        batch.push(envelope(&ev));
                        if batch.len() >= MAX_BATCH_EVENTS {
                            flush(&mut batch, sink.as_ref());
                            deadline = None;
                        }
                    }
                    Err(BusRecvError::Lagged(n)) => {
                        tracing::warn!("event pump lagged, {n} events dropped");
                    }
                    // Closed only after the buffer drained: flush below and exit.
                    Err(BusRecvError::Closed) => break,
                    // recv() never reports Empty; kept so the match stays exhaustive.
                    Err(BusRecvError::Empty) => {}
                }
            }
        }
    }

    flush(&mut batch, sink.as_ref());
}
