//! T080: the process logging stack.
//!
//! `tracing` output keeps going to stderr exactly as before, and every event the
//! active filter lets through is also published on the application bus as
//! [`AppEvent::LogLine`], which the event pump (T037) forwards to the WebView and
//! the log panel (T049b) renders.
//!
//! The subscriber must be installed on the first line of `main`, but the bus does
//! not exist yet — it is created with the `App` inside `glue::Assembly`. The layer
//! therefore publishes through a slot that [`LogBridge::attach`] fills once the
//! App is up (R73); anything logged in that gap only reaches stderr.

use std::fmt;
use std::sync::{Arc, OnceLock};

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

use kafka_app::bus::{AppEvent, EventBus};

/// Where the layer publishes; empty until [`LogBridge::attach`].
type Slot = Arc<OnceLock<EventBus>>;

/// Bridges `tracing` to the bus: one `log.line` per accepted event.
struct BusLogs {
    bus: Slot,
}

impl<S: Subscriber> Layer<S> for BusLogs {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // Dropping events before `attach` is the documented boot gap (R73).
        let Some(bus) = self.bus.get() else {
            return;
        };
        let metadata = event.metadata();
        let mut fields = Fields::default();
        event.record(&mut fields);
        bus.publish(AppEvent::LogLine {
            level: level_name(*metadata.level()).to_string(),
            target: metadata.target().to_string(),
            message: fields.render(),
        });
    }
}

/// `tracing` levels are uppercase constants; the panel shows them lowercase.
fn level_name(level: Level) -> &'static str {
    if level == Level::ERROR {
        "error"
    } else if level == Level::WARN {
        "warn"
    } else if level == Level::INFO {
        "info"
    } else if level == Level::DEBUG {
        "debug"
    } else {
        "trace"
    }
}

/// One event's fields, flattened into the single string the wire shape has room
/// for (R74): the `message` field first, then every other field.
#[derive(Default)]
struct Fields {
    message: String,
    extra: Vec<String>,
}

impl Fields {
    fn render(&self) -> String {
        let mut parts = Vec::with_capacity(1 + self.extra.len());
        if !self.message.is_empty() {
            parts.push(self.message.clone());
        }
        parts.extend(self.extra.iter().cloned());
        parts.join(" ")
    }

    fn push(&mut self, field: &Field, text: String) {
        if field.name() == "message" && self.message.is_empty() {
            self.message = text;
        } else {
            self.extra.push(format!("{}={text}", field.name()));
        }
    }
}

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.push(field, format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.push(field, value.to_string());
    }
}

/// Handle to the installed subscriber. Cloneable; one per process.
#[derive(Clone, Default)]
pub struct LogBridge {
    bus: Slot,
}

impl LogBridge {
    /// Points the layer at the application bus — call it as soon as the `App`
    /// exists. Only the first call takes effect.
    pub fn attach(&self, bus: EventBus) {
        let _ = self.bus.set(bus);
    }
}

/// Installs the process subscriber: `RUST_LOG` (default `info`) to stderr as
/// before, plus the bus layer. Returns the handle `main` attaches the App's bus
/// to.
pub fn install() -> LogBridge {
    let bridge = LogBridge::default();
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(BusLogs { bus: Arc::clone(&bridge.bus) })
        .with(tracing_subscriber::fmt::layer())
        .init();
    bridge
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use webview_bridge::pump::{EventPump, EventSink};

    /// The bus layer alone (no stderr), pinned to an already-filled slot.
    fn subscriber(bus: &EventBus) -> impl Subscriber + Send + Sync + 'static {
        let slot: Slot = Arc::new(OnceLock::new());
        // A brand-new slot is always empty, so this always succeeds.
        let _ = slot.set(bus.clone());
        tracing_subscriber::registry().with(BusLogs { bus: slot })
    }

    fn lines(rx: &mut kafka_app::bus::BusReceiver) -> Vec<(String, String, String)> {
        let mut out = Vec::new();
        while let Ok(event) = rx.try_recv() {
            if let AppEvent::LogLine { level, target, message } = event {
                out.push((level, target, message));
            }
        }
        out
    }

    #[test]
    fn an_event_before_attach_is_dropped_quietly() {
        let slot: Slot = Arc::new(OnceLock::new());
        let subscriber = tracing_subscriber::registry().with(BusLogs { bus: slot });
        tracing::subscriber::with_default(subscriber, || tracing::info!("nobody is listening"));
    }

    #[test]
    fn one_event_becomes_one_log_line_with_its_fields() {
        let bus = EventBus::default();
        let mut rx = bus.subscribe();
        tracing::subscriber::with_default(subscriber(&bus), || {
            tracing::info!(target: "kafka_app::store", path = "/tmp/db", "local store opened");
        });

        let lines = lines(&mut rx);
        assert_eq!(lines.len(), 1, "got {lines:?}");
        let (level, target, message) = &lines[0];
        assert_eq!(level, "info");
        assert_eq!(target, "kafka_app::store");
        assert!(message.starts_with("local store opened"), "got {message}");
        assert!(message.contains("path="), "got {message}");
    }

    #[test]
    fn severity_is_lowercased() {
        let bus = EventBus::default();
        let mut rx = bus.subscribe();
        tracing::subscriber::with_default(subscriber(&bus), || {
            tracing::warn!("careful");
            tracing::error!("broken");
        });

        let levels: Vec<String> = lines(&mut rx).into_iter().map(|(level, ..)| level).collect();
        assert_eq!(levels, ["warn", "error"]);
    }

    #[tokio::test]
    async fn log_lines_reach_the_event_pump_sink() {
        #[derive(Default)]
        struct FakeSink(Mutex<Vec<String>>);

        impl EventSink for FakeSink {
            fn push_batch(&self, batch_json: &str) {
                self.0.lock().expect("sink lock").push(batch_json.to_string());
            }
        }

        let bus = EventBus::default();
        let sink = Arc::new(FakeSink::default());
        let pump = EventPump::spawn(&bus, sink.clone());
        tracing::subscriber::with_default(subscriber(&bus), || {
            tracing::info!(target: "glue", "router ready");
        });
        pump.shutdown().await;

        let batches = sink.0.lock().expect("sink lock").join("");
        assert!(batches.contains(r#""event":"log.line""#), "got {batches}");
        assert!(batches.contains("router ready"), "got {batches}");
    }
}
