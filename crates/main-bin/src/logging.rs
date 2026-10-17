//! T080: the process logging stack.
//!
//! `tracing` output keeps going to stderr exactly as before, and every event the
//! active filter lets through is also published on the application bus as
//! [`AppEvent::LogLine`], which the event pump (T037) forwards to the WebView and
//! the log panel (T049b) renders.
//!
//! T083a adds a third sink: the same events also land in
//! `<data_dir>/logs/app.log`, rotated by size ([`FileWriter`]) so a week-long
//! session cannot fill the disk.
//!
//! The subscriber must be installed on the first line of `main`, but the bus does
//! not exist yet — it is created with the `App` inside `glue::Assembly`. The layer
//! therefore publishes through a slot that [`LogBridge::attach`] fills once the
//! App is up (R73); anything logged in that gap only reaches stderr and the file.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;
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

/// Where the log file lives inside the data directory (R89: not a setting).
const LOG_DIR: &str = "logs";
const LOG_FILE: &str = "app.log";
/// Rotate once the live file would grow past this; `KEEP` rolled files stay.
const MAX_BYTES: u64 = 1 << 20;
const KEEP: usize = 3;

/// `<data_dir>/logs/app.log` plus its rotations, as a `tracing` sink.
///
/// One record is buffered and written on drop, so a rotation can never split a
/// line (item 1). Every filesystem error is swallowed (R88): if the log cannot be
/// written, the app keeps running on stderr alone.
#[derive(Clone)]
struct FileWriter {
    state: Arc<Mutex<RolledFile>>,
}

impl FileWriter {
    fn new(live: PathBuf, max_bytes: u64, keep: usize) -> Self {
        let mut file = RolledFile { live, max_bytes, keep, handle: None, written: 0 };
        file.handle = file.open();
        Self { state: Arc::new(Mutex::new(file)) }
    }
}

impl<'a> MakeWriter<'a> for FileWriter {
    type Writer = LineWriter<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        LineWriter { sink: self, line: Vec::new() }
    }
}

/// Buffers one formatted record; the actual write happens on drop.
struct LineWriter<'a> {
    sink: &'a FileWriter,
    line: Vec<u8>,
}

impl Write for LineWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.line.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for LineWriter<'_> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.sink.state.lock() {
            state.append(&self.line);
        }
    }
}

/// The live file plus the size bookkeeping rotation needs.
struct RolledFile {
    live: PathBuf,
    max_bytes: u64,
    keep: usize,
    handle: Option<File>,
    written: u64,
}

impl RolledFile {
    /// Append one whole line, rolling first if it would not fit. Never fails out.
    fn append(&mut self, line: &[u8]) {
        if line.is_empty() {
            return;
        }
        if self.handle.is_none() {
            // Retry: the directory may have become writable since the last try.
            let reopened = self.open();
            self.handle = reopened;
        }
        if self.handle.is_none() {
            return;
        }
        // `written > 0` keeps one oversized line from rolling the file forever.
        if self.written > 0 && self.written + line.len() as u64 > self.max_bytes {
            self.roll();
        }
        if let Some(handle) = self.handle.as_mut() {
            if handle.write_all(line).is_ok() {
                self.written += line.len() as u64;
            } else {
                self.handle = None;
            }
        }
    }

    /// `app.log` → `app.log.1`, `.1` → `.2`, …; the one at `keep` is dropped.
    fn roll(&mut self) {
        if let Some(mut handle) = self.handle.take() {
            let _ = handle.flush();
        }
        let _ = std::fs::remove_file(roll_path(&self.live, self.keep));
        for index in (1..self.keep).rev() {
            let from = roll_path(&self.live, index);
            if from.exists() {
                let _ = std::fs::rename(&from, roll_path(&self.live, index + 1));
            }
        }
        let _ = std::fs::rename(&self.live, roll_path(&self.live, 1));
        self.written = 0;
        let reopened = self.open();
        self.handle = reopened;
    }

    /// Open in append mode, creating the directory on the way; `None` on any
    /// failure, which turns this sink into a no-op (R88).
    fn open(&self) -> Option<File> {
        if let Some(dir) = self.live.parent() {
            std::fs::create_dir_all(dir).ok()?;
        }
        OpenOptions::new().create(true).append(true).open(&self.live).ok()
    }
}

/// `app.log` + index → `app.log.1`; via `OsString` so non-UTF-8 paths survive.
fn roll_path(live: &Path, index: usize) -> PathBuf {
    let mut name = live.as_os_str().to_os_string();
    name.push(format!(".{index}"));
    PathBuf::from(name)
}

/// Installs the process subscriber: `RUST_LOG` (default `info`) to stderr as
/// before, plus the bus layer and the rotating file under the data directory.
/// Returns the handle `main` attaches the App's bus to.
pub fn install() -> LogBridge {
    let bridge = LogBridge::default();
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let live = crate::glue::data_dir().join(LOG_DIR).join(LOG_FILE);
    let file = FileWriter::new(live, MAX_BYTES, KEEP);
    tracing_subscriber::registry()
        .with(filter)
        .with(BusLogs { bus: Arc::clone(&bridge.bus) })
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::fmt::layer().with_ansi(false).with_writer(file))
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

    /// T083a: a scratch directory of its own, so rotation tests cannot collide.
    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("rs-kafka-ui-log-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        dir
    }

    /// Feed the writer the way the subscriber does: one buffered record per call.
    fn write_line(writer: &FileWriter, line: &str) {
        let mut guard = writer.make_writer();
        guard.write_all(line.as_bytes()).expect("buffered write");
        drop(guard);
    }

    fn read(path: &Path) -> String {
        std::fs::read_to_string(path).unwrap_or_default()
    }

    #[test]
    fn every_line_lands_in_the_live_file() {
        let dir = scratch("live");
        let writer = FileWriter::new(dir.join(LOG_FILE), 1 << 20, KEEP);

        write_line(&writer, "first\n");
        write_line(&writer, "second\n");

        assert_eq!(read(&dir.join(LOG_FILE)), "first\nsecond\n");
        assert!(!dir.join("app.log.1").exists(), "a small file must not roll");
    }

    #[test]
    fn the_file_rolls_and_keeps_only_the_newest_copies() {
        let dir = scratch("roll");
        // Room for one 20-byte line per file, two rolled files kept.
        let writer = FileWriter::new(dir.join(LOG_FILE), 32, 2);

        for index in 0..8 {
            write_line(&writer, &format!("line-{index}-padpadpadpad\n"));
        }

        assert!(dir.join("app.log.1").exists(), "the live file must have rolled");
        assert!(dir.join("app.log.2").exists(), "the older copy shifts to .2");
        assert!(!dir.join("app.log.3").exists(), "keep=2 drops the oldest");
        let live = read(&dir.join(LOG_FILE));
        assert!(live.contains("line-7"), "the newest line is live: {live}");
        assert!(!live.contains("line-1"), "an old line is not live: {live}");
    }

    #[test]
    fn an_unusable_directory_degrades_quietly() {
        let dir = scratch("blocked");
        // A regular file where the log's parent directory would have to be.
        let blocker = dir.join("blocker");
        std::fs::write(&blocker, b"not a directory").expect("blocker");

        write_line(&FileWriter::new(blocker.join(LOG_FILE), 32, KEEP), "nowhere to go\n");

        assert!(!blocker.join(LOG_FILE).exists(), "no file, no panic, no error");
    }

    #[test]
    fn one_oversized_line_is_written_before_rolling() {
        let dir = scratch("oversized");
        let writer = FileWriter::new(dir.join(LOG_FILE), 8, KEEP);

        write_line(&writer, &format!("{}\n", "x".repeat(64)));
        write_line(&writer, "short\n");

        assert!(read(&dir.join(LOG_FILE)).contains("short"), "the short line rolled first");
        assert!(read(&dir.join("app.log.1")).contains("xxxx"), "the long line is not lost");
    }
}
