//! tao + wry shell window: event loop, WebView, and static asset serving.
//!
//! Two asset sources (T031): `Embedded` (compile-time `frontend/dist` via
//! include_dir) and `DevDir` (a disk directory override). Assets are served to
//! the page over a custom `app://` protocol. Pure logic (content-type table,
//! path sanitising, asset reads) is separated from window creation so it can
//! be unit-tested without a real window.
//!
//! This module must NOT depend on kafka-app/kafka-driver types. It owns the
//! window-side half of the bridge (T038b): the `UserEvent`/`RpcHandler`/
//! `BridgeGuard` hooks, the `WrySink` and the `POST app://localhost/rpc` route.
//! What the hooks are wired *to* (App, Router, EventPump) lives in main-bin.

use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use include_dir::{Dir, include_dir};
use tao::dpi::LogicalSize;
use tao::event::{Event, WindowEvent};
use tao::event_loop::{ControlFlow, EventLoop, EventLoopBuilder, EventLoopProxy};
use tao::platform::run_return::EventLoopExtRunReturn;
use tao::window::{Window, WindowBuilder};
use wry::WebViewBuilder;
use wry::http::Request;
use wry::http::Response;
use wry::http::Method;
use wry::http::header::CONTENT_TYPE;

use crate::pump::EventSink;

/// Env var read by main-bin: when set, assets are served from this directory.
pub const DEV_DIR_ENV: &str = "KAFKA_UI_DEV_DIR";

/// Custom protocol scheme the page and its assets are served under.
pub const APP_SCHEME: &str = "app";

/// Compile-time embedded placeholder page (replaced by the Vue build in phase 4).
static EMBEDDED: Dir<'static> = include_dir!("$CARGO_MANIFEST_DIR/../../frontend/dist");

/// Where the shell page and its assets come from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageSource {
    /// Serve from the compile-time embedded `frontend/dist`.
    Embedded,
    /// Serve from a directory on disk (dev iteration).
    DevDir(PathBuf),
}

/// Shell window options for [`run_window`].
#[derive(Debug, Clone)]
pub struct WindowOptions {
    pub title: String,
    pub page: PageSource,
}

/// Content-type for a served asset path; `None` for unknown extensions.
pub fn content_type_for(path: &str) -> Option<&'static str> {
    let ext = Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase())?;
    Some(match ext.as_str() {
        "html" => "text/html; charset=utf-8",
        "js" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "json" => "application/json",
        _ => return None,
    })
}

/// Percent-decode a URI path (`%XX` sequences). `None` on malformed input.
fn percent_decode(raw: &str) -> Option<String> {
    let bytes = raw.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let hi = (*bytes.get(i + 1)? as char).to_digit(16)?;
                let lo = (*bytes.get(i + 2)? as char).to_digit(16)?;
                out.push((hi * 16 + lo) as u8);
                i += 3;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

/// Map a request URI path to a safe root-relative asset path.
///
/// Rejects directory traversal: `..`/`.` segments, absolute paths (drive
/// letters, UNC, backslashes), empty segments, and percent-encoded variants
/// such as `%2e%2e`. The root path `/` maps to `index.html`.
pub fn sanitize_uri_path(uri_path: &str) -> Option<String> {
    let decoded = percent_decode(uri_path)?;
    if decoded.contains('\\') {
        return None; // backslashes: never valid in served URI paths (UNC / Windows abs paths)
    }
    let rel = decoded.strip_prefix('/').unwrap_or(&decoded);
    if rel.is_empty() {
        return Some("index.html".to_owned());
    }
    let mut segs: Vec<&str> = Vec::new();
    for seg in rel.split('/') {
        if seg.is_empty() || seg == "." || seg == ".." || seg.contains(':') {
            return None;
        }
        segs.push(seg);
    }
    Some(segs.join("/"))
}

/// Read an asset from the embedded include_dir tree.
pub fn read_embedded(rel: &str) -> Option<&'static [u8]> {
    EMBEDDED.get_file(rel).map(|f| f.contents())
}

/// Read an asset from a dev directory, rejecting paths that resolve outside
/// the root (symlink/containment check on top of [`sanitize_uri_path`]).
pub fn read_dev(root: &Path, rel: &str) -> Option<Vec<u8>> {
    let joined = root.join(rel);
    if !joined.is_file() {
        return None;
    }
    let root_c = root.canonicalize().ok()?;
    let file_c = joined.canonicalize().ok()?;
    if !file_c.starts_with(&root_c) {
        return None;
    }
    std::fs::read(&file_c).ok()
}

fn not_found() -> Response<Cow<'static, [u8]>> {
    Response::builder()
        .status(404)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Cow::Borrowed("404 Not Found".as_bytes()))
        .expect("static 404 response is valid")
}

/// Build the HTTP response for an `app://` request against the given source.
/// Pure logic: no window required, directly unit-testable.
pub fn build_response(source: &PageSource, uri_path: &str) -> Response<Cow<'static, [u8]>> {
    let rel = match sanitize_uri_path(uri_path) {
        Some(r) => r,
        None => return not_found(),
    };
    let ct = content_type_for(&rel).unwrap_or("application/octet-stream");
    match source {
        PageSource::Embedded => match read_embedded(&rel) {
            Some(bytes) => Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ct)
                .body(Cow::Borrowed(bytes))
                .expect("static asset response is valid"),
            None => not_found(),
        },
        PageSource::DevDir(dir) => match read_dev(dir, &rel) {
            Some(bytes) => Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ct)
                .body(Cow::Owned(bytes))
                .expect("static asset response is valid"),
            None => not_found(),
        },
    }
}

/// URI path the page POSTs its bridge commands to (`app://localhost/rpc`).
const RPC_PATH: &str = "/rpc";

/// A batch of bridge events posted from any thread onto the window's loop.
/// The run loop turns each one into an `evaluate_script` call on the WebView.
#[derive(Debug, Clone)]
pub enum UserEvent {
    /// One event batch, already serialized as a JSON array of `EventEnvelope`.
    Batch(String),
}

/// Synchronous RPC entry point: raw request JSON in, response JSON out. wry
/// calls the protocol closure synchronously, so the glue closure drives its
/// async router with `Handle::block_on` before returning.
pub type RpcHandler = Arc<dyn Fn(String) -> String + Send + Sync>;

/// Teardown hook the run loop calls on `CloseRequested`, while the loop can
/// still be pumped: stop the event pump, shut the App down, release whatever
/// else the assembly owns. Called exactly once.
pub trait BridgeGuard: Send {
    fn on_close(self: Box<Self>);
}

/// Ready hook: called once the window and WebView exist. It gets the event-loop
/// proxy it needs to build an [`WrySink`] and returns the close-time guard.
pub type OnReady =
    Box<dyn FnOnce(&Window, &EventLoopProxy<UserEvent>) -> Box<dyn BridgeGuard> + Send>;

/// The JS one batch of bridge events is evaluated as. Pure, so the wire shape
/// is unit-testable without a window: the batch JSON is embedded verbatim as a
/// JS array literal, and a page without the hook simply short-circuits.
pub fn event_script(batch_json: &str) -> String {
    format!("window.__bridgeEvents && window.__bridgeEvents({batch_json})")
}

/// [`EventSink`] handing batches to the window thread through the loop proxy.
/// `send_event` never blocks and never waits for JS; a closed loop (window
/// already gone) is only a warning, since the pump is being stopped anyway.
pub struct WrySink {
    proxy: EventLoopProxy<UserEvent>,
}

impl WrySink {
    pub fn new(proxy: EventLoopProxy<UserEvent>) -> Self {
        Self { proxy }
    }
}

impl EventSink for WrySink {
    fn push_batch(&self, batch_json: &str) {
        if let Err(err) = self.proxy.send_event(UserEvent::Batch(batch_json.to_owned())) {
            tracing::warn!("event batch dropped, window loop is closed: {err}");
        }
    }
}

/// One JSON response body (the RPC answer; body is always JSON — phase-4
/// frontends call `r.json()` on it).
fn text_response(status: u16, body: String) -> Response<Cow<'static, [u8]>> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json; charset=utf-8")
        .body(Cow::Owned(body.into_bytes()))
        .expect("plain text response is valid")
}

/// Route one `app://` request for a bridged window: `POST /rpc` hands its body
/// to `rpc` and answers 200 `application/json` with the router's response JSON;
/// every other request is the usual static asset lookup (404 otherwise).
/// Pure logic: no window required, directly unit-testable.
fn bridge_response(
    source: &PageSource,
    rpc: &RpcHandler,
    req: &Request<Vec<u8>>,
) -> Response<Cow<'static, [u8]>> {
    if req.method() == Method::POST && req.uri().path() == RPC_PATH {
        let raw = String::from_utf8_lossy(req.body()).into_owned();
        return text_response(200, rpc(raw));
    }
    build_response(source, req.uri().path())
}

/// Create the tao window (1280x800) + wry WebView on the **main thread** and
/// block until the window is closed (`ControlFlow::Exit`), then return `Ok`.
pub fn run_window(opts: WindowOptions) -> anyhow::Result<()> {
    let WindowOptions { title, page } = opts;
    let mut event_loop = EventLoop::new();
    let window = WindowBuilder::new()
        .with_title(title)
        .with_inner_size(LogicalSize::new(1280.0, 800.0))
        .build(&event_loop)
        .context("failed to create shell window")?;

    let source = page;
    let _webview = WebViewBuilder::new()
        .with_custom_protocol(APP_SCHEME.to_owned(), move |_id, req| {
            build_response(&source, req.uri().path())
        })
        .with_url(format!("{APP_SCHEME}://localhost/index.html"))
        .build(&window)
        .context("failed to create WebView (is the WebView2 runtime installed?)")?;

    let code = event_loop.run_return(move |event, _target, control_flow| {
        *control_flow = ControlFlow::Wait;
        if let Event::WindowEvent {
            event: WindowEvent::CloseRequested,
            ..
        } = event
        {
            *control_flow = ControlFlow::Exit;
        }
    });
    if code == 0 {
        Ok(())
    } else {
        anyhow::bail!("event loop exited with code {code}");
    }
}

/// [`run_window`] plus the full bridge (T038b): the page talks to `rpc` over
/// `POST app://localhost/rpc`, `on_ready` gets the loop proxy it needs to push
/// events back (see [`WrySink`]) and hands back the close-time guard. Devtools
/// are enabled so the manual checklist can drive the RPC from the console.
///
/// Blocks on the **main thread** until the window closes, then returns `Ok`.
pub fn run_window_with(
    opts: WindowOptions,
    rpc: RpcHandler,
    on_ready: OnReady,
) -> anyhow::Result<()> {
    let WindowOptions { title, page } = opts;
    let mut event_loop = EventLoopBuilder::<UserEvent>::with_user_event().build();
    let window = WindowBuilder::new()
        .with_title(title)
        .with_inner_size(LogicalSize::new(1280.0, 800.0))
        .build(&event_loop)
        .context("failed to create shell window")?;

    let source = page;
    let webview = WebViewBuilder::new()
        .with_custom_protocol(APP_SCHEME.to_owned(), move |_id, req| {
            bridge_response(&source, &rpc, &req)
        })
        .with_devtools(true)
        .with_url(format!("{APP_SCHEME}://localhost/index.html"))
        .build(&window)
        .context("failed to create WebView (is the WebView2 runtime installed?)")?;

    // The assembly spawns the event pump here, so the WebView really exists and
    // the proxy it captures can never target a loop that was never built.
    let proxy = event_loop.create_proxy();
    let mut guard = Some(on_ready(&window, &proxy));

    let code = event_loop.run_return(move |event, _target, control_flow| {
        *control_flow = ControlFlow::Wait;
        match event {
            Event::UserEvent(UserEvent::Batch(batch)) => {
                // wry 0.57 renamed `evaluate` to `evaluate_script`; same call.
                if let Err(err) = webview.evaluate_script(&event_script(&batch)) {
                    tracing::warn!("failed to deliver an event batch: {err}");
                }
            }
            Event::WindowEvent {
                event: WindowEvent::CloseRequested,
                ..
            } => {
                if let Some(guard) = guard.take() {
                    guard.on_close();
                }
                *control_flow = ControlFlow::Exit;
            }
            _ => {}
        }
    });
    if code == 0 {
        Ok(())
    } else {
        anyhow::bail!("event loop exited with code {code}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_type_table() {
        assert_eq!(
            content_type_for("index.html"),
            Some("text/html; charset=utf-8")
        );
        assert_eq!(
            content_type_for("a.js"),
            Some("text/javascript; charset=utf-8")
        );
        assert_eq!(content_type_for("a.css"), Some("text/css; charset=utf-8"));
        assert_eq!(content_type_for("a.svg"), Some("image/svg+xml"));
        assert_eq!(content_type_for("a.png"), Some("image/png"));
        assert_eq!(content_type_for("a.ico"), Some("image/x-icon"));
        assert_eq!(content_type_for("a.json"), Some("application/json"));
        assert_eq!(content_type_for("a.HTML"), Some("text/html; charset=utf-8"));
        assert_eq!(content_type_for("a.wasm"), None);
        assert_eq!(content_type_for("noext"), None);
    }

    #[test]
    fn traversal_rejected() {
        assert_eq!(sanitize_uri_path("/../secret.txt"), None);
        assert_eq!(sanitize_uri_path("/a/../../b"), None);
        assert_eq!(sanitize_uri_path("/%2e%2e/secret.txt"), None);
        assert_eq!(sanitize_uri_path("/%2E%2E%5Csecret.txt"), None);
        // absolute paths: drive letter, UNC, backslash separators
        assert_eq!(sanitize_uri_path("/C:/Windows/x.ini"), None);
        assert_eq!(sanitize_uri_path("/c:\\windows\\x.ini"), None);
        assert_eq!(sanitize_uri_path("/server\\share\\x"), None);
        assert_eq!(sanitize_uri_path("/a//b"), None);
        // normal paths accepted
        assert_eq!(sanitize_uri_path("/"), Some("index.html".to_owned()));
        assert_eq!(
            sanitize_uri_path("/index.html"),
            Some("index.html".to_owned())
        );
        assert_eq!(
            sanitize_uri_path("/assets/app.js"),
            Some("assets/app.js".to_owned())
        );
    }

    #[test]
    fn embedded_index_has_pinned_elements() {
        let bytes = read_embedded("index.html").expect("embedded index.html");
        let html = std::str::from_utf8(bytes).expect("utf-8 html");
        assert!(html.contains("<title>rs-kafka-ui-client</title>"));
        assert!(html.contains("shell OK"));
        assert!(html.contains("<div id=\"app\"></div>"));
    }

    #[test]
    fn dev_dir_hit_and_miss() {
        let root = std::env::temp_dir().join(format!("kafka-ui-t031-{}", std::process::id()));
        std::fs::create_dir_all(&root).expect("create temp dir");
        std::fs::write(root.join("index.html"), b"dev shell OK").expect("write");
        assert_eq!(
            read_dev(&root, "index.html").as_deref(),
            Some(&b"dev shell OK"[..])
        );
        assert_eq!(read_dev(&root, "missing.html"), None);
        let resp = build_response(&PageSource::DevDir(root.clone()), "/missing.html");
        assert_eq!(resp.status(), 404);
        let resp = build_response(&PageSource::DevDir(root.clone()), "/index.html");
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.headers()[CONTENT_TYPE], "text/html; charset=utf-8");
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn build_response_rejects_traversal_and_serves_embedded() {
        for bad in ["/../Cargo.toml", "/%2e%2e/x", "/C:/x"] {
            let resp = build_response(&PageSource::Embedded, bad);
            assert_eq!(resp.status(), 404, "{bad} must be rejected");
        }
        let resp = build_response(&PageSource::Embedded, "/index.html");
        assert_eq!(resp.status(), 200);
        assert!(
            std::str::from_utf8(resp.body())
                .unwrap()
                .contains("shell OK")
        );
    }

    #[test]
    fn event_script_is_a_guarded_call_with_the_batch_embedded_verbatim() {
        let batch = r#"[{"event":"message.received","payload":{"topic":"orders"},"ts":1}]"#;
        let js = event_script(batch);
        assert_eq!(
            js,
            format!("window.__bridgeEvents && window.__bridgeEvents({batch})")
        );
        // the parenthesised part must still be exactly the batch JSON
        let inner = &js[js.find('(').expect("call") + 1..js.len() - 1];
        let parsed: serde_json::Value = serde_json::from_str(inner).expect("batch json");
        assert_eq!(parsed[0]["event"], "message.received");
    }

    #[test]
    fn bridge_response_routes_post_rpc_and_keeps_assets() {
        let rpc: RpcHandler = Arc::new(|raw| format!("echo:{raw}"));

        let req = Request::builder()
            .method(Method::POST)
            .uri("app://localhost/rpc")
            .body(br#"{"id":"1","cmd":"cluster.list"}"#.to_vec())
            .expect("request");
        let resp = bridge_response(&PageSource::Embedded, &rpc, &req);
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.headers()[CONTENT_TYPE], "application/json; charset=utf-8");
        assert_eq!(
            std::str::from_utf8(resp.body()).unwrap(),
            r#"echo:{"id":"1","cmd":"cluster.list"}"#
        );

        // assets keep being served over GET
        let req = Request::builder()
            .uri("app://localhost/index.html")
            .body(Vec::new())
            .expect("request");
        let resp = bridge_response(&PageSource::Embedded, &rpc, &req);
        assert_eq!(resp.status(), 200);
        assert!(std::str::from_utf8(resp.body()).unwrap().contains("shell OK"));

        // everything else is 404: a missing asset, and /rpc without POST
        for (method, uri) in [
            (Method::GET, "app://localhost/missing.js"),
            (Method::GET, "app://localhost/rpc"),
            (Method::POST, "app://localhost/../secret"),
        ] {
            let req = Request::builder()
                .method(method.clone())
                .uri(uri)
                .body(Vec::new())
                .expect("request");
            let resp = bridge_response(&PageSource::Embedded, &rpc, &req);
            assert_eq!(resp.status(), 404, "{method} {uri}");
        }
    }
}
