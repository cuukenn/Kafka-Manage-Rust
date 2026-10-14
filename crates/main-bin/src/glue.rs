//! T038b startup assembly: the pieces `main` wires into the shell window.
//!
//! [`Assembly`] owns the multi-thread tokio runtime, the `App` (SQLite store +
//! services) and the [`Router`] with all six command tables registered. The
//! window reaches the async router through the synchronous [`RpcHandler`]
//! closure (RPC over `app://localhost/rpc`), and the shell's ready hook spawns
//! the [`EventPump`] into the WebView and hands back the close-time guard.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use tokio::runtime::{Handle, Runtime};

use kafka_app::App;
use kafka_app::store::{APP_DIR_NAME, DB_FILE_NAME, Store, default_data_dir};
use webview_bridge::handlers::{register_cluster, register_config};
use webview_bridge::handlers_consume::register_consume;
use webview_bridge::handlers_group::register_group;
use webview_bridge::handlers_mock::register_mock;
use webview_bridge::handlers_topic::register_topic_and_produce;
use webview_bridge::pump::{EventPump, PumpHandle};
use webview_bridge::router::Router;
use webview_bridge::shell::{BridgeGuard, OnReady, RpcHandler, WrySink};

/// Last-resort body for the RPC route: even if an envelope somehow cannot be
/// serialized, the `{id,ok,...}` wire contract still has to hold.
const FALLBACK_ERROR: &str = concat!(
    r#"{"id":"","ok":false,"error":{"kind":"internal","#,
    r#""message":"bridge failed to serialize the response","retryable":false}}"#,
);

/// What the running process owns: the runtime the commands run on, the App
/// behind them, and the command table the RPC closure routes through.
pub struct Assembly {
    pub rt: Runtime,
    pub app: App,
    pub router: Router,
}

impl Assembly {
    /// Opens the local SQLite database under the platform data directory and
    /// assembles on it — what `main` uses.
    pub fn open_default() -> anyhow::Result<Self> {
        let dir = data_dir();
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("cannot create data directory {}", dir.display()))?;
        let db = dir.join(DB_FILE_NAME);
        let store =
            Store::open(&db).with_context(|| format!("cannot open database {}", db.display()))?;
        tracing::info!(path = %db.display(), "local store opened");
        Self::new(store)
    }

    /// Assembles on a caller-supplied store; tests pass an in-memory database.
    pub fn new(store: Store) -> anyhow::Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("kafka-ui-io")
            .build()
            .context("cannot start the tokio runtime")?;
        let app = App::builder().store(store).build();
        let router = Router::new();
        register_cluster(&app, &router);
        register_config(&app, &router);
        register_topic_and_produce(&app, &router);
        register_consume(&app, &router);
        register_group(&app, &router);
        register_mock(&app, &router);
        tracing::info!("router ready: cluster/config/topic+produce/consume/group/mock registered");
        Ok(Self { rt, app, router })
    }

    /// The `POST app://localhost/rpc` body handler. wry calls it synchronously
    /// from a WebView callback thread, so it drives the async router with
    /// `Handle::block_on` and returns the envelope as JSON text.
    pub fn rpc(&self) -> RpcHandler {
        let router = self.router.clone();
        let handle = self.rt.handle().clone();
        Arc::new(move |raw: String| {
            let response = handle.block_on(router.handle(&raw));
            serde_json::to_string(&response).unwrap_or_else(|err| {
                tracing::error!("response envelope could not be serialized: {err}");
                FALLBACK_ERROR.to_owned()
            })
        })
    }
}

/// Builds the shell's ready hook: it spawns the event pump feeding the WebView
/// and returns the guard that stops the pump and shuts the App down on close.
pub fn ready_hook(handle: &Handle, app: App) -> OnReady {
    let handle = handle.clone();
    Box::new(move |_window, proxy| {
        // `EventPump::spawn` needs a runtime context; the handle provides one.
        let _enter = handle.enter();
        let pump = EventPump::spawn(app.bus(), Arc::new(WrySink::new(proxy.clone())));
        tracing::info!("event pump online: batches go to window.__bridgeEvents");
        Box::new(ShutdownGuard { pump, app, handle })
    })
}

/// What the run loop calls on `CloseRequested`: pump first, then the App
/// (subscriptions stopped, clusters disconnected). Batches the pump has not
/// flushed when the window loop exits never reach the page — harmless, the
/// page is closing anyway. Blocking the window thread here is intended — it
/// is the last thing the loop does before exiting.
struct ShutdownGuard {
    pump: PumpHandle,
    app: App,
    handle: Handle,
}

impl BridgeGuard for ShutdownGuard {
    fn on_close(self: Box<Self>) {
        let Self { pump, app, handle } = *self;
        handle.block_on(async move {
            pump.shutdown().await;
            app.shutdown().await;
        });
        tracing::info!("bridge shut down cleanly");
    }
}

/// Data directory: `dirs::data_local_dir()/rs-kafka-ui-client`, falling back to
/// kafka-app's own resolution chain when the platform has no local data dir.
fn data_dir() -> PathBuf {
    match dirs::data_local_dir() {
        Some(base) => base.join(APP_DIR_NAME),
        None => default_data_dir(),
    }
}
