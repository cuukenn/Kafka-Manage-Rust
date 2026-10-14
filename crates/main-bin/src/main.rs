//! Entry point (T038b): logging, local data directory, App/Router assembly,
//! then the bridged shell window — RPC over `POST app://localhost/rpc` into the
//! router, Rust events pushed in batches to `window.__bridgeEvents`.

use std::path::PathBuf;
use std::time::Duration;

use webview_bridge::shell::{DEV_DIR_ENV, PageSource, WindowOptions, run_window_with};

mod glue;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let page = match std::env::var_os(DEV_DIR_ENV) {
        Some(dir) if !dir.is_empty() => PageSource::DevDir(PathBuf::from(dir)),
        _ => PageSource::Embedded,
    };

    let assembly = glue::Assembly::open_default()?;
    let rpc = assembly.rpc();
    // The App moves into the ready hook (the guard owns it); the runtime stays
    // owned here so the rpc closure's Handle stays valid for every request.
    let glue::Assembly { rt, app, .. } = assembly;
    run_window_with(
        WindowOptions {
            title: "rs-kafka-ui-client".to_owned(),
            page,
        },
        rpc,
        glue::ready_hook(rt.handle(), app),
    )?;
    // Bounded shutdown instead of bare drop: drains spawned tasks (e.g. the
    // mock pump) with a cap, so a T068-style idle-exit defect cannot hang
    // process exit on `Runtime::drop`.
    rt.shutdown_timeout(Duration::from_secs(5));
    Ok(())
}
