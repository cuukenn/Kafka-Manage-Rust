//! Entry point (T038b): logging (T080 also mirrors `tracing` onto the bus),
//! local data directory, App/Router assembly, then the bridged shell window —
//! RPC over `POST app://localhost/rpc` into the router, Rust events pushed in
//! batches to `window.__bridgeEvents`.

use std::path::PathBuf;
use std::time::Duration;

use webview_bridge::shell::{DEV_DIR_ENV, PageSource, WindowOptions, run_window_with};

mod glue;
mod logging;

fn main() -> anyhow::Result<()> {
    let logs = logging::install();

    let page = match std::env::var_os(DEV_DIR_ENV) {
        Some(dir) if !dir.is_empty() => PageSource::DevDir(PathBuf::from(dir)),
        _ => PageSource::Embedded,
    };

    let assembly = glue::Assembly::open_default()?;
    let rpc = assembly.rpc();
    // The App moves into the ready hook (the guard owns it); the runtime stays
    // owned here so the rpc closure's Handle stays valid for every request.
    let glue::Assembly { rt, app, .. } = assembly;
    // T080: from here on the bus layer can publish, so anything logged below
    // (including the window's own startup lines) shows up in the log panel.
    logs.attach(app.bus().clone());
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
