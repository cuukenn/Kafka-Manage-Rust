//! Entry point. Opens the tao+wry shell window (T038 wires up the full
//! startup sequence: logging, config dir, App, Router IPC, event pump).

use std::path::PathBuf;

use webview_bridge::shell::{DEV_DIR_ENV, PageSource, WindowOptions, run_window};

fn main() -> anyhow::Result<()> {
    let page = match std::env::var_os(DEV_DIR_ENV) {
        Some(dir) if !dir.is_empty() => PageSource::DevDir(PathBuf::from(dir)),
        _ => PageSource::Embedded,
    };
    run_window(WindowOptions {
        title: "rs-kafka-ui-client".to_owned(),
        page,
    })
}
