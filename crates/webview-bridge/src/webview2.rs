//! WebView2 runtime preflight (T091).
//!
//! On Windows wry needs the **WebView2 Runtime** before it can create a WebView;
//! without it `WebViewBuilder::build` fails, and the only thing the user saw was
//! a bare "failed to create WebView (is the WebView2 runtime installed?)" — no
//! name, no download link, no window. This module answers, before the window is
//! attempted, *is the runtime there* and *what should we say if it is not*.
//!
//! Detection reads the **filesystem** (R93) rather than the registry or the
//! WebView2 API, so every rule below is a pure function the unit tests drive from
//! a temporary tree: this crate is developed on macOS, where none of the Windows
//! lookup can be compiled or run.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Where the WebView2 Runtime lives on Windows, relative to `Program Files (x86)`.
#[cfg(target_os = "windows")]
const RUNTIME_SUBDIR: &str = r"Microsoft\EdgeWebView\Application";
/// The executable every usable runtime version directory contains.
const RUNTIME_EXE: &str = "msedgewebview2.exe";
/// Microsoft's evergreen-bootstrapper link, offered when the runtime is missing.
pub const DOWNLOAD_URL: &str = "https://go.microsoft.com/fwlink/p/?LinkId=2124703";
/// Manual-verification hook (R96): forces the "missing" branch on any platform.
pub const FORCE_MISSING_ENV: &str = "KAFKA_UI_FAKE_WEBVIEW2_MISSING";
/// Tracing target for both the warning and the log-file copy (T083a).
const LOG_TARGET: &str = "webview_bridge::webview2";

/// The directory holding the installed versions, when this host has one.
#[cfg(target_os = "windows")]
fn runtime_root() -> Option<PathBuf> {
    let base = std::env::var_os("ProgramFiles(x86)").or_else(|| std::env::var_os("ProgramFiles"))?;
    Some(PathBuf::from(base).join(RUNTIME_SUBDIR))
}

/// macOS and Linux host their WebView with the system, so there is no directory
/// to look in — the caller reads `None` as "nothing to check here".
#[cfg(not(target_os = "windows"))]
fn runtime_root() -> Option<PathBuf> {
    None
}

/// The newest installed version under `root`, or `None` when nothing usable is
/// there. Only directories that actually contain `msedgewebview2.exe` count.
pub fn installed_version_in(root: &Path) -> Option<String> {
    let entries = std::fs::read_dir(root).ok()?;
    let mut candidates = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() || !path.join(RUNTIME_EXE).is_file() {
            continue;
        }
        if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
            candidates.push(name.to_string());
        }
    }
    newest_version(&candidates)
}

/// The highest version among `names`, comparing numerically per component so
/// `100.0.0` beats `9.0.0` (a string compare gets that backwards).
fn newest_version(names: &[String]) -> Option<String> {
    names
        .iter()
        .filter_map(|name| parse_version(name).map(|parts| (parts, name)))
        .max_by(|left, right| left.0.cmp(&right.0))
        .map(|(_, name)| name.clone())
}

/// `1.2.3.4` -> `[1, 2, 3, 4]`; anything that is not at least two dotted numbers
/// (a stray `logs` directory, say) is not a version.
fn parse_version(name: &str) -> Option<Vec<u32>> {
    let parts = name
        .split('.')
        .map(|part| part.parse().ok())
        .collect::<Option<Vec<u32>>>()?;
    (parts.len() >= 2).then_some(parts)
}

/// R96: `KAFKA_UI_FAKE_WEBVIEW2_MISSING=1` pretends the runtime is absent, so the
/// prompt path can be seen by hand on a machine that has no Windows runtime.
pub fn forced_missing() -> bool {
    std::env::var_os(FORCE_MISSING_ENV).is_some_and(|value| !value.is_empty())
}

/// True when this platform can host a WebView: always on macOS/Linux (the system
/// provides it), and on Windows when an installed runtime was found.
pub fn is_available() -> bool {
    if forced_missing() {
        return false;
    }
    match runtime_root() {
        Some(root) => installed_version_in(&root).is_some(),
        None => true,
    }
}

/// What the user is told when the runtime is missing.
pub fn missing_message() -> String {
    format!("the WebView2 runtime is not installed;\nget it from {DOWNLOAD_URL} and start again")
}

/// Best effort: open the download page in the default browser. `false` means
/// nothing opened (no `open`/`xdg-open`/`cmd` available), which is not fatal —
/// the URL is in the message either way.
pub fn open_download_page() -> bool {
    let mut command = if cfg!(target_os = "windows") {
        let mut command = Command::new("cmd");
        command.args(["/C", "start", "", DOWNLOAD_URL]);
        command
    } else if cfg!(target_os = "macos") {
        let mut command = Command::new("open");
        command.arg(DOWNLOAD_URL);
        command
    } else {
        let mut command = Command::new("xdg-open");
        command.arg(DOWNLOAD_URL);
        command
    };
    command.status().is_ok()
}

/// [`preflight`] with the browser hand-off injected, so the tests can assert the
/// missing-runtime path without a real browser window opening (`cargo test` must
/// not launch one).
pub fn preflight_with(open: &dyn Fn() -> bool) -> anyhow::Result<()> {
    if is_available() {
        return Ok(());
    }
    let message = missing_message();
    tracing::warn!(target: LOG_TARGET, "{}", message);
    if !open() {
        tracing::warn!(target: LOG_TARGET, "could not open {} in a browser", DOWNLOAD_URL);
    }
    anyhow::bail!("{message}")
}

/// Run before the window is created: `Ok` when a WebView can be hosted. Otherwise
/// a warning (stderr, and the T083a log file), a best-effort hand-off to the
/// download page, and an `Err` that `main`'s `?` turns into a non-zero exit —
/// which beats the old bare "failed to create WebView" with no cause named.
pub fn preflight() -> anyhow::Result<()> {
    preflight_with(&open_download_page)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build `root/<version>/msedgewebview2.exe` (or a bare directory when the
    /// version is present but the executable is not).
    fn install_version(root: &Path, version: &str, exe: bool) {
        let dir = root.join(version);
        std::fs::create_dir_all(&dir).expect("create version dir");
        if exe {
            std::fs::write(dir.join(RUNTIME_EXE), b"stub").expect("write exe stub");
        }
    }

    fn scratch(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("kafka-ui-webview2-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        dir
    }

    #[test]
    fn the_newest_installed_version_wins_numerically() {
        let root = scratch("newest");
        install_version(&root, "9.0.0.0", true);
        install_version(&root, "100.0.2210.91", true);

        assert_eq!(installed_version_in(&root).as_deref(), Some("100.0.2210.91"));
    }

    #[test]
    fn a_version_directory_without_the_executable_does_not_count() {
        let root = scratch("no-exe");
        install_version(&root, "120.0.0.0", false);
        std::fs::write(root.join("stray.txt"), b"hi").expect("write stray file");

        assert_eq!(installed_version_in(&root), None);
        assert_eq!(installed_version_in(&root.join("absent")), None);
    }

    #[test]
    fn only_dotted_numbers_are_versions() {
        assert_eq!(parse_version("120.0.2210.91"), Some(vec![120, 0, 2210, 91]));
        assert_eq!(parse_version("120"), None);
        assert_eq!(parse_version("120.x"), None);
    }

    #[test]
    fn forcing_missing_flips_availability_and_fails_the_preflight() {
        // One test owns this variable on purpose: cargo runs a binary's tests in
        // parallel threads, so two tests toggling one env var would race (R96).
        // Edition 2024 makes the setters unsafe; nothing else reads it here.
        unsafe { std::env::set_var(FORCE_MISSING_ENV, "1") };

        assert!(forced_missing());
        assert!(!is_available());
        // The false opener also covers "could not open a browser", still an Err.
        let error = preflight_with(&|| false).expect_err("preflight must fail when missing");
        assert!(error.to_string().contains(DOWNLOAD_URL), "{error}");

        unsafe { std::env::remove_var(FORCE_MISSING_ENV) };
        assert!(!forced_missing());
        assert!(is_available(), "macOS/Linux host the WebView themselves");
        assert!(preflight().is_ok());
    }
}
