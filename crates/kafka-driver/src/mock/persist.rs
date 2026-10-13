//! File-backed persistence for mock cluster snapshots (T017).
//!
//! `MockState` is already `Serialize`/`Deserialize`, so persistence is a thin
//! wrapper around `serde_json` with one extra guarantee: the target file is
//! never observed half-written. Every save writes `<path>.tmp` first and
//! renames it over the target, so a concurrent reader sees either the previous
//! snapshot or the new one — rename is atomic on Windows and POSIX alike.

use std::path::{Path, PathBuf};

use kafka_core::error::DriverError;

use super::state::MockState;

/// Persists one `MockState` as JSON at a fixed path.
#[derive(Debug, Clone)]
pub struct MockPersistence {
    path: PathBuf,
}

impl MockPersistence {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Snapshot `state` to disk atomically: write `<path>.tmp`, then rename.
    pub fn save(&self, state: &MockState) -> Result<(), DriverError> {
        let json = serde_json::to_string(state)
            .map_err(|e| DriverError::other(format!("mock snapshot serialize failed: {e}")))?;
        let tmp = self.tmp_path();
        std::fs::write(&tmp, json).map_err(|e| {
            DriverError::other(format!("mock snapshot write failed at {}: {e}", tmp.display()))
        })?;
        std::fs::rename(&tmp, &self.path).map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            DriverError::other(format!(
                "mock snapshot commit failed at {}: {e}",
                self.path.display()
            ))
        })
    }

    /// Read the snapshot back. A missing file is `Ok(None)`, not an error:
    /// a first run has nothing to restore. A corrupt file is a `DriverError`
    /// carrying a readable message — never a panic.
    pub fn load(&self) -> Result<Option<MockState>, DriverError> {
        let bytes = match std::fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(DriverError::other(format!(
                    "mock snapshot read failed at {}: {e}",
                    self.path.display()
                )))
            }
        };
        let state: MockState = serde_json::from_slice(&bytes).map_err(|e| {
            DriverError::other(format!(
                "corrupt mock snapshot at {}: {e}",
                self.path.display()
            ))
        })?;
        Ok(Some(state))
    }

    /// Remove the snapshot. Already-absent is success: clearing is idempotent.
    pub fn clear(&self) -> Result<(), DriverError> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(DriverError::other(format!(
                "mock snapshot remove failed at {}: {e}",
                self.path.display()
            ))),
        }
    }

    fn tmp_path(&self) -> PathBuf {
        let mut os = self.path.as_os_str().to_os_string();
        os.push(".tmp");
        PathBuf::from(os)
    }
}
