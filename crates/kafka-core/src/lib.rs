//! Shared vocabulary for the whole application: cluster/topic/message/group models,
//! the unified error type, and the JSON protocol spoken between Rust and the WebView.
//!
//! This crate performs no IO and must stay free of driver/UI dependencies so every
//! other crate can depend on it.

pub mod cluster;
pub mod dto;
pub mod error;
pub mod group;
pub mod message;
pub mod protocol;
pub mod topic;

pub use error::{DriverError, ErrorKind, StorageError};

/// Re-exported so downstream crates do not need to depend on `uuid` directly.
pub type Uuid = uuid::Uuid;

/// Milliseconds since Unix epoch, used for every timestamp crossing the driver boundary.
pub type TimestampMs = i64;

/// Newtype identifying one cluster configuration stored locally.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct ClusterId(pub String);

impl ClusterId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().simple().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for ClusterId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ClusterId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
