use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// Coarse error taxonomy shared with the UI so the frontend can branch on failure
/// type without parsing human readable text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    NotConnected,
    Connection,
    Timeout,
    Auth,
    Ssl,
    NotFound,
    AlreadyExists,
    InvalidConfig,
    OffsetOutOfRange,
    Unsupported,
    Group,
    Broker,
    Storage,
    Cancelled,
    Internal,
}

impl ErrorKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NotConnected => "not_connected",
            Self::Connection => "connection",
            Self::Timeout => "timeout",
            Self::Auth => "auth",
            Self::Ssl => "ssl",
            Self::NotFound => "not_found",
            Self::AlreadyExists => "already_exists",
            Self::InvalidConfig => "invalid_config",
            Self::OffsetOutOfRange => "offset_out_of_range",
            Self::Unsupported => "unsupported",
            Self::Group => "group",
            Self::Broker => "broker",
            Self::Storage => "storage",
            Self::Cancelled => "cancelled",
            Self::Internal => "internal",
        }
    }

    /// Every variant, in declaration order.
    ///
    /// Tables keyed by error kind (the UI hint in
    /// [`protocol`](crate::protocol), for one) iterate this so a variant cannot be
    /// added without a decision about it: this list stops compiling first, and the
    /// `hint_for` match stops compiling next.
    pub const ALL: [Self; 15] = [
        Self::NotConnected,
        Self::Connection,
        Self::Timeout,
        Self::Auth,
        Self::Ssl,
        Self::NotFound,
        Self::AlreadyExists,
        Self::InvalidConfig,
        Self::OffsetOutOfRange,
        Self::Unsupported,
        Self::Group,
        Self::Broker,
        Self::Storage,
        Self::Cancelled,
        Self::Internal,
    ];

    /// Whether an operation that failed with this code may be retried unchanged.
    ///
    /// The coarse, wire-facing half of the classification: `ErrorDto` carries only an
    /// `ErrorKind`, so the UI's "retry" affordance is decided here. `DriverError` keeps a
    /// finer-grained answer on top of this (see [`DriverError::is_retryable`]) because
    /// `ErrorKind` cannot distinguish e.g. `Rebalance` from `GroupNotFound` — both map to
    /// [`ErrorKind::Group`], and only the former is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::NotConnected | Self::Connection | Self::Timeout | Self::Broker)
    }
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// `anyhow::Error` is not `Clone`, but `DriverError` deliberately is (a failure has
/// to reach both the log and the UI). This wrapper keeps both intents: the payload is
/// an `Arc`, and `Display` renders the whole anyhow chain (`{:#}`). `source()` is left
/// unwired — `anyhow::Error` does not implement `std::error::Error`, so use
/// [`AnyError::inner`] to reach `chain()` or `downcast_ref()`.
#[derive(Debug, Clone)]
pub struct AnyError(Arc<anyhow::Error>);

impl AnyError {
    /// The wrapped error, for callers that want anyhow's `chain()` or `downcast_ref()`.
    pub fn inner(&self) -> &anyhow::Error {
        &self.0
    }
}

impl fmt::Display for AnyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `{:#}` prints the whole chain as "outer context: inner cause".
        write!(f, "{:#}", *self.0)
    }
}

impl std::error::Error for AnyError {}

impl From<anyhow::Error> for AnyError {
    fn from(error: anyhow::Error) -> Self {
        Self(Arc::new(error))
    }
}

/// The single error type every driver and service returns.
///
/// Deliberately `Clone` so a failure can be pushed both to logs and to the UI.
#[derive(Debug, Clone, thiserror::Error)]
pub enum DriverError {
    #[error("not connected (cluster {0})")]
    NotConnected(String),

    #[error("failed to reach {server}: {message}")]
    Connection { server: String, message: String },

    #[error("operation '{operation}' timed out after {elapsed_ms} ms")]
    Timeout { operation: String, elapsed_ms: u64 },

    #[error("authentication failed: {0}")]
    Auth(String),

    #[error("ssl error: {0}")]
    Ssl(String),

    #[error("topic '{0}' not found")]
    TopicNotFound(String),

    #[error("topic '{0}' already exists")]
    TopicAlreadyExists(String),

    #[error("partition {partition} not found in topic '{topic}'")]
    PartitionNotFound { topic: String, partition: u32 },

    #[error("group '{0}' not found")]
    GroupNotFound(String),

    #[error("offset {requested} out of range for {topic}-{partition} (valid {low}..{high})")]
    OffsetOutOfRange {
        topic: String,
        partition: u32,
        requested: i64,
        low: i64,
        high: i64,
    },

    #[error("invalid config for '{field}': {message}")]
    InvalidConfig { field: String, message: String },

    #[error("unsupported by this driver: {0}")]
    Unsupported(String),

    #[error("broker error {code} ({name}): {message}")]
    Broker {
        code: i16,
        name: String,
        message: String,
    },

    #[error("rebalance in progress: {0}")]
    Rebalance(String),

    #[error("operation cancelled")]
    Cancelled,

    #[error(transparent)]
    Other(#[from] AnyError),
}

impl DriverError {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::NotConnected(_) => ErrorKind::NotConnected,
            Self::Connection { .. } => ErrorKind::Connection,
            Self::Timeout { .. } => ErrorKind::Timeout,
            Self::Auth(_) => ErrorKind::Auth,
            Self::Ssl(_) => ErrorKind::Ssl,
            Self::TopicNotFound(_) | Self::PartitionNotFound { .. } | Self::GroupNotFound(_) => {
                ErrorKind::NotFound
            }
            Self::TopicAlreadyExists(_) => ErrorKind::AlreadyExists,
            Self::InvalidConfig { .. } => ErrorKind::InvalidConfig,
            Self::OffsetOutOfRange { .. } => ErrorKind::OffsetOutOfRange,
            Self::Unsupported(_) => ErrorKind::Unsupported,
            Self::Broker { .. } => ErrorKind::Broker,
            Self::Rebalance(_) => ErrorKind::Group,
            Self::Cancelled => ErrorKind::Cancelled,
            Self::Other(_) => ErrorKind::Internal,
        }
    }

    /// Whether the operation may be retried unchanged. Used by the retry wrapper.
    ///
    /// Delegates to [`ErrorKind::is_retryable`] and adds the one case `ErrorKind`
    /// cannot express: `Rebalance` (its kind is `Group`, which also covers
    /// `GroupNotFound`, and a missing group must not be retried).
    pub fn is_retryable(&self) -> bool {
        self.kind().is_retryable() || matches!(self, Self::Rebalance(_))
    }

    pub fn timeout(operation: impl Into<String>, elapsed_ms: u64) -> Self {
        Self::Timeout { operation: operation.into(), elapsed_ms }
    }

    pub fn invalid(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self::InvalidConfig { field: field.into(), message: message.into() }
    }

    pub fn unsupported(what: impl Into<String>) -> Self {
        Self::Unsupported(what.into())
    }

    pub fn other(message: impl Into<String>) -> Self {
        Self::Other(AnyError::from(anyhow::anyhow!(message.into())))
    }
}

/// Lets `?` lift an `anyhow::Error` straight into `DriverError`: the `#[from]` on
/// `Other` only covers `AnyError`, and `?` does not chain two conversions.
impl From<anyhow::Error> for DriverError {
    fn from(error: anyhow::Error) -> Self {
        Self::Other(AnyError::from(error))
    }
}

/// Errors raised by the local SQLite store (cluster configs, mock snapshots).
/// Kept free of `rusqlite` types so this crate stays IO-free.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("storage io: {0}")]
    Io(String),

    #[error("storage sql: {0}")]
    Sql(String),

    #[error("record not found: {0}")]
    NotFound(String),

    #[error("corrupt stored data: {0}")]
    Corrupt(String),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl StorageError {
    pub fn kind(&self) -> ErrorKind {
        ErrorKind::Storage
    }
}
