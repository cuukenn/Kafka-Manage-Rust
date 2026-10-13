//! Business operations behind the wire commands: what the router calls, never the wire
//! itself. Services own the store, the bus and the live driver registry.

pub mod clusters;
pub mod consume;
pub mod groups;
pub mod produce;
pub mod topics;

pub use clusters::{ClusterService, ClusterView};
pub use consume::{ConsumeService, SubscriptionInfo};
pub use groups::GroupService;
pub use produce::{DEFAULT_MAX_VALUE_BYTES, ProduceReport, ProduceService};
pub use topics::{NO_CACHE, TopicService};

use kafka_core::error::{DriverError, ErrorKind, StorageError};
use kafka_core::protocol::ErrorDto;

use crate::store::Store;

/// Error type shared by every service call: driver failures plus local store failures.
///
/// The router turns this into a [`ErrorDto`] — keeping both sources in one type means the
/// wire layer never has to know whether a failure came from Kafka or from SQLite.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error(transparent)]
    Driver(#[from] DriverError),

    #[error(transparent)]
    Storage(#[from] StorageError),
}

impl ServiceError {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::Driver(err) => err.kind(),
            Self::Storage(err) => err.kind(),
        }
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Driver(err) => err.is_retryable(),
            // A half written local row is not something the UI should retry blindly.
            Self::Storage(_) => false,
        }
    }

    /// Wire form for `ResponseEnvelope.error` and `AppEvent::Error`.
    pub fn to_dto(&self) -> ErrorDto {
        match self {
            Self::Driver(err) => ErrorDto::from_driver(err),
            Self::Storage(err) => ErrorDto::new(self.kind(), err.to_string()),
        }
    }
}

impl From<ServiceError> for DriverError {
    fn from(err: ServiceError) -> Self {
        match err {
            ServiceError::Driver(err) => err,
            ServiceError::Storage(err) => DriverError::from(anyhow::anyhow!(err)),
        }
    }
}

/// Runs one blocking store call on the blocking pool so SQLite never occupies an async
/// worker. `Store` is a cheap `Clone` (one `Arc`), which is what lets it cross the task
/// boundary without a lifetime.
pub(crate) async fn store_task<T, F>(store: &Store, f: F) -> Result<T, ServiceError>
where
    T: Send + 'static,
    F: FnOnce(Store) -> Result<T, ServiceError> + Send + 'static,
{
    let store = store.clone();
    tokio::task::spawn_blocking(move || f(store))
        .await
        .map_err(|err| ServiceError::Storage(StorageError::Other(anyhow::anyhow!("store task failed: {err}"))))?
}
