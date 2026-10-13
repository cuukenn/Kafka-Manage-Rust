//! Driver abstraction and implementations.
//!
//! * [`KafkaDriver`] is the seam every service depends on.
//! * [`mock::MockKafkaDriver`] is the default, dependency-free in-memory implementation.
//! * the `real` module (feature `real`) is the rdkafka-backed driver, filled in at phase 6.

pub mod driver;
pub mod mock;

#[cfg(feature = "real")]
pub mod real;

pub use driver::{DriverCapabilities, DriverEvent, DriverKind, DynDriver, KafkaDriver, driver_for};
pub use mock::MockKafkaDriver;
