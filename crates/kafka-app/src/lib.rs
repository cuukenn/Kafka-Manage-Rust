//! Business layer: services over `Arc<dyn KafkaDriver>`, the global event bus
//! and local SQLite persistence. (Phase 2 fills this in.)

pub mod bus;
pub mod driver_guard;
pub mod services;
pub mod store;
