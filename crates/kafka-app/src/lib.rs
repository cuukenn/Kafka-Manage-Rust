//! Business layer: services over `Arc<dyn KafkaDriver>`, the global event bus
//! and local SQLite persistence. (Phase 2 fills this in.)
//!
//! [`App`] wires those parts into the single entry point the bridge will drive.

pub mod bus;
pub mod driver_guard;
pub mod services;
pub mod store;

use crate::bus::EventBus;
use crate::services::{
    ClusterService, ConsumeService, GroupService, NO_CACHE, ProduceService, TopicService,
};
use crate::store::Store;

/// Store, bus and all five services sharing one driver registry. This is the
/// object the bridge layer constructs once and routes every command through.
#[derive(Debug)]
pub struct App {
    store: Store,
    bus: EventBus,
    clusters: ClusterService,
    topics: TopicService,
    produce: ProduceService,
    consume: ConsumeService,
    groups: GroupService,
}

impl App {
    /// Wires everything onto `store` and `bus`. Topic list caching stays off
    /// ([`NO_CACHE`]) until startup config (T038) can enable it.
    pub fn new(store: Store, bus: EventBus) -> Self {
        let clusters = ClusterService::new(store.clone(), bus.clone());
        Self {
            store,
            bus,
            topics: TopicService::new(clusters.clone(), NO_CACHE),
            produce: ProduceService::new(clusters.clone()),
            consume: ConsumeService::new(clusters.clone()),
            groups: GroupService::new(clusters.clone()),
            clusters,
        }
    }

    pub fn builder() -> AppBuilder {
        AppBuilder::default()
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn bus(&self) -> &EventBus {
        &self.bus
    }

    pub fn clusters(&self) -> &ClusterService {
        &self.clusters
    }

    pub fn topics(&self) -> &TopicService {
        &self.topics
    }

    pub fn produce(&self) -> &ProduceService {
        &self.produce
    }

    pub fn consume(&self) -> &ConsumeService {
        &self.consume
    }

    pub fn groups(&self) -> &GroupService {
        &self.groups
    }

    /// Teardown in order: stop every subscription, then disconnect every cluster.
    pub async fn shutdown(&self) {
        self.consume.stop_all().await;
        self.clusters.disconnect_all().await;
    }
}

/// Minimal builder for [`App::new`]: explicit store/bus or in-memory defaults;
/// startup config (real data directory, cache TTL) plugs in here when T038 lands.
#[derive(Debug, Default)]
pub struct AppBuilder {
    store: Option<Store>,
    bus: Option<EventBus>,
}

impl AppBuilder {
    pub fn store(mut self, store: Store) -> Self {
        self.store = Some(store);
        self
    }

    pub fn bus(mut self, bus: EventBus) -> Self {
        self.bus = Some(bus);
        self
    }

    pub fn build(self) -> App {
        let store = match self.store {
            Some(store) => store,
            None => Store::open_in_memory().expect("in-memory SQLite store must open"),
        };
        App::new(store, self.bus.unwrap_or_default())
    }
}
