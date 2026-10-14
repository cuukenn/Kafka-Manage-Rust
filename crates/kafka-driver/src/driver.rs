//! The `KafkaDriver` abstraction every service depends on, plus the driver kinds
//! and capability flags the UI uses to enable or grey out actions.

use std::sync::Arc;

use async_trait::async_trait;

use kafka_core::cluster::{ClusterConfig, ClusterInfo};
use kafka_core::error::DriverError;
use kafka_core::group::{
    CommitOffsetsReq, GroupDetail, GroupInfo, GroupOffsetEntry, ListGroupOffsetsReq, ResetOffsetsReq,
    ResetOffsetsResult,
};
use kafka_core::message::{MessageStream, ProduceReq, ProduceResult, SubscribeReq};
use kafka_core::topic::{
    CreateTopicReq, DeleteTopicsReq, DescribeTopicsReq, ListOffsetsReq, OffsetRange, TopicDeleteResult,
    TopicDetail, TopicInfo, UpdateTopicConfigReq,
};

use crate::mock;
#[cfg(feature = "real")]
use crate::real;

/// Which implementation backs a cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriverKind {
    Mock,
    Real,
}

impl DriverKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Mock => "mock",
            Self::Real => "real",
        }
    }
}

/// What a driver can do, so the frontend can hide unsupported buttons instead of
/// surfacing failures after the user clicks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DriverCapabilities {
    pub admin_api: bool,
    pub topic_config_write: bool,
    pub consumer_groups: bool,
    pub offset_reset: bool,
    pub offset_commit: bool,
    pub live_stream: bool,
    pub server_side_filter: bool,
    pub sasl: bool,
    pub ssl: bool,
    /// Mock only: latency / failure injection.
    pub fault_injection: bool,
    /// Mock only: seeding and resetting fixture data.
    pub fixtures: bool,
}

/// Domain changes a driver emits so the app layer can refresh the UI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriverEvent {
    TopicCreated { topic: String },
    TopicDeleted { topic: String },
    TopicConfigChanged { topic: String },
    MessagesAppended { topic: String, count: usize },
    GroupOffsetsReset { group: String, partitions: usize },
    OffsetsCommitted { group: String, partitions: usize },
    ConnectionLost { reason: String },
}

/// The single seam between the business layer and Kafka.
///
/// `#[async_trait]` is required: plain `async fn` in a trait is not object safe, and
/// services hold `Arc<dyn KafkaDriver>` to swap mock and real at runtime.
#[async_trait]
pub trait KafkaDriver: Send + Sync + 'static {
    // ---- lifecycle -------------------------------------------------------
    async fn connect(&self, cfg: &ClusterConfig) -> Result<ClusterInfo, DriverError>;
    async fn disconnect(&self) -> Result<(), DriverError>;
    async fn is_connected(&self) -> bool;
    async fn cluster_info(&self) -> Result<ClusterInfo, DriverError>;

    // ---- topics ----------------------------------------------------------
    async fn list_topics(&self) -> Result<Vec<TopicInfo>, DriverError>;
    async fn describe_topics(&self, req: &DescribeTopicsReq) -> Result<Vec<TopicDetail>, DriverError>;
    async fn create_topic(&self, req: &CreateTopicReq) -> Result<(), DriverError>;
    async fn delete_topics(&self, req: &DeleteTopicsReq) -> Result<Vec<TopicDeleteResult>, DriverError>;
    async fn update_topic_config(&self, req: &UpdateTopicConfigReq) -> Result<(), DriverError>;
    async fn list_offsets(&self, req: &ListOffsetsReq) -> Result<Vec<OffsetRange>, DriverError>;

    // ---- produce / consume ----------------------------------------------
    async fn produce(&self, req: ProduceReq) -> Result<Vec<ProduceResult>, DriverError>;
    async fn subscribe(&self, req: SubscribeReq) -> Result<MessageStream, DriverError>;

    // ---- consumer groups -------------------------------------------------
    async fn list_consumer_groups(&self) -> Result<Vec<GroupInfo>, DriverError>;
    async fn describe_consumer_groups(&self, ids: &[String]) -> Result<Vec<GroupDetail>, DriverError>;
    async fn list_group_offsets(&self, req: &ListGroupOffsetsReq) -> Result<Vec<GroupOffsetEntry>, DriverError>;
    async fn reset_offsets(&self, req: &ResetOffsetsReq) -> Result<ResetOffsetsResult, DriverError>;
    async fn commit_offsets(&self, req: CommitOffsetsReq) -> Result<(), DriverError>;

    // ---- introspection ---------------------------------------------------
    fn capabilities(&self) -> DriverCapabilities;
    fn kind(&self) -> DriverKind;

    /// Mock control seam: downcast to the concrete driver.
    ///
    /// The mock control plane (`MockKafkaDriver::reset`, `pin_rng_seed`,
    /// `faults_handle`) is intentionally not on this trait, yet the bridge holds
    /// only `DynDriver` and `mock.*` commands need to reach it. Overridden by
    /// `MockKafkaDriver` to return `Some(self)`; every real driver keeps the
    /// default and must answer `None`.
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        None
    }

    /// Subscription to domain events. `None` when the driver does not emit them.
    async fn events(&self) -> Option<tokio::sync::broadcast::Receiver<DriverEvent>> {
        None
    }
}

/// Shared handle used across services.
pub type DynDriver = Arc<dyn KafkaDriver>;

/// Builds the driver for a cluster config: mock when `cfg.mock`, real otherwise.
pub fn driver_for(cfg: &ClusterConfig) -> DynDriver {
    if cfg.mock {
        Arc::new(mock::MockKafkaDriver::new())
    } else {
        #[cfg(feature = "real")]
        {
            Arc::new(real::RealKafkaDriver::new())
        }
        #[cfg(not(feature = "real"))]
        {
            Arc::new(PlaceholderRealDriver)
        }
    }
}

/// Compiled in when the `real` feature is off, so a non-mock cluster fails with a
/// clear message instead of a missing-method panic.
#[cfg(not(feature = "real"))]
struct PlaceholderRealDriver;

#[cfg(not(feature = "real"))]
const NOT_BUILT: &str = "real Kafka driver is not compiled in; rebuild with --features real";

#[cfg(not(feature = "real"))]
#[async_trait]
impl KafkaDriver for PlaceholderRealDriver {
    async fn connect(&self, _cfg: &ClusterConfig) -> Result<ClusterInfo, DriverError> {
        Err(DriverError::unsupported(NOT_BUILT))
    }
    async fn disconnect(&self) -> Result<(), DriverError> {
        Err(DriverError::unsupported(NOT_BUILT))
    }
    async fn is_connected(&self) -> bool {
        false
    }
    async fn cluster_info(&self) -> Result<ClusterInfo, DriverError> {
        Err(DriverError::unsupported(NOT_BUILT))
    }
    async fn list_topics(&self) -> Result<Vec<TopicInfo>, DriverError> {
        Err(DriverError::unsupported(NOT_BUILT))
    }
    async fn describe_topics(&self, _req: &DescribeTopicsReq) -> Result<Vec<TopicDetail>, DriverError> {
        Err(DriverError::unsupported(NOT_BUILT))
    }
    async fn create_topic(&self, _req: &CreateTopicReq) -> Result<(), DriverError> {
        Err(DriverError::unsupported(NOT_BUILT))
    }
    async fn delete_topics(&self, _req: &DeleteTopicsReq) -> Result<Vec<TopicDeleteResult>, DriverError> {
        Err(DriverError::unsupported(NOT_BUILT))
    }
    async fn update_topic_config(&self, _req: &UpdateTopicConfigReq) -> Result<(), DriverError> {
        Err(DriverError::unsupported(NOT_BUILT))
    }
    async fn list_offsets(&self, _req: &ListOffsetsReq) -> Result<Vec<OffsetRange>, DriverError> {
        Err(DriverError::unsupported(NOT_BUILT))
    }
    async fn produce(&self, _req: ProduceReq) -> Result<Vec<ProduceResult>, DriverError> {
        Err(DriverError::unsupported(NOT_BUILT))
    }
    async fn subscribe(&self, _req: SubscribeReq) -> Result<MessageStream, DriverError> {
        Err(DriverError::unsupported(NOT_BUILT))
    }
    async fn list_consumer_groups(&self) -> Result<Vec<GroupInfo>, DriverError> {
        Err(DriverError::unsupported(NOT_BUILT))
    }
    async fn describe_consumer_groups(&self, _ids: &[String]) -> Result<Vec<GroupDetail>, DriverError> {
        Err(DriverError::unsupported(NOT_BUILT))
    }
    async fn list_group_offsets(&self, _req: &ListGroupOffsetsReq) -> Result<Vec<GroupOffsetEntry>, DriverError> {
        Err(DriverError::unsupported(NOT_BUILT))
    }
    async fn reset_offsets(&self, _req: &ResetOffsetsReq) -> Result<ResetOffsetsResult, DriverError> {
        Err(DriverError::unsupported(NOT_BUILT))
    }
    async fn commit_offsets(&self, _req: CommitOffsetsReq) -> Result<(), DriverError> {
        Err(DriverError::unsupported(NOT_BUILT))
    }
    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities::default()
    }
    fn kind(&self) -> DriverKind {
        DriverKind::Real
    }
}
