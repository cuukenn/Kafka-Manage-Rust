//! The rdkafka-backed driver, compiled only under `--features real`.
//!
//! T070 lands the skeleton: config mapping (see [`config`]) and the connection
//! lifecycle (`connect`/`disconnect`/`is_connected`/`cluster_info`). Domain
//! methods answer `Unsupported` until T071–T076 fill them in, so each of those
//! cards is gated by the same `cargo check --features real`.
//!
//! librdkafka's client API is blocking, so calls from here run in `spawn_blocking`.

mod config;

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::metadata::Metadata;
use tokio::sync::RwLock;

use kafka_core::cluster::{BrokerInfo, ClusterConfig, ClusterInfo};
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

pub use config::client_config;

use crate::driver::{DriverCapabilities, DriverKind, KafkaDriver};

/// A live connection: the admin client (kept alive so its background threads run)
/// plus the metadata snapshot from `connect`.
struct Session {
    admin: Arc<AdminClient<DefaultClientContext>>,
    info: ClusterInfo,
}

/// A driver backed by a real Kafka cluster via librdkafka.
pub struct RealKafkaDriver {
    session: RwLock<Option<Session>>,
    /// Remembered across a `disconnect` so [`DriverError::NotConnected`] can still
    /// name the cluster the caller asked about.
    name: RwLock<String>,
}

impl Default for RealKafkaDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl RealKafkaDriver {
    pub fn new() -> Self {
        Self { session: RwLock::new(None), name: RwLock::new(String::new()) }
    }

    fn require_unsupported(&self, what: &str) -> DriverError {
        DriverError::unsupported(format!("real driver: {what}"))
    }
}

/// Turns a failed metadata fetch into the right taxonomy entry: a broker that never
/// answered is a `Timeout`, anything else is a `Connection` failure.
fn metadata_error(cfg: &ClusterConfig, timeout: Duration, err: KafkaError) -> DriverError {
    if matches!(err, KafkaError::MetadataFetch(RDKafkaErrorCode::RequestTimedOut)) {
        return DriverError::timeout("connect", timeout.as_millis() as u64);
    }
    DriverError::Connection { server: cfg.bootstrap_joined(), message: err.to_string() }
}

/// Best-effort [`ClusterInfo`] from a metadata response.
///
/// librdkafka's metadata carries brokers but neither the cluster id nor the
/// controller id, so both stay `None` (R30).
fn cluster_info_from(metadata: &Metadata, cfg: &ClusterConfig, latency: Duration) -> ClusterInfo {
    let brokers = metadata
        .brokers()
        .iter()
        .map(|b| BrokerInfo {
            node_id: b.id(),
            host: b.host().to_string(),
            port: u16::try_from(b.port()).unwrap_or_default(),
            rack: None,
        })
        .collect();
    ClusterInfo {
        cluster_id: None,
        controller_id: None,
        brokers,
        kafka_version: cfg.extra.get("broker.version").cloned(),
        latency_ms: Some(latency.as_millis() as u64),
    }
}

#[async_trait]
impl KafkaDriver for RealKafkaDriver {
    async fn connect(&self, cfg: &ClusterConfig) -> Result<ClusterInfo, DriverError> {
        let conf = config::client_config(cfg)?;
        let admin = conf.create::<AdminClient<DefaultClientContext>>().map_err(|e| {
            DriverError::Connection { server: cfg.bootstrap_joined(), message: e.to_string() }
        })?;
        let admin = Arc::new(admin);

        let timeout = Duration::from_millis(cfg.timeouts.metadata_ms.max(1));
        let fetched = admin.clone();
        let started = Instant::now();
        let metadata = tokio::task::spawn_blocking(move || fetched.inner().fetch_metadata(None, timeout))
            .await
            .map_err(|e| DriverError::other(format!("metadata task failed: {e}")))?
            .map_err(|e| metadata_error(cfg, timeout, e))?;

        let info = cluster_info_from(&metadata, cfg, started.elapsed());
        *self.name.write().await = cfg.name.clone();
        *self.session.write().await = Some(Session { admin, info: info.clone() });

        tracing::info!(cluster = %cfg.name, brokers = info.brokers.len(), "real cluster connected");
        Ok(info)
    }

    async fn disconnect(&self) -> Result<(), DriverError> {
        let session = self.session.write().await.take();
        if let Some(session) = session {
            let admin = session.admin;
            // librdkafka tears down internal threads on drop, which blocks: hand it
            // to the blocking pool rather than stalling the runtime.
            let _ = tokio::task::spawn_blocking(move || drop(admin)).await;
            tracing::info!("real cluster disconnected");
        }
        Ok(())
    }

    async fn is_connected(&self) -> bool {
        self.session.read().await.is_some()
    }

    async fn cluster_info(&self) -> Result<ClusterInfo, DriverError> {
        match self.session.read().await.as_ref() {
            Some(session) => Ok(session.info.clone()),
            None => Err(DriverError::NotConnected(self.name.read().await.clone())),
        }
    }

    async fn list_topics(&self) -> Result<Vec<TopicInfo>, DriverError> {
        Err(self.require_unsupported("list_topics lands in T071"))
    }

    async fn describe_topics(&self, _req: &DescribeTopicsReq) -> Result<Vec<TopicDetail>, DriverError> {
        Err(self.require_unsupported("describe_topics lands in T071"))
    }

    async fn create_topic(&self, _req: &CreateTopicReq) -> Result<(), DriverError> {
        Err(self.require_unsupported("create_topic lands in T072"))
    }

    async fn delete_topics(&self, _req: &DeleteTopicsReq) -> Result<Vec<TopicDeleteResult>, DriverError> {
        Err(self.require_unsupported("delete_topics lands in T072"))
    }

    async fn update_topic_config(&self, _req: &UpdateTopicConfigReq) -> Result<(), DriverError> {
        Err(self.require_unsupported("update_topic_config lands in T072"))
    }

    async fn list_offsets(&self, _req: &ListOffsetsReq) -> Result<Vec<OffsetRange>, DriverError> {
        Err(self.require_unsupported("list_offsets lands in T074"))
    }

    async fn produce(&self, _req: ProduceReq) -> Result<Vec<ProduceResult>, DriverError> {
        Err(self.require_unsupported("produce lands in T073"))
    }

    async fn subscribe(&self, _req: SubscribeReq) -> Result<MessageStream, DriverError> {
        Err(self.require_unsupported("subscribe lands in T074"))
    }

    async fn list_consumer_groups(&self) -> Result<Vec<GroupInfo>, DriverError> {
        Err(self.require_unsupported("list_consumer_groups lands in T075"))
    }

    async fn describe_consumer_groups(&self, _ids: &[String]) -> Result<Vec<GroupDetail>, DriverError> {
        Err(self.require_unsupported("describe_consumer_groups lands in T075"))
    }

    async fn list_group_offsets(
        &self,
        _req: &ListGroupOffsetsReq,
    ) -> Result<Vec<GroupOffsetEntry>, DriverError> {
        Err(self.require_unsupported("list_group_offsets lands in T075"))
    }

    async fn reset_offsets(&self, _req: &ResetOffsetsReq) -> Result<ResetOffsetsResult, DriverError> {
        Err(self.require_unsupported("reset_offsets lands in T075"))
    }

    async fn commit_offsets(&self, _req: CommitOffsetsReq) -> Result<(), DriverError> {
        Err(self.require_unsupported("commit_offsets lands in T075"))
    }

    /// Only what this card implements; each card flips on what it lands (T071–T076).
    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities::default()
    }

    fn kind(&self) -> DriverKind {
        DriverKind::Real
    }
}
