//! The rdkafka-backed driver, compiled only under `--features real`.
//!
//! T070 lands the skeleton: config mapping (see [`config`]) and the connection
//! lifecycle (`connect`/`disconnect`/`is_connected`/`cluster_info`). T071a adds
//! `list_topics` and the per-operation session plumbing, T071b adds
//! `describe_topics` (see [`topic`]), T072a adds topic create/delete and T072b the
//! config writer (see [`admin`]), and T073 adds batch production (see [`producer`]).
//! T074a adds offset lookups (see [`consumer`]) and T074b the subscription stream,
//! T075a consumer-group metadata (see [`group`]), T075b their committed offsets and
//! T075c the offset writers. Every trait method is implemented; T076 only widens the
//! config mapping (`config`), so its card is gated by the same `cargo check`.
//!
//! librdkafka's client API is blocking, so calls from here run in `spawn_blocking`.

mod admin;
mod config;
mod consumer;
mod group;
mod producer;
mod topic;

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

/// A live connection: the admin client (kept alive so its background threads run),
/// the producer every batch is enqueued into, the config it was opened with, and
/// the metadata snapshot from `connect`.
struct Session {
    admin: Arc<AdminClient<DefaultClientContext>>,
    producer: producer::Producer,
    cfg: ClusterConfig,
    info: ClusterInfo,
}

/// What a domain operation needs from a live session: the clients to call, the
/// config whose timeouts bound them, and the broker count `connect` saw (used by
/// the create-topic validation so it costs no extra metadata round trip; R38).
/// Taken once per operation so no method holds the session lock across an await.
pub(super) struct Live {
    pub(super) admin: Arc<AdminClient<DefaultClientContext>>,
    pub(super) producer: producer::Producer,
    pub(super) cfg: ClusterConfig,
    pub(super) brokers: usize,
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

    /// Snapshot of the live session, or `NotConnected` naming the cluster.
    pub(super) async fn live(&self) -> Result<Live, DriverError> {
        match self.session.read().await.as_ref() {
            Some(session) => Ok(Live {
                admin: session.admin.clone(),
                producer: session.producer.clone(),
                cfg: session.cfg.clone(),
                brokers: session.info.brokers.len(),
            }),
            None => Err(DriverError::NotConnected(self.name.read().await.clone())),
        }
    }
}

/// Turns a failed metadata fetch into the right taxonomy entry: a broker that never
/// answered is a `Timeout`, anything else is a `Connection` failure.
fn metadata_error(server: &str, operation: &str, timeout: Duration, err: KafkaError) -> DriverError {
    if matches!(err, KafkaError::MetadataFetch(RDKafkaErrorCode::RequestTimedOut)) {
        return DriverError::timeout(operation, timeout.as_millis() as u64);
    }
    DriverError::Connection { server: server.to_string(), message: err.to_string() }
}

/// Fetches a metadata snapshot; librdkafka's client is blocking, so it runs on the
/// blocking pool (R31). `topic` narrows the request to one topic.
pub(super) async fn fetch_metadata(
    live: &Live,
    topic: Option<&str>,
) -> Result<Metadata, DriverError> {
    let admin = live.admin.clone();
    let timeout = Duration::from_millis(live.cfg.timeouts.metadata_ms.max(1));
    let server = live.cfg.bootstrap_joined();
    let topic = topic.map(str::to_string);
    tokio::task::spawn_blocking(move || admin.inner().fetch_metadata(topic.as_deref(), timeout))
        .await
        .map_err(|e| DriverError::other(format!("metadata task failed: {e}")))?
        .map_err(|e| metadata_error(&server, "fetch_metadata", timeout, e))
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
        let producer =
            producer::producer_config(cfg)?.create::<producer::Producer>().map_err(|e| {
                DriverError::Connection { server: cfg.bootstrap_joined(), message: e.to_string() }
            })?;

        let probe = Live { admin: Arc::new(admin), producer, cfg: cfg.clone(), brokers: 0 };
        let started = Instant::now();
        let metadata = fetch_metadata(&probe, None).await?;

        let info = cluster_info_from(&metadata, cfg, started.elapsed());
        *self.name.write().await = cfg.name.clone();
        *self.session.write().await = Some(Session {
            admin: probe.admin,
            producer: probe.producer,
            cfg: cfg.clone(),
            info: info.clone(),
        });

        tracing::info!(cluster = %cfg.name, brokers = info.brokers.len(), "real cluster connected");
        Ok(info)
    }

    async fn disconnect(&self) -> Result<(), DriverError> {
        let session = self.session.write().await.take();
        if let Some(session) = session {
            let admin = session.admin;
            let producer = session.producer;
            // librdkafka tears down internal threads on drop, which blocks: hand it
            // to the blocking pool rather than stalling the runtime.
            let _ = tokio::task::spawn_blocking(move || drop((admin, producer))).await;
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
        let live = self.live().await?;
        let metadata = fetch_metadata(&live, None).await?;
        Ok(topic::infos(&metadata))
    }

    async fn describe_topics(&self, req: &DescribeTopicsReq) -> Result<Vec<TopicDetail>, DriverError> {
        let live = self.live().await?;
        // The metadata snapshot is consumed before the awaits below: librdkafka's
        // metadata is not `Send`, so it must not cross a suspension point (R35).
        let metadata = fetch_metadata(&live, None).await?;
        let planned = topic::plan(metadata, req)?;
        topic::assemble(&live, planned).await
    }

    async fn create_topic(&self, req: &CreateTopicReq) -> Result<(), DriverError> {
        self.real_create_topic(req).await
    }

    async fn delete_topics(&self, req: &DeleteTopicsReq) -> Result<Vec<TopicDeleteResult>, DriverError> {
        self.real_delete_topics(req).await
    }

    async fn update_topic_config(&self, req: &UpdateTopicConfigReq) -> Result<(), DriverError> {
        self.real_update_topic_config(req).await
    }

    async fn list_offsets(&self, req: &ListOffsetsReq) -> Result<Vec<OffsetRange>, DriverError> {
        self.real_list_offsets(req).await
    }

    async fn produce(&self, req: ProduceReq) -> Result<Vec<ProduceResult>, DriverError> {
        self.real_produce(req).await
    }

    async fn subscribe(&self, req: SubscribeReq) -> Result<MessageStream, DriverError> {
        self.real_subscribe(req).await
    }

    async fn list_consumer_groups(&self) -> Result<Vec<GroupInfo>, DriverError> {
        let live = self.live().await?;
        group::list(&live).await
    }

    async fn describe_consumer_groups(&self, ids: &[String]) -> Result<Vec<GroupDetail>, DriverError> {
        let live = self.live().await?;
        group::describe(&live, ids).await
    }

    async fn list_group_offsets(
        &self,
        req: &ListGroupOffsetsReq,
    ) -> Result<Vec<GroupOffsetEntry>, DriverError> {
        let live = self.live().await?;
        group::offsets(&live, req).await
    }

    async fn reset_offsets(
        &self,
        req: &ResetOffsetsReq,
    ) -> Result<ResetOffsetsResult, DriverError> {
        let live = self.live().await?;
        group::reset(&live, req).await
    }

    async fn commit_offsets(&self, req: CommitOffsetsReq) -> Result<(), DriverError> {
        let live = self.live().await?;
        group::commit(&live, req).await
    }

    /// Only what the landed cards implement; each card flips on what it lands
    /// (T071–T076).
    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            admin_api: true,
            topic_config_write: true,
            consumer_groups: true,
            offset_reset: true,
            offset_commit: true,
            sasl: true,
            ssl: true,
            ..DriverCapabilities::default()
        }
    }

    fn kind(&self) -> DriverKind {
        DriverKind::Real
    }
}
