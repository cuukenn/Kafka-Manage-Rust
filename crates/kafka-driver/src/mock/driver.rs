//! In-memory Kafka driver: full topic/produce/consume/group semantics with no
//! external services, plus latency and fault injection for UI hardening.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use tokio::sync::{broadcast, RwLock};
use tokio_stream::wrappers::ReceiverStream;

use kafka_core::cluster::{BrokerInfo, ClusterConfig, ClusterInfo};
use kafka_core::error::DriverError;
use kafka_core::group::{
    CommitOffsetsReq, GroupDetail, GroupInfo, GroupOffsetEntry, ListGroupOffsetsReq, ResetOffsetsReq,
    ResetOffsetsResult, ResetSpec,
};
use kafka_core::message::{
    MessageStream, OffsetStart, ProduceReq, ProduceResult, StreamControls, SubscribeReq,
};
use kafka_core::protocol::now_ms;
use kafka_core::topic::{
    CreateTopicReq, DeleteTopicsReq, DescribeTopicsReq, ListOffsetsReq, OffsetRange, OffsetSpec,
    TopicDeleteResult, TopicDetail, TopicInfo, TopicPartition, UpdateTopicConfigReq,
};

use crate::driver::{DriverCapabilities, DriverEvent, DriverKind, KafkaDriver};

use super::fault::{FaultInjection, Rng};
use super::pump::{self, PumpOptions};
use super::state::{CommittedOffset, MockGroup, MockMessage, MockState, default_brokers, split_tp_key, tp_key};

/// Broker error code Kafka uses when a partition has no leader.
const LEADER_NOT_AVAILABLE: i16 = 5;

/// A mock Kafka cluster. One instance per mock cluster config.
#[derive(Debug, Clone)]
pub struct MockKafkaDriver {
    state: Arc<RwLock<MockState>>,
    faults: Arc<RwLock<FaultInjection>>,
    seed: Arc<AtomicU64>,
    /// Set by `pin_rng_seed()`: skips the wall-clock mix in `rng()` (T078).
    rng_pinned: Arc<AtomicBool>,
    /// Set by `seeded()` / `request_seed()`, consumed by `connect`.
    will_seed: Arc<AtomicBool>,
    events: broadcast::Sender<DriverEvent>,
}

impl Default for MockKafkaDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl MockKafkaDriver {
    /// Empty cluster: three brokers, no topics, no groups.
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(256);
        Self {
            state: Arc::new(RwLock::new(MockState::default())),
            faults: Arc::new(RwLock::new(FaultInjection::default())),
            seed: Arc::new(AtomicU64::new(now_ms() as u64)),
            rng_pinned: Arc::new(AtomicBool::new(false)),
            will_seed: Arc::new(AtomicBool::new(false)),
            events,
        }
    }

    /// Cluster pre-loaded with fixture topics, messages and consumer groups.
    ///
    /// Fixtures load on the next `connect`, because the cluster id and broker list
    /// come from the config the user actually saved.
    pub fn seeded() -> Self {
        let driver = Self::new();
        driver.request_seed();
        driver
    }

    pub fn state_handle(&self) -> Arc<RwLock<MockState>> {
        self.state.clone()
    }

    pub fn faults_handle(&self) -> Arc<RwLock<FaultInjection>> {
        self.faults.clone()
    }

    pub async fn faults(&self) -> FaultInjection {
        self.faults.read().await.clone()
    }

    pub async fn set_faults(&self, faults: FaultInjection) {
        tracing::debug!(?faults, "mock fault injection updated");
        *self.faults.write().await = faults;
    }

    /// Drop every topic, group and injected fault. Connection state is kept.
    pub async fn reset(&self) {
        let mut state = self.state.write().await;
        state.topics.clear();
        state.groups.clear();
        state.round_robin = 0;
        *self.faults.write().await = FaultInjection::default();
        tracing::info!("mock cluster reset");
    }

    /// Load the fixture dataset (idempotent: existing topics are left alone).
    pub async fn seed(&self) {
        let mut state = self.state.write().await;
        seed_fixtures(&mut state);
    }

    /// JSON snapshot of the whole mock cluster, for SQLite persistence.
    pub async fn snapshot_json(&self) -> Result<String, DriverError> {
        let state = self.state.read().await;
        serde_json::to_string(&*state)
            .map_err(|e| DriverError::other(format!("mock snapshot failed: {e}")))
    }

    /// Restore the DATA portion of a previously taken snapshot (T079): `topics`,
    /// `groups`, `round_robin` and `version` come from the snapshot, while
    /// identity/connection fields (`connected`/`config`/`brokers`/`cluster_id`/
    /// `controller_id`) and the live `active_pumps` counter keep the running
    /// driver's current values — restoring after a config edit must not
    /// resurrect stale brokers nor a phantom `connected` flag. RNG pin state
    /// (`seed`/`rng_pinned`) lives on the driver itself, outside `MockState`,
    /// so a restore never touches it either (live config, not data).
    pub async fn restore_json(&self, json: &str) -> Result<(), DriverError> {
        let restored: MockState = serde_json::from_str(json)
            .map_err(|e| DriverError::other(format!("mock restore failed: {e}")))?;
        // One write lock: the data swap must be atomic for concurrent readers.
        let mut state = self.state.write().await;
        // Persisted data ← snapshot (see the field partition on `MockState`).
        state.topics = restored.topics;
        state.groups = restored.groups;
        state.round_robin = restored.round_robin;
        state.version = restored.version;
        // Live identity/connection stays untouched: `cluster_id`, `controller_id`,
        // `brokers`, `connected`, `config`.
        // Live pump counter stays untouched: pumps spawned before this restore
        // decrement the CURRENT Arc, so installing a fresh one would make
        // `active_pumps()` under-report (nobody ever decrements the new Arc).
        Ok(())
    }

    fn rng(&self) -> Rng {
        let counter = self.seed.fetch_add(0x9E37_79B9, Ordering::Relaxed);
        if self.rng_pinned.load(Ordering::Relaxed) {
            // Pinned (test determinism, T078): the counter alone derives the roll,
            // so the same pinned seed replays the same sequence on any instance.
            Rng::new(counter)
        } else {
            Rng::new(counter ^ (now_ms() as u64).rotate_left(17))
        }
    }

    /// Applies injected latency then injected failure for `operation`.
    async fn apply_faults(&self, operation: &str) -> Result<(), DriverError> {
        let (latency_ms, injected) = {
            let faults = self.faults.read().await;
            if faults.all_off() {
                return Ok(());
            }
            let mut rng = self.rng();
            (faults.latency_ms(&mut rng), faults.check(operation, &mut rng))
        };
        if latency_ms > 0 {
            tokio::time::sleep(Duration::from_millis(latency_ms)).await;
        }
        match injected {
            Some(err) => {
                tracing::warn!(operation, err = %err, "mock fault injected");
                Err(err)
            }
            None => Ok(()),
        }
    }

    async fn require_connected(&self) -> Result<(), DriverError> {
        let state = self.state.read().await;
        if state.connected {
            Ok(())
        } else {
            let name = state.config.as_ref().map(|c| c.name.clone()).unwrap_or_else(|| "mock".to_string());
            Err(DriverError::NotConnected(name))
        }
    }

    async fn poll_ms(&self) -> u64 {
        self.state
            .read()
            .await
            .config
            .as_ref()
            .map(|c| c.timeouts.poll_ms)
            .unwrap_or(200)
            .max(1)
    }

    fn emit(&self, event: DriverEvent) {
        let _ = self.events.send(event);
    }

    async fn leader_down(&self, topic: &str, partition: u32) -> bool {
        self.faults.read().await.is_leader_down(topic, partition)
    }

    fn leader_not_available(topic: &str, partition: u32) -> DriverError {
        DriverError::Broker {
            code: LEADER_NOT_AVAILABLE,
            name: "LEADER_NOT_AVAILABLE".into(),
            message: format!("leader for {topic}-{partition} is not available (injected)"),
        }
    }
}

impl MockKafkaDriver {
    /// Marks this instance to load fixtures on next connect.
    pub fn request_seed(&self) {
        self.will_seed.store(true, Ordering::SeqCst);
    }

    /// True when a fixture load is still pending (consumed by `connect`).
    pub fn seed_pending(&self) -> bool {
        self.will_seed.load(Ordering::SeqCst)
    }

    /// Pins the fault-injection RNG to `seed` **for test determinism**: the roll
    /// counter restarts at `seed` and `rng()` stops mixing in the wall clock, so
    /// the same seed replays the same latency/failure sequence on any instance —
    /// including mid-range probabilities (`0.0 < p < 1.0`). Unpinned drivers keep
    /// the wall-clock mix and behave exactly as before (T078).
    pub fn pin_rng_seed(&self, seed: u64) {
        self.seed.store(seed, Ordering::SeqCst);
        self.rng_pinned.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl KafkaDriver for MockKafkaDriver {
    async fn connect(&self, cfg: &ClusterConfig) -> Result<ClusterInfo, DriverError> {
        cfg.validate()?;
        self.apply_faults("connect").await?;

        let mut rng = self.rng();
        let injected = {
            let faults = self.faults.read().await;
            if faults.fail_connect {
                Some(DriverError::Connection {
                    server: cfg.bootstrap_joined(),
                    message: if faults.connect_message.is_empty() {
                        "connection refused by mock cluster (injected)".into()
                    } else {
                        faults.connect_message.clone()
                    },
                })
            } else {
                faults.check("connect", &mut rng)
            }
        };
        if let Some(err) = injected {
            return Err(err);
        }

        let brokers = brokers_from_config(cfg);
        let controller = brokers.iter().map(|b| b.node_id).min().unwrap_or(1);
        let cluster_id = format!("mock-{}", slug(&cfg.name));

        let (should_seed, version) = {
            let mut state = self.state.write().await;
            state.brokers = brokers.clone();
            state.controller_id = controller;
            state.connected = true;
            state.config = Some(cfg.clone());
            state.cluster_id = cluster_id.clone();
            let should_seed = self.will_seed.swap(false, Ordering::SeqCst);
            if should_seed {
                seed_fixtures(&mut state);
            }
            (should_seed, state.version.clone())
        };
        if should_seed {
            self.emit(DriverEvent::TopicCreated { topic: "*".into() });
        }

        tracing::info!(cluster = %cfg.name, brokers = brokers.len(), "mock cluster connected");
        Ok(ClusterInfo {
            cluster_id: Some(cluster_id),
            controller_id: Some(controller),
            brokers,
            kafka_version: Some(version),
            latency_ms: Some(0),
        })
    }

    async fn disconnect(&self) -> Result<(), DriverError> {
        let mut state = self.state.write().await;
        if !state.connected {
            return Ok(());
        }
        state.connected = false;
        tracing::info!("mock cluster disconnected");
        Ok(())
    }

    async fn is_connected(&self) -> bool {
        self.state.read().await.connected
    }

    async fn cluster_info(&self) -> Result<ClusterInfo, DriverError> {
        let state = self.state.read().await;
        if !state.connected {
            return Err(DriverError::NotConnected(
                state.config.as_ref().map(|c| c.name.clone()).unwrap_or_else(|| "mock".into()),
            ));
        }
        Ok(ClusterInfo {
            cluster_id: Some(state.cluster_id.clone()),
            controller_id: Some(state.controller_id),
            brokers: state.brokers.clone(),
            kafka_version: Some(state.version.clone()),
            latency_ms: Some(0),
        })
    }

    async fn list_topics(&self) -> Result<Vec<TopicInfo>, DriverError> {
        self.require_connected().await?;
        self.apply_faults("list_topics").await?;
        let state = self.state.read().await;
        Ok(state
            .topics
            .values()
            .map(|t| TopicInfo {
                name: t.name.clone(),
                internal: t.internal,
                partition_count: t.partition_count(),
                replication_factor: t.replication_factor(),
                configs: t.configs.clone(),
            })
            .collect())
    }

    async fn describe_topics(&self, req: &DescribeTopicsReq) -> Result<Vec<TopicDetail>, DriverError> {
        self.require_connected().await?;
        self.apply_faults("describe_topics").await?;
        let state = self.state.read().await;

        let names: Vec<String> = if req.names.is_empty() {
            state
                .topics
                .keys()
                .filter(|name| req.include_internal || !state.topics[*name].internal)
                .cloned()
                .collect()
        } else {
            req.names.clone()
        };

        let mut out = Vec::with_capacity(names.len());
        for name in names {
            let topic = state.topics.get(&name).ok_or_else(|| DriverError::TopicNotFound(name.clone()))?;
            if topic.internal && !req.include_internal && req.names.contains(&name) {
                // Explicitly asked for an internal topic: allowed.
            }
            out.push(TopicDetail {
                name: topic.name.clone(),
                internal: topic.internal,
                partitions: topic.partitions.iter().map(|p| p.detail()).collect(),
                configs: topic.configs.clone(),
            });
        }
        Ok(out)
    }

    async fn create_topic(&self, req: &CreateTopicReq) -> Result<(), DriverError> {
        self.require_connected().await?;
        self.apply_faults("create_topic").await?;
        let broker_count = self.state.read().await.broker_count();
        req.validate(broker_count)?;

        let mut state = self.state.write().await;
        if state.has_topic(&req.name) {
            return Err(DriverError::TopicAlreadyExists(req.name.clone()));
        }
        let topic = state.insert_topic(
            &req.name,
            req.num_partitions,
            req.replication_factor,
            &req.replica_assignment,
            req.configs.clone(),
        );
        state.topics.insert(req.name.clone(), topic);
        drop(state);

        self.emit(DriverEvent::TopicCreated { topic: req.name.clone() });
        tracing::info!(topic = %req.name, partitions = req.num_partitions, "mock topic created");
        Ok(())
    }

    async fn delete_topics(&self, req: &DeleteTopicsReq) -> Result<Vec<TopicDeleteResult>, DriverError> {
        self.require_connected().await?;
        self.apply_faults("delete_topics").await?;
        let mut state = self.state.write().await;
        let mut results = Vec::with_capacity(req.names.len());
        let mut deleted = Vec::new();
        for name in &req.names {
            match state.topics.get(name) {
                None => results.push(TopicDeleteResult {
                    name: name.clone(),
                    error: Some(format!("unknown topic {name}")),
                }),
                Some(topic) if topic.internal => results.push(TopicDeleteResult {
                    name: name.clone(),
                    error: Some(format!("refusing to delete internal topic {name}")),
                }),
                Some(_) => {
                    state.topics.remove(name);
                    results.push(TopicDeleteResult { name: name.clone(), error: None });
                    deleted.push(name.clone());
                }
            }
        }
        drop(state);
        for topic in deleted {
            self.emit(DriverEvent::TopicDeleted { topic });
        }
        Ok(results)
    }

    async fn update_topic_config(&self, req: &UpdateTopicConfigReq) -> Result<(), DriverError> {
        self.require_connected().await?;
        self.apply_faults("update_topic_config").await?;
        if req.topic.trim().is_empty() {
            return Err(DriverError::invalid("topic", "topic must not be empty"));
        }
        let mut state = self.state.write().await;
        let topic = state.topics.get_mut(&req.topic).ok_or_else(|| DriverError::TopicNotFound(req.topic.clone()))?;
        for (key, value) in &req.set {
            if key.trim().is_empty() {
                return Err(DriverError::invalid("set", "config key must not be empty"));
            }
            topic.configs.insert(key.clone(), value.clone());
        }
        for key in &req.unset {
            topic.configs.remove(key);
        }
        drop(state);
        self.emit(DriverEvent::TopicConfigChanged { topic: req.topic.clone() });
        Ok(())
    }

    async fn list_offsets(&self, req: &ListOffsetsReq) -> Result<Vec<OffsetRange>, DriverError> {
        self.require_connected().await?;
        self.apply_faults("list_offsets").await?;
        let state = self.state.read().await;

        let targets: Vec<TopicPartition> = if req.partitions.is_empty() {
            state
                .topics
                .values()
                .flat_map(|t| t.partitions.iter().map(|p| TopicPartition::new(&t.name, p.id)))
                .collect()
        } else {
            req.partitions.clone()
        };

        let mut out = Vec::with_capacity(targets.len());
        for tp in targets {
            let topic =
                state.topics.get(&tp.topic).ok_or_else(|| DriverError::TopicNotFound(tp.topic.clone()))?;
            let Some(partition) = topic.find_partition(tp.partition) else {
                return Err(DriverError::PartitionNotFound { topic: tp.topic.clone(), partition: tp.partition });
            };
            let (low, high) = (partition.beginning_offset(), partition.end_offset());
            let resolved = match req.spec {
                OffsetSpec::Earliest => Some(low),
                OffsetSpec::Latest => Some(high),
                OffsetSpec::Timestamp(ts) => Some(partition.first_offset_at_or_after(ts)),
            };
            out.push(OffsetRange {
                topic: tp.topic.clone(),
                partition: tp.partition,
                low,
                high,
                resolved_offset: resolved,
            });
        }
        Ok(out)
    }

    async fn produce(&self, req: ProduceReq) -> Result<Vec<ProduceResult>, DriverError> {
        req.validate()?;
        self.require_connected().await?;
        self.apply_faults("produce").await?;

        let mut state = self.state.write().await;
        // Partition ids and the round-robin picks are reserved before the topic is borrowed
        // mutably: advancing the cursor needs `&mut state`, which cannot coexist with the
        // `get_mut` borrow below. Only messages without an explicit partition consume a pick.
        let ids = state
            .topics
            .get(&req.topic)
            .ok_or_else(|| DriverError::TopicNotFound(req.topic.clone()))?
            .partition_ids();
        let picks = state.take_round_robin_picks(
            ids.len(),
            req.messages.iter().filter(|m| m.partition.is_none()).count(),
        );
        let topic = state.topics.get_mut(&req.topic).ok_or_else(|| DriverError::TopicNotFound(req.topic.clone()))?;

        let mut results = Vec::with_capacity(req.messages.len());
        let mut appended = 0usize;
        let mut picks = picks.into_iter();
        for message in &req.messages {
            let partition_id = match message.partition {
                Some(p) => p,
                None => ids[picks.next().expect("one pick reserved per message without a partition")],
            };
            let Some(partition) = topic.find_partition_mut(partition_id) else {
                results.push(ProduceResult {
                    topic: req.topic.clone(),
                    partition: partition_id,
                    error: Some(format!("unknown topic {} partition {}", req.topic, partition_id),),
                    ..Default::default()
                });
                continue;
            };
            if !partition.has_leader() || self.leader_down(&req.topic, partition_id).await {
                results.push(ProduceResult {
                    topic: req.topic.clone(),
                    partition: partition_id,
                    error: Some(format!("LEADER_NOT_AVAILABLE for {}-{}", req.topic, partition_id)),
                    ..Default::default()
                });
                continue;
            }

            let timestamp = message.timestamp.unwrap_or_else(now_ms);
            let timestamp_type = if message.timestamp.is_some() {
                kafka_core::message::TimestampType::CreateTime
            } else {
                kafka_core::message::TimestampType::LogAppendTime
            };
            let offset = partition.push(MockMessage {
                key: message.key.clone(),
                value: message.value.clone(),
                headers: message.headers.clone(),
                timestamp,
                timestamp_type,
            });
            appended += 1;
            results.push(ProduceResult {
                topic: req.topic.clone(),
                partition: partition_id,
                offset,
                timestamp,
                error: None,
            });
        }
        drop(state);

        if appended > 0 {
            self.emit(DriverEvent::MessagesAppended { topic: req.topic.clone(), count: appended });
        }
        Ok(results)
    }

    async fn subscribe(&self, req: SubscribeReq) -> Result<MessageStream, DriverError> {
        req.validate()?;
        self.require_connected().await?;
        self.apply_faults("subscribe").await?;

        let poll_ms = self.poll_ms().await;
        let mut cursors: HashMap<u32, i64> = HashMap::new();

        let targets: Vec<u32> = {
            let state = self.state.read().await;
            let topic = state.topics.get(&req.topic).ok_or_else(|| DriverError::TopicNotFound(req.topic.clone()))?;
            let targets = req.targets(&topic.partition_ids());
            for &pid in &targets {
                let partition = topic
                    .find_partition(pid)
                    .ok_or(DriverError::PartitionNotFound { topic: req.topic.clone(), partition: pid })?;
                let (low, high) = (partition.beginning_offset(), partition.end_offset());
                let start = match req.start {
                    OffsetStart::Earliest => low,
                    OffsetStart::Latest => high,
                    OffsetStart::Offset(offset) => {
                        if offset < low || offset > high {
                            return Err(DriverError::OffsetOutOfRange {
                                topic: req.topic.clone(),
                                partition: pid,
                                requested: offset,
                                low,
                                high,
                            });
                        }
                        offset
                    }
                    OffsetStart::Timestamp(ts) => partition.first_offset_at_or_after(ts),
                };
                cursors.insert(pid, start);
            }
            targets
        };

        for &pid in &targets {
            if self.leader_down(&req.topic, pid).await {
                return Err(Self::leader_not_available(&req.topic, pid));
            }
        }

        let (controls, commands) = StreamControls::channel(req.subscription_id.clone());
        let handle = pump::spawn(
            self.state.clone(),
            PumpOptions {
                topic: req.topic.clone(),
                partitions: targets,
                cursors,
                follow: req.follow,
                max_messages: req.max_messages,
                filter: req.filter.clone(),
                poll_ms,
            },
            commands,
        );

        let stream = ReceiverStream::new(handle.receiver).boxed();
        tracing::debug!(subscription = %req.subscription_id, topic = %req.topic, start = req.start.as_str(), "mock subscription opened");
        Ok(MessageStream::new(req.subscription_id.clone(), stream, controls))
    }

    async fn list_consumer_groups(&self) -> Result<Vec<GroupInfo>, DriverError> {
        self.require_connected().await?;
        self.apply_faults("list_consumer_groups").await?;
        let state = self.state.read().await;
        Ok(state
            .groups
            .values()
            .map(|g| GroupInfo {
                group_id: g.id.clone(),
                state: g.state,
                member_count: g.member_count(),
                protocol_type: g.protocol_type.clone(),
                partition_assignor: g.partition_assignor.clone(),
                coordinator_id: Some(state.controller_id),
                is_local: false,
            })
            .collect())
    }

    async fn describe_consumer_groups(&self, ids: &[String]) -> Result<Vec<GroupDetail>, DriverError> {
        self.require_connected().await?;
        self.apply_faults("describe_consumer_groups").await?;
        let state = self.state.read().await;
        let targets: Vec<String> = if ids.is_empty() {
            state.groups.keys().cloned().collect()
        } else {
            ids.to_vec()
        };
        let mut out = Vec::with_capacity(targets.len());
        for id in targets {
            let group = state.groups.get(&id).ok_or_else(|| DriverError::GroupNotFound(id.clone()))?;
            out.push(GroupDetail {
                info: GroupInfo {
                    group_id: group.id.clone(),
                    state: group.state,
                    member_count: group.member_count(),
                    protocol_type: group.protocol_type.clone(),
                    partition_assignor: group.partition_assignor.clone(),
                    coordinator_id: Some(state.controller_id),
                    is_local: false,
                },
                members: group.members.clone(),
            });
        }
        Ok(out)
    }

    async fn list_group_offsets(&self, req: &ListGroupOffsetsReq) -> Result<Vec<GroupOffsetEntry>, DriverError> {
        self.require_connected().await?;
        self.apply_faults("list_group_offsets").await?;
        if req.group_id.trim().is_empty() {
            return Err(DriverError::invalid("groupId", "group id must not be empty"));
        }
        let state = self.state.read().await;
        let group = state.groups.get(&req.group_id).ok_or_else(|| DriverError::GroupNotFound(req.group_id.clone()))?;

        let mut out = Vec::with_capacity(group.committed.len());
        for (key, committed) in &group.committed {
            let Some((topic, partition)) = split_tp_key(key) else { continue };
            if !req.topics.is_empty() && !req.topics.contains(&topic) {
                continue;
            }
            let lag = if req.with_lag {
                state
                    .topics
                    .get(&topic)
                    .and_then(|t| t.find_partition(partition))
                    .map(|p| (p.end_offset() - committed.offset).max(0))
            } else {
                None
            };
            out.push(GroupOffsetEntry {
                topic,
                partition,
                offset: committed.offset,
                metadata: committed.metadata.clone(),
                committed_at: Some(committed.committed_at),
                lag,
            });
        }
        Ok(out)
    }

    async fn reset_offsets(&self, req: &ResetOffsetsReq) -> Result<ResetOffsetsResult, DriverError> {
        req.validate()?;
        self.require_connected().await?;
        self.apply_faults("reset_offsets").await?;

        let mut state = self.state.write().await;
        let group = state.groups.get(&req.group_id).ok_or_else(|| DriverError::GroupNotFound(req.group_id.clone()))?;

        let targets: Vec<TopicPartition> = if !req.partitions.is_empty() {
            req.partitions.clone()
        } else if !req.topics.is_empty() {
            let mut list = Vec::new();
            for topic in &req.topics {
                let t = state.topics.get(topic).ok_or_else(|| DriverError::TopicNotFound(topic.clone()))?;
                list.extend(t.partition_ids().into_iter().map(|p| TopicPartition::new(topic, p)));
            }
            list
        } else {
            group
                .committed
                .keys()
                .filter_map(|k| split_tp_key(k))
                .map(|(topic, partition)| TopicPartition::new(topic, partition))
                .collect()
        };

        let mut applied = Vec::with_capacity(targets.len());
        for tp in &targets {
            let topic = state.topics.get(&tp.topic).ok_or_else(|| DriverError::TopicNotFound(tp.topic.clone()))?;
            let partition = topic
                .find_partition(tp.partition)
                .ok_or(DriverError::PartitionNotFound { topic: tp.topic.clone(), partition: tp.partition })?;
            let (low, high) = (partition.beginning_offset(), partition.end_offset());
            let offset = match req.spec {
                ResetSpec::Earliest => low,
                ResetSpec::Latest => high,
                ResetSpec::ToOffset(value) => {
                    if value < low || value > high {
                        return Err(DriverError::OffsetOutOfRange {
                            topic: tp.topic.clone(),
                            partition: tp.partition,
                            requested: value,
                            low,
                            high,
                        });
                    }
                    value
                }
                ResetSpec::ToTimestamp(ts) => partition.first_offset_at_or_after(ts),
            };
            applied.push(kafka_core::topic::TopicPartitionOffset {
                topic: tp.topic.clone(),
                partition: tp.partition,
                offset,
                metadata: String::new(),
                timestamp: None,
                leader_epoch: None,
            });
        }

        if !req.dry_run {
            let now = now_ms();
            let group = state.groups.get_mut(&req.group_id).expect("group existed above");
            for entry in &applied {
                group.committed.insert(
                    tp_key(&entry.topic, entry.partition),
                    CommittedOffset { offset: entry.offset, metadata: String::new(), committed_at: now },
                );
            }
        }
        let count = applied.len();
        drop(state);

        if !req.dry_run {
            self.emit(DriverEvent::GroupOffsetsReset { group: req.group_id.clone(), partitions: count });
        }
        Ok(ResetOffsetsResult { group_id: req.group_id.clone(), applied, dry_run: req.dry_run, error: None })
    }

    async fn commit_offsets(&self, req: CommitOffsetsReq) -> Result<(), DriverError> {
        self.require_connected().await?;
        self.apply_faults("commit_offsets").await?;
        let Some(group_id) = req.group_id.as_deref().filter(|g| !g.trim().is_empty()) else {
            return Err(DriverError::unsupported("mock commit requires an explicit group id"));
        };
        if req.offsets.is_empty() {
            return Err(DriverError::invalid("offsets", "at least one offset is required"));
        }

        let mut state = self.state.write().await;
        for entry in &req.offsets {
            let topic = state.topics.get(&entry.topic).ok_or_else(|| DriverError::TopicNotFound(entry.topic.clone()))?;
            let partition = topic
                .find_partition(entry.partition)
                .ok_or(DriverError::PartitionNotFound { topic: entry.topic.clone(), partition: entry.partition })?;
            if entry.offset < 0 || entry.offset > partition.end_offset() {
                return Err(DriverError::OffsetOutOfRange {
                    topic: entry.topic.clone(),
                    partition: entry.partition,
                    requested: entry.offset,
                    low: partition.beginning_offset(),
                    high: partition.end_offset(),
                });
            }
        }
        let group = state
            .groups
            .entry(group_id.to_string())
            .or_insert_with(|| {
                let mut g = MockGroup::new(group_id);
                g.state = kafka_core::group::GroupState::Empty;
                g
            });
        let now = now_ms();
        for entry in &req.offsets {
            group.committed.insert(
                tp_key(&entry.topic, entry.partition),
                CommittedOffset { offset: entry.offset, metadata: entry.metadata.clone(), committed_at: now },
            );
        }
        let count = req.offsets.len();
        drop(state);

        self.emit(DriverEvent::OffsetsCommitted { group: group_id.to_string(), partitions: count });
        Ok(())
    }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            admin_api: true,
            topic_config_write: true,
            consumer_groups: true,
            offset_reset: true,
            offset_commit: true,
            live_stream: true,
            server_side_filter: true,
            sasl: false,
            ssl: false,
            fault_injection: true,
            fixtures: true,
        }
    }

    fn kind(&self) -> DriverKind {
        DriverKind::Mock
    }

    /// The only driver that answers the control-plane seam: `mock.*` bridge
    /// commands (T085) reach `reset()` / `pin_rng_seed()` / `faults_handle()`
    /// by downcasting through this reference.
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }

    async fn events(&self) -> Option<broadcast::Receiver<DriverEvent>> {
        Some(self.events.subscribe())
    }
}

/// Derives the mock broker list from the saved bootstrap servers, falling back to
/// the standard three-broker layout.
fn brokers_from_config(cfg: &ClusterConfig) -> Vec<BrokerInfo> {
    if cfg.bootstrap_servers.is_empty() {
        return default_brokers();
    }
    cfg.bootstrap_servers
        .iter()
        .enumerate()
        .map(|(index, server)| {
            let host_port = server.split_once("://").map(|(_, rest)| rest).unwrap_or(server.as_str());
            let (host, port) = match host_port.rsplit_once(':') {
                Some((h, p)) => (h.to_string(), p.parse().unwrap_or(9092)),
                None => (host_port.to_string(), 9092),
            };
            BrokerInfo { node_id: index as i32 + 1, host, port, rack: None }
        })
        .collect()
}

fn slug(name: &str) -> String {
    let out: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '-' })
        .collect();
    let trimmed = out.trim_matches('-');
    if trimmed.is_empty() { "cluster".to_string() } else { trimmed.to_string() }
}

/// Fixture dataset used by the UI in mock mode: three topics, a handful of messages
/// and two consumer groups with committed offsets.
pub fn seed_fixtures(state: &mut MockState) {
    let now = now_ms();

    let mut orders = state.insert_topic(
        "orders",
        3,
        3,
        &[],
        BTreeMap::from([
            ("retention.ms".to_string(), "604800000".to_string()),
            ("cleanup.policy".to_string(), "delete".to_string()),
        ]),
    );
    for (index, (partition, key, payload)) in [
        (0u32, "customer-1", r#"{"orderId":"A-1001","total":42.5,"currency":"USD"}"#),
        (0, "customer-2", r#"{"orderId":"A-1002","total":7.99,"currency":"EUR"}"#),
        (0, "customer-1", r#"{"orderId":"A-1003","total":120.00,"currency":"USD"}"#),
        (1, "customer-3", r#"{"orderId":"A-1004","total":13.0,"currency":"USD"}"#),
        (1, "customer-2", r#"{"orderId":"A-1005","total":88.0,"currency":"GBP"}"#),
        (2, "customer-4", r#"{"orderId":"A-1006","total":5.0,"currency":"USD"}"#),
    ]
    .into_iter()
    .enumerate()
    {
        if let Some(p) = orders.find_partition_mut(partition) {
            p.push(MockMessage {
                key: Some(key.as_bytes().to_vec()),
                value: payload.as_bytes().to_vec(),
                headers: vec![
                    kafka_core::message::MessageHeader::text("content-type", "application/json"),
                    kafka_core::message::MessageHeader::text("trace-origin", "seed"),
                ],
                timestamp: now - (1000 * (60 - index as i64)),
                timestamp_type: kafka_core::message::TimestampType::CreateTime,
            });
        }
    }
    state.topics.insert("orders".into(), orders);

    let mut audit = state.insert_topic("audit-log", 1, 1, &[], BTreeMap::from([
        ("cleanup.policy".to_string(), "compact".to_string()),
        ("min.cleanable.dirty.ratio".to_string(), "0.1".to_string()),
    ]));
    if let Some(p) = audit.find_partition_mut(0) {
        for (key, payload) in [("session-1", r#"{"event":"login"}"#), ("session-1", r#"{"event":"logout"}"#)] {
            p.push(MockMessage {
                key: Some(key.as_bytes().to_vec()),
                value: payload.as_bytes().to_vec(),
                headers: Vec::new(),
                timestamp: now - 30_000,
                timestamp_type: kafka_core::message::TimestampType::CreateTime,
            });
        }
    }
    state.topics.insert("audit-log".into(), audit);

    let internal = state.insert_topic("__consumer_offsets", 3, 3, &[], BTreeMap::new());
    state.topics.insert("__consumer_offsets".into(), internal);

    let mut checkout = MockGroup::new("checkout-service");
    checkout.state = kafka_core::group::GroupState::Stable;
    checkout.partition_assignor = "range".into();
    checkout.members = vec![
        kafka_core::group::GroupMember {
            member_id: "checkout-1".into(),
            client_id: "checkout-pod-7d9f".into(),
            client_host: "/10.1.4.21".into(),
            assignments: vec![TopicPartition::new("orders", 0), TopicPartition::new("orders", 1)],
        },
        kafka_core::group::GroupMember {
            member_id: "checkout-2".into(),
            client_id: "checkout-pod-4c2b".into(),
            client_host: "/10.1.4.22".into(),
            assignments: vec![TopicPartition::new("orders", 2)],
        },
    ];
    checkout.committed.insert(
        tp_key("orders", 0),
        CommittedOffset { offset: 2, metadata: String::new(), committed_at: now - 15_000 },
    );
    checkout.committed.insert(
        tp_key("orders", 1),
        CommittedOffset { offset: 1, metadata: String::new(), committed_at: now - 15_000 },
    );
    state.groups.insert(checkout.id.clone(), checkout);

    let mut etl = MockGroup::new("analytics-etl");
    etl.state = kafka_core::group::GroupState::Empty;
    etl.partition_assignor = "roundrobin".into();
    etl.committed.insert(tp_key("audit-log", 0), CommittedOffset { offset: 1, metadata: "v1".into(), committed_at: now - 90_000 });
    state.groups.insert(etl.id.clone(), etl);
}
