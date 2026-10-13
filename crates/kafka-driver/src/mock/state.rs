//! In-memory cluster state for the mock driver: brokers, topics, partitions with
//! their message logs, and consumer groups with committed offsets.
//!
//! Offsets are the message log index, so `beginning_offset` is always 0 and
//! `end_offset` is the log length — matching a Kafka partition with no retention.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use kafka_core::cluster::{BrokerInfo, ClusterConfig};
use kafka_core::group::{GroupMember, GroupState};
use kafka_core::message::{MessageHeader, TimestampType};

/// Key for offset maps. JSON object keys must be strings, so `(topic, partition)`
/// pairs are flattened into `"{topic}#{partition}"`.
pub fn tp_key(topic: &str, partition: u32) -> String {
    format!("{topic}#{partition}")
}

pub fn split_tp_key(key: &str) -> Option<(String, u32)> {
    let (topic, part) = key.split_once('#')?;
    Some((topic.to_string(), part.parse().ok()?))
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct MockMessage {
    #[serde(default)]
    pub key: Option<Vec<u8>>,
    #[serde(default)]
    pub value: Vec<u8>,
    #[serde(default)]
    pub headers: Vec<MessageHeader>,
    #[serde(default)]
    pub timestamp: i64,
    #[serde(default)]
    pub timestamp_type: TimestampType,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct MockPartition {
    #[serde(default)]
    pub id: u32,
    /// `-1` means no leader, mirroring the wire protocol.
    #[serde(default)]
    pub leader_id: i32,
    #[serde(default)]
    pub replica_ids: Vec<i32>,
    #[serde(default)]
    pub isr_ids: Vec<i32>,
    #[serde(default)]
    pub messages: Vec<MockMessage>,
    #[serde(default)]
    pub leader_epoch: i32,
}

impl MockPartition {
    pub fn beginning_offset(&self) -> i64 {
        0
    }
    pub fn end_offset(&self) -> i64 {
        self.messages.len() as i64
    }
    pub fn has_leader(&self) -> bool {
        self.leader_id >= 0
    }
    pub fn push(&mut self, message: MockMessage) -> i64 {
        let offset = self.end_offset();
        self.messages.push(message);
        offset
    }
    /// First offset whose timestamp is `>= ts`, or `end_offset` when none qualifies.
    pub fn first_offset_at_or_after(&self, ts: i64) -> i64 {
        let mut lo = 0usize;
        let mut hi = self.messages.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.messages[mid].timestamp >= ts {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        lo as i64
    }
    pub fn detail(&self) -> kafka_core::topic::PartitionDetail {
        kafka_core::topic::PartitionDetail {
            id: self.id,
            leader_id: self.leader_id,
            replica_ids: self.replica_ids.clone(),
            isr_ids: self.isr_ids.clone(),
            beginning_offset: self.beginning_offset(),
            end_offset: self.end_offset(),
            leader_epoch: Some(self.leader_epoch),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct MockTopic {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub internal: bool,
    #[serde(default)]
    pub partitions: Vec<MockPartition>,
    #[serde(default)]
    pub configs: BTreeMap<String, String>,
    #[serde(default)]
    pub created_at: i64,
}

impl MockTopic {
    pub fn partition_count(&self) -> u32 {
        self.partitions.len() as u32
    }
    pub fn replication_factor(&self) -> i16 {
        self.partitions
            .iter()
            .map(|p| p.replica_ids.len() as i16)
            .max()
            .unwrap_or(1)
    }
    pub fn partition_ids(&self) -> Vec<u32> {
        self.partitions.iter().map(|p| p.id).collect()
    }
    pub fn find_partition(&self, id: u32) -> Option<&MockPartition> {
        self.partitions.iter().find(|p| p.id == id)
    }
    pub fn find_partition_mut(&mut self, id: u32) -> Option<&mut MockPartition> {
        self.partitions.iter_mut().find(|p| p.id == id)
    }
    pub fn total_messages(&self) -> i64 {
        self.partitions.iter().map(|p| p.end_offset()).sum()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct CommittedOffset {
    #[serde(default)]
    pub offset: i64,
    #[serde(default)]
    pub metadata: String,
    #[serde(default)]
    pub committed_at: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct MockGroup {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub state: GroupState,
    #[serde(default)]
    pub protocol_type: String,
    #[serde(default)]
    pub partition_assignor: String,
    #[serde(default)]
    pub members: Vec<GroupMember>,
    /// `(topic, partition) -> committed offset`, see [`tp_key`].
    #[serde(default)]
    pub committed: BTreeMap<String, CommittedOffset>,
}

impl MockGroup {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            state: GroupState::Empty,
            protocol_type: "consumer".into(),
            partition_assignor: "range".into(),
            members: Vec::new(),
            committed: BTreeMap::new(),
        }
    }
    pub fn member_count(&self) -> usize {
        self.members.len()
    }
}

/// Everything the mock driver knows. Cheap to clone for snapshots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct MockState {
    #[serde(default)]
    pub cluster_id: String,
    #[serde(default)]
    pub controller_id: i32,
    #[serde(default)]
    pub brokers: Vec<BrokerInfo>,
    #[serde(default)]
    pub connected: bool,
    #[serde(default)]
    pub config: Option<ClusterConfig>,
    #[serde(default)]
    pub topics: BTreeMap<String, MockTopic>,
    #[serde(default)]
    pub groups: BTreeMap<String, MockGroup>,
    /// Round-robin cursor used when a produce request omits a partition.
    #[serde(default)]
    pub round_robin: usize,
    /// Mock broker version string reported to the UI.
    #[serde(default = "default_mock_version")]
    pub version: String,
}

fn default_mock_version() -> String {
    "3.7.0-mock".to_string()
}

impl Default for MockState {
    fn default() -> Self {
        Self {
            cluster_id: "mock-cluster".to_string(),
            controller_id: 1,
            brokers: default_brokers(),
            connected: false,
            config: None,
            topics: BTreeMap::new(),
            groups: BTreeMap::new(),
            round_robin: 0,
            version: default_mock_version(),
        }
    }
}

/// Three brokers on 9092..=9094, the shape most local Kafka setups have.
pub fn default_brokers() -> Vec<BrokerInfo> {
    (1..=3)
        .map(|id| BrokerInfo {
            node_id: id,
            host: "localhost".into(),
            port: 9091 + id as u16,
            rack: Some(format!("rack-{}", (id - 1) % 3)),
        })
        .collect()
}

impl MockState {
    pub fn broker_count(&self) -> usize {
        self.brokers.len()
    }

    pub fn has_topic(&self, name: &str) -> bool {
        self.topics.contains_key(name)
    }

    /// Creates a topic with partitions spread round-robin over the brokers.
    pub fn insert_topic(
        &mut self,
        name: &str,
        num_partitions: u32,
        replication_factor: i16,
        replica_assignment: &[Vec<i32>],
        configs: BTreeMap<String, String>,
    ) -> MockTopic {
        let broker_ids: Vec<i32> = self.brokers.iter().map(|b| b.node_id).collect();
        let partitions = (0..num_partitions)
            .map(|i| {
                let replicas = if !replica_assignment.is_empty() {
                    replica_assignment[i as usize].clone()
                } else {
                    let rf = replication_factor.max(1) as usize;
                    (0..rf).map(|r| broker_ids[(i as usize + r) % broker_ids.len().max(1)]).collect()
                };
                MockPartition {
                    id: i,
                    leader_id: replicas.first().copied().unwrap_or(1),
                    replica_ids: replicas.clone(),
                    isr_ids: replicas,
                    messages: Vec::new(),
                    leader_epoch: 0,
                }
            })
            .collect();
        MockTopic {
            name: name.to_string(),
            internal: name.starts_with("__"),
            partitions,
            configs,
            created_at: kafka_core::protocol::now_ms(),
        }
    }

    /// Marks a partition leaderless; used by fault injection and tests.
    pub fn set_leader(&mut self, topic: &str, partition: u32, leader_id: i32) {
        if let Some(t) = self.topics.get_mut(topic) {
            if let Some(p) = t.find_partition_mut(partition) {
                p.leader_id = leader_id;
                p.leader_epoch += 1;
                if leader_id < 0 {
                    p.isr_ids.clear();
                } else if !p.isr_ids.contains(&leader_id) {
                    p.isr_ids.push(leader_id);
                }
            }
        }
    }

    pub fn next_round_robin(&mut self, modulo: usize) -> usize {
        if modulo == 0 {
            return 0;
        }
        let pick = self.round_robin % modulo;
        self.round_robin = self.round_robin.wrapping_add(1);
        pick
    }

    /// Reserve `count` round-robin slots in one go, returned as indices into a list of
    /// `modulo` partitions. The driver needs this because it cannot hold a mutable borrow
    /// of the topic while it advances the cursor through `&mut self`.
    pub fn take_round_robin_picks(&mut self, modulo: usize, count: usize) -> Vec<usize> {
        let base = self.round_robin;
        self.round_robin = base.wrapping_add(count);
        if modulo == 0 {
            return Vec::new();
        }
        (0..count).map(|k| (base + k) % modulo).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tp_key_round_trip() {
        let key = tp_key("orders", 3);
        assert_eq!(key, "orders#3");
        assert_eq!(split_tp_key(&key), Some(("orders".to_string(), 3)));
        assert_eq!(split_tp_key("nope"), None);
    }

    #[test]
    fn first_offset_at_or_after_is_binary_searched() {
        let mut p = MockPartition { id: 0, leader_id: 1, ..Default::default() };
        for (offset, ts) in [100i64, 200, 200, 300].into_iter().enumerate() {
            p.messages.push(MockMessage { timestamp: ts, value: vec![offset as u8], ..Default::default() });
        }
        assert_eq!(p.first_offset_at_or_after(0), 0);
        assert_eq!(p.first_offset_at_or_after(150), 1);
        assert_eq!(p.first_offset_at_or_after(200), 1);
        assert_eq!(p.first_offset_at_or_after(250), 3);
        assert_eq!(p.first_offset_at_or_after(999), p.end_offset());
    }

    #[test]
    fn topic_spreads_partitions_over_brokers() {
        let mut state = MockState::default();
        let topic = state.insert_topic("orders", 4, 3, &[], BTreeMap::new());
        assert_eq!(topic.partition_count(), 4);
        assert_eq!(topic.replication_factor(), 3);
        assert_eq!(topic.partitions[0].leader_id, 1);
        assert_eq!(topic.partitions[2].leader_id, 3);
        assert_eq!(topic.partitions[3].leader_id, 1, "wraps around the broker list");
    }

    #[test]
    fn leader_reassignment_updates_isr_and_epoch() {
        let mut state = MockState::default();
        let topic = state.insert_topic("t", 1, 1, &[], BTreeMap::new());
        state.topics.insert("t".into(), topic);
        state.set_leader("t", 0, -1);
        assert_eq!(state.topics["t"].partitions[0].leader_id, -1);
        assert!(state.topics["t"].partitions[0].isr_ids.is_empty());
        state.set_leader("t", 0, 2);
        assert_eq!(state.topics["t"].partitions[0].leader_epoch, 2);
        assert_eq!(state.topics["t"].partitions[0].isr_ids, vec![2]);
    }
}
