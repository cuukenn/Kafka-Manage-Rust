use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::DriverError;

fn default_partitions() -> u32 {
    1
}
fn default_replication() -> i16 {
    1
}

/// Topic row as shown in the UI tree/list.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TopicInfo {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub internal: bool,
    #[serde(default)]
    pub partition_count: u32,
    #[serde(default)]
    pub replication_factor: i16,
    /// Retained topic-level configs (the driver filters to non-default entries).
    #[serde(default)]
    pub configs: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PartitionDetail {
    #[serde(default)]
    pub id: u32,
    /// `-1` when there is no leader (mirrors the Kafka wire protocol).
    #[serde(default)]
    pub leader_id: i32,
    #[serde(default)]
    pub replica_ids: Vec<i32>,
    #[serde(default)]
    pub isr_ids: Vec<i32>,
    #[serde(default)]
    pub beginning_offset: i64,
    #[serde(default)]
    pub end_offset: i64,
    #[serde(default)]
    pub leader_epoch: Option<i32>,
}

impl PartitionDetail {
    pub fn message_count(&self) -> i64 {
        (self.end_offset - self.beginning_offset).max(0)
    }
    pub fn is_under_replicated(&self) -> bool {
        self.isr_ids.len() != self.replica_ids.len()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TopicDetail {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub internal: bool,
    #[serde(default)]
    pub partitions: Vec<PartitionDetail>,
    #[serde(default)]
    pub configs: BTreeMap<String, String>,
}

impl From<TopicInfo> for TopicDetail {
    fn from(value: TopicInfo) -> Self {
        Self { name: value.name, internal: value.internal, partitions: Vec::new(), configs: value.configs }
    }
}

/// Topic + partition pair, the unit of offset and assignment bookkeeping.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicPartition {
    pub topic: String,
    pub partition: u32,
}

impl TopicPartition {
    pub fn new(topic: impl Into<String>, partition: u32) -> Self {
        Self { topic: topic.into(), partition }
    }
}

impl fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.topic, self.partition)
    }
}

/// Low/high watermark pair for one partition.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct OffsetRange {
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub partition: u32,
    #[serde(default)]
    pub low: i64,
    #[serde(default)]
    pub high: i64,
    /// Concrete offset the driver resolved for the request: `low` for earliest,
    /// `high` for latest, first offset at/after the timestamp for `Timestamp`.
    #[serde(default)]
    pub resolved_offset: Option<i64>,
}

impl OffsetRange {
    pub fn count(&self) -> i64 {
        (self.high - self.low).max(0)
    }
    pub fn contains(&self, offset: i64) -> bool {
        offset >= self.low && offset < self.high
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TopicPartitionOffset {
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub partition: u32,
    #[serde(default)]
    pub offset: i64,
    #[serde(default)]
    pub metadata: String,
    #[serde(default)]
    pub timestamp: Option<i64>,
    #[serde(default)]
    pub leader_epoch: Option<i32>,
}

/// How to resolve a partition's start offset when describing or listing offsets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type", content = "value")]
pub enum OffsetSpec {
    #[default]
    Earliest,
    Latest,
    Timestamp(i64),
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ListOffsetsReq {
    /// Empty means every partition of every topic.
    #[serde(default)]
    pub partitions: Vec<TopicPartition>,
    #[serde(default)]
    pub spec: OffsetSpec,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct DescribeTopicsReq {
    /// Empty means every topic.
    #[serde(default)]
    pub names: Vec<String>,
    #[serde(default)]
    pub include_internal: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct CreateTopicReq {
    #[serde(default)]
    pub name: String,
    #[serde(default = "default_partitions")]
    pub num_partitions: u32,
    #[serde(default = "default_replication")]
    pub replication_factor: i16,
    /// Explicit broker assignment per partition; overrides `replication_factor` when set.
    #[serde(default)]
    pub replica_assignment: Vec<Vec<i32>>,
    #[serde(default)]
    pub configs: BTreeMap<String, String>,
}

impl Default for CreateTopicReq {
    fn default() -> Self {
        Self {
            name: String::new(),
            num_partitions: default_partitions(),
            replication_factor: default_replication(),
            replica_assignment: Vec::new(),
            configs: BTreeMap::new(),
        }
    }
}

impl CreateTopicReq {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into(), ..Default::default() }
    }

    pub fn validate(&self, broker_count: usize) -> Result<(), DriverError> {
        if self.name.trim().is_empty() {
            return Err(DriverError::invalid("name", "topic name must not be empty"));
        }
        if self.name.len() > 249 {
            return Err(DriverError::invalid("name", "topic name must be 249 characters or fewer"));
        }
        let valid = self
            .name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
        if !valid {
            return Err(DriverError::invalid(
                "name",
                "topic name may only contain letters, digits, '.', '_' and '-'",
            ));
        }
        if self.num_partitions == 0 {
            return Err(DriverError::invalid("numPartitions", "must be at least 1"));
        }
        if self.replica_assignment.is_empty() {
            if self.replication_factor < 1 {
                return Err(DriverError::invalid("replicationFactor", "must be at least 1"));
            }
            if (self.replication_factor as usize) > broker_count.max(1) {
                return Err(DriverError::invalid(
                    "replicationFactor",
                    format!("cannot exceed broker count ({})", broker_count.max(1)),
                ));
            }
        } else if self.replica_assignment.len() != self.num_partitions as usize {
            return Err(DriverError::invalid(
                "replicaAssignment",
                format!("{} entries but numPartitions={}", self.replica_assignment.len(), self.num_partitions),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct DeleteTopicsReq {
    #[serde(default)]
    pub names: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TopicDeleteResult {
    #[serde(default)]
    pub name: String,
    /// `None` means the topic was deleted (or queued for deletion).
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct UpdateTopicConfigReq {
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub set: BTreeMap<String, String>,
    /// Config keys to reset to their broker default.
    #[serde(default)]
    pub unset: Vec<String>,
}
