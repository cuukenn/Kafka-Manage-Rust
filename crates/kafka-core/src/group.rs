use serde::{Deserialize, Serialize};

use crate::error::DriverError;
use crate::topic::TopicPartition;

/// Consumer group state exactly as the broker reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupState {
    Empty,
    Stable,
    PreparingRebalance,
    CompletingRebalance,
    Dead,
    #[default]
    Unknown,
}

impl GroupState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Empty => "Empty",
            Self::Stable => "Stable",
            Self::PreparingRebalance => "PreparingRebalance",
            Self::CompletingRebalance => "CompletingRebalance",
            Self::Dead => "Dead",
            Self::Unknown => "Unknown",
        }
    }
    pub fn from_broker(raw: &str) -> Self {
        match raw {
            "Empty" => Self::Empty,
            "Stable" => Self::Stable,
            "PreparingRebalance" => Self::PreparingRebalance,
            "CompletingRebalance" => Self::CompletingRebalance,
            "Dead" => Self::Dead,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GroupInfo {
    #[serde(default)]
    pub group_id: String,
    #[serde(default)]
    pub state: GroupState,
    #[serde(default)]
    pub member_count: usize,
    #[serde(default)]
    pub protocol_type: String,
    #[serde(default)]
    pub partition_assignor: String,
    #[serde(default)]
    pub coordinator_id: Option<i32>,
    /// `true` when the group is driven by this application's own subscriptions.
    #[serde(default)]
    pub is_local: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GroupMember {
    #[serde(default)]
    pub member_id: String,
    #[serde(default)]
    pub client_id: String,
    #[serde(default)]
    pub client_host: String,
    #[serde(default)]
    pub assignments: Vec<TopicPartition>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GroupDetail {
    #[serde(default)]
    pub info: GroupInfo,
    #[serde(default)]
    pub members: Vec<GroupMember>,
}

/// One committed offset row, optionally joined with the log end offset for lag.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GroupOffsetEntry {
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub partition: u32,
    #[serde(default)]
    pub offset: i64,
    #[serde(default)]
    pub metadata: String,
    #[serde(default)]
    pub committed_at: Option<i64>,
    /// `end_offset - committed offset`; `None` when the log end is unknown.
    #[serde(default)]
    pub lag: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ListGroupOffsetsReq {
    #[serde(default)]
    pub group_id: String,
    /// Empty means every topic the group has offsets for.
    #[serde(default)]
    pub topics: Vec<String>,
    /// When true, lag is computed by fetching log end offsets.
    #[serde(default = "bool_true")]
    pub with_lag: bool,
}

fn bool_true() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type", content = "value")]
pub enum ResetSpec {
    #[default]
    Earliest,
    Latest,
    ToOffset(i64),
    ToTimestamp(i64),
}

impl ResetSpec {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Earliest => "earliest",
            Self::Latest => "latest",
            Self::ToOffset(_) => "to_offset",
            Self::ToTimestamp(_) => "to_timestamp",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ResetOffsetsReq {
    #[serde(default)]
    pub group_id: String,
    /// Empty means every topic with committed offsets for the group.
    #[serde(default)]
    pub topics: Vec<String>,
    #[serde(default)]
    pub partitions: Vec<TopicPartition>,
    #[serde(default)]
    pub spec: ResetSpec,
    /// Compute the new offsets without writing them.
    #[serde(default)]
    pub dry_run: bool,
}

impl ResetOffsetsReq {
    pub fn validate(&self) -> Result<(), DriverError> {
        if self.group_id.trim().is_empty() {
            return Err(DriverError::invalid("groupId", "group id must not be empty"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ResetOffsetsResult {
    #[serde(default)]
    pub group_id: String,
    #[serde(default)]
    pub applied: Vec<TopicPartitionOffset>,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub error: Option<String>,
}

use crate::topic::TopicPartitionOffset;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct CommitOffsetsReq {
    /// When `None` the driver commits offsets owned by its own subscription.
    #[serde(default)]
    pub group_id: Option<String>,
    #[serde(default)]
    pub offsets: Vec<TopicPartitionOffset>,
}
