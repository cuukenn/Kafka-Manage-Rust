use futures::stream::{BoxStream, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::error::DriverError;
use crate::topic::TopicPartition;

/// Identifies one live message subscription (one UI "message stream" panel).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SubscriptionId(pub String);

impl SubscriptionId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().simple().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for SubscriptionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct MessageHeader {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub value: Option<Vec<u8>>,
}

impl MessageHeader {
    pub fn new(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self { key: key.into(), value: Some(value.into()) }
    }
    pub fn text(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self { key: key.into(), value: Some(value.into().into_bytes()) }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampType {
    #[default]
    CreateTime,
    LogAppendTime,
    Unknown,
}

/// A consumed message exactly as the broker delivered it (raw bytes preserved).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct MessageEvent {
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub partition: u32,
    #[serde(default)]
    pub offset: i64,
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
    #[serde(default)]
    pub leader_epoch: Option<i32>,
}

impl MessageEvent {
    pub fn size(&self) -> usize {
        self.value.len() + self.key.as_ref().map(|k| k.len()).unwrap_or(0)
    }
    pub fn key_utf8(&self) -> Option<&str> {
        self.key.as_deref().and_then(|k| std::str::from_utf8(k).ok())
    }
    pub fn value_utf8(&self) -> Option<&str> {
        std::str::from_utf8(&self.value).ok()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ProduceMessage {
    #[serde(default)]
    pub key: Option<Vec<u8>>,
    #[serde(default)]
    pub value: Vec<u8>,
    #[serde(default)]
    pub headers: Vec<MessageHeader>,
    /// Force a partition; when `None` the driver picks one (mock: round-robin).
    #[serde(default)]
    pub partition: Option<u32>,
    #[serde(default)]
    pub timestamp: Option<i64>,
}

impl ProduceMessage {
    pub fn text(value: impl Into<String>) -> Self {
        Self { value: value.into().into_bytes(), ..Default::default() }
    }
    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into().into_bytes());
        self
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ProduceReq {
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub messages: Vec<ProduceMessage>,
}

impl ProduceReq {
    pub fn one(topic: impl Into<String>, message: ProduceMessage) -> Self {
        Self { topic: topic.into(), messages: vec![message] }
    }
    pub fn validate(&self) -> Result<(), DriverError> {
        if self.topic.trim().is_empty() {
            return Err(DriverError::invalid("topic", "topic must not be empty"));
        }
        if self.messages.is_empty() {
            return Err(DriverError::invalid("messages", "at least one message is required"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ProduceResult {
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub partition: u32,
    #[serde(default)]
    pub offset: i64,
    #[serde(default)]
    pub timestamp: i64,
    /// Per-message failure; a batch may partially succeed.
    #[serde(default)]
    pub error: Option<String>,
}

/// Where a subscription begins reading a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type", content = "value")]
pub enum OffsetStart {
    #[default]
    Earliest,
    Latest,
    Offset(i64),
    Timestamp(i64),
}

impl OffsetStart {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Earliest => "earliest",
            Self::Latest => "latest",
            Self::Offset(_) => "offset",
            Self::Timestamp(_) => "timestamp",
        }
    }
}

/// Server-side filter applied before messages are pushed to the UI, so a noisy
/// topic does not flood the WebView.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SearchFilter {
    #[serde(default)]
    pub query: String,
    #[serde(default = "bool_true")]
    pub match_value: bool,
    #[serde(default)]
    pub match_key: bool,
    #[serde(default)]
    pub case_sensitive: bool,
    #[serde(default)]
    pub regex: bool,
}

fn bool_true() -> bool {
    true
}

/// Hand-written to mirror the serde defaults (notably `match_value: true`),
/// so `SearchFilter::default()` and a wire `{}` describe the same filter.
impl Default for SearchFilter {
    fn default() -> Self {
        Self {
            query: String::new(),
            match_value: true,
            match_key: false,
            case_sensitive: false,
            regex: false,
        }
    }
}

impl SearchFilter {
    pub fn is_empty(&self) -> bool {
        self.query.trim().is_empty()
    }

    /// Naive substring/regex-free matching. The app layer upgrades this to a real
    /// regex when the `fancy-regex`/`regex` feature is enabled.
    pub fn matches(&self, message: &MessageEvent) -> bool {
        if self.is_empty() {
            return true;
        }
        let needle = if self.case_sensitive {
            self.query.clone()
        } else {
            self.query.to_lowercase()
        };
        let mut hay: Vec<String> = Vec::with_capacity(2);
        if self.match_value {
            hay.push(String::from_utf8_lossy(&message.value).into_owned());
        }
        if self.match_key {
            if let Some(k) = &message.key {
                hay.push(String::from_utf8_lossy(k).into_owned());
            }
        }
        hay.iter().any(|h| {
            if self.case_sensitive {
                h.contains(&needle)
            } else {
                h.to_lowercase().contains(&needle)
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SubscribeReq {
    #[serde(default)]
    pub subscription_id: SubscriptionId,
    #[serde(default)]
    pub topic: String,
    /// Empty means every partition.
    #[serde(default)]
    pub partitions: Vec<u32>,
    #[serde(default)]
    pub start: OffsetStart,
    /// `true` keeps the subscription open and streams new messages as they arrive.
    #[serde(default)]
    pub follow: bool,
    /// Hard stop after N messages (bounded read). `None` = unbounded.
    #[serde(default)]
    pub max_messages: Option<u64>,
    #[serde(default)]
    pub filter: Option<SearchFilter>,
    #[serde(default)]
    pub max_bytes_per_partition: Option<i32>,
}

impl Default for SubscribeReq {
    fn default() -> Self {
        Self {
            subscription_id: SubscriptionId::new(),
            topic: String::new(),
            partitions: Vec::new(),
            start: OffsetStart::Earliest,
            follow: false,
            max_messages: None,
            filter: None,
            max_bytes_per_partition: None,
        }
    }
}

impl SubscribeReq {
    pub fn new(topic: impl Into<String>) -> Self {
        Self { topic: topic.into(), ..Default::default() }
    }
    pub fn validate(&self) -> Result<(), DriverError> {
        if self.topic.trim().is_empty() {
            return Err(DriverError::invalid("topic", "topic must not be empty"));
        }
        if let Some(0) = self.max_messages {
            return Err(DriverError::invalid("maxMessages", "must be greater than 0"));
        }
        Ok(())
    }
    pub fn targets(&self, all_partitions: &[u32]) -> Vec<u32> {
        if self.partitions.is_empty() { all_partitions.to_vec() } else { self.partitions.clone() }
    }
}

/// Out-of-band control channel for a running subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamCommand {
    Pause,
    Resume,
    Seek { topic: String, partition: u32, offset: i64 },
    Stop,
}

impl StreamCommand {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Pause => "pause",
            Self::Resume => "resume",
            Self::Seek { .. } => "seek",
            Self::Stop => "stop",
        }
    }
}

/// Cheap handle used by the UI/service layer to steer a live subscription.
#[derive(Debug, Clone)]
pub struct StreamControls {
    id: SubscriptionId,
    tx: mpsc::UnboundedSender<StreamCommand>,
}

impl StreamControls {
    pub fn channel(id: SubscriptionId) -> (Self, mpsc::UnboundedReceiver<StreamCommand>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { id, tx }, rx)
    }

    pub fn id(&self) -> &SubscriptionId {
        &self.id
    }

    pub fn send(&self, command: StreamCommand) -> Result<(), DriverError> {
        self.tx.send(command).map_err(|_| DriverError::Cancelled)
    }

    pub fn pause(&self) -> Result<(), DriverError> {
        self.send(StreamCommand::Pause)
    }
    pub fn resume(&self) -> Result<(), DriverError> {
        self.send(StreamCommand::Resume)
    }
    pub fn seek(&self, topic: impl Into<String>, partition: u32, offset: i64) -> Result<(), DriverError> {
        self.send(StreamCommand::Seek { topic: topic.into(), partition, offset })
    }
    pub fn stop(&self) -> Result<(), DriverError> {
        self.send(StreamCommand::Stop)
    }
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

/// A live message stream plus its control handle.
///
/// The stream is `'static` and boxed so it can be moved into a Tokio task; the
/// controls are cloned freely into command handlers.
pub struct MessageStream {
    id: SubscriptionId,
    inner: BoxStream<'static, Result<MessageEvent, DriverError>>,
    controls: StreamControls,
}

impl std::fmt::Debug for MessageStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageStream").field("id", &self.id).finish_non_exhaustive()
    }
}

impl MessageStream {
    pub fn new(
        id: SubscriptionId,
        inner: BoxStream<'static, Result<MessageEvent, DriverError>>,
        controls: StreamControls,
    ) -> Self {
        Self { id, inner, controls }
    }

    pub fn id(&self) -> &SubscriptionId {
        &self.id
    }

    pub fn controls(&self) -> StreamControls {
        self.controls.clone()
    }

    pub fn into_stream(self) -> BoxStream<'static, Result<MessageEvent, DriverError>> {
        self.inner
    }

    pub async fn next(&mut self) -> Option<Result<MessageEvent, DriverError>> {
        self.inner.next().await
    }
}

impl Stream for MessageStream {
    type Item = Result<MessageEvent, DriverError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.inner.as_mut().poll_next(cx)
    }
}

/// Convenience: build a `TopicPartition` list from a subscription's resolved targets.
pub fn topic_partitions(topic: &str, partitions: &[u32]) -> Vec<TopicPartition> {
    partitions.iter().map(|p| TopicPartition::new(topic, *p)).collect()
}

#[cfg(test)]
mod tests {
    use super::SearchFilter;

    /// T069: the Rust-side default must agree with the wire-side default
    /// (an empty JSON object), so `SearchFilter::default()` and
    /// `serde_json::from_str::<SearchFilter>("{}")` are the same filter.
    #[test]
    fn search_filter_default_matches_serde_default() {
        let from_rust = SearchFilter::default();
        let from_wire: SearchFilter =
            serde_json::from_str("{}").expect("an empty JSON object is a valid SearchFilter");
        assert_eq!(from_rust, from_wire, "SearchFilter::default() must equal the serde default");
        assert_eq!(from_rust.query, from_wire.query, "query");
        assert_eq!(from_rust.match_value, from_wire.match_value, "match_value");
        assert_eq!(from_rust.match_key, from_wire.match_key, "match_key");
        assert_eq!(from_rust.case_sensitive, from_wire.case_sensitive, "case_sensitive");
        assert_eq!(from_rust.regex, from_wire.regex, "regex");
    }
}
