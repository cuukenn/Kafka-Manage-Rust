use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use serde::{Deserialize, Serialize};

use crate::message::{MessageEvent, MessageHeader, TimestampType};
use crate::topic::TopicInfo;

/// Default cap on the value bytes handed to the UI. Anything larger is truncated
/// and flagged so the frontend can offer "download full message".
pub const DEFAULT_MAX_VALUE_BYTES: usize = 64 * 1024;

/// UI-facing message payload.
///
/// Raw bytes never cross the WebView boundary as `Vec<u8>` (serde_json would emit a
/// number array). Instead every byte field ships twice: a best-effort UTF-8 string
/// for display and a base64 copy for exact round-tripping / binary download.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct MessageDto {
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub partition: u32,
    #[serde(default)]
    pub offset: i64,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub key_b64: Option<String>,
    #[serde(default)]
    pub value: String,
    #[serde(default)]
    pub value_b64: String,
    /// `false` when the bytes are not valid UTF-8 (binary payload).
    #[serde(default)]
    pub value_is_utf8: bool,
    #[serde(default)]
    pub size: usize,
    #[serde(default)]
    pub timestamp: i64,
    #[serde(default)]
    pub timestamp_type: TimestampType,
    #[serde(default)]
    pub headers: Vec<HeaderDto>,
    /// `true` when `value`/`value_b64` were cut at `max_bytes`.
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub leader_epoch: Option<i32>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct HeaderDto {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub value_b64: Option<String>,
}

impl HeaderDto {
    pub fn from_header(header: &MessageHeader) -> Self {
        match &header.value {
            None => Self { key: header.key.clone(), value: None, value_b64: None },
            Some(raw) => Self {
                key: header.key.clone(),
                value: Some(String::from_utf8_lossy(raw).into_owned()),
                value_b64: Some(B64.encode(raw)),
            },
        }
    }
}

impl MessageDto {
    pub fn from_event(event: &MessageEvent) -> Self {
        Self::from_event_capped(event, DEFAULT_MAX_VALUE_BYTES)
    }

    pub fn from_event_capped(event: &MessageEvent, max_bytes: usize) -> Self {
        let truncated = event.value.len() > max_bytes;
        let slice = &event.value[..event.value.len().min(max_bytes)];
        let value_utf8 = std::str::from_utf8(slice).ok();

        Self {
            topic: event.topic.clone(),
            partition: event.partition,
            offset: event.offset,
            key: event.key_utf8().map(str::to_owned),
            key_b64: event.key.as_deref().map(|k| B64.encode(k)),
            value: value_utf8.map(str::to_owned).unwrap_or_default(),
            value_b64: B64.encode(slice),
            value_is_utf8: value_utf8.is_some(),
            size: event.size(),
            timestamp: event.timestamp,
            timestamp_type: event.timestamp_type,
            headers: event.headers.iter().map(HeaderDto::from_header).collect(),
            truncated,
            leader_epoch: event.leader_epoch,
        }
    }

    /// Rebuild the raw event from a DTO (used when the UI edits and re-sends a message).
    pub fn to_event(&self) -> MessageEvent {
        let value = B64
            .decode(&self.value_b64)
            .or_else(|_| B64.decode(self.value.as_bytes()))
            .unwrap_or_else(|_| self.value.as_bytes().to_vec());
        let key = self
            .key_b64
            .as_deref()
            .and_then(|k| B64.decode(k).ok())
            .or_else(|| self.key.as_ref().map(|k| k.clone().into_bytes()));
        MessageEvent {
            topic: self.topic.clone(),
            partition: self.partition,
            offset: self.offset,
            key,
            value,
            headers: self
                .headers
                .iter()
                .map(|h| MessageHeader {
                    key: h.key.clone(),
                    value: h
                        .value_b64
                        .as_deref()
                        .and_then(|v| B64.decode(v).ok())
                        .or_else(|| h.value.as_ref().map(|v| v.clone().into_bytes())),
                })
                .collect(),
            timestamp: self.timestamp,
            timestamp_type: self.timestamp_type,
            leader_epoch: self.leader_epoch,
        }
    }
}

/// Topic list row enriched with the aggregate message count, ready for the UI table.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TopicRow {
    #[serde(flatten)]
    pub info: TopicInfo,
    #[serde(default)]
    pub message_count: i64,
    #[serde(default)]
    pub under_replicated_partitions: u32,
}

/// Encode raw bytes for a request going the other way (UI → driver).
pub fn decode_bytes(value: Option<&str>, b64: Option<&str>) -> Option<Vec<u8>> {
    if let Some(b) = b64 {
        if let Ok(v) = B64.decode(b) {
            return Some(v);
        }
    }
    value.map(str::as_bytes).map(<[u8]>::to_vec)
}

pub fn encode_b64(bytes: &[u8]) -> String {
    B64.encode(bytes)
}
