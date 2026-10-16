use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{DriverError, ErrorKind};

/// Command names spoken by the WebView. Kept as constants so the frontend and the
/// Rust router cannot drift apart silently.
pub mod cmd {
    pub const CLUSTER_LIST: &str = "cluster.list";
    pub const CLUSTER_SAVE: &str = "cluster.save";
    pub const CLUSTER_DELETE: &str = "cluster.delete";
    pub const CLUSTER_CONNECT: &str = "cluster.connect";
    pub const CLUSTER_DISCONNECT: &str = "cluster.disconnect";
    pub const CLUSTER_INFO: &str = "cluster.info";
    pub const CLUSTER_EXPORT: &str = "cluster.export";
    pub const CLUSTER_IMPORT: &str = "cluster.import";

    pub const TOPIC_LIST: &str = "topic.list";
    pub const TOPIC_DESCRIBE: &str = "topic.describe";
    pub const TOPIC_CREATE: &str = "topic.create";
    pub const TOPIC_DELETE: &str = "topic.delete";
    pub const TOPIC_UPDATE_CONFIG: &str = "topic.updateConfig";
    pub const TOPIC_OFFSETS: &str = "topic.offsets";

    pub const PRODUCE_SEND: &str = "produce.send";

    pub const CONSUBSCRIBE_START: &str = "consume.subscribe";
    pub const CONSUBSCRIBE_STOP: &str = "consume.unsubscribe";
    pub const CONSUME_PAUSE: &str = "consume.pause";
    pub const CONSUME_RESUME: &str = "consume.resume";
    pub const CONSUME_SEEK: &str = "consume.seek";
    pub const CONSUME_LIST: &str = "consume.list";

    pub const GROUP_LIST: &str = "group.list";
    pub const GROUP_DESCRIBE: &str = "group.describe";
    pub const GROUP_OFFSETS: &str = "group.offsets";
    pub const GROUP_RESET_OFFSETS: &str = "group.resetOffsets";

    pub const CONFIG_GET: &str = "config.get";
    pub const CONFIG_SET: &str = "config.set";

    pub const MOCK_RESET: &str = "mock.reset";
    pub const MOCK_SEED: &str = "mock.seed";
    pub const MOCK_FAULTS: &str = "mock.faults";
}

/// Event names pushed from Rust to the WebView (never answered).
pub mod event {
    pub const CONNECTION_STATE: &str = "connection.state";
    pub const MESSAGE_RECEIVED: &str = "message.received";
    pub const SUBSCRIPTION_STATE: &str = "subscription.state";
    pub const SUBSCRIPTION_DONE: &str = "subscription.done";
    pub const TOPIC_CHANGED: &str = "topic.changed";
    pub const GROUP_CHANGED: &str = "group.changed";
    pub const PRODUCE_RESULT: &str = "produce.result";
    pub const LOG_LINE: &str = "log.line";
    pub const ERROR: &str = "error";
}

/// One request from the UI. `payload` is decoded per-command by the router.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestEnvelope {
    pub id: String,
    pub cmd: String,
    #[serde(default)]
    pub payload: Value,
}

/// Machine readable error body shipped to the UI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorDto {
    pub kind: String,
    pub message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

impl ErrorDto {
    pub fn from_driver(error: &DriverError) -> Self {
        Self {
            kind: error.kind().as_str().to_string(),
            message: error.to_string(),
            retryable: error.is_retryable(),
            hint: hint_for(error.kind()),
        }
    }

    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind: kind.as_str().to_string(),
            message: message.into(),
            retryable: kind.is_retryable(),
            hint: hint_for(kind),
        }
    }
}

/// The one line of advice the UI shows under an error message (T081).
///
/// Deliberately an exhaustive match with no `_` arm: every `ErrorKind` carries a
/// decision here, so a new variant cannot ship without one. `None` means the
/// message alone is the whole story — see the two arms that say so.
fn hint_for(kind: ErrorKind) -> Option<String> {
    let text = match kind {
        ErrorKind::NotConnected => "Connect to the cluster first, or switch the cluster to mock mode.",
        ErrorKind::Connection => "Check bootstrap servers and network reachability.",
        ErrorKind::Timeout => "Increase the request timeout in cluster settings.",
        ErrorKind::Auth => "Check SASL mechanism, username and password.",
        ErrorKind::Ssl => "Check CA / certificate paths and hostname verification.",
        ErrorKind::NotFound => "Refresh the list; the topic, partition or group may have been removed.",
        ErrorKind::AlreadyExists => "Pick another name, or open the existing topic.",
        // R76: the driver builds this message itself and names the offending
        // field plus the reason, so a second sentence would only restate it.
        ErrorKind::InvalidConfig => return None,
        ErrorKind::OffsetOutOfRange => {
            "Pick earliest or latest, or a timestamp inside the retention window."
        }
        ErrorKind::Unsupported => "This operation is not available in mock mode; use a real cluster.",
        ErrorKind::Group => {
            "The group may be rebalancing or gone; retry, then refresh the group list."
        }
        ErrorKind::Broker => "The broker rejected the request; check the broker log for this code.",
        ErrorKind::Storage => {
            "The local database could not be read or written; check disk space and file permissions."
        }
        // R76: the user asked for the cancellation; nothing to advise.
        ErrorKind::Cancelled => return None,
        ErrorKind::Internal => "This looks like a bug; check the log panel and report it.",
    };
    Some(text.into())
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseEnvelope<T = Value> {
    pub id: String,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorDto>,
    /// Round-trip duration in ms, useful for the UI's request log panel.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u64>,
}

impl<T: Serialize> ResponseEnvelope<T> {
    pub fn ok(id: impl Into<String>, data: T) -> Self {
        Self { id: id.into(), ok: true, data: Some(data), error: None, elapsed_ms: None }
    }

    pub fn ok_timed(id: impl Into<String>, data: T, elapsed_ms: u64) -> Self {
        Self { id: id.into(), ok: true, data: Some(data), error: None, elapsed_ms: Some(elapsed_ms) }
    }
}

impl ResponseEnvelope<Value> {
    pub fn empty(id: impl Into<String>) -> Self {
        Self { id: id.into(), ok: true, data: Some(Value::Null), error: None, elapsed_ms: None }
    }

    pub fn err(id: impl Into<String>, error: &DriverError) -> Self {
        Self {
            id: id.into(),
            ok: false,
            data: None,
            error: Some(ErrorDto::from_driver(error)),
            elapsed_ms: None,
        }
    }

    pub fn err_dto(id: impl Into<String>, error: ErrorDto) -> Self {
        Self { id: id.into(), ok: false, data: None, error: Some(error), elapsed_ms: None }
    }

    pub fn unsupported(id: impl Into<String>, what: &str) -> Self {
        Self::err_dto(id, ErrorDto::new(ErrorKind::Unsupported, format!("'{what}' is not implemented yet")))
    }
}

/// One event pushed to the UI.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventEnvelope<T = Value> {
    pub event: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<T>,
    /// Server timestamp (ms epoch) so the UI can order events from different sources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ts: Option<i64>,
}

impl<T: Serialize> EventEnvelope<T> {
    pub fn new(name: impl Into<String>, payload: T) -> Self {
        Self { event: name.into(), payload: Some(payload), ts: Some(now_ms()) }
    }
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Parse a raw JS payload string into a request envelope.
pub fn parse_request(raw: &str) -> Result<RequestEnvelope, ErrorDto> {
    serde_json::from_str(raw)
        .map_err(|e| ErrorDto::new(ErrorKind::InvalidConfig, format!("malformed request: {e}")))
}

/// Decode the payload of one command into its typed request struct.
pub fn payload_as<T: serde::de::DeserializeOwned>(payload: &Value) -> Result<T, DriverError> {
    serde_json::from_value(payload.clone())
        .map_err(|e| DriverError::invalid("payload", format!("invalid payload: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_round_trip() {
        let raw = r#"{"id":"r1","cmd":"topic.list","payload":{"clusterId":"c1"}}"#;
        let req = parse_request(raw).expect("parses");
        assert_eq!(req.id, "r1");
        assert_eq!(req.cmd, cmd::TOPIC_LIST);
        assert_eq!(req.payload["clusterId"], "c1");
    }

    #[test]
    fn missing_payload_defaults_to_null() {
        let req = parse_request(r#"{"id":"r2","cmd":"cluster.list"}"#).expect("parses");
        assert!(req.payload.is_null());
    }

    #[test]
    fn error_envelope_carries_kind_and_hint() {
        let err = DriverError::NotConnected("c1".into());
        let env = ResponseEnvelope::err("r3", &err);
        let json = serde_json::to_value(&env).expect("serialises");
        assert_eq!(json["ok"], false);
        assert_eq!(json["error"]["kind"], "not_connected");
        assert_eq!(json["error"]["retryable"], true);
        assert!(json["error"]["hint"].is_string());
    }

    #[test]
    fn error_dto_new_uses_the_retry_classification() {
        for kind in [
            ErrorKind::NotConnected,
            ErrorKind::Connection,
            ErrorKind::Timeout,
            ErrorKind::Broker,
        ] {
            assert!(ErrorDto::new(kind, "boom").retryable, "{kind} must be retryable");
        }
        for kind in [
            ErrorKind::InvalidConfig,
            ErrorKind::NotFound,
            ErrorKind::Auth,
            ErrorKind::Storage,
            ErrorKind::Group,
            ErrorKind::Cancelled,
        ] {
            assert!(!ErrorDto::new(kind, "boom").retryable, "{kind} must not be retryable");
        }
    }

    #[test]
    fn ok_envelope_omits_empty_fields() {
        let json = serde_json::to_value(ResponseEnvelope::ok("r4", serde_json::json!({ "n": 1 })))
            .expect("serialises");
        assert_eq!(json["ok"], true);
        assert!(json.get("error").is_none());
        assert!(json.get("elapsedMs").is_none());
    }

    /// T081: every kind has a decided hint, and the undecided-looking ones are named.
    #[test]
    fn every_error_kind_has_a_deliberate_hint() {
        for kind in ErrorKind::ALL {
            let hinted = !matches!(kind, ErrorKind::InvalidConfig | ErrorKind::Cancelled);
            let hint = ErrorDto::new(kind, "boom").hint;
            assert_eq!(hint.is_some(), hinted, "{kind} hint coverage changed");
            if let Some(text) = hint {
                assert!(!text.trim().is_empty(), "{kind} hint must not be blank");
            }
        }
    }

    /// T081: a hint answers for exactly one kind — copy-paste reuse would mean one of
    /// them is answering the wrong question.
    #[test]
    fn hints_are_never_reused_between_kinds() {
        let mut seen: Vec<(ErrorKind, String)> = Vec::new();
        for kind in ErrorKind::ALL {
            let Some(hint) = ErrorDto::new(kind, "boom").hint else {
                continue;
            };
            if let Some((owner, _)) = seen.iter().find(|(_, text)| *text == hint) {
                panic!("{kind} reuses the hint of {owner}: {hint}");
            }
            seen.push((kind, hint));
        }
        assert_eq!(seen.len(), ErrorKind::ALL.len() - 2, "13 kinds carry advice");
    }

    /// T081: hint must not depend on which constructor produced the DTO.
    #[test]
    fn both_error_dto_constructors_agree_on_the_hint() {
        for kind in ErrorKind::ALL {
            let Some(error) = driver_error_of(kind) else {
                // `Storage` is only reachable through `StorageError`, which is not a
                // `DriverError`, so `from_driver` can never carry it.
                assert_eq!(kind, ErrorKind::Storage, "{kind} needs a DriverError to compare");
                continue;
            };
            assert_eq!(error.kind(), kind, "the sample for {kind} drifted");
            assert_eq!(
                ErrorDto::from_driver(&error).hint,
                ErrorDto::new(kind, "boom").hint,
                "{kind} hints differ between the two constructors"
            );
        }
    }

    /// One representative `DriverError` per kind, for the agreement test above.
    fn driver_error_of(kind: ErrorKind) -> Option<DriverError> {
        let error = match kind {
            ErrorKind::NotConnected => DriverError::NotConnected("c1".into()),
            ErrorKind::Connection => DriverError::Connection {
                server: "localhost:9092".into(),
                message: "refused".into(),
            },
            ErrorKind::Timeout => DriverError::timeout("connect", 1),
            ErrorKind::Auth => DriverError::Auth("bad credentials".into()),
            ErrorKind::Ssl => DriverError::Ssl("handshake failed".into()),
            ErrorKind::NotFound => DriverError::TopicNotFound("t1".into()),
            ErrorKind::AlreadyExists => DriverError::TopicAlreadyExists("t1".into()),
            ErrorKind::InvalidConfig => DriverError::invalid("topic", "must not be empty"),
            ErrorKind::OffsetOutOfRange => DriverError::OffsetOutOfRange {
                topic: "t1".into(),
                partition: 0,
                requested: 99,
                low: 0,
                high: 3,
            },
            ErrorKind::Unsupported => DriverError::unsupported("not here yet"),
            ErrorKind::Group => DriverError::Rebalance("rebalancing".into()),
            ErrorKind::Broker => DriverError::Broker {
                code: 3,
                name: "UnknownTopicOrPartition".into(),
                message: "unknown".into(),
            },
            ErrorKind::Storage => return None,
            ErrorKind::Cancelled => DriverError::Cancelled,
            ErrorKind::Internal => DriverError::other("boom"),
        };
        Some(error)
    }
}
