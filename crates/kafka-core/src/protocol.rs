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

fn hint_for(kind: ErrorKind) -> Option<String> {
    match kind {
        ErrorKind::NotConnected => Some("Connect to the cluster first, or switch the cluster to mock mode.".into()),
        ErrorKind::Connection => Some("Check bootstrap servers and network reachability.".into()),
        ErrorKind::Timeout => Some("Increase the request timeout in cluster settings.".into()),
        ErrorKind::Auth => Some("Check SASL mechanism, username and password.".into()),
        ErrorKind::Ssl => Some("Check CA / certificate paths and hostname verification.".into()),
        ErrorKind::OffsetOutOfRange => Some("Pick earliest or latest, or a timestamp inside the retention window.".into()),
        ErrorKind::Unsupported => Some("This operation is not available in mock mode; use a real cluster.".into()),
        _ => None,
    }
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
}
