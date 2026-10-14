//! Transport-agnostic command router: raw JSON string in, `ResponseEnvelope` out.
//! Pipeline: parse envelope → command table → async handler → response. No IO,
//! no tao/wry, no real `App` — wiring the facade is T033+.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use futures::future::BoxFuture;
use kafka_core::protocol::{ErrorDto, RequestEnvelope, ResponseEnvelope};
use kafka_core::ErrorKind;
use serde_json::Value;

/// A registered command handler: takes the raw payload, returns the `data` body
/// or an `ErrorDto`. Object-safe and `Send + Sync` for the bridge event loop.
pub type Handler =
    Arc<dyn Fn(Value) -> BoxFuture<'static, Result<Value, ErrorDto>> + Send + Sync>;

type Table = HashMap<String, Handler>;
type ParseError = (String, ErrorDto);

/// The command table. `Clone` (state behind `Arc`) so the event loop and the
/// tests can hold independent handles to the same routing state.
#[derive(Clone, Default)]
pub struct Router {
    handlers: Arc<RwLock<Table>>,
}

impl Router {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `handler` for `cmd`. Duplicate registration overrides (last wins).
    pub fn register(&self, cmd: impl Into<String>, handler: Handler) {
        self.handlers.write().unwrap_or_else(|e| e.into_inner()).insert(cmd.into(), handler);
    }

    /// Route one raw request. Never panics: malformed input becomes an error
    /// response; the wire contract holds `id` is always present, empty when no
    /// usable id was extractable.
    pub async fn handle(&self, raw: &str) -> ResponseEnvelope {
        match parse_request(raw) {
            Ok(req) => self.dispatch(req).await,
            Err((id, error)) => ResponseEnvelope::err_dto(id, error),
        }
    }

    async fn dispatch(&self, req: RequestEnvelope) -> ResponseEnvelope {
        let RequestEnvelope { id, cmd, payload } = req;
        // Locks are never held across .await and nothing panics while held, so
        // poisoning cannot occur; recover anyway instead of ever panicking.
        let handler = self.handlers.read().unwrap_or_else(|e| e.into_inner()).get(&cmd).cloned();
        let Some(handler) = handler else {
            let msg = format!("no handler registered for command '{cmd}'");
            return ResponseEnvelope::err_dto(id, ErrorDto::new(ErrorKind::Internal, msg));
        };
        // The handler runs on a spawned task as a panic boundary: a panicking
        // handler must not unwind through `Handle::block_on` in the wry
        // protocol callback and kill the app — its JoinError becomes an
        // Internal error response instead.
        let result = match tokio::spawn(handler(payload)).await {
            Ok(result) => result,
            Err(join_err) => {
                return ResponseEnvelope::err_dto(
                    id,
                    ErrorDto::new(ErrorKind::Internal, format!("handler panicked: {join_err}")),
                );
            }
        };
        match result {
            Ok(data) => ResponseEnvelope::ok(id, data),
            Err(error) => ResponseEnvelope::err_dto(id, error),
        }
    }
}

/// Two-stage envelope parse. Stage 1: JSON syntax (bad JSON → `id = ""`).
/// Stage 2: field extraction — missing `id` defaults to `""`, missing `cmd` is
/// an error, missing `payload` becomes `null`. Errors carry the id if any.
fn parse_request(raw: &str) -> Result<RequestEnvelope, ParseError> {
    let value: Value = serde_json::from_str(raw)
        .map_err(|e| (String::new(), malformed(format!("malformed request: {e}"))))?;
    let obj = value.as_object().ok_or_else(|| (String::new(), malformed("malformed request: not a JSON object")))?;
    let id = obj.get("id").and_then(Value::as_str).unwrap_or_default().to_string();
    let Some(cmd) = obj.get("cmd").and_then(Value::as_str) else {
        return Err((id, malformed("malformed request: missing 'cmd' field")));
    };
    let payload = obj.get("payload").cloned().unwrap_or(Value::Null);
    Ok(RequestEnvelope { id, cmd: cmd.to_string(), payload })
}

/// Malformed-envelope error; `ErrorKind::InvalidConfig` is never retryable.
fn malformed(message: impl Into<String>) -> ErrorDto {
    ErrorDto::new(ErrorKind::InvalidConfig, message)
}
