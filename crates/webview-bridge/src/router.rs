//! Transport-agnostic command router: raw JSON string in, `ResponseEnvelope` out.
//! Pipeline: parse envelope → command table → async handler → response. No IO,
//! no tao/wry, no real `App` — wiring the facade is T033+.
//!
//! Two anti-hang guards: T082a bounds every request with a global deadline, and
//! T082b adds the built-in `request.cancel` command, which drops a request that
//! is still in flight instead of letting it run to completion.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::Duration;

use futures::future::BoxFuture;
use kafka_core::protocol::{cmd, ErrorDto, RequestEnvelope, ResponseEnvelope};
use kafka_core::ErrorKind;
use serde_json::{json, Value};
use tokio::task::AbortHandle;

/// A registered command handler: takes the raw payload, returns the `data` body
/// or an `ErrorDto`. Object-safe and `Send + Sync` for the bridge event loop.
pub type Handler =
    Arc<dyn Fn(Value) -> BoxFuture<'static, Result<Value, ErrorDto>> + Send + Sync>;

type Table = HashMap<String, Handler>;
type ParseError = (String, ErrorDto);

/// How long one request may run before the router answers `timeout` and aborts
/// the handler (T082a). A backstop: the driver's own deadlines come first.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// In-flight handlers by envelope id. Plain `std::sync::Mutex`, never held
/// across an `.await`.
type Inflight = Arc<Mutex<HashMap<String, AbortHandle>>>;

/// The command table. `Clone` (state behind `Arc`) so the event loop and the
/// tests can hold independent handles to the same routing state.
#[derive(Clone)]
pub struct Router {
    handlers: Arc<RwLock<Table>>,
    inflight: Inflight,
    timeout: Duration,
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

impl Router {
    /// A router with `DEFAULT_TIMEOUT` and the built-in `request.cancel` command.
    pub fn new() -> Self {
        let router = Self {
            handlers: Arc::new(RwLock::new(Table::new())),
            inflight: Arc::new(Mutex::new(HashMap::new())),
            timeout: DEFAULT_TIMEOUT,
        };
        router.register_cancel();
        router
    }

    /// Override the global deadline (T082a): `Router::new().with_timeout(..)`.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// How many requests are in flight right now (T082b). The UI's cancel button
    /// counts on this, and the tests assert it returns to zero.
    pub fn inflight(&self) -> usize {
        self.lock_inflight().len()
    }

    /// Register `handler` for `cmd`. Duplicate registration overrides (last wins).
    pub fn register(&self, cmd: impl Into<String>, handler: Handler) {
        self.handlers.write().unwrap_or_else(|e| e.into_inner()).insert(cmd.into(), handler);
    }

    /// Drop an in-flight request by envelope id (T082b). `true` when *this* call
    /// did the cancelling; an id that already answered, timed out, or never
    /// existed is `false` -- dropping nothing is not an error.
    pub fn cancel(&self, id: &str) -> bool {
        match self.lock_inflight().remove(id) {
            Some(handle) => {
                handle.abort();
                true
            }
            None => false,
        }
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
        // handler must not unwind through `Handle::block_on` in the wry protocol
        // callback. T082a stops waiting on this handle after `self.timeout`;
        // T082b keeps it addressable so `request.cancel` can abort it early.
        let mut task = tokio::spawn(handler(payload));
        self.track(&id, task.abort_handle());
        let outcome = tokio::time::timeout(self.timeout, &mut task).await;
        self.lock_inflight().remove(&id);
        let result = match outcome {
            Ok(Ok(result)) => result,
            // Aborted by `request.cancel`: the handler never produced a value.
            Ok(Err(join_err)) if join_err.is_cancelled() => {
                let error = cancelled(&id);
                return ResponseEnvelope::err_dto(id, error);
            }
            Ok(Err(join_err)) => {
                return ResponseEnvelope::err_dto(
                    id,
                    ErrorDto::new(ErrorKind::Internal, format!("handler panicked: {join_err}")),
                );
            }
            Err(_elapsed) => {
                task.abort();
                let error = deadline(&cmd, self.timeout);
                return ResponseEnvelope::err_dto(id, error);
            }
        };
        match result {
            Ok(data) => ResponseEnvelope::ok(id, data),
            Err(error) => ResponseEnvelope::err_dto(id, error),
        }
    }

    /// Remember `handle` under `id` so `request.cancel` can reach it. An id-less
    /// envelope is not addressable, so it is not tracked (R83).
    fn track(&self, id: &str, handle: AbortHandle) {
        if id.is_empty() {
            return;
        }
        self.lock_inflight().insert(id.to_string(), handle);
    }

    /// Poisoning is impossible (see `dispatch`); recover instead of panicking.
    fn lock_inflight(&self) -> MutexGuard<'_, HashMap<String, AbortHandle>> {
        self.inflight.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The built-in `request.cancel` (T082b): `{ id }` in, `{ id, cancelled }` out.
    fn register_cancel(&self) {
        let inflight = Arc::clone(&self.inflight);
        self.register(
            cmd::REQUEST_CANCEL,
            Arc::new(move |payload: Value| {
                let inflight = Arc::clone(&inflight);
                Box::pin(async move {
                    let id = payload.get("id").and_then(Value::as_str).unwrap_or_default();
                    if id.is_empty() {
                        return Err(ErrorDto::new(
                            ErrorKind::InvalidConfig,
                            "request.cancel needs the 'id' of the request to drop",
                        ));
                    }
                    let live = inflight.lock().unwrap_or_else(|e| e.into_inner()).remove(id);
                    if let Some(handle) = &live {
                        handle.abort();
                    }
                    Ok(json!({ "id": id, "cancelled": live.is_some() }))
                })
            }),
        );
    }
}

/// `timeout` DTO for a request the router cut off (T082a, R81).
fn deadline(cmd: &str, timeout: Duration) -> ErrorDto {
    ErrorDto::new(
        ErrorKind::Timeout,
        format!("request '{cmd}' exceeded the {} ms deadline", timeout.as_millis()),
    )
}

/// `cancelled` DTO for a request the UI dropped (T082b, R82).
fn cancelled(id: &str) -> ErrorDto {
    ErrorDto::new(ErrorKind::Cancelled, format!("request '{id}' was cancelled"))
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
