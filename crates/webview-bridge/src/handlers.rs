//! Real-`App` command wiring: the helpers every command card reuses, plus the
//! `cluster.*` and `config.*` tables. Each command body gets its own clone of the
//! service handle it needs, so the table stays `'static + Send + Sync`.

use std::sync::Arc;

use futures::future::BoxFuture;
use kafka_app::services::ServiceError;
use kafka_app::store::ImportConflict;
use kafka_app::App;
use kafka_core::cluster::ClusterConfig;
use kafka_core::error::StorageError;
use kafka_core::protocol::{cmd, ErrorDto};
use kafka_core::{ClusterId, ErrorKind};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::router::{Handler, Router};

/// What a handler yields: the `data` body, or the error the UI renders.
pub type Outcome = Result<Value, ErrorDto>;

/// The handler template the table holds: `Arc<dyn Fn(Value) -> BoxFuture<..>>`.
pub fn handler<F>(f: F) -> Handler
where
    F: Fn(Value) -> BoxFuture<'static, Outcome> + Send + Sync + 'static,
{
    Arc::new(f)
}

/// Register one command whose body needs its own clone of `state` per request:
/// a service handle cannot be moved into the `async` block on every call, so the
/// closure clones it first, keeping the `Fn` bound while sharing one registry.
pub fn register_with<State, F, Fut>(router: &Router, cmd: &str, state: State, body: F)
where
    State: Clone + Send + Sync + 'static,
    F: Fn(State, Value) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Outcome> + Send + 'static,
{
    let handler = handler(move |payload| {
        let state = state.clone();
        Box::pin(body(state, payload))
    });
    router.register(cmd, handler);
}

/// Decode a payload; the message names `cmd` so the UI's request log can point
/// at the offending call instead of a bare serde string.
pub fn parse_payload<T: DeserializeOwned>(payload: &Value, cmd: &str) -> Result<T, ErrorDto> {
    serde_json::from_value(payload.clone())
        .map_err(|e| ErrorDto::new(ErrorKind::InvalidConfig, format!("invalid payload for '{cmd}': {e}")))
}

/// Serialize one `data` body; a failure here is the bridge's own fault.
pub fn to_data<T: Serialize>(data: T) -> Result<Value, ErrorDto> {
    serde_json::to_value(&data).map_err(|e| ErrorDto::new(ErrorKind::Internal, format!("failed to serialize response: {e}")))
}

/// `clusterId` out of a payload; missing or malformed is the UI's bug.
pub fn cluster_id_of(payload: &Value) -> Result<ClusterId, ErrorDto> {
    let raw = payload.get("clusterId").and_then(Value::as_str).ok_or_else(|| invalid("payload is missing a 'clusterId' string"))?;
    serde_json::from_value(Value::String(raw.to_string()))
        .map_err(|e| invalid(format!("invalid 'clusterId' '{raw}': {e}")))
}

/// Store-only commands must not block the UI thread: SQLite work goes to the
/// blocking pool, where a crashed task becomes an error instead of a hang.
pub async fn blocking<T, F>(f: F) -> Result<T, ErrorDto>
where
    F: FnOnce() -> Result<T, StorageError> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(result) => result.map_err(storage_dto),
        Err(err) => Err(ErrorDto::new(ErrorKind::Internal, format!("blocking store task failed: {err}"))),
    }
}

/// `ServiceError -> ErrorDto` on the wire side. `to_dto()` cannot see inside
/// [`StorageError`] — every variant collapses to the `storage` kind — so a stale
/// cluster id is re-typed as `not_found` here: the UI must treat it as a typo,
/// not as a failing disk.
pub(crate) fn service_dto(err: ServiceError) -> ErrorDto {
    if let ServiceError::Storage(StorageError::NotFound(_)) = &err {
        return ErrorDto::new(ErrorKind::NotFound, err.to_string());
    }
    err.to_dto()
}

/// `StorageError -> ErrorDto`; the error's own kind decides `retryable` (T065).
fn storage_dto(err: StorageError) -> ErrorDto {
    ErrorDto::new(err.kind(), err.to_string())
}

/// Bad input from the UI: reported verbatim, never retryable.
fn invalid(message: impl Into<String>) -> ErrorDto {
    ErrorDto::new(ErrorKind::InvalidConfig, message)
}

/// `cluster.import`: the exported document plus how existing ids behave.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ImportPayload {
    json: String,
    /// Wire string, decoded by [`conflict_of`] — `ImportConflict` has no serde impl.
    #[serde(default)]
    on_conflict: Option<String>,
}

/// `config.*`: the KV key; `value` only arrives with `config.set`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConfigPayload {
    key: String,
    #[serde(default)]
    value: Value,
}

/// `"skip" | "overwrite" | "fail"`; the default keeps imports all-or-nothing.
fn conflict_of(raw: Option<String>) -> Result<ImportConflict, ErrorDto> {
    match raw.as_deref() {
        None | Some("fail") => Ok(ImportConflict::Fail),
        Some("skip") => Ok(ImportConflict::Skip),
        Some("overwrite") => Ok(ImportConflict::Overwrite),
        Some(other) => Err(invalid(format!("unknown 'onConflict' '{other}' (expected skip, overwrite or fail)"))),
    }
}

/// Register the 8 `cluster.*` commands against a real [`App`].
pub fn register_cluster(app: &App, router: &Router) {
    let clusters = app.clusters().clone();
    let store = app.store().clone();

    register_with(router, cmd::CLUSTER_LIST, clusters.clone(), |clusters, _| async move {
        to_data(clusters.list().await.map_err(service_dto)?)
    });

    register_with(router, cmd::CLUSTER_SAVE, clusters.clone(), |clusters, payload| async move {
        let cfg: ClusterConfig = parse_payload(&payload, cmd::CLUSTER_SAVE)?;
        let saved = clusters.save(&cfg).await.map_err(service_dto)?;
        to_data(saved)
    });

    register_with(router, cmd::CLUSTER_DELETE, clusters.clone(), |clusters, payload| async move {
        let id = cluster_id_of(&payload)?;
        to_data(clusters.delete(&id).await.map_err(service_dto)?)
    });

    register_with(router, cmd::CLUSTER_CONNECT, clusters.clone(), |clusters, payload| async move {
        let id = cluster_id_of(&payload)?;
        let info = clusters.connect(&id).await.map_err(service_dto)?;
        to_data(info)
    });

    register_with(router, cmd::CLUSTER_DISCONNECT, clusters.clone(), |clusters, payload| async move {
        let id = cluster_id_of(&payload)?;
        clusters.disconnect(&id).await.map_err(service_dto)?;
        Ok(Value::Null)
    });

    register_with(router, cmd::CLUSTER_INFO, clusters.clone(), |clusters, payload| async move {
        let id = cluster_id_of(&payload)?;
        let info = clusters.info(&id).await.map_err(service_dto)?;
        to_data(info)
    });

    register_with(router, cmd::CLUSTER_EXPORT, store.clone(), |store, _| async move {
        let document = blocking(move || store.export_clusters()).await?;
        serde_json::from_str::<Value>(&document)
            .map_err(|e| ErrorDto::new(ErrorKind::Internal, format!("cluster export produced invalid JSON: {e}")))
    });

    register_with(router, cmd::CLUSTER_IMPORT, store.clone(), |store, payload| async move {
        let body: ImportPayload = parse_payload(&payload, cmd::CLUSTER_IMPORT)?;
        let on_conflict = conflict_of(body.on_conflict)?;
        let document = body.json;
        let report = blocking(move || store.import_clusters(&document, on_conflict)).await?;
        Ok(json!({
            "imported": report.imported,
            "overwritten": report.overwritten,
            "skipped": report.skipped,
        }))
    });
}

/// Register the 2 `config.*` commands. Settings live in the store's KV table.
pub fn register_config(app: &App, router: &Router) {
    let store = app.store().clone();

    register_with(router, cmd::CONFIG_SET, store.clone(), |store, payload| async move {
        let body: ConfigPayload = parse_payload(&payload, cmd::CONFIG_SET)?;
        let stored = serde_json::to_string(&body.value)
            .map_err(|e| ErrorDto::new(ErrorKind::Internal, format!("config value is not serializable: {e}")))?;
        blocking(move || store.set_raw(&body.key, &stored)).await?;
        Ok(Value::Null)
    });

    register_with(router, cmd::CONFIG_GET, store.clone(), |store, payload| async move {
        let body: ConfigPayload = parse_payload(&payload, cmd::CONFIG_GET)?;
        let key = body.key.clone();
        let stored = blocking(move || store.get_raw(&key)).await?;
        match stored {
            None => Ok(Value::Null),
            Some(stored) => serde_json::from_str::<Value>(&stored)
                .map_err(|e| ErrorDto::new(ErrorKind::Internal, format!("stored config '{}' is not valid JSON: {e}", body.key))),
        }
    });
}
