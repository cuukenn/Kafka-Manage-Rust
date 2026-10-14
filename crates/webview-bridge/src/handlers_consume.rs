//! T035: the 6 `consume.*` commands wired against a real [`App`]. Same template
//! as `handlers.rs` / `handlers_topic.rs`: each command body gets its own clone
//! of the service handle. Per ledger ruling R6 a missing `subscriptionId` gets
//! its UUID from `SubscribeReq::default()` via the serde default — the bridge
//! never invents ids. Forwarding stream messages to JS is the event pump's job
//! (T037), not this table's.

use kafka_app::App;
use kafka_core::ClusterId;
use kafka_core::message::{SubscribeReq, SubscriptionId};
use kafka_core::protocol::cmd;
use serde::Deserialize;
use serde_json::Value;

use crate::handlers::{parse_payload, register_with, service_dto, to_data};
use crate::router::Router;

/// `consume.subscribe`: `{clusterId, ...SubscribeReq fields}` — the request
/// DTO's own fields sit beside `clusterId` on the wire. `default` keeps partial
/// payloads decodable so the service's own `validate()` — not serde — produces
/// the domain errors, and an absent `subscriptionId` falls back to the UUID
/// from `SubscribeReq::default()` (ruling R6).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct SubscribePayload {
    cluster_id: ClusterId,
    #[serde(flatten)]
    req: SubscribeReq,
}

/// `consume.unsubscribe` / `pause` / `resume`: `{subscriptionId}`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SubPayload {
    subscription_id: SubscriptionId,
}

/// `consume.seek`: `{subscriptionId, topic, partition, offset}`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SeekPayload {
    subscription_id: SubscriptionId,
    topic: String,
    partition: u32,
    offset: i64,
}

/// Register the 6 `consume.*` commands against a real [`App`].
pub fn register_consume(app: &App, router: &Router) {
    let consume = app.consume().clone();

    register_with(
        router,
        cmd::CONSUBSCRIBE_START,
        consume.clone(),
        |consume, payload| async move {
            let body: SubscribePayload = parse_payload(&payload, cmd::CONSUBSCRIBE_START)?;
            to_data(
                consume
                    .start(&body.cluster_id, body.req)
                    .await
                    .map_err(service_dto)?,
            )
        },
    );

    register_with(
        router,
        cmd::CONSUBSCRIBE_STOP,
        consume.clone(),
        |consume, payload| async move {
            let body: SubPayload = parse_payload(&payload, cmd::CONSUBSCRIBE_STOP)?;
            to_data(
                consume
                    .stop(&body.subscription_id)
                    .await
                    .map_err(service_dto)?,
            )
        },
    );

    register_with(
        router,
        cmd::CONSUME_PAUSE,
        consume.clone(),
        |consume, payload| async move {
            let body: SubPayload = parse_payload(&payload, cmd::CONSUME_PAUSE)?;
            consume.pause(&body.subscription_id).map_err(service_dto)?;
            Ok(Value::Null)
        },
    );

    register_with(
        router,
        cmd::CONSUME_RESUME,
        consume.clone(),
        |consume, payload| async move {
            let body: SubPayload = parse_payload(&payload, cmd::CONSUME_RESUME)?;
            consume.resume(&body.subscription_id).map_err(service_dto)?;
            Ok(Value::Null)
        },
    );

    register_with(
        router,
        cmd::CONSUME_SEEK,
        consume.clone(),
        |consume, payload| async move {
            let body: SeekPayload = parse_payload(&payload, cmd::CONSUME_SEEK)?;
            consume
                .seek(
                    &body.subscription_id,
                    body.topic,
                    body.partition,
                    body.offset,
                )
                .map_err(service_dto)?;
            Ok(Value::Null)
        },
    );

    register_with(
        router,
        cmd::CONSUME_LIST,
        consume,
        |consume, _| async move { to_data(consume.list()) },
    );
}
