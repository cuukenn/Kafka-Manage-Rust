//! T034: the 6 `topic.*` commands plus `produce.send`, wired against a real
//! [`App`]. Same template as `handlers.rs`: each command body gets its own
//! clone of the service handle, and every payload is `{clusterId, ...req}`
//! with the request DTO flattened next to the cluster id.

use kafka_app::App;
use kafka_core::ClusterId;
use kafka_core::message::ProduceReq;
use kafka_core::protocol::cmd;
use kafka_core::topic::{
    CreateTopicReq, DeleteTopicsReq, DescribeTopicsReq, ListOffsetsReq, UpdateTopicConfigReq,
};
use serde::Deserialize;
use serde_json::Value;

use crate::handlers::{cluster_id_of, parse_payload, register_with, service_dto, to_data};
use crate::router::Router;

/// `{clusterId, ...req fields}`: the request DTO's own fields sit beside
/// `clusterId` on the wire. `default` keeps partial payloads decodable so the
/// service's own `validate()` — not serde — produces the domain errors.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct XPayload<R> {
    cluster_id: ClusterId,
    #[serde(flatten)]
    req: R,
}

/// `produce.send`: `{clusterId, requests: [ProduceReq...]}` is one call; a
/// partially failing batch still answers `ok:true` — the per-message failures
/// live inside the report (ledger ruling R6).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct ProducePayload {
    cluster_id: ClusterId,
    #[serde(default)]
    requests: Vec<ProduceReq>,
}

/// Register the 6 `topic.*` commands and `produce.send` against a real [`App`].
pub fn register_topic_and_produce(app: &App, router: &Router) {
    let topics = app.topics().clone();
    let produce = app.produce().clone();

    register_with(
        router,
        cmd::TOPIC_LIST,
        topics.clone(),
        |topics, payload| async move {
            let id = cluster_id_of(&payload)?;
            to_data(topics.list(&id).await.map_err(service_dto)?)
        },
    );

    register_with(
        router,
        cmd::TOPIC_DESCRIBE,
        topics.clone(),
        |topics, payload| async move {
            let body: XPayload<DescribeTopicsReq> = parse_payload(&payload, cmd::TOPIC_DESCRIBE)?;
            to_data(
                topics
                    .describe(&body.cluster_id, &body.req)
                    .await
                    .map_err(service_dto)?,
            )
        },
    );

    register_with(
        router,
        cmd::TOPIC_CREATE,
        topics.clone(),
        |topics, payload| async move {
            let body: XPayload<CreateTopicReq> = parse_payload(&payload, cmd::TOPIC_CREATE)?;
            topics
                .create(&body.cluster_id, &body.req)
                .await
                .map_err(service_dto)?;
            Ok(Value::Null)
        },
    );

    register_with(
        router,
        cmd::TOPIC_DELETE,
        topics.clone(),
        |topics, payload| async move {
            let body: XPayload<DeleteTopicsReq> = parse_payload(&payload, cmd::TOPIC_DELETE)?;
            to_data(
                topics
                    .delete(&body.cluster_id, &body.req)
                    .await
                    .map_err(service_dto)?,
            )
        },
    );

    register_with(
        router,
        cmd::TOPIC_UPDATE_CONFIG,
        topics.clone(),
        |topics, payload| async move {
            let body: XPayload<UpdateTopicConfigReq> =
                parse_payload(&payload, cmd::TOPIC_UPDATE_CONFIG)?;
            topics
                .update_config(&body.cluster_id, &body.req)
                .await
                .map_err(service_dto)?;
            Ok(Value::Null)
        },
    );

    register_with(
        router,
        cmd::TOPIC_OFFSETS,
        topics.clone(),
        |topics, payload| async move {
            let body: XPayload<ListOffsetsReq> = parse_payload(&payload, cmd::TOPIC_OFFSETS)?;
            to_data(
                topics
                    .offsets(&body.cluster_id, &body.req)
                    .await
                    .map_err(service_dto)?,
            )
        },
    );

    register_with(
        router,
        cmd::PRODUCE_SEND,
        produce,
        |produce, payload| async move {
            let body: ProducePayload = parse_payload(&payload, cmd::PRODUCE_SEND)?;
            let report = produce
                .send_batch(&body.cluster_id, &body.requests)
                .await
                .map_err(service_dto)?;
            to_data(report)
        },
    );
}
