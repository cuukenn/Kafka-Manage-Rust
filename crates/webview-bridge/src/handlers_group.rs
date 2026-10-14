//! T036: the 4 `group.*` commands (`list` / `describe` / `offsets` /
//! `resetOffsets`), wired against a real [`App`]. Same template as
//! `handlers.rs` and `handlers_topic.rs`: one clone of `GroupService` per
//! command body, and every payload is `{clusterId, ...req}` with the request
//! DTO's own fields flattened next to the cluster id.

use kafka_app::App;
use kafka_core::ClusterId;
use kafka_core::group::{ListGroupOffsetsReq, ResetOffsetsReq};
use kafka_core::protocol::cmd;
use serde::Deserialize;

use crate::handlers::{cluster_id_of, parse_payload, register_with, service_dto, to_data};
use crate::router::Router;

/// `{clusterId, ...req fields}`: `default` keeps a partial payload decodable, so
/// the service and driver validations — not serde — produce the domain errors
/// (`invalid_config` for a blank `groupId`, `not_found` for an unknown group).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct XPayload<R> {
    cluster_id: ClusterId,
    #[serde(flatten)]
    req: R,
}

/// `group.describe`: `ids` is optional on the wire; an empty list means "every
/// group the driver reports", which is exactly the service's own convention.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct DescribePayload {
    cluster_id: ClusterId,
    #[serde(default)]
    ids: Vec<String>,
}

/// Register the 4 `group.*` commands against a real [`App`].
///
/// `group.resetOffsets` answers the driver's `ResetOffsetsResult` verbatim
/// (`{groupId, applied, dryRun, error}`); the `GROUP_CHANGED` event is the
/// service's decision (it skips dry runs), never the bridge's.
pub fn register_group(app: &App, router: &Router) {
    let groups = app.groups().clone();

    register_with(
        router,
        cmd::GROUP_LIST,
        groups.clone(),
        |groups, payload| async move {
            let id = cluster_id_of(&payload)?;
            to_data(groups.list(&id).await.map_err(service_dto)?)
        },
    );

    register_with(
        router,
        cmd::GROUP_DESCRIBE,
        groups.clone(),
        |groups, payload| async move {
            let body: DescribePayload = parse_payload(&payload, cmd::GROUP_DESCRIBE)?;
            to_data(
                groups
                    .describe(&body.cluster_id, &body.ids)
                    .await
                    .map_err(service_dto)?,
            )
        },
    );

    register_with(
        router,
        cmd::GROUP_OFFSETS,
        groups.clone(),
        |groups, payload| async move {
            let body: XPayload<ListGroupOffsetsReq> = parse_payload(&payload, cmd::GROUP_OFFSETS)?;
            to_data(
                groups
                    .offsets(&body.cluster_id, &body.req)
                    .await
                    .map_err(service_dto)?,
            )
        },
    );

    register_with(
        router,
        cmd::GROUP_RESET_OFFSETS,
        groups.clone(),
        |groups, payload| async move {
            let body: XPayload<ResetOffsetsReq> =
                parse_payload(&payload, cmd::GROUP_RESET_OFFSETS)?;
            to_data(
                groups
                    .reset(&body.cluster_id, &body.req)
                    .await
                    .map_err(service_dto)?,
            )
        },
    );
}
