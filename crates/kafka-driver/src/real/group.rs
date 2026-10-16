//! Consumer-group metadata: the group list and per-group member detail.
//!
//! librdkafka's group API is the deprecated `rd_kafka_list_groups`, which the
//! rdkafka crate exposes as `Client::fetch_group_list`. It answers with a native
//! snapshot that is neither `Send` nor `Sync`, so it is parsed into owned DTOs
//! inside the blocking closure that fetched it (R31/R35).

use std::time::Duration;

use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::groups::GroupList;

use kafka_core::error::DriverError;
use kafka_core::group::{GroupDetail, GroupInfo, GroupMember, GroupState};
use kafka_core::topic::TopicPartition;

use super::Live;

/// Every group the cluster knows about.
pub(super) async fn list(live: &Live) -> Result<Vec<GroupInfo>, DriverError> {
    Ok(fetch(live, None).await?.list)
}

/// Member detail for the named groups; an empty `ids` means every group.
///
/// A named group the broker does not know is a `GroupNotFound`, matching the mock.
pub(super) async fn describe(live: &Live, ids: &[String]) -> Result<Vec<GroupDetail>, DriverError> {
    if ids.is_empty() {
        return Ok(fetch(live, None).await?.details);
    }
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        match fetch(live, Some(id.clone())).await?.details.into_iter().next() {
            Some(detail) => out.push(detail),
            // librdkafka reports a group it cannot find by leaving it out of the
            // list rather than by returning an error (R57).
            None => return Err(DriverError::GroupNotFound(id.clone())),
        }
    }
    Ok(out)
}

/// Both views of one group-list response, so a single fetch can answer either call.
struct Snapshot {
    list: Vec<GroupInfo>,
    details: Vec<GroupDetail>,
}

async fn fetch(live: &Live, group: Option<String>) -> Result<Snapshot, DriverError> {
    let admin = live.admin.clone();
    let server = live.cfg.bootstrap_joined();
    let timeout = Duration::from_millis(live.cfg.timeouts.request_ms.max(1));
    let outcome = tokio::task::spawn_blocking(move || {
        let list = admin.inner().fetch_group_list(group.as_deref(), timeout)?;
        Ok::<Snapshot, KafkaError>(convert(&list))
    })
    .await
    .map_err(|e| DriverError::other(format!("group task failed: {e}")))?;
    outcome.map_err(|e| group_error(&server, timeout, e))
}

fn convert(list: &GroupList) -> Snapshot {
    let mut groups = Vec::with_capacity(list.groups().len());
    let mut details = Vec::with_capacity(list.groups().len());
    for group in list.groups() {
        let members: Vec<GroupMember> = group
            .members()
            .iter()
            .map(|member| GroupMember {
                member_id: member.id().to_string(),
                client_id: member.client_id().to_string(),
                client_host: member.client_host().to_string(),
                assignments: assignments(member.assignment()),
            })
            .collect();
        // librdkafka exposes neither the coordinator nor anything tying a group to
        // this driver, so both stay unset (R55/R56).
        let info = GroupInfo {
            group_id: group.name().to_string(),
            state: GroupState::from_broker(group.state()),
            member_count: members.len(),
            protocol_type: group.protocol_type().to_string(),
            partition_assignor: group.protocol().to_string(),
            coordinator_id: None,
            is_local: false,
        };
        details.push(GroupDetail { info: info.clone(), members });
        groups.push(info);
    }
    Snapshot { list: groups, details }
}

/// Decodes a member's serialized `ConsumerProtocolAssignment`.
///
/// The wire shape is an `int16` version, an `int32` topic count, then per topic an
/// `int16`-length topic name, an `int32` partition count and that many `int32`
/// partition ids, closed by a `bytes` user-data field nothing here reads. A member
/// with no assignment yet — or a group speaking some other protocol — yields no
/// partitions rather than an error (R57).
fn assignments(raw: Option<&[u8]>) -> Vec<TopicPartition> {
    raw.and_then(decode_assignment).unwrap_or_default()
}

fn decode_assignment(raw: &[u8]) -> Option<Vec<TopicPartition>> {
    let mut reader = Reader { bytes: raw, at: 0 };
    reader.i16()?;
    let topics = reader.i32()?;
    let mut out = Vec::new();
    for _ in 0..topics.max(0) {
        let topic = reader.text()?.to_string();
        let partitions = reader.i32()?;
        for _ in 0..partitions.max(0) {
            let id = reader.i32()?;
            out.push(TopicPartition::new(topic.clone(), u32::try_from(id).unwrap_or_default()));
        }
    }
    Some(out)
}

/// A minimal big-endian reader for the ConsumerProtocol assignment bytes.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, count: usize) -> Option<&'a [u8]> {
        let end = self.at.checked_add(count)?;
        let slice = self.bytes.get(self.at..end)?;
        self.at = end;
        Some(slice)
    }

    fn i16(&mut self) -> Option<i16> {
        let bytes = self.take(2)?;
        Some(i16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn i32(&mut self) -> Option<i32> {
        let bytes = self.take(4)?;
        Some(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// A Kafka `string`: `int16` length (`-1` is null) followed by UTF-8.
    fn text(&mut self) -> Option<&'a str> {
        let length = self.i16()?;
        if length < 0 {
            return Some("");
        }
        std::str::from_utf8(self.take(length as usize)?).ok()
    }
}

fn group_error(server: &str, timeout: Duration, err: KafkaError) -> DriverError {
    if matches!(err, KafkaError::GroupListFetch(RDKafkaErrorCode::RequestTimedOut)) {
        return DriverError::timeout("fetch_group_list", timeout.as_millis() as u64);
    }
    DriverError::Connection { server: server.to_string(), message: err.to_string() }
}
