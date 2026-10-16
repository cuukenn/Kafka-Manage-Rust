//! Consumer-group metadata: the group list and per-group member detail.
//!
//! librdkafka's group API is the deprecated `rd_kafka_list_groups`, which the
//! rdkafka crate exposes as `Client::fetch_group_list`. It answers with a native
//! snapshot that is neither `Send` nor `Sync`, so it is parsed into owned DTOs
//! inside the blocking closure that fetched it (R31/R35).

use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::groups::GroupList;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

use kafka_core::error::DriverError;
use kafka_core::group::{
    CommitOffsetsReq, GroupDetail, GroupInfo, GroupMember, GroupOffsetEntry, GroupState,
    ListGroupOffsetsReq, ResetOffsetsReq, ResetOffsetsResult, ResetSpec,
};
use kafka_core::topic::{TopicPartition, TopicPartitionOffset};

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

/// A one-shot consumer that speaks for an explicit group. Nothing is ever polled
/// or joined: the calls below address the group's coordinator directly.
fn group_consumer(live: &Live, group_id: &str) -> Result<BaseConsumer, DriverError> {
    super::consumer::consumer_config_for(&live.cfg, group_id)?
        .create::<BaseConsumer>()
        .map_err(|e| DriverError::Connection {
            server: live.cfg.bootstrap_joined(),
            message: e.to_string(),
        })
}

/// Writes committed offsets for one group.
pub(super) async fn commit(live: &Live, req: CommitOffsetsReq) -> Result<(), DriverError> {
    let Some(group_id) = req.group_id.as_deref().filter(|id| !id.trim().is_empty()) else {
        return Err(DriverError::unsupported("real commit requires an explicit group id"));
    };
    if req.offsets.is_empty() {
        return Err(DriverError::invalid("offsets", "at least one offset is required"));
    }

    let consumer = group_consumer(live, group_id)?;
    let mut list = TopicPartitionList::with_capacity(req.offsets.len());
    for entry in &req.offsets {
        let partition = super::consumer::to_i32(entry.partition);
        list.add_partition_offset(&entry.topic, partition, Offset::Offset(entry.offset))
            .map_err(|e| DriverError::invalid("offsets", e.to_string()))?;
        if !entry.metadata.is_empty() {
            if let Some(mut element) = list.find_partition(&entry.topic, partition) {
                element.set_metadata(&entry.metadata);
            }
        }
    }
    commit_list(live, consumer, list).await
}

/// Computes a group's target offsets and, unless this is a dry run, writes them.
pub(super) async fn reset(
    live: &Live,
    req: &ResetOffsetsReq,
) -> Result<ResetOffsetsResult, DriverError> {
    req.validate()?;
    if !exists(live, &req.group_id).await? {
        return Err(DriverError::GroupNotFound(req.group_id.clone()));
    }
    let targets = reset_targets(live, req).await?;
    let marks = super::consumer::watermarks(live, &targets).await?;

    let resolved: Vec<i64> = match req.spec {
        ResetSpec::Earliest => marks.iter().map(|(low, _)| *low).collect(),
        ResetSpec::Latest => marks.iter().map(|(_, high)| *high).collect(),
        ResetSpec::ToOffset(value) => {
            let mut out = Vec::with_capacity(targets.len());
            for (target, (low, high)) in targets.iter().zip(&marks) {
                if value < *low || value > *high {
                    return Err(DriverError::OffsetOutOfRange {
                        topic: target.topic.clone(),
                        partition: target.partition,
                        requested: value,
                        low: *low,
                        high: *high,
                    });
                }
                out.push(value);
            }
            out
        }
        ResetSpec::ToTimestamp(timestamp) => {
            super::consumer::at_or_after(live, &targets, &marks, timestamp).await?
        }
    };

    let applied: Vec<TopicPartitionOffset> = targets
        .iter()
        .zip(&resolved)
        .map(|(target, offset)| TopicPartitionOffset {
            topic: target.topic.clone(),
            partition: target.partition,
            offset: *offset,
            metadata: String::new(),
            timestamp: None,
            leader_epoch: None,
        })
        .collect();

    if !req.dry_run && !applied.is_empty() {
        let consumer = group_consumer(live, &req.group_id)?;
        let mut list = TopicPartitionList::with_capacity(applied.len());
        for entry in &applied {
            list.add_partition_offset(
                &entry.topic,
                super::consumer::to_i32(entry.partition),
                Offset::Offset(entry.offset),
            )
            .map_err(|e| DriverError::invalid("partitions", e.to_string()))?;
        }
        commit_list(live, consumer, list).await?;
    }

    tracing::info!(
        group = %req.group_id,
        partitions = applied.len(),
        dry_run = req.dry_run,
        "real group offsets reset"
    );
    Ok(ResetOffsetsResult {
        group_id: req.group_id.clone(),
        applied,
        dry_run: req.dry_run,
        error: None,
    })
}

/// The partitions a reset covers: the explicit ones, else the named topics'
/// partitions, else whatever the group already committed to.
async fn reset_targets(
    live: &Live,
    req: &ResetOffsetsReq,
) -> Result<Vec<TopicPartition>, DriverError> {
    if !req.partitions.is_empty() {
        check_partitions(live, &req.partitions).await?;
        return Ok(req.partitions.clone());
    }
    if !req.topics.is_empty() {
        return offset_targets(live, &req.topics).await;
    }
    let committed = offsets(
        live,
        &ListGroupOffsetsReq {
            group_id: req.group_id.clone(),
            topics: Vec::new(),
            with_lag: false,
        },
    )
    .await?;
    Ok(committed
        .into_iter()
        .map(|entry| TopicPartition::new(entry.topic, entry.partition))
        .collect())
}

/// Explicit partitions still have to name a real topic and partition, otherwise
/// the failure surfaces later as an opaque watermark transport error.
async fn check_partitions(live: &Live, wanted: &[TopicPartition]) -> Result<(), DriverError> {
    if wanted.is_empty() {
        return Ok(());
    }
    let metadata = super::fetch_metadata(live, None).await?;
    for target in wanted {
        let topic = metadata
            .topics()
            .iter()
            .find(|entry| entry.name() == target.topic && entry.error().is_none())
            .ok_or_else(|| DriverError::TopicNotFound(target.topic.clone()))?;
        let known = topic
            .partitions()
            .iter()
            .any(|p| p.error().is_none() && p.id() == super::consumer::to_i32(target.partition));
        if !known {
            return Err(DriverError::PartitionNotFound {
                topic: target.topic.clone(),
                partition: target.partition,
            });
        }
    }
    Ok(())
}

/// Commits a prepared list synchronously, so a returned `Ok` means the coordinator
/// stored it rather than merely queued it.
async fn commit_list(
    live: &Live,
    consumer: BaseConsumer,
    list: TopicPartitionList,
) -> Result<(), DriverError> {
    let server = live.cfg.bootstrap_joined();
    let timeout = Duration::from_millis(live.cfg.timeouts.request_ms.max(1));
    tokio::task::spawn_blocking(move || consumer.commit(&list, CommitMode::Sync))
        .await
        .map_err(|e| DriverError::other(format!("commit task failed: {e}")))?
        .map_err(|e| group_error("commit", &server, timeout, e))
}

/// Both views of one group-list response, so a single fetch can answer either call.
struct Snapshot {
    list: Vec<GroupInfo>,
    details: Vec<GroupDetail>,
}

/// Whether the broker knows this group at all.
pub(super) async fn exists(live: &Live, id: &str) -> Result<bool, DriverError> {
    Ok(!fetch(live, Some(id.to_string())).await?.list.is_empty())
}

/// The committed offsets of one group, with lag when the caller asked for it.
pub(super) async fn offsets(
    live: &Live,
    req: &ListGroupOffsetsReq,
) -> Result<Vec<GroupOffsetEntry>, DriverError> {
    if req.group_id.trim().is_empty() {
        return Err(DriverError::invalid("groupId", "group id must not be empty"));
    }
    if !exists(live, &req.group_id).await? {
        return Err(DriverError::GroupNotFound(req.group_id.clone()));
    }

    let targets = offset_targets(live, &req.topics).await?;
    if targets.is_empty() {
        return Ok(Vec::new());
    }
    let consumer = group_consumer(live, &req.group_id)?;

    let mut requested = TopicPartitionList::with_capacity(targets.len());
    for target in &targets {
        requested.add_partition(&target.topic, i32::try_from(target.partition).unwrap_or(i32::MAX));
    }

    let server = live.cfg.bootstrap_joined();
    let timeout = Duration::from_millis(live.cfg.timeouts.request_ms.max(1));
    let committed = tokio::task::spawn_blocking(move || consumer.committed_offsets(requested, timeout))
        .await
        .map_err(|e| DriverError::other(format!("committed task failed: {e}")))?
        .map_err(|e| group_error("offset fetch", &server, timeout, e))?;

    // Only partitions the group actually committed are rows; anything else in the
    // response is `Offset::Invalid`, i.e. "never committed here" (R58).
    let rows: Vec<(TopicPartition, i64, String)> = committed
        .elements()
        .into_iter()
        .filter_map(|element| match element.offset() {
            Offset::Offset(offset) => Some((
                TopicPartition::new(
                    element.topic(),
                    u32::try_from(element.partition()).unwrap_or_default(),
                ),
                offset,
                element.metadata().to_string(),
            )),
            _ => None,
        })
        .collect();

    let highs: Option<Vec<i64>> = if req.with_lag {
        let wanted: Vec<TopicPartition> = rows.iter().map(|(target, _, _)| target.clone()).collect();
        match super::consumer::watermarks(live, &wanted).await {
            Ok(marks) => Some(marks.into_iter().map(|(_, high)| high).collect()),
            Err(error) => {
                tracing::warn!(%error, partitions = wanted.len(), "log end offsets unavailable; lag left empty");
                None
            }
        }
    } else {
        None
    };

    Ok(rows
        .into_iter()
        .enumerate()
        .map(|(index, (target, offset, metadata))| GroupOffsetEntry {
            topic: target.topic,
            partition: target.partition,
            offset,
            metadata,
            committed_at: None,
            lag: highs.as_ref().map(|highs| (highs[index] - offset).max(0)),
        })
        .collect())
}

/// The partitions a group-offset request covers: the named topics' healthy
/// partitions, or every partition of every non-internal topic when none are named
/// (R60).
async fn offset_targets(live: &Live, topics: &[String]) -> Result<Vec<TopicPartition>, DriverError> {
    let metadata = super::fetch_metadata(live, None).await?;
    let mut out = Vec::new();
    let mut seen: Vec<&str> = Vec::new();
    for entry in metadata.topics() {
        if entry.error().is_some() {
            continue;
        }
        let name = entry.name();
        let wanted = if topics.is_empty() {
            !super::topic::is_internal(name)
        } else {
            topics.iter().any(|topic| topic == name)
        };
        if !wanted {
            continue;
        }
        seen.push(name);
        for partition in entry.partitions() {
            if partition.error().is_some() {
                continue;
            }
            out.push(TopicPartition::new(
                name,
                u32::try_from(partition.id()).unwrap_or_default(),
            ));
        }
    }
    if let Some(missing) = topics.iter().find(|topic| !seen.contains(&topic.as_str())) {
        return Err(DriverError::TopicNotFound(missing.clone()));
    }
    Ok(out)
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
    outcome.map_err(|e| group_error("fetch_group_list", &server, timeout, e))
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

fn group_error(operation: &str, server: &str, timeout: Duration, err: KafkaError) -> DriverError {
    let timed_out = matches!(
        err,
        KafkaError::GroupListFetch(RDKafkaErrorCode::RequestTimedOut)
            | KafkaError::ConsumerCommit(RDKafkaErrorCode::RequestTimedOut)
    );
    if timed_out {
        return DriverError::timeout(operation, timeout.as_millis() as u64);
    }
    DriverError::Connection { server: server.to_string(), message: err.to_string() }
}
