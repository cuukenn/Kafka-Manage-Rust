//! The consuming side of the real driver: offset lookups (T074a) and, in T074b,
//! the `subscribe` stream.
//!
//! Only a consumer can answer "first offset at or after this timestamp", so a
//! timestamp query borrows a throwaway one (R45); plain beginning/end lookups run
//! off the admin client's `fetch_watermarks`, the same source `describe_topics`
//! reads.

use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::ClientConfig;

use kafka_core::cluster::ClusterConfig;
use kafka_core::error::DriverError;
use kafka_core::topic::{ListOffsetsReq, OffsetRange, OffsetSpec, TopicPartition};

use super::{fetch_metadata, topic, Live};

/// The client behind every read-only offset query.
///
/// Partitions are always assigned explicitly and no group is ever joined, so
/// nothing is auto-committed; `enable.partition.eof` is what lets a bounded read
/// notice a drained partition (T074b). `cfg.extra` still lands last, so a user can
/// undo either (R47).
pub(super) fn consumer_config(cfg: &ClusterConfig) -> Result<ClientConfig, DriverError> {
    let mut conf = super::config::client_config(cfg)?;
    conf.set("enable.auto.commit", "false").set("enable.partition.eof", "true");
    for (key, value) in &cfg.extra {
        conf.set(key, value);
    }
    Ok(conf)
}

impl super::RealKafkaDriver {
    /// Beginning/end offsets for the requested partitions plus the offset the spec
    /// resolves to. An empty request means every partition of every visible topic,
    /// internal topics excluded (R46).
    pub(super) async fn real_list_offsets(
        &self,
        req: &ListOffsetsReq,
    ) -> Result<Vec<OffsetRange>, DriverError> {
        let live = self.live().await?;
        let targets = targets(&live, &req.partitions).await?;
        let marks = watermarks(&live, &targets).await?;
        let resolved = match req.spec {
            OffsetSpec::Earliest => marks.iter().map(|(low, _)| *low).collect(),
            OffsetSpec::Latest => marks.iter().map(|(_, high)| *high).collect(),
            OffsetSpec::Timestamp(timestamp) => at_or_after(&live, &targets, &marks, timestamp).await?,
        };

        Ok(targets
            .iter()
            .zip(marks)
            .zip(resolved)
            .map(|((target, (low, high)), resolved)| OffsetRange {
                topic: target.topic.clone(),
                partition: target.partition,
                low,
                high,
                resolved_offset: Some(resolved),
            })
            .collect())
    }
}

/// Resolve the requested partitions against live metadata, so a missing topic or
/// partition is reported before any offset is fetched.
async fn targets(
    live: &Live,
    requested: &[TopicPartition],
) -> Result<Vec<TopicPartition>, DriverError> {
    let metadata = fetch_metadata(live, None).await?;

    if requested.is_empty() {
        let mut all = Vec::new();
        for entry in metadata.topics() {
            if entry.error().is_some() || topic::is_internal(entry.name()) {
                continue;
            }
            for partition in entry.partitions() {
                if partition.error().is_none() {
                    all.push(TopicPartition::new(entry.name(), to_u32(partition.id())));
                }
            }
        }
        return Ok(all);
    }

    let mut wanted = Vec::with_capacity(requested.len());
    for target in requested {
        let entry = metadata
            .topics()
            .iter()
            .find(|entry| entry.name() == target.topic && entry.error().is_none())
            .ok_or_else(|| DriverError::TopicNotFound(target.topic.clone()))?;
        let known = entry
            .partitions()
            .iter()
            .any(|p| p.error().is_none() && to_u32(p.id()) == target.partition);
        if !known {
            return Err(DriverError::PartitionNotFound {
                topic: target.topic.clone(),
                partition: target.partition,
            });
        }
        wanted.push(target.clone());
    }
    Ok(wanted)
}

/// `fetch_watermarks` is blocking, so the whole list is collected inside one hop to
/// the blocking pool rather than one hop per partition.
async fn watermarks(
    live: &Live,
    targets: &[TopicPartition],
) -> Result<Vec<(i64, i64)>, DriverError> {
    if targets.is_empty() {
        return Ok(Vec::new());
    }
    let admin = live.admin.clone();
    let timeout = Duration::from_millis(live.cfg.timeouts.metadata_ms.max(1));
    let wanted = as_raw(targets);

    let gathered = tokio::task::spawn_blocking(move || {
        wanted
            .iter()
            .map(|(name, partition)| admin.inner().fetch_watermarks(name, *partition, timeout))
            .collect::<Result<Vec<(i64, i64)>, KafkaError>>()
    })
    .await
    .map_err(|e| DriverError::other(format!("watermark task failed: {e}")))?;

    gathered.map_err(|err| transport(live, err))
}

/// The first offset at or after `timestamp`, per partition. A partition whose log
/// holds nothing that new falls back to its high watermark, which is what the mock
/// reports too (R48).
async fn at_or_after(
    live: &Live,
    targets: &[TopicPartition],
    marks: &[(i64, i64)],
    timestamp: i64,
) -> Result<Vec<i64>, DriverError> {
    if targets.is_empty() {
        return Ok(Vec::new());
    }
    let consumer = consumer_config(&live.cfg)?.create::<BaseConsumer>().map_err(|e| {
        DriverError::Connection { server: live.cfg.bootstrap_joined(), message: e.to_string() }
    })?;
    let timeout = Duration::from_millis(live.cfg.timeouts.request_ms.max(1));
    let wanted = as_raw(targets);

    let found = tokio::task::spawn_blocking(move || {
        let mut list = TopicPartitionList::new();
        for (name, partition) in &wanted {
            list.add_partition_offset(name, *partition, Offset::Offset(timestamp))?;
        }
        let resolved = consumer.offsets_for_times(list, timeout)?;
        Ok::<Vec<Option<i64>>, KafkaError>(
            wanted
                .iter()
                .map(|(name, partition)| {
                    resolved
                        .find_partition(name, *partition)
                        .and_then(|elem| elem.offset().to_raw())
                        .filter(|raw| *raw >= 0)
                })
                .collect(),
        )
    })
    .await
    .map_err(|e| DriverError::other(format!("offset lookup task failed: {e}")))?;

    Ok(found
        .map_err(|err| transport(live, err))?
        .into_iter()
        .zip(marks)
        .map(|(found, (_, high))| found.unwrap_or(*high))
        .collect())
}

/// Partition ids cross the FFI boundary as `i32` but travel as `u32`.
fn to_u32(partition: i32) -> u32 {
    u32::try_from(partition).unwrap_or_default()
}

/// `targets` in librdkafka's own coordinates.
fn as_raw(targets: &[TopicPartition]) -> Vec<(String, i32)> {
    targets
        .iter()
        .map(|target| (target.topic.clone(), i32::try_from(target.partition).unwrap_or(i32::MAX)))
        .collect()
}

fn transport(live: &Live, err: KafkaError) -> DriverError {
    DriverError::Connection { server: live.cfg.bootstrap_joined(), message: err.to_string() }
}
