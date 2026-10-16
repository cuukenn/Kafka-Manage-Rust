//! The read-only topic surface: `list_topics` (T071a) and `describe_topics`
//! (T071b), both fed by the same metadata snapshot.
//!
//! Everything here reads from a single metadata snapshot; only the per-partition
//! watermarks and the topic configs need extra round trips, and the watermarks are
//! batched into one blocking task. Two things librdkafka does not expose are
//! filled in by other means: internal topics are recognised by Kafka's `__` name
//! reservation, and `leader_epoch` stays `None`.

use std::collections::BTreeMap;
use std::time::Duration;

use rdkafka::admin::{AdminOptions, OwnedResourceSpecifier, ResourceSpecifier};
use rdkafka::error::KafkaError;
use rdkafka::metadata::{Metadata, MetadataTopic};

use kafka_core::error::DriverError;
use kafka_core::topic::{DescribeTopicsReq, PartitionDetail, TopicDetail, TopicInfo};

use super::Live;

/// Kafka reserves `__`-prefixed names for its own topics, and librdkafka's
/// metadata carries no internal flag, so the prefix is the only signal available.
pub(super) fn is_internal(name: &str) -> bool {
    name.starts_with("__")
}

/// Topic overviews. `configs` is left empty on purpose (R33): the list view does
/// not render configuration and filling it would cost an extra `DescribeConfigs`
/// round trip on every refresh. `describe_topics` carries them instead.
pub(super) fn infos(metadata: &Metadata) -> Vec<TopicInfo> {
    metadata
        .topics()
        .iter()
        .filter(|topic| topic.error().is_none())
        .map(|topic| TopicInfo {
            name: topic.name().to_string(),
            internal: is_internal(topic.name()),
            partition_count: topic.partitions().len() as u32,
            replication_factor: topic
                .partitions()
                .iter()
                .map(|p| p.replicas().len())
                .max()
                .unwrap_or_default() as i16,
            configs: BTreeMap::new(),
        })
        .collect()
}

/// A topic picked out by [`plan`], copied out of the metadata with its partitions.
pub(super) struct Planned {
    pub(super) name: String,
    pub(super) internal: bool,
    pub(super) partitions: Vec<PartitionPlan>,
}

/// One partition's identity, copied out of the metadata.
pub(super) struct PartitionPlan {
    pub(super) id: i32,
    pub(super) leader_id: i32,
    pub(super) replicas: Vec<i32>,
    pub(super) isr: Vec<i32>,
}

/// Selects the topics `req` asks for and copies their metadata into owned data.
///
/// `MetadataTopic` wraps raw librdkafka pointers and is neither `Send` nor `Sync`,
/// so it cannot be held across an await (R35); everything the async half needs is
/// extracted here first.
///
/// The selection mirrors the mock driver: an empty `names` means "everything
/// visible" (internal topics only when `include_internal`), while a named request
/// means exactly those topics — and a name the cluster does not know is a
/// `TopicNotFound` (also when the metadata carries an error for it; R36).
/// Explicitly naming an internal topic is allowed.
pub(super) fn plan(metadata: Metadata, req: &DescribeTopicsReq) -> Result<Vec<Planned>, DriverError> {
    let visible = |topic: &MetadataTopic| topic.error().is_none();
    let selected: Vec<&MetadataTopic> = if req.names.is_empty() {
        metadata
            .topics()
            .iter()
            .filter(|topic| visible(topic) && (req.include_internal || !is_internal(topic.name())))
            .collect()
    } else {
        let known: BTreeMap<&str, &MetadataTopic> = metadata
            .topics()
            .iter()
            .filter(|topic| visible(topic))
            .map(|topic| (topic.name(), topic))
            .collect();
        let mut picked = Vec::with_capacity(req.names.len());
        for name in &req.names {
            match known.get(name.as_str()) {
                Some(topic) => picked.push(*topic),
                None => return Err(DriverError::TopicNotFound(name.clone())),
            }
        }
        picked
    };

    Ok(selected
        .into_iter()
        .map(|topic| Planned {
            name: topic.name().to_string(),
            internal: is_internal(topic.name()),
            partitions: topic
                .partitions()
                .iter()
                .map(|p| PartitionPlan {
                    id: p.id(),
                    leader_id: p.leader(),
                    replicas: p.replicas().to_vec(),
                    isr: p.isr().to_vec(),
                })
                .collect(),
        })
        .collect())
}

/// Fills a planned topic list with configs and watermark offsets.
pub(super) async fn assemble(
    live: &Live,
    planned: Vec<Planned>,
) -> Result<Vec<TopicDetail>, DriverError> {
    let names: Vec<&str> = planned.iter().map(|topic| topic.name.as_str()).collect();
    let configs = overridden_configs(live, &names).await?;
    let marks = watermarks(live, &planned).await?;

    Ok(planned
        .into_iter()
        .map(|topic| {
            let configs = configs.get(&topic.name).cloned().unwrap_or_default();
            let partitions = topic
                .partitions
                .into_iter()
                .map(|p| {
                    let (beginning_offset, end_offset) = marks
                        .get(&topic.name)
                        .and_then(|per_partition| per_partition.get(&p.id))
                        .copied()
                        .unwrap_or((-1, -1));
                    PartitionDetail {
                        id: u32::try_from(p.id).unwrap_or_default(),
                        leader_id: p.leader_id,
                        replica_ids: p.replicas,
                        isr_ids: p.isr,
                        beginning_offset,
                        end_offset,
                        // librdkafka's metadata does not carry the leader epoch.
                        leader_epoch: None,
                    }
                })
                .collect();
            TopicDetail { name: topic.name, internal: topic.internal, partitions, configs }
        })
        .collect())
}

/// Topic-level configuration for every named topic in one `DescribeConfigs` call.
///
/// Only entries that were actually overridden are kept (R34): `!is_default` drops
/// everything still at its Kafka default, and sensitive values never leave the
/// broker. Shared with the config writer in [`super::admin`], which needs the
/// current overrides as the base of its non-incremental replace (R37).
pub(super) async fn overridden_configs(
    live: &Live,
    names: &[&str],
) -> Result<BTreeMap<String, BTreeMap<String, String>>, DriverError> {
    if names.is_empty() {
        return Ok(BTreeMap::new());
    }
    let specs: Vec<ResourceSpecifier<'_>> =
        names.iter().map(|name| ResourceSpecifier::Topic(name)).collect();
    let results = live
        .admin
        .describe_configs(specs.iter(), &AdminOptions::new())
        .await
        .map_err(|e| DriverError::Connection {
            server: live.cfg.bootstrap_joined(),
            message: e.to_string(),
        })?;

    let mut out = BTreeMap::new();
    for result in results {
        // A resource-level error names a Kafka code, not a transport failure.
        let resource =
            result.map_err(|code| DriverError::other(format!("describe_configs failed: {code}")))?;
        let name = match resource.specifier {
            OwnedResourceSpecifier::Topic(name) => name,
            _ => continue,
        };
        let entries = resource
            .entries
            .into_iter()
            .filter(|entry| !entry.is_default && !entry.is_sensitive)
            .filter_map(|entry| entry.value.map(|value| (entry.name, value)))
            .collect();
        out.insert(name, entries);
    }
    Ok(out)
}

/// Beginning/end offsets for every partition of the given topics, gathered in one
/// blocking task so a wide topic costs one hop to the pool rather than many.
async fn watermarks(
    live: &Live,
    topics: &[Planned],
) -> Result<BTreeMap<String, BTreeMap<i32, (i64, i64)>>, DriverError> {
    let admin = live.admin.clone();
    let timeout = Duration::from_millis(live.cfg.timeouts.metadata_ms.max(1));
    let targets: Vec<(String, i32)> = topics
        .iter()
        .flat_map(|topic| topic.partitions.iter().map(|p| (topic.name.clone(), p.id)))
        .collect();
    if targets.is_empty() {
        return Ok(BTreeMap::new());
    }

    let gathered = tokio::task::spawn_blocking(move || {
        let mut out: BTreeMap<String, BTreeMap<i32, (i64, i64)>> = BTreeMap::new();
        for (topic, partition) in targets {
            let marks = admin.inner().fetch_watermarks(&topic, partition, timeout)?;
            out.entry(topic).or_default().insert(partition, marks);
        }
        Ok(out)
    })
    .await
    .map_err(|e| DriverError::other(format!("watermark task failed: {e}")))?;

    gathered.map_err(|err: KafkaError| DriverError::Connection {
        server: live.cfg.bootstrap_joined(),
        message: err.to_string(),
    })
}
