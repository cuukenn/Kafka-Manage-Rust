//! The read-only topic surface, filled in one method per card: [`infos`] backs
//! `list_topics` (T071a), and T071b adds `describe_topics`.
//!
//! Everything here reads from a single metadata snapshot. One thing librdkafka
//! does not expose is the internal flag, so Kafka's `__` name reservation is the
//! signal used instead.

use std::collections::BTreeMap;

use rdkafka::metadata::Metadata;

use kafka_core::topic::TopicInfo;

/// Kafka reserves `__`-prefixed names for its own topics, and librdkafka's
/// metadata carries no internal flag, so the prefix is the only signal available.
fn is_internal(name: &str) -> bool {
    name.starts_with("__")
}

/// Topic overviews. `configs` is left empty on purpose (R33): the list view does
/// not render configuration and filling it would cost an extra `DescribeConfigs`
/// round trip on every refresh. T071b's `describe_topics` carries them instead.
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
