//! Topic write operations for the real driver: creating a topic and deleting
//! topics (T072a), plus the configuration writer (T072b).
//!
//! `rdkafka::admin::AdminClient`'s write API is future-based, so unlike the
//! blocking `Client` reads in [`super::topic`] these calls are awaited directly
//! (R31 covers the blocking calls only).
//!
//! Two librdkafka limits shape what follows:
//!
//! * `NewTopic` takes either one replication factor shared by all partitions or a
//!   full per-partition assignment, which is exactly the choice `CreateTopicReq`
//!   already offers.
//! * `alter_configs` is the *non-incremental* AlterConfigs API — it resets every
//!   parameter the request does not name. Sending only the caller's `set` map
//!   would therefore wipe the topic's other overrides, so the current overrides
//!   are read back first and merged in (R37).

use std::collections::BTreeMap;
use std::time::Duration;

use rdkafka::admin::{AdminOptions, AlterConfig, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};

use kafka_core::error::DriverError;
use kafka_core::topic::{
    CreateTopicReq, DeleteTopicsReq, DescribeTopicsReq, TopicDeleteResult, UpdateTopicConfigReq,
};

use super::{fetch_metadata, topic, Live};

impl super::RealKafkaDriver {
    /// Creates one topic.
    ///
    /// The request is validated against the broker count learnt at connect time
    /// before anything is sent, so an impossible request never reaches the broker
    /// (the mock driver validates in the same place).
    pub(super) async fn real_create_topic(&self, req: &CreateTopicReq) -> Result<(), DriverError> {
        let live = self.live().await?;
        req.validate(live.brokers)?;

        let assignment: Vec<Vec<i32>> = req.replica_assignment.clone();
        let replicas: Vec<&[i32]> = assignment.iter().map(Vec::as_slice).collect();
        let replication = if replicas.is_empty() {
            TopicReplication::Fixed(i32::from(req.replication_factor))
        } else {
            TopicReplication::Variable(&replicas)
        };

        let partitions = i32::try_from(req.num_partitions).unwrap_or(i32::MAX);
        let mut topic = NewTopic::new(&req.name, partitions, replication);
        for (key, value) in &req.configs {
            topic = topic.set(key, value);
        }

        let answers = live
            .admin
            .create_topics([&topic], &options(&live))
            .await
            .map_err(|e| transport_error(&live, "create_topics", e))?;

        match answers.into_iter().next() {
            Some(Ok(_)) => {
                tracing::info!(topic = %req.name, partitions, "real topic created");
                Ok(())
            }
            Some(Err((_, code))) => Err(create_error(&req.name, code)),
            None => Err(DriverError::other("create_topics returned no result")),
        }
    }

    /// Deletes the named topics, answering once per name in request order.
    ///
    /// A name the cluster does not know and a refused internal topic are reported
    /// as per-item errors rather than failing the whole call, which is what the
    /// mock driver does and what the UI renders.
    pub(super) async fn real_delete_topics(
        &self,
        req: &DeleteTopicsReq,
    ) -> Result<Vec<TopicDeleteResult>, DriverError> {
        let live = self.live().await?;
        if req.names.is_empty() {
            return Ok(Vec::new());
        }

        // Kafka only lets an internal topic go once nothing writes to it, and the
        // mock refuses them outright; refusing here keeps both drivers agreeing on
        // what the UI may offer.
        let deletable: Vec<String> =
            req.names.iter().filter(|name| !topic::is_internal(name)).cloned().collect();

        let mut answered: BTreeMap<String, Option<String>> = BTreeMap::new();
        if !deletable.is_empty() {
            let names: Vec<&str> = deletable.iter().map(String::as_str).collect();
            let results = live
                .admin
                .delete_topics(&names, &options(&live))
                .await
                .map_err(|e| transport_error(&live, "delete_topics", e))?;
            for result in results {
                match result {
                    Ok(name) => {
                        answered.insert(name, None);
                    }
                    Err((name, code)) => {
                        let message = delete_error(&name, code);
                        answered.insert(name, Some(message));
                    }
                }
            }
        }

        Ok(req
            .names
            .iter()
            .map(|name| {
                let error = if topic::is_internal(name) {
                    Some(format!("refusing to delete internal topic {name}"))
                } else {
                    answered.get(name).cloned().unwrap_or_else(|| Some(format!("unknown topic {name}")))
                };
                if error.is_none() {
                    tracing::info!(topic = %name, "real topic deleted");
                }
                TopicDeleteResult { name: name.clone(), error }
            })
            .collect())
    }

    /// Applies `set` and `unset` to a topic's dynamic configuration.
    ///
    /// The topic's current overrides are read back first and merged, because
    /// librdkafka's `alter_configs` resets to default anything the request leaves
    /// out (R37). A topic the cluster does not know surfaces as
    /// [`DriverError::TopicNotFound`].
    pub(super) async fn real_update_topic_config(
        &self,
        req: &UpdateTopicConfigReq,
    ) -> Result<(), DriverError> {
        let live = self.live().await?;
        if req.topic.trim().is_empty() {
            return Err(DriverError::invalid("topic", "topic must not be empty"));
        }
        if req.set.keys().any(|key| key.trim().is_empty()) {
            return Err(DriverError::invalid("set", "config key must not be empty"));
        }
        if req.set.is_empty() && req.unset.is_empty() {
            return Ok(());
        }

        // Kafka answers an `AlterConfigs` for a topic it does not have with success
        // when the change set is empty, so existence is checked explicitly rather
        // than inferred from the write's outcome (R39). Internal topics are allowed:
        // their configuration is writable.
        let ask = DescribeTopicsReq { names: vec![req.topic.clone()], include_internal: true };
        let metadata = fetch_metadata(&live, Some(&req.topic)).await?;
        topic::plan(metadata, &ask)?;

        let mut merged = topic::overridden_configs(&live, &[req.topic.as_str()])
            .await?
            .remove(req.topic.as_str())
            .unwrap_or_default();
        for (key, value) in &req.set {
            merged.insert(key.clone(), value.clone());
        }
        for key in &req.unset {
            merged.remove(key);
        }

        let mut alter = AlterConfig::new(ResourceSpecifier::Topic(&req.topic));
        for (key, value) in &merged {
            alter = alter.set(key, value);
        }

        let answers = live
            .admin
            .alter_configs([&alter], &options(&live))
            .await
            .map_err(|e| transport_error(&live, "alter_configs", e))?;

        match answers.into_iter().next() {
            Some(Ok(_)) => {
                tracing::info!(topic = %req.topic, overrides = merged.len(), "real topic config updated");
                Ok(())
            }
            Some(Err((_, code))) => Err(config_error(&req.topic, code)),
            None => Err(DriverError::other("alter_configs returned no result")),
        }
    }
}

/// Admin calls are bounded by the request budget, not the metadata one.
fn options(live: &Live) -> AdminOptions {
    let timeout = Duration::from_millis(live.cfg.timeouts.request_ms.max(1));
    AdminOptions::new().request_timeout(Some(timeout))
}

/// The call itself failed — it never reached the broker, or the broker went away.
/// This is distinct from the per-resource codes below, which are answers.
fn transport_error(live: &Live, operation: &str, err: KafkaError) -> DriverError {
    if matches!(err, KafkaError::AdminOp(RDKafkaErrorCode::RequestTimedOut)) {
        return DriverError::timeout(operation, live.cfg.timeouts.request_ms.max(1));
    }
    DriverError::Connection {
        server: live.cfg.bootstrap_joined(),
        message: format!("{operation}: {err}"),
    }
}

/// `CreateTopics` answers once per topic, so the code *is* the error.
fn create_error(name: &str, code: RDKafkaErrorCode) -> DriverError {
    match code {
        RDKafkaErrorCode::TopicAlreadyExists => DriverError::TopicAlreadyExists(name.to_string()),
        RDKafkaErrorCode::InvalidPartitions => DriverError::invalid("numPartitions", code.to_string()),
        RDKafkaErrorCode::InvalidReplicationFactor => {
            DriverError::invalid("replicationFactor", code.to_string())
        }
        RDKafkaErrorCode::InvalidConfig => DriverError::invalid("configs", code.to_string()),
        other => broker_error(other),
    }
}

/// Per-item delete failures travel as text, so they read the way the mock's do;
/// `UnknownTopicOrPartition` is the one with a wording of its own.
fn delete_error(name: &str, code: RDKafkaErrorCode) -> String {
    match code {
        RDKafkaErrorCode::UnknownTopicOrPartition => format!("unknown topic {name}"),
        other => other.to_string(),
    }
}
/// `AlterConfigs` answers once per resource. A topic the cluster does not know is
/// the one case worth a taxonomy entry of its own; the rest are broker codes.
fn config_error(topic: &str, code: RDKafkaErrorCode) -> DriverError {
    match code {
        RDKafkaErrorCode::UnknownTopicOrPartition => DriverError::TopicNotFound(topic.to_string()),
        RDKafkaErrorCode::InvalidConfig => DriverError::invalid("set", code.to_string()),
        other => broker_error(other),
    }
}

/// A broker-authored code with no closer taxonomy entry.
fn broker_error(code: RDKafkaErrorCode) -> DriverError {
    DriverError::Broker { code: code as i16, name: format!("{code:?}"), message: code.to_string() }
}
