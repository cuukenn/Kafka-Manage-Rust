//! Topic write operations for the real driver: creating a topic and deleting
//! topics (T072a); the configuration writer follows in T072b.
//!
//! `rdkafka::admin::AdminClient`'s write API is future-based, so unlike the
//! blocking `Client` reads in [`super::topic`] these calls are awaited directly
//! (R31 covers the blocking calls only).
//!
//! `NewTopic` takes either one replication factor shared by all partitions or a
//! full per-partition assignment, which is exactly the choice `CreateTopicReq`
//! already offers.

use std::collections::BTreeMap;
use std::time::Duration;

use rdkafka::admin::{AdminOptions, NewTopic, TopicReplication};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};

use kafka_core::error::DriverError;
use kafka_core::topic::{CreateTopicReq, DeleteTopicsReq, TopicDeleteResult};

use super::{topic, Live};

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

/// A broker-authored code with no closer taxonomy entry.
fn broker_error(code: RDKafkaErrorCode) -> DriverError {
    DriverError::Broker { code: code as i16, name: format!("{code:?}"), message: code.to_string() }
}
