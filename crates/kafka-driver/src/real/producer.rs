//! Message production for the real driver (T073).
//!
//! The batch is enqueued in full before any delivery report is awaited, so
//! librdkafka can pack it into as few Produce requests as it likes; the reports
//! are then put back in request order, which is the order the mock answers in.
//!
//! `allow.auto.create.topics=false`, set in [`super::config`], is what keeps a
//! metadata probe from quietly creating the topic it was only asking about (R42).

use futures::future::join_all;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::future_producer::{Delivery, DeliveryFuture};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use rdkafka::Message;

use kafka_core::cluster::ClusterConfig;
use kafka_core::error::DriverError;
use kafka_core::message::{ProduceMessage, ProduceReq, ProduceResult};
use kafka_core::topic::DescribeTopicsReq;

use super::{fetch_metadata, topic};

/// The producer `connect` builds; every batch reuses it, and cloning it is only
/// a handle copy onto the same librdkafka instance.
pub(super) type Producer = FutureProducer;

/// The producer's half of the configuration: [`super::config::client_config`]
/// plus the durability this driver promises.
pub(super) fn producer_config(cfg: &ClusterConfig) -> Result<ClientConfig, DriverError> {
    let mut conf = super::config::client_config(cfg)?;
    let request_ms = cfg.timeouts.request_ms.max(1);
    conf.set("enable.idempotence", "true")
        .set("acks", "all")
        // librdkafka insists on `delivery.timeout.ms >= linger.ms + request.timeout.ms`,
        // and the request budget is the one the user chose, so linger gives way (R43).
        .set("linger.ms", "0")
        .set("delivery.timeout.ms", request_ms.to_string());

    // `client_config` already ran the escape hatch; run it once more so it stays
    // the last word over the producer-only keys set above.
    for (key, value) in &cfg.extra {
        conf.set(key, value);
    }

    Ok(conf)
}

impl super::RealKafkaDriver {
    /// Produces one batch, answering once per message in request order.
    pub(super) async fn real_produce(
        &self,
        req: ProduceReq,
    ) -> Result<Vec<ProduceResult>, DriverError> {
        req.validate()?;
        let live = self.live().await?;

        // The mock answers a produce against an unknown topic with a whole-call
        // `TopicNotFound`, and an explicit partition the topic does not have with
        // a per-message error. One metadata snapshot buys both answers up front.
        let ask = DescribeTopicsReq { names: vec![req.topic.clone()], include_internal: true };
        let metadata = fetch_metadata(&live, Some(&req.topic)).await?;
        let Some(planned) = topic::plan(metadata, &ask)?.into_iter().next() else {
            return Err(DriverError::TopicNotFound(req.topic.clone()));
        };
        let known: Vec<i32> = planned.partitions.iter().map(|partition| partition.id).collect();

        let count = req.messages.len();
        let mut outcomes: Vec<Option<Result<Delivery, String>>> = Vec::with_capacity(count);
        outcomes.resize_with(count, || None);

        let mut queued: Vec<(usize, DeliveryFuture)> = Vec::with_capacity(count);
        for (index, message) in req.messages.iter().enumerate() {
            match enqueue(&live.producer, &req.topic, message, &known) {
                Ok(delivery) => queued.push((index, delivery)),
                Err(text) => outcomes[index] = Some(Err(text)),
            }
        }

        let topic_name = req.topic.as_str();
        let reports = join_all(queued.into_iter().map(|(index, delivery)| async move {
            let outcome = match delivery.await {
                Ok(Ok(delivery)) => Ok(delivery),
                Ok(Err((error, message))) => {
                    Err(describe_error(topic_name, message.partition(), error))
                }
                Err(canceled) => Err(format!("delivery report dropped: {canceled}")),
            };
            (index, outcome)
        }))
        .await;
        for (index, outcome) in reports {
            outcomes[index] = Some(outcome);
        }

        let results: Vec<ProduceResult> = outcomes
            .into_iter()
            .enumerate()
            .map(|(index, outcome)| {
                let message = &req.messages[index];
                match outcome.expect("every message is accounted for exactly once") {
                    Ok(delivery) => ProduceResult {
                        topic: req.topic.clone(),
                        partition: u32::try_from(delivery.partition).unwrap_or_default(),
                        offset: delivery.offset,
                        timestamp: delivery.timestamp.to_millis().unwrap_or_default(),
                        error: None,
                    },
                    Err(text) => ProduceResult {
                        topic: req.topic.clone(),
                        partition: message.partition.unwrap_or_default(),
                        offset: 0,
                        timestamp: 0,
                        error: Some(text),
                    },
                }
            })
            .collect();

        let delivered = results.iter().filter(|result| result.error.is_none()).count();
        if delivered > 0 {
            tracing::info!(topic = %req.topic, delivered, total = results.len(), "real messages produced");
        }
        Ok(results)
    }
}

/// Queues one message, or says in the caller's terms why it cannot be queued.
///
/// A forced partition the topic does not have is refused here, the way the mock
/// refuses it, so no round trip is spent learning what the metadata already said.
fn enqueue(
    producer: &Producer,
    topic: &str,
    message: &ProduceMessage,
    known: &[i32],
) -> Result<DeliveryFuture, String> {
    let forced = message.partition.map(|raw| (raw, i32::try_from(raw).unwrap_or(i32::MAX)));
    if let Some((raw, id)) = forced {
        if !known.contains(&id) {
            return Err(format!("unknown topic {topic} partition {raw}"));
        }
    }

    let mut record: FutureRecord<'_, Vec<u8>, Vec<u8>> = FutureRecord::to(topic);
    record.payload = Some(&message.value);
    record.key = message.key.as_ref();
    record.partition = forced.map(|(_, id)| id);
    record.timestamp = message.timestamp;
    if !message.headers.is_empty() {
        let mut headers = OwnedHeaders::new_with_capacity(message.headers.len());
        for header in &message.headers {
            headers = headers.insert(Header { key: &header.key, value: header.value.as_deref() });
        }
        record.headers = Some(headers);
    }

    // Enqueuing is synchronous: librdkafka copies what it needs, so the record's
    // borrow of `message` ends here and the delivery future is self-contained.
    producer.send_result(record).map_err(|(error, _record)| error.to_string())
}

/// Per-message failure text, kept close to the mock's wording. `partition` is
/// `-1` when the message never reached one.
fn describe_error(topic: &str, partition: i32, error: KafkaError) -> String {
    match error.rdkafka_error_code() {
        Some(RDKafkaErrorCode::UnknownTopicOrPartition) => format!("unknown topic {topic}"),
        Some(RDKafkaErrorCode::UnknownPartition) if partition >= 0 => {
            format!("unknown topic {topic} partition {partition}")
        }
        Some(RDKafkaErrorCode::UnknownPartition) => format!("unknown partition for {topic}"),
        Some(RDKafkaErrorCode::LeaderNotAvailable) if partition >= 0 => {
            format!("LEADER_NOT_AVAILABLE for {topic}-{partition}")
        }
        Some(RDKafkaErrorCode::LeaderNotAvailable) => format!("LEADER_NOT_AVAILABLE for {topic}"),
        _ => error.to_string(),
    }
}
