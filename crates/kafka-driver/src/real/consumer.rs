//! The consuming side of the real driver: offset lookups (T074a) and the
//! `subscribe` stream (T074b).
//!
//! Only a consumer can answer "first offset at or after this timestamp", so a
//! timestamp query borrows a throwaway one (R45); plain beginning/end lookups run
//! off the admin client's `fetch_watermarks`, the same source `describe_topics`
//! reads.
//!
//! A subscription never joins a group: partitions are assigned explicitly and
//! nothing is committed (R49). The pump polls on the blocking pool, because
//! librdkafka's `poll` blocks, and reads `StreamControls` between polls.

use std::collections::HashSet;
use std::time::Duration;

use futures::StreamExt;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Headers, Message, Timestamp};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::ClientConfig;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use kafka_core::cluster::ClusterConfig;
use kafka_core::error::DriverError;
use kafka_core::message::{
    MessageEvent, MessageHeader, MessageStream, OffsetStart, SearchFilter, StreamCommand,
    StreamControls, SubscribeReq, TimestampType,
};
use kafka_core::topic::{ListOffsetsReq, OffsetRange, OffsetSpec, TopicPartition};

use super::{fetch_metadata, topic, Live};

/// Bounded so a fast producer cannot grow the queue without limit; the blocking
/// `send` turns that into backpressure all the way back to librdkafka's queue.
const CHANNEL_DEPTH: usize = 256;

/// The synthetic `group.id` the reader runs under: `assign` never joins a group and
/// `enable.auto.commit=false` writes nothing, but librdkafka still refuses
/// `rd_kafka_assign` without a `group.id` ("Local: Unknown group"), so one is set
/// (R50); `cfg.extra` can override it, and `enable.partition.eof` (which lets a
/// bounded read notice a drained partition) still lands first (R47).
const READER_GROUP: &str = "rs-kafka-ui-reader";

/// The client behind every read-only offset query.
pub(super) fn consumer_config(cfg: &ClusterConfig) -> Result<ClientConfig, DriverError> {
    consumer_config_for(cfg, READER_GROUP)
}

/// The same consumer settings under an explicit group id, for the calls that must
/// speak for a named group rather than for this driver's read-only reader (T075b).
pub(super) fn consumer_config_for(
    cfg: &ClusterConfig,
    group_id: &str,
) -> Result<ClientConfig, DriverError> {
    let mut conf = super::config::client_config(cfg)?;
    conf.set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "true");
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

    /// Open a subscription: own the requested partitions from a resolved start
    /// offset and hand the caller a stream plus its control handle.
    ///
    /// No consumer group is ever joined, so nothing is auto-committed here (R51).
    pub(super) async fn real_subscribe(
        &self,
        req: SubscribeReq,
    ) -> Result<MessageStream, DriverError> {
        req.validate()?;
        let live = self.live().await?;
        let targets = selected(&live, &req).await?;
        let marks = watermarks(&live, &targets).await?;
        let starts = starts(&live, &req, &targets, &marks).await?;

        let server = live.cfg.bootstrap_joined();
        let consumer = consumer_config(&live.cfg)?.create::<BaseConsumer>().map_err(|e| {
            DriverError::Connection { server: server.clone(), message: e.to_string() }
        })?;
        let mut assignment = TopicPartitionList::new();
        for (target, start) in targets.iter().zip(&starts) {
            assignment
                .add_partition_offset(&target.topic, to_i32(target.partition), Offset::Offset(*start))
                .map_err(|err| transport(&server, &err))?;
        }
        consumer.assign(&assignment).map_err(|err| transport(&server, &err))?;

        let plan = Pump {
            topic: req.topic.clone(),
            targets: as_raw(&targets),
            follow: req.follow,
            max_messages: req.max_messages,
            filter: req.filter.clone(),
            poll: Duration::from_millis(live.cfg.timeouts.poll_ms.max(1)),
        };
        let (controls, commands) = StreamControls::channel(req.subscription_id.clone());
        let receiver = pump(consumer, server, plan, commands);
        tracing::debug!(subscription = %req.subscription_id, topic = %req.topic, "real subscription opened");
        Ok(MessageStream::new(
            req.subscription_id.clone(),
            ReceiverStream::new(receiver).boxed(),
            controls,
        ))
    }
}

/// Partitions this subscription will own. `SubscribeReq::targets` hands back the
/// requested ids verbatim, so an id the topic does not have has to be caught here.
async fn selected(live: &Live, req: &SubscribeReq) -> Result<Vec<TopicPartition>, DriverError> {
    let metadata = fetch_metadata(live, Some(&req.topic)).await?;
    let entry = metadata
        .topics()
        .iter()
        .find(|entry| entry.name() == req.topic && entry.error().is_none())
        .ok_or_else(|| DriverError::TopicNotFound(req.topic.clone()))?;
    let known: Vec<u32> = entry
        .partitions()
        .iter()
        .filter(|partition| partition.error().is_none())
        .map(|partition| to_u32(partition.id()))
        .collect();

    let mut targets = Vec::new();
    for id in req.targets(&known) {
        if !known.contains(&id) {
            return Err(DriverError::PartitionNotFound { topic: req.topic.clone(), partition: id });
        }
        targets.push(TopicPartition::new(req.topic.clone(), id));
    }
    Ok(targets)
}

/// Where each partition starts reading.
async fn starts(
    live: &Live,
    req: &SubscribeReq,
    targets: &[TopicPartition],
    marks: &[(i64, i64)],
) -> Result<Vec<i64>, DriverError> {
    Ok(match req.start {
        OffsetStart::Earliest => marks.iter().map(|(low, _)| *low).collect(),
        OffsetStart::Latest => marks.iter().map(|(_, high)| *high).collect(),
        OffsetStart::Timestamp(timestamp) => at_or_after(live, targets, marks, timestamp).await?,
        OffsetStart::Offset(offset) => {
            for (target, (low, high)) in targets.iter().zip(marks) {
                if offset < *low || offset > *high {
                    return Err(DriverError::OffsetOutOfRange {
                        topic: target.topic.clone(),
                        partition: target.partition,
                        requested: offset,
                        low: *low,
                        high: *high,
                    });
                }
            }
            vec![offset; targets.len()]
        }
    })
}

/// Everything a pump needs, owned, so it can move onto the blocking pool.
struct Pump {
    topic: String,
    targets: Vec<(String, i32)>,
    follow: bool,
    max_messages: Option<u64>,
    filter: Option<SearchFilter>,
    poll: Duration,
}

/// Move the assigned consumer onto the blocking pool. The join handle is dropped on
/// purpose: the task ends by itself on `Stop`, when the reader is gone, or when a
/// bounded read has drained every partition.
fn pump(
    consumer: BaseConsumer,
    server: String,
    plan: Pump,
    mut commands: mpsc::UnboundedReceiver<StreamCommand>,
) -> mpsc::Receiver<Result<MessageEvent, DriverError>> {
    let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);
    tokio::task::spawn_blocking(move || {
        let _ = run(&consumer, &server, &plan, &tx, &mut commands);
    });
    rx
}

fn run(
    consumer: &BaseConsumer,
    server: &str,
    plan: &Pump,
    tx: &mpsc::Sender<Result<MessageEvent, DriverError>>,
    commands: &mut mpsc::UnboundedReceiver<StreamCommand>,
) -> Result<(), DriverError> {
    if plan.targets.is_empty() {
        return Ok(());
    }
    let mut state = Watch { paused: false, drained: HashSet::new() };
    let mut sent: u64 = 0;
    // A message fetched just before a pause landed; held so the pause is not
    // silently skipped. It can never grow past one, because a paused pump stops
    // polling (R54).
    let mut held: Option<MessageEvent> = None;

    loop {
        // Commands win over the poll timeout, so pause/stop land promptly.
        if !apply_commands(consumer, server, plan, commands, &mut state)? || tx.is_closed() {
            return Ok(());
        }
        if !state.paused {
            if let Some(event) = held.take() {
                if !deliver(event, plan, &mut sent, tx) {
                    return Ok(());
                }
                continue;
            }
        }
        if state.paused {
            std::thread::sleep(plan.poll);
            continue;
        }

        match consumer.poll(plan.poll) {
            Some(Ok(message)) => {
                state.drained.remove(&message.partition());
                let event = event_from(&message);
                // A pause or a stop may have arrived while we were blocked in poll.
                if !apply_commands(consumer, server, plan, commands, &mut state)? {
                    return Ok(());
                }
                if tx.is_closed() {
                    return Ok(());
                }
                if state.paused {
                    held = Some(event);
                    continue;
                }
                if !deliver(event, plan, &mut sent, tx) {
                    return Ok(());
                }
            }
            // With `enable.partition.eof` on, this is how a bounded read learns a
            // partition is exhausted (R51).
            Some(Err(KafkaError::PartitionEOF(partition))) => {
                state.drained.insert(partition);
                if !plan.follow && state.drained.len() == plan.targets.len() {
                    return Ok(());
                }
            }
            Some(Err(error)) => {
                // Hand it to the reader before ending, so a mid-stream failure is
                // not swallowed.
                let _ = tx.blocking_send(Err(transport(server, &error)));
                return Ok(());
            }
            None => {}
        }
    }
}

/// What a pump tracks between polls.
struct Watch {
    paused: bool,
    /// Partitions that reported `PartitionEOF`; a bounded read ends once every
    /// assigned partition is in here.
    drained: HashSet<i32>,
}

/// Apply every queued command. Returns `false` once the pump must stop.
fn apply_commands(
    consumer: &BaseConsumer,
    server: &str,
    plan: &Pump,
    commands: &mut mpsc::UnboundedReceiver<StreamCommand>,
    state: &mut Watch,
) -> Result<bool, DriverError> {
    loop {
        match commands.try_recv() {
            Ok(StreamCommand::Stop) | Err(mpsc::error::TryRecvError::Disconnected) => return Ok(false),
            Ok(StreamCommand::Pause) => state.paused = true,
            Ok(StreamCommand::Resume) => state.paused = false,
            Ok(StreamCommand::Seek { partition, offset, .. }) => {
                let target = to_i32(partition);
                // Only assigned partitions can be seeked; the rest is ignored, the
                // same way the mock ignores a seek it does not own.
                if plan.targets.iter().any(|(_, id)| *id == target) {
                    consumer
                        .seek(&plan.topic, target, Offset::Offset(offset.max(0)), plan.poll)
                        .map_err(|err| transport(server, &err))?;
                    // A partition seeked into is not drained any more, or a bounded
                    // read would end on the next tick.
                    state.drained.remove(&target);
                }
            }
            Err(mpsc::error::TryRecvError::Empty) => return Ok(true),
        }
    }
}

/// Hand one event to the reader, filtered and counted. `false` means the pump is
/// done: the reader is gone, or `max_messages` has been reached.
fn deliver(
    event: MessageEvent,
    plan: &Pump,
    sent: &mut u64,
    tx: &mpsc::Sender<Result<MessageEvent, DriverError>>,
) -> bool {
    if plan.filter.as_ref().is_some_and(|filter| !filter.matches(&event)) {
        return true;
    }
    if tx.blocking_send(Ok(event)).is_err() {
        return false;
    }
    *sent += 1;
    !plan.max_messages.is_some_and(|max| *sent >= max)
}

/// Copy what the reader needs out of a borrowed message; the borrow dies here.
fn event_from(message: &BorrowedMessage<'_>) -> MessageEvent {
    let timestamp = message.timestamp();
    MessageEvent {
        topic: message.topic().to_string(),
        partition: to_u32(message.partition()),
        offset: message.offset(),
        key: message.key().map(<[u8]>::to_vec),
        value: message.payload().map(<[u8]>::to_vec).unwrap_or_default(),
        headers: headers_of(message),
        timestamp: timestamp.to_millis().unwrap_or_default(),
        timestamp_type: match timestamp {
            Timestamp::CreateTime(_) => TimestampType::CreateTime,
            Timestamp::LogAppendTime(_) => TimestampType::LogAppendTime,
            Timestamp::NotAvailable => TimestampType::Unknown,
        },
        leader_epoch: None,
    }
}

fn headers_of(message: &BorrowedMessage<'_>) -> Vec<MessageHeader> {
    message
        .headers()
        .map(|headers| {
            headers
                .iter()
                .map(|header| MessageHeader {
                    key: header.key.to_string(),
                    value: header.value.map(<[u8]>::to_vec),
                })
                .collect()
        })
        .unwrap_or_default()
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
pub(super) async fn watermarks(
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

    gathered.map_err(|err| transport(&live.cfg.bootstrap_joined(), &err))
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
        .map_err(|err| transport(&live.cfg.bootstrap_joined(), &err))?
        .into_iter()
        .zip(marks)
        .map(|(found, (_, high))| found.unwrap_or(*high))
        .collect())
}

/// Partition ids cross the FFI boundary as `i32` but travel as `u32`.
fn to_u32(partition: i32) -> u32 {
    u32::try_from(partition).unwrap_or_default()
}

fn to_i32(partition: u32) -> i32 {
    i32::try_from(partition).unwrap_or(i32::MAX)
}

/// `targets` in librdkafka's own coordinates.
fn as_raw(targets: &[TopicPartition]) -> Vec<(String, i32)> {
    targets.iter().map(|target| (target.topic.clone(), to_i32(target.partition))).collect()
}

fn transport(server: &str, err: &KafkaError) -> DriverError {
    DriverError::Connection { server: server.to_string(), message: err.to_string() }
}
