//! T102: long-run leak probe on the mock driver.
//!
//! Repeats the whole live-subscription lifecycle many times in one process. The
//! driver's own `active_pumps()` counter is read throughout — a pump that is not
//! released after `stop` is a leak the counter witnesses directly — and each cycle
//! deletes its topic so that anything RSS accumulates cannot be written off as
//! "the data grew". RSS and file-descriptor counts are measured from outside by
//! `scripts/leak-check.sh`, because a process cannot honestly sample its own
//! descriptor table.
//!
//! Usage: `cargo run -p kafka-driver --example leak_probe [cycles] [batch]`
//! Defaults: 3000 cycles of 50 messages.

use std::time::Instant;

use kafka_core::cluster::ClusterConfig;
use kafka_core::message::{OffsetStart, ProduceMessage, ProduceReq, SubscribeReq};
use kafka_core::topic::{CreateTopicReq, DeleteTopicsReq};
use kafka_driver::driver::KafkaDriver;
use kafka_driver::mock::MockKafkaDriver;

const TOPIC: &str = "leak-probe";
const PARTITIONS: u32 = 4;
const DEFAULT_CYCLES: usize = 3_000;
const DEFAULT_BATCH: usize = 50;
const WARMUP: usize = 25;
const REPORT_EVERY: usize = 250;

/// One full lifecycle: create a topic, produce a batch, open a live subscription,
/// drain the batch, stop the stream, delete the topic. Every step is the one the
/// UI drives, and deleting the topic keeps the retained set to a single batch so
/// that RSS growth cannot be explained away as "the data grew".
async fn cycle(driver: &MockKafkaDriver, batch: usize) -> anyhow::Result<()> {
    driver
        .create_topic(&CreateTopicReq {
            name: TOPIC.to_string(),
            num_partitions: PARTITIONS,
            ..Default::default()
        })
        .await
        .map_err(|e| anyhow::anyhow!("create_topic failed: {e}"))?;

    let messages: Vec<ProduceMessage> =
        (0..batch).map(|i| ProduceMessage::text(format!("value-{i}"))).collect();
    driver
        .produce(ProduceReq { topic: TOPIC.to_string(), messages })
        .await
        .map_err(|e| anyhow::anyhow!("produce failed: {e}"))?;

    let mut stream = driver
        .subscribe(SubscribeReq {
            topic: TOPIC.to_string(),
            start: OffsetStart::Earliest,
            follow: true,
            ..Default::default()
        })
        .await
        .map_err(|e| anyhow::anyhow!("subscribe failed: {e}"))?;

    // Bounded by `batch`: the topic always holds at least that much, live or not.
    let mut seen = 0usize;
    while seen < batch {
        match stream.next().await {
            Some(Ok(_)) => seen += 1,
            Some(Err(e)) => return Err(anyhow::anyhow!("stream error: {e}")),
            None => break,
        }
    }
    if seen != batch {
        anyhow::bail!("expected {batch} live messages, saw {seen}");
    }

    // T063's path: the control handle stops the pump mid-stream. Bounded so a
    // pump that ignored `Stop` shows up as a failure instead of a hung probe.
    if stream.controls().stop().is_err() {
        anyhow::bail!("stop rejected: the pump was already gone");
    }
    let drained = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while stream.next().await.is_some() {}
    })
    .await;
    drop(stream);
    if drained.is_err() {
        anyhow::bail!("the stream did not end 5 s after stop: pump not released");
    }

    // A metadata read per cycle: also the cheapest way to notice per-call leaks.
    driver.list_topics().await.map_err(|e| anyhow::anyhow!("list_topics failed: {e}"))?;

    // Delete last, so the retained set never exceeds one batch of messages.
    let deleted = driver
        .delete_topics(&DeleteTopicsReq { names: vec![TOPIC.to_string()] })
        .await
        .map_err(|e| anyhow::anyhow!("delete_topics failed: {e}"))?;
    if let Some(result) = deleted.iter().find(|r| r.error.is_some()) {
        anyhow::bail!("delete_topics left {} behind: {:?}", result.name, result.error);
    }
    Ok(())
}

/// The driver's live-pump counter, read without holding the state lock for long.
async fn pumps(driver: &MockKafkaDriver) -> u64 {
    let handle = driver.state_handle();
    let guard = handle.read().await;
    guard.active_pumps()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let cycles: usize = match args.next() {
        Some(raw) => raw.parse().map_err(|e| anyhow::anyhow!("cycles: {e}"))?,
        None => DEFAULT_CYCLES,
    };
    let batch: usize = match args.next() {
        Some(raw) => raw.parse().map_err(|e| anyhow::anyhow!("batch: {e}"))?,
        None => DEFAULT_BATCH,
    };

    let driver = MockKafkaDriver::new();
    driver.connect(&ClusterConfig::mock("leak-probe")).await.map_err(|e| anyhow::anyhow!("connect: {e}"))?;

    for _ in 0..WARMUP {
        cycle(&driver, batch).await?;
    }
    let baseline = pumps(&driver).await;
    println!("warmup done after {WARMUP} cycles: pumps={baseline}");

    let started = Instant::now();
    for index in 1..=cycles {
        cycle(&driver, batch).await?;
        if index % REPORT_EVERY == 0 {
            let now = pumps(&driver).await;
            println!("cycle={index} elapsed={:.1}s pumps={now}", started.elapsed().as_secs_f64());
        }
    }

    let handle = driver.state_handle();
    let state = handle.read().await;
    let leaked = state.active_pumps();
    let topic_left = state.has_topic(TOPIC);
    drop(state);
    drop(handle);

    driver.disconnect().await.map_err(|e| anyhow::anyhow!("disconnect: {e}"))?;
    let after = pumps(&driver).await;

    println!(
        "done: cycles={cycles} batch={batch} elapsed={:.1}s pumps={leaked} after_disconnect={after} topic_left={topic_left}",
        started.elapsed().as_secs_f64()
    );
    if leaked != baseline {
        anyhow::bail!("pump leak: baseline {baseline} -> {leaked} after {cycles} cycles");
    }
    if after != 0 {
        anyhow::bail!("disconnect left {after} pump(s) running");
    }
    if topic_left {
        anyhow::bail!("the probe topic was never deleted: retained set grew");
    }
    Ok(())
}
