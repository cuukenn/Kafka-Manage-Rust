//! T101: Mock driver throughput — produce and consume.

use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use kafka_core::cluster::ClusterConfig;
use kafka_core::message::{OffsetStart, ProduceMessage, ProduceReq, SubscribeReq};
use kafka_core::topic::CreateTopicReq;
use kafka_driver::driver::KafkaDriver;
use kafka_driver::mock::MockKafkaDriver;
use tokio::runtime::Runtime;

const TOPIC: &str = "bench-topic";
const BATCH: usize = 100;

async fn bench_driver() -> MockKafkaDriver {
    let driver = MockKafkaDriver::new();
    driver.connect(&ClusterConfig::mock("bench")).await.expect("connect");
    let mut req = CreateTopicReq::new(TOPIC);
    req.num_partitions = 1;
    driver.create_topic(&req).await.expect("create_topic");
    driver
}

fn bench_produce(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let driver = rt.block_on(bench_driver());
    let batch: Vec<ProduceMessage> =
        (0..BATCH).map(|i| ProduceMessage::text(format!("value-{i}"))).collect();
    let mut group = c.benchmark_group("mock_produce");
    group.throughput(Throughput::Elements(BATCH as u64));
    group.bench_function("batch", |b| {
        b.iter(|| {
            rt.block_on(async {
                driver
                    .produce(ProduceReq { topic: TOPIC.to_string(), messages: batch.clone() })
                    .await
                    .expect("produce")
            })
        })
    });
    group.finish();
}

fn bench_consume(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let driver = rt.block_on(async {
        let driver = bench_driver().await;
        let batch: Vec<ProduceMessage> =
            (0..BATCH).map(|i| ProduceMessage::text(format!("value-{i}"))).collect();
        driver
            .produce(ProduceReq { topic: TOPIC.to_string(), messages: batch })
            .await
            .expect("produce");
        driver
    });
    let mut group = c.benchmark_group("mock_consume");
    group.throughput(Throughput::Elements(BATCH as u64));
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));
    group.bench_function("drain", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut stream = driver
                    .subscribe(SubscribeReq {
                        topic: TOPIC.to_string(),
                        start: OffsetStart::Earliest,
                        follow: false,
                        max_messages: Some(BATCH as u64),
                        ..Default::default()
                    })
                    .await
                    .expect("subscribe");
                let mut seen = 0usize;
                while let Some(event) = stream.next().await {
                    event.expect("event");
                    seen += 1;
                }
                seen
            })
        })
    });
    group.finish();
}

criterion_group!(benches, bench_produce, bench_consume);
criterion_main!(benches);
