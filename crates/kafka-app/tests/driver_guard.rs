//! T029 验收：`GuardedDriver` — 超时、重试、幂等分类、退避上限、透传。
//!
//! 计数装饰器 `Counting` 包住 Mock 驱动，逐次记录调用次数，并能让某个操作的**前 N 次**
//! 调用返回可重试错误。故障注入只用 `0.0` / `1.0` 两档概率（见 `fault.rs::roll`），
//! 所以结果与随机 seed 无关；只有退避/总预算两个用例读墙钟，断言都留了宽边界。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::broadcast;

use kafka_app::driver_guard::{Backoff, GuardedDriver, RetryPolicy};
use kafka_core::cluster::{ClusterConfig, ClusterInfo};
use kafka_core::error::{DriverError, ErrorKind};
use kafka_core::group::{
    CommitOffsetsReq, GroupDetail, GroupInfo, GroupOffsetEntry, ListGroupOffsetsReq, ResetOffsetsReq,
    ResetOffsetsResult,
};
use kafka_core::message::{MessageStream, ProduceMessage, ProduceReq, ProduceResult, SubscribeReq};
use kafka_core::topic::{
    CreateTopicReq, DeleteTopicsReq, DescribeTopicsReq, ListOffsetsReq, OffsetRange, TopicDeleteResult,
    TopicDetail, TopicInfo, UpdateTopicConfigReq,
};
use kafka_driver::driver::{DriverCapabilities, DriverEvent, DriverKind, DynDriver, KafkaDriver};
use kafka_driver::mock::{FaultInjection, LatencyRange, MockKafkaDriver};

/// 不测超时的用例用宽超时，避免把「重试」测成「超时」测试。
const ROOMY: Duration = Duration::from_millis(500);
/// 测超时的用例：单次 10ms，Mock 固定睡 60ms。
const TIGHT: Duration = Duration::from_millis(10);
const SLOW_MS: u64 = 60;
/// 退避归零，让重试次数成为唯一变量。
const NO_PAUSE: Backoff = Backoff { initial: Duration::ZERO, factor: 1, max: Duration::ZERO };

/// 100% 失败，只作用于 `op`（`fail_probability = 1.0` 与 seed 无关）。
fn always_fail(op: &str) -> FaultInjection {
    FaultInjection { fail_probability: 1.0, fail_operation: Some(op.to_string()), ..Default::default() }
}

/// 每次调用先固定睡 `ms`，用来撞 `per_call_timeout`。
fn slow(ms: u64) -> FaultInjection {
    FaultInjection { latency: LatencyRange { min_ms: ms, max_ms: ms }, ..Default::default() }
}

/// 不重试、不退避，只留一个宽超时。
fn once() -> RetryPolicy {
    RetryPolicy::off().per_call(ROOMY)
}

/// 重试 `attempts` 次、不退避、总预算宽裕 —— 只考察次数。
fn retries(attempts: u32) -> RetryPolicy {
    RetryPolicy::off().attempts(attempts).per_call(ROOMY).backoff(NO_PAUSE)
}

/// 记录每个操作被调了几次，并可让前 N 次调用直接失败。
#[derive(Debug, Default)]
struct Counting {
    mock: MockKafkaDriver,
    calls: Mutex<BTreeMap<&'static str, u32>>,
    fail_first: Mutex<BTreeMap<&'static str, u32>>,
}

impl Counting {
    fn new() -> Self {
        Self { mock: MockKafkaDriver::new(), ..Default::default() }
    }

    /// 用来设置故障注入 / 读原始状态。
    fn mock(&self) -> &MockKafkaDriver {
        &self.mock
    }

    /// 让 `op` 的前 `times` 次调用返回可重试错误，之后交回 Mock。
    fn fail_first_calls(&self, op: &'static str, times: u32) {
        self.fail_first.lock().expect("fail plan").insert(op, times);
    }

    fn calls(&self, op: &str) -> u32 {
        *self.calls.lock().expect("calls").get(op).unwrap_or(&0)
    }

    /// 记一次调用；仍在「前 N 次」窗口内就注入一次可重试失败。
    fn hit(&self, op: &'static str) -> Result<(), DriverError> {
        *self.calls.lock().expect("calls").entry(op).or_default() += 1;
        let mut plan = self.fail_first.lock().expect("fail plan");
        if let Some(left) = plan.get_mut(op) {
            if *left > 0 {
                *left -= 1;
                return Err(DriverError::Connection {
                    server: op.to_string(),
                    message: "injected by counting driver".into(),
                });
            }
        }
        Ok(())
    }
}

#[async_trait]
impl KafkaDriver for Counting {
    async fn connect(&self, cfg: &ClusterConfig) -> Result<ClusterInfo, DriverError> {
        self.hit("connect")?;
        self.mock.connect(cfg).await
    }
    async fn disconnect(&self) -> Result<(), DriverError> {
        self.hit("disconnect")?;
        self.mock.disconnect().await
    }
    async fn is_connected(&self) -> bool {
        *self.calls.lock().expect("calls").entry("is_connected").or_default() += 1;
        self.mock.is_connected().await
    }
    async fn cluster_info(&self) -> Result<ClusterInfo, DriverError> {
        self.hit("cluster_info")?;
        self.mock.cluster_info().await
    }
    async fn list_topics(&self) -> Result<Vec<TopicInfo>, DriverError> {
        self.hit("list_topics")?;
        self.mock.list_topics().await
    }
    async fn describe_topics(&self, req: &DescribeTopicsReq) -> Result<Vec<TopicDetail>, DriverError> {
        self.hit("describe_topics")?;
        self.mock.describe_topics(req).await
    }
    async fn create_topic(&self, req: &CreateTopicReq) -> Result<(), DriverError> {
        self.hit("create_topic")?;
        self.mock.create_topic(req).await
    }
    async fn delete_topics(&self, req: &DeleteTopicsReq) -> Result<Vec<TopicDeleteResult>, DriverError> {
        self.hit("delete_topics")?;
        self.mock.delete_topics(req).await
    }
    async fn update_topic_config(&self, req: &UpdateTopicConfigReq) -> Result<(), DriverError> {
        self.hit("update_topic_config")?;
        self.mock.update_topic_config(req).await
    }
    async fn list_offsets(&self, req: &ListOffsetsReq) -> Result<Vec<OffsetRange>, DriverError> {
        self.hit("list_offsets")?;
        self.mock.list_offsets(req).await
    }
    async fn produce(&self, req: ProduceReq) -> Result<Vec<ProduceResult>, DriverError> {
        self.hit("produce")?;
        self.mock.produce(req).await
    }
    async fn subscribe(&self, req: SubscribeReq) -> Result<MessageStream, DriverError> {
        self.hit("subscribe")?;
        self.mock.subscribe(req).await
    }
    async fn list_consumer_groups(&self) -> Result<Vec<GroupInfo>, DriverError> {
        self.hit("list_consumer_groups")?;
        self.mock.list_consumer_groups().await
    }
    async fn describe_consumer_groups(&self, ids: &[String]) -> Result<Vec<GroupDetail>, DriverError> {
        self.hit("describe_consumer_groups")?;
        self.mock.describe_consumer_groups(ids).await
    }
    async fn list_group_offsets(&self, req: &ListGroupOffsetsReq) -> Result<Vec<GroupOffsetEntry>, DriverError> {
        self.hit("list_group_offsets")?;
        self.mock.list_group_offsets(req).await
    }
    async fn reset_offsets(&self, req: &ResetOffsetsReq) -> Result<ResetOffsetsResult, DriverError> {
        self.hit("reset_offsets")?;
        self.mock.reset_offsets(req).await
    }
    async fn commit_offsets(&self, req: CommitOffsetsReq) -> Result<(), DriverError> {
        self.hit("commit_offsets")?;
        self.mock.commit_offsets(req).await
    }
    fn capabilities(&self) -> DriverCapabilities {
        self.mock.capabilities()
    }
    fn kind(&self) -> DriverKind {
        self.mock.kind()
    }
    async fn events(&self) -> Option<broadcast::Receiver<DriverEvent>> {
        self.mock.events().await
    }
}

/// 建好护栏 + 计数驱动，并完成一次连接（连接本身也计数）。
async fn connected(policy: RetryPolicy) -> (GuardedDriver, Arc<Counting>) {
    let counting = Arc::new(Counting::new());
    let driver: DynDriver = counting.clone();
    let guard = GuardedDriver::with_policy(driver, policy);
    guard.connect(&ClusterConfig::mock("guard-test")).await.expect("connect");
    (guard, counting)
}

fn unguarded(policy: RetryPolicy, counting: &Arc<Counting>) -> GuardedDriver {
    let driver: DynDriver = counting.clone();
    GuardedDriver::with_policy(driver, policy)
}

// ---- 重试次数 -------------------------------------------------------------

#[tokio::test]
async fn injected_total_failure_reaches_exactly_max_attempts() {
    let (guard, counting) = connected(retries(4)).await;
    counting.mock().set_faults(always_fail("list_topics")).await;

    let err = guard.list_topics().await.expect_err("every attempt fails");
    assert_eq!(err.kind(), ErrorKind::Connection);
    assert_eq!(counting.calls("list_topics"), 4);
}

#[tokio::test]
async fn first_failure_then_success_uses_two_attempts() {
    let (guard, counting) = connected(retries(3)).await;
    counting.fail_first_calls("list_topics", 1);

    let topics = guard.list_topics().await.expect("recovers on attempt 2");
    assert!(topics.is_empty());
    assert_eq!(counting.calls("list_topics"), 2);
}

#[tokio::test]
async fn flaky_connect_recovers_on_the_third_attempt() {
    let counting = Arc::new(Counting::new());
    counting.fail_first_calls("connect", 2);
    let guard = unguarded(retries(3), &counting);

    let info = guard.connect(&ClusterConfig::mock("guard-test")).await.expect("connect on attempt 3");
    assert!(info.cluster_id.is_some());
    assert!(!info.brokers.is_empty());
    assert_eq!(counting.calls("connect"), 3);
    assert!(guard.is_connected().await);
}

#[tokio::test]
async fn off_policy_disables_retrying() {
    let (guard, counting) = connected(RetryPolicy::off()).await;
    counting.mock().set_faults(always_fail("list_topics")).await;

    let err = guard.list_topics().await.expect_err("no retry allowed");
    assert_eq!(err.kind(), ErrorKind::Connection);
    assert_eq!(counting.calls("list_topics"), 1);
}

// ---- 错误分类 -------------------------------------------------------------

#[tokio::test]
async fn non_retryable_errors_return_after_one_attempt() {
    let (guard, counting) = connected(retries(5)).await;

    let err = guard
        .describe_topics(&DescribeTopicsReq { names: vec!["ghost".into()], ..Default::default() })
        .await
        .expect_err("unknown topic");
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert_eq!(counting.calls("describe_topics"), 1);

    let mut set = BTreeMap::new();
    set.insert("retention.ms".to_string(), "86400000".to_string());
    let err = guard
        .update_topic_config(&UpdateTopicConfigReq { topic: "ghost".into(), set, ..Default::default() })
        .await
        .expect_err("unknown topic");
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert_eq!(counting.calls("update_topic_config"), 1);

    let bad = ClusterConfig { name: String::new(), ..ClusterConfig::mock("guard-test") };
    let fresh = Arc::new(Counting::new());
    let fresh_guard = unguarded(retries(5), &fresh);
    let err = fresh_guard.connect(&bad).await.expect_err("invalid config");
    assert_eq!(err.kind(), ErrorKind::InvalidConfig);
    assert_eq!(fresh.calls("connect"), 1);
}

#[tokio::test]
async fn not_connected_is_retryable_so_the_chain_runs_to_the_limit() {
    let counting = Arc::new(Counting::new());
    let guard = unguarded(retries(3), &counting);

    let err = guard.list_topics().await.expect_err("never connected");
    assert_eq!(err.kind(), ErrorKind::NotConnected);
    assert_eq!(counting.calls("list_topics"), 3);
}

#[tokio::test]
async fn unsafe_operations_stop_at_the_first_attempt() {
    let (guard, counting) = connected(retries(4)).await;

    counting.mock().set_faults(always_fail("produce")).await;
    let err = guard
        .produce(ProduceReq::one("orders", ProduceMessage::text("x")))
        .await
        .expect_err("unsafe must not retry");
    assert_eq!(err.kind(), ErrorKind::Connection);
    assert_eq!(counting.calls("produce"), 1);

    counting.mock().set_faults(always_fail("create_topic")).await;
    let err = guard.create_topic(&CreateTopicReq::new("orders")).await.expect_err("unsafe must not retry");
    assert_eq!(err.kind(), ErrorKind::Connection);
    assert_eq!(counting.calls("create_topic"), 1);

    counting.mock().set_faults(always_fail("delete_topics")).await;
    let err = guard
        .delete_topics(&DeleteTopicsReq { names: vec!["orders".into()] })
        .await
        .expect_err("unsafe must not retry");
    assert_eq!(err.kind(), ErrorKind::Connection);
    assert_eq!(counting.calls("delete_topics"), 1);
}

#[tokio::test]
async fn unsafe_operations_retry_only_when_the_policy_asks_for_it() {
    let (guard, counting) = connected(retries(3).retry_unsafe(true)).await;
    counting.mock().set_faults(always_fail("produce")).await;

    let err = guard
        .produce(ProduceReq::one("orders", ProduceMessage::text("x")))
        .await
        .expect_err("still failing");
    assert_eq!(err.kind(), ErrorKind::Connection);
    assert_eq!(counting.calls("produce"), 3);
}

// ---- 超时 ---------------------------------------------------------------

#[tokio::test]
async fn slow_call_hits_the_per_call_timeout() {
    let (guard, counting) = connected(once().per_call(TIGHT)).await;
    counting.mock().set_faults(slow(SLOW_MS)).await;

    let err = guard.list_topics().await.expect_err("too slow");
    assert_eq!(err.kind(), ErrorKind::Timeout);
    assert!(err.to_string().contains("list_topics"), "{err}");
    assert_eq!(counting.calls("list_topics"), 1);
}

#[tokio::test]
async fn a_timed_out_attempt_is_retried_like_any_other() {
    let (guard, counting) = connected(retries(3).per_call(TIGHT)).await;
    counting.mock().set_faults(slow(SLOW_MS)).await;

    let err = guard.list_topics().await.expect_err("too slow x3");
    assert_eq!(err.kind(), ErrorKind::Timeout);
    assert_eq!(counting.calls("list_topics"), 3);
}

#[tokio::test]
async fn max_total_caps_the_retry_chain() {
    let policy = RetryPolicy::off()
        .attempts(10)
        .per_call(TIGHT)
        .max_total(Duration::from_millis(60))
        .backoff(NO_PAUSE);
    let (guard, counting) = connected(policy).await;
    counting.mock().set_faults(slow(SLOW_MS)).await;

    let started = Instant::now();
    let err = guard.list_topics().await.expect_err("budget spent");
    let spent = started.elapsed();

    assert_eq!(err.kind(), ErrorKind::Timeout);
    let attempts = counting.calls("list_topics");
    // 每次约 10ms，60ms 预算 → 远少于 attempts(10)，但肯定不止一次。
    assert!((2..=6).contains(&attempts), "attempts {attempts}");
    assert!(spent < Duration::from_millis(300), "spent {spent:?}");
}

#[tokio::test]
async fn backoff_grows_and_is_capped() {
    let backoff = Backoff { initial: Duration::from_millis(40), factor: 3, max: Duration::from_millis(60) };
    let policy = RetryPolicy::off()
        .attempts(4)
        .per_call(ROOMY)
        .max_total(Duration::from_secs(5))
        .backoff(backoff);
    let (guard, counting) = connected(policy).await;
    counting.mock().set_faults(always_fail("list_topics")).await;

    let started = Instant::now();
    let err = guard.list_topics().await.expect_err("every attempt fails");
    let spent = started.elapsed();

    assert_eq!(err.kind(), ErrorKind::Connection);
    assert_eq!(counting.calls("list_topics"), 4);
    // 三段退避 40 + 60 + 60 = 160ms（factor 3 被 max=60 截住）。
    assert!(spent >= Duration::from_millis(150), "spent {spent:?}");
    assert!(spent < Duration::from_millis(1_500), "spent {spent:?}");
}

// ---- 订阅与透传 -----------------------------------------------------------

#[tokio::test]
async fn subscribe_is_single_shot_even_when_the_policy_allows_retries() {
    let (guard, counting) = connected(retries(5)).await;
    counting.mock().set_faults(always_fail("subscribe")).await;

    let err = guard.subscribe(SubscribeReq::new("orders")).await.expect_err("never retried");
    assert_eq!(err.kind(), ErrorKind::Connection);
    assert_eq!(counting.calls("subscribe"), 1);

    let (guard, counting) = connected(once().attempts(5).per_call(TIGHT)).await;
    counting.mock().set_faults(slow(SLOW_MS)).await;

    let err = guard.subscribe(SubscribeReq::new("orders")).await.expect_err("slow setup");
    assert_eq!(err.kind(), ErrorKind::Timeout);
    assert_eq!(counting.calls("subscribe"), 1);
}

#[tokio::test]
async fn introspection_is_never_timed_out() {
    let (guard, counting) = connected(once().per_call(TIGHT)).await;
    counting.mock().set_faults(slow(SLOW_MS)).await;

    let started = Instant::now();
    assert!(guard.is_connected().await);
    assert!(started.elapsed() < Duration::from_millis(SLOW_MS), "{:?}", started.elapsed());
    assert_eq!(counting.calls("is_connected"), 1);
}

#[tokio::test]
async fn capabilities_kind_and_events_are_forwarded() {
    let (guard, counting) = connected(once()).await;

    let caps = guard.capabilities();
    assert_eq!(caps, DriverCapabilities { admin_api: true, fault_injection: true, fixtures: true, ..caps });
    assert_eq!(guard.kind(), DriverKind::Mock);

    let mut rx = guard.events().await.expect("driver event stream");
    guard.create_topic(&CreateTopicReq::new("orders")).await.expect("create");
    assert_eq!(rx.recv().await.expect("one event"), DriverEvent::TopicCreated { topic: "orders".into() });
    assert_eq!(counting.calls("create_topic"), 1);
    assert!(rx.try_recv().is_err(), "the guard must not replay or double-subscribe");
}
