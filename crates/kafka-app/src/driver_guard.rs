//! Timeout and retry guard around any driver, so a slow or flaky broker can never hang
//! the UI thread or hand a transient error straight to the frontend.
//!
//! The guard is a decorator: it implements [`KafkaDriver`] and holds a [`DynDriver`], so
//! services keep calling the same trait. Wiring it in is the app facade's job
//! (`services::clusters::ClusterService` hands out the driver, T030 wraps it).

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::{Instant, timeout, timeout_at};

use kafka_core::cluster::{ClusterConfig, ClusterInfo};
use kafka_core::error::DriverError;
use kafka_core::group::{
    CommitOffsetsReq, GroupDetail, GroupInfo, GroupOffsetEntry, ListGroupOffsetsReq, ResetOffsetsReq,
    ResetOffsetsResult,
};
use kafka_core::message::{MessageStream, ProduceReq, ProduceResult, SubscribeReq};
use kafka_core::topic::{
    CreateTopicReq, DeleteTopicsReq, DescribeTopicsReq, ListOffsetsReq, OffsetRange, TopicDeleteResult,
    TopicDetail, TopicInfo, UpdateTopicConfigReq,
};
use kafka_driver::driver::{DriverCapabilities, DriverKind, DynDriver, KafkaDriver};

/// How long one attempt may take, and how the retries are spaced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    /// Pause before the second attempt.
    pub initial: Duration,
    /// Multiplier applied to the pause after every failed attempt.
    pub factor: u32,
    /// Ceiling for the pause, so a long retry chain cannot drift into minutes.
    pub max: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self { initial: Duration::from_millis(200), factor: 2, max: Duration::from_millis(2_000) }
    }
}

impl Backoff {
    const fn none() -> Self {
        Self { initial: Duration::ZERO, factor: 1, max: Duration::ZERO }
    }

    fn next(&self, current: Duration) -> Duration {
        let factor = self.factor.max(1);
        (current * factor).min(self.max.max(current))
    }
}

/// Retry budget for one guarded call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total calls per guarded operation, including the first one.
    pub max_attempts: u32,
    /// Deadline for a single attempt.
    pub per_call_timeout: Duration,
    /// Overall deadline across all attempts and pauses — this is what keeps the UI responsive.
    pub max_total: Duration,
    pub backoff: Backoff,
    /// Retry operations that may have reached the broker already (produce, create topic,
    /// offset reset). Off by default: a duplicate message is worse than a visible failure.
    pub retry_unsafe: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            per_call_timeout: Duration::from_millis(4_000),
            max_total: Duration::from_millis(10_000),
            backoff: Backoff::default(),
            retry_unsafe: false,
        }
    }
}

impl RetryPolicy {
    /// No retries and effectively no timeout — used where the caller owns the timing.
    pub const fn off() -> Self {
        Self {
            max_attempts: 1,
            per_call_timeout: Duration::from_secs(36_000),
            max_total: Duration::from_secs(36_000),
            backoff: Backoff::none(),
            retry_unsafe: false,
        }
    }

    pub fn attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts.max(1);
        self
    }

    pub fn per_call(mut self, per_call_timeout: Duration) -> Self {
        self.per_call_timeout = per_call_timeout;
        self
    }

    pub fn max_total(mut self, max_total: Duration) -> Self {
        self.max_total = max_total;
        self
    }

    pub fn backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }

    pub fn retry_unsafe(mut self, retry_unsafe: bool) -> Self {
        self.retry_unsafe = retry_unsafe;
        self
    }

    /// How much of the overall budget is left at `started`.
    fn remaining(&self, started: Instant) -> Option<Duration> {
        let elapsed = started.elapsed();
        if elapsed >= self.max_total {
            None
        } else {
            Some(self.max_total - elapsed)
        }
    }
}

/// Whether a failed attempt may be repeated without side effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Idem {
    /// Read-only, or a call whose repeat is harmless (connect, disconnect).
    Safe,
    /// May already have been applied by the broker. Retried only when
    /// [`RetryPolicy::retry_unsafe`] is set.
    Unsafe,
    /// Never retried: setting up a subscription stream twice would double the pump.
    Never,
}

/// Driver decorator that enforces the [`RetryPolicy`] on every call.
#[derive(Clone)]
pub struct GuardedDriver {
    inner: DynDriver,
    policy: RetryPolicy,
}

/// Hand-written: `DynDriver` is `Arc<dyn KafkaDriver>`, and the trait is not `Debug`,
/// so a derived impl would not compile.
impl fmt::Debug for GuardedDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GuardedDriver")
            .field("kind", &self.inner.kind())
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl GuardedDriver {
    pub fn new(inner: DynDriver) -> Self {
        Self { inner, policy: RetryPolicy::default() }
    }

    pub fn with_policy(inner: DynDriver, policy: RetryPolicy) -> Self {
        Self { inner, policy }
    }

    /// The wrapped driver, for callers that must bypass the guard.
    pub fn inner(&self) -> &DynDriver {
        &self.inner
    }

    pub fn into_inner(self) -> DynDriver {
        self.inner
    }

    pub fn policy(&self) -> RetryPolicy {
        self.policy
    }

    fn may_retry(&self, idem: Idem, err: &DriverError) -> bool {
        match idem {
            Idem::Never => false,
            Idem::Unsafe => self.policy.retry_unsafe && err.is_retryable(),
            Idem::Safe => err.is_retryable(),
        }
    }

    /// Runs one operation under the policy. `f` receives a fresh driver handle per attempt
    /// so the returned future owns its state and never borrows the guard.
    async fn guarded_call<T, F, Fut>(&self, op: &'static str, idem: Idem, f: F) -> Result<T, DriverError>
    where
        F: Fn(DynDriver) -> Fut,
        Fut: Future<Output = Result<T, DriverError>> + Send,
        T: Send,
    {
        let started = Instant::now();
        let max_attempts = self.policy.max_attempts.max(1);
        let mut attempt: u32 = 0;
        let mut pause = self.policy.backoff.initial;
        let mut last: Option<DriverError> = None;

        loop {
            let Some(remaining) = self.policy.remaining(started) else {
                return Err(self.exhausted(op, &mut last, attempt));
            };
            attempt += 1;

            match timeout(remaining.min(self.policy.per_call_timeout), f(Arc::clone(&self.inner))).await {
                Ok(Ok(value)) => {
                    if attempt > 1 {
                        tracing::debug!(operation = op, attempts = attempt, "driver call recovered");
                    }
                    return Ok(value);
                }
                Ok(Err(err)) => {
                    let retryable = self.may_retry(idem, &err);
                    if !retryable || attempt >= max_attempts {
                        return Err(err);
                    }
                    tracing::warn!(
                        operation = op,
                        attempt = attempt,
                        max_attempts = max_attempts,
                        kind = err.kind().as_str(),
                        "driver call failed, retrying: {err}"
                    );
                    self.pause_and_backoff(&mut pause, remaining).await;
                    last = Some(err);
                }
                Err(_elapsed) => {
                    let spent = started.elapsed().as_millis().min(u64::MAX as u128) as u64;
                    let err = DriverError::timeout(op, spent);
                    if attempt >= max_attempts {
                        return Err(err);
                    }
                    tracing::warn!(
                        operation = op,
                        attempt = attempt,
                        max_attempts = max_attempts,
                        "driver call timed out, retrying after {}ms",
                        pause.as_millis()
                    );
                    self.pause_and_backoff(&mut pause, remaining).await;
                    last = Some(err);
                }
            }
        }
    }

    /// Sleeps `pause`, but never past the overall budget, then grows the pause.
    async fn pause_and_backoff(&self, pause: &mut Duration, remaining: Duration) {
        let wait = Duration::min(*pause, remaining);
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        *pause = self.policy.backoff.next(*pause);
    }

    fn exhausted(&self, op: &'static str, last: &mut Option<DriverError>, attempts: u32) -> DriverError {
        let err = last
            .take()
            .unwrap_or_else(|| DriverError::timeout(op, self.policy.max_total.as_millis() as u64));
        tracing::warn!(operation = op, attempts = attempts, "retry budget exhausted: {err}");
        err
    }
}

#[async_trait]
impl KafkaDriver for GuardedDriver {
    async fn connect(&self, cfg: &ClusterConfig) -> Result<ClusterInfo, DriverError> {
        // Repeating a connect is harmless: the driver replaces its clients.
        let cfg = cfg.clone();
        self.guarded_call("connect", Idem::Safe, move |inner| {
            let cfg = cfg.clone();
            async move { inner.connect(&cfg).await }
        })
        .await
    }

    async fn disconnect(&self) -> Result<(), DriverError> {
        self.guarded_call("disconnect", Idem::Safe, |inner| async move { inner.disconnect().await })
            .await
    }

    /// Introspection, not an operation: never timed out, or a paused stream would look dead.
    async fn is_connected(&self) -> bool {
        self.inner.is_connected().await
    }

    async fn cluster_info(&self) -> Result<ClusterInfo, DriverError> {
        self.guarded_call("cluster_info", Idem::Safe, |inner| async move { inner.cluster_info().await })
            .await
    }

    async fn list_topics(&self) -> Result<Vec<TopicInfo>, DriverError> {
        self.guarded_call("list_topics", Idem::Safe, |inner| async move { inner.list_topics().await }).await
    }

    async fn describe_topics(&self, req: &DescribeTopicsReq) -> Result<Vec<TopicDetail>, DriverError> {
        let req = req.clone();
        self.guarded_call("describe_topics", Idem::Safe, move |inner| {
            let req = req.clone();
            async move { inner.describe_topics(&req).await }
        })
        .await
    }

    async fn create_topic(&self, req: &CreateTopicReq) -> Result<(), DriverError> {
        let req = req.clone();
        self.guarded_call("create_topic", Idem::Unsafe, move |inner| {
            let req = req.clone();
            async move { inner.create_topic(&req).await }
        })
        .await
    }

    async fn delete_topics(&self, req: &DeleteTopicsReq) -> Result<Vec<TopicDeleteResult>, DriverError> {
        let req = req.clone();
        self.guarded_call("delete_topics", Idem::Unsafe, move |inner| {
            let req = req.clone();
            async move { inner.delete_topics(&req).await }
        })
        .await
    }

    async fn update_topic_config(&self, req: &UpdateTopicConfigReq) -> Result<(), DriverError> {
        let req = req.clone();
        self.guarded_call("update_topic_config", Idem::Unsafe, move |inner| {
            let req = req.clone();
            async move { inner.update_topic_config(&req).await }
        })
        .await
    }

    async fn list_offsets(&self, req: &ListOffsetsReq) -> Result<Vec<OffsetRange>, DriverError> {
        let req = req.clone();
        self.guarded_call("list_offsets", Idem::Safe, move |inner| {
            let req = req.clone();
            async move { inner.list_offsets(&req).await }
        })
        .await
    }

    async fn produce(&self, req: ProduceReq) -> Result<Vec<ProduceResult>, DriverError> {
        self.guarded_call("produce", Idem::Unsafe, move |inner| {
            let req = req.clone();
            async move { inner.produce(req).await }
        })
        .await
    }

    /// Only the setup is guarded — the stream itself must not be retried, and its own
    /// lifetime is owned by the consume service.
    async fn subscribe(&self, req: SubscribeReq) -> Result<MessageStream, DriverError> {
        let deadline = Instant::now() + self.policy.per_call_timeout;
        match timeout_at(deadline, self.inner.subscribe(req)).await {
            Ok(result) => result,
            Err(_elapsed) => Err(DriverError::timeout("subscribe", self.policy.per_call_timeout.as_millis() as u64)),
        }
    }

    async fn list_consumer_groups(&self) -> Result<Vec<GroupInfo>, DriverError> {
        self.guarded_call("list_consumer_groups", Idem::Safe, |inner| async move { inner.list_consumer_groups().await })
            .await
    }

    async fn describe_consumer_groups(&self, ids: &[String]) -> Result<Vec<GroupDetail>, DriverError> {
        let ids = ids.to_vec();
        self.guarded_call("describe_consumer_groups", Idem::Safe, move |inner| {
            let ids = ids.clone();
            async move { inner.describe_consumer_groups(&ids).await }
        })
        .await
    }

    async fn list_group_offsets(&self, req: &ListGroupOffsetsReq) -> Result<Vec<GroupOffsetEntry>, DriverError> {
        let req = req.clone();
        self.guarded_call("list_group_offsets", Idem::Safe, move |inner| {
            let req = req.clone();
            async move { inner.list_group_offsets(&req).await }
        })
        .await
    }

    async fn reset_offsets(&self, req: &ResetOffsetsReq) -> Result<ResetOffsetsResult, DriverError> {
        let req = req.clone();
        self.guarded_call("reset_offsets", Idem::Unsafe, move |inner| {
            let req = req.clone();
            async move { inner.reset_offsets(&req).await }
        })
        .await
    }

    async fn commit_offsets(&self, req: CommitOffsetsReq) -> Result<(), DriverError> {
        self.guarded_call("commit_offsets", Idem::Unsafe, move |inner| {
            let req = req.clone();
            async move { inner.commit_offsets(req).await }
        })
        .await
    }

    fn capabilities(&self) -> DriverCapabilities {
        self.inner.capabilities()
    }

    fn kind(&self) -> DriverKind {
        self.inner.kind()
    }

    /// Forwarded as-is: the guard does not re-subscribe, so one call is exactly one
    /// subscription on the driver's own event channel.
    async fn events(&self) -> Option<tokio::sync::broadcast::Receiver<kafka_driver::driver::DriverEvent>> {
        self.inner.events().await
    }
}
