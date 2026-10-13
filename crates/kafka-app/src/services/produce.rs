//! 生产消息：前置校验 → 调驱动 → 逐条回执 + 汇总计数 → 发 `ProduceResult`。
//!
//! 一批里部分失败不算调用失败：驱动逐条返回 `ProduceResult.error`，服务只汇总计数，
//! 前端按条展示。只有「整个请求打不到驱动」（未连接、参数不合法）才返回 `Err`。

use kafka_core::message::{ProduceReq, ProduceResult};
use kafka_core::error::DriverError;
use kafka_core::ClusterId;
use serde::Serialize;

use super::ServiceError;
use super::clusters::ClusterService;
use crate::bus::{AppEvent, EventBus};

/// 单条消息 value 的默认上限（1 MiB），与前端「粘贴大文本」的量级一致。
pub const DEFAULT_MAX_VALUE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone)]
pub struct ProduceService {
    clusters: ClusterService,
    max_value_bytes: usize,
}

/// 一次调用的全部回执。逐条是否成功看 `error`（`None` 即成功），计数已预先算好。
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ProduceReport {
    #[serde(default)]
    pub results: Vec<ProduceResult>,
    #[serde(default)]
    pub ok_count: usize,
    #[serde(default)]
    pub error_count: usize,
}

impl ProduceReport {
    pub fn is_ok(&self) -> bool {
        self.error_count == 0
    }

    /// 失败条目的可读描述：`topic-partition: error`。
    pub fn failures(&self) -> Vec<String> {
        self.results
            .iter()
            .filter(|r| r.error.is_some())
            .map(|r| {
                format!(
                    "{}-{}: {}",
                    r.topic,
                    r.partition,
                    r.error.clone().unwrap_or_default()
                )
            })
            .collect()
    }

    fn push_all(&mut self, results: &[ProduceResult]) {
        for result in results {
            if result.error.is_none() {
                self.ok_count += 1;
            } else {
                self.error_count += 1;
            }
            self.results.push(result.clone());
        }
    }
}

impl ProduceService {
    pub fn new(clusters: ClusterService) -> Self {
        Self {
            clusters,
            max_value_bytes: DEFAULT_MAX_VALUE_BYTES,
        }
    }

    /// 链式改单条 value 上限，`0` 会被抬到 1 字节，避免出现永远发不出去的配置。
    pub fn with_max_value_bytes(mut self, bytes: usize) -> Self {
        self.max_value_bytes = bytes.max(1);
        self
    }

    pub fn clusters(&self) -> &ClusterService {
        &self.clusters
    }

    pub fn bus(&self) -> &EventBus {
        self.clusters.bus()
    }

    pub fn max_value_bytes(&self) -> usize {
        self.max_value_bytes
    }

    /// 发一个 `ProduceReq`（可以只有一条，也可以多条）。
    pub async fn send_one(
        &self,
        cluster_id: &ClusterId,
        req: ProduceReq,
    ) -> Result<ProduceReport, ServiceError> {
        let reqs = [req];
        self.send(cluster_id, &reqs).await
    }

    /// 一次调用发多个 topic 的批次；某个请求整体失败时，它的每条消息记为失败条目，
    /// 其余请求继续发完。
    pub async fn send_batch(
        &self,
        cluster_id: &ClusterId,
        reqs: &[ProduceReq],
    ) -> Result<ProduceReport, ServiceError> {
        if reqs.is_empty() {
            return Err(ServiceError::Driver(DriverError::invalid(
                "requests",
                "at least one produce request is required",
            )));
        }
        self.send(cluster_id, reqs).await
    }

    async fn send(
        &self,
        cluster_id: &ClusterId,
        reqs: &[ProduceReq],
    ) -> Result<ProduceReport, ServiceError> {
        // 前置校验：不合法的批次根本不碰驱动，也不发事件。
        self.validate(reqs)?;
        let driver = self.clusters.driver(cluster_id)?;

        let mut report = ProduceReport::default();
        for req in reqs {
            match driver.produce(req.clone()).await {
                Ok(results) => report.push_all(&results),
                Err(err) => {
                    let reason = err.to_string();
                    let synthetic: Vec<ProduceResult> = req
                        .messages
                        .iter()
                        .map(|message| ProduceResult {
                            topic: req.topic.clone(),
                            partition: message.partition.unwrap_or(0),
                            error: Some(reason.clone()),
                            ..Default::default()
                        })
                        .collect();
                    tracing::warn!(cluster = %cluster_id, topic = %req.topic, error = %reason, "produce request failed");
                    report.push_all(&synthetic);
                }
            }
        }

        self.publish(cluster_id, &report);
        Ok(report)
    }

    fn validate(&self, reqs: &[ProduceReq]) -> Result<(), ServiceError> {
        for (index, req) in reqs.iter().enumerate() {
            req.validate()?;
            for (position, message) in req.messages.iter().enumerate() {
                if message.value.len() > self.max_value_bytes {
                    return Err(ServiceError::Driver(DriverError::invalid(
                        format!("requests[{index}].messages[{position}].value"),
                        format!(
                            "value is {} bytes, limit is {} bytes",
                            message.value.len(),
                            self.max_value_bytes
                        ),
                    )));
                }
            }
        }
        Ok(())
    }

    fn publish(&self, cluster_id: &ClusterId, report: &ProduceReport) {
        self.clusters.bus().publish(AppEvent::ProduceResult {
            cluster_id: cluster_id.to_string(),
            results: report.results.clone(),
            ok_count: report.ok_count,
            error_count: report.error_count,
        });
    }
}
