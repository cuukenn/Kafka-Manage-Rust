//! 消费组业务封装：列表 / 详情 / 位点 / 位点重置。
//!
//! 服务本身不持有状态，只做参数校验、驱动调用与事件发布；
//! 消费组的真实状态始终来自驱动（mock 或 real）。

use kafka_core::ClusterId;
use kafka_core::error::DriverError;
use kafka_core::group::{
    GroupDetail,
    GroupInfo,
    GroupOffsetEntry,
    ListGroupOffsetsReq,
    ResetOffsetsReq,
    ResetOffsetsResult,
};

use crate::bus::{AppEvent, EventBus};

use super::ServiceError;
use super::clusters::ClusterService;

/// 消费组服务。克隆廉价，共享同一份驱动注册表与事件总线。
#[derive(Debug, Clone)]
pub struct GroupService {
    clusters: ClusterService,
}

impl GroupService {
    pub fn new(clusters: ClusterService) -> Self {
        Self { clusters }
    }

    pub fn clusters(&self) -> &ClusterService {
        &self.clusters
    }

    pub fn bus(&self) -> &EventBus {
        self.clusters.bus()
    }

    /// 消费组列表（含本应用自己产生的订阅组，由驱动标记 `is_local`）。
    pub async fn list(&self, cluster_id: &ClusterId) -> Result<Vec<GroupInfo>, ServiceError> {
        let driver = self.clusters.driver(cluster_id)?;
        Ok(driver.list_consumer_groups().await?)
    }

    /// 消费组详情。`ids` 为空表示全部；未知 id 由驱动报 `GroupNotFound`。
    pub async fn describe(
        &self,
        cluster_id: &ClusterId,
        ids: &[String],
    ) -> Result<Vec<GroupDetail>, ServiceError> {
        for id in ids {
            require_group_id(id)?;
        }
        let driver = self.clusters.driver(cluster_id)?;
        Ok(driver.describe_consumer_groups(ids).await?)
    }

    /// 已提交位点。`req.with_lag` 为真时驱动会顺带取日志末端算 lag。
    pub async fn offsets(
        &self,
        cluster_id: &ClusterId,
        req: &ListGroupOffsetsReq,
    ) -> Result<Vec<GroupOffsetEntry>, ServiceError> {
        require_group_id(&req.group_id)?;
        let driver = self.clusters.driver(cluster_id)?;
        Ok(driver.list_group_offsets(req).await?)
    }

    /// 位点重置。`dry_run` 只计算不写入，也不发事件；
    /// 真正写入成功后发一次 `GROUP_CHANGED`，返回值原样透传给前端。
    pub async fn reset(
        &self,
        cluster_id: &ClusterId,
        req: &ResetOffsetsReq,
    ) -> Result<ResetOffsetsResult, ServiceError> {
        req.validate()?;
        let driver = self.clusters.driver(cluster_id)?;
        let result = driver.reset_offsets(req).await?;
        if !result.dry_run && result.error.is_none() {
            self.publish_change(cluster_id, &result.group_id, result.applied.len());
        }
        Ok(result)
    }

    fn publish_change(&self, cluster_id: &ClusterId, group_id: &str, partitions: usize) {
        self.bus().publish(AppEvent::GroupChanged {
            cluster_id: cluster_id.to_string(),
            group_id: group_id.to_string(),
            partitions,
        });
    }
}

/// 空 group id 属于参数错误，先于连接检查，避免离线时给出误导性的 `not_connected`。
fn require_group_id(group_id: &str) -> Result<(), ServiceError> {
    if group_id.trim().is_empty() {
        return Err(ServiceError::Driver(DriverError::invalid(
            "groupId",
            "group id must not be empty",
        )));
    }
    Ok(())
}
