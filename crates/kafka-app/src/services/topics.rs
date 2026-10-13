//! Topic 业务薄封装：调用驱动 → 成功后清缓存 + 发 `TopicChanged`。
//!
//! 缓存只覆盖 `list`（列表页最频繁、代价最高），TTL 由构造参数决定，
//! `Duration::ZERO` 表示完全不缓存。任何写操作成功后都会让该集群的缓存失效，
//! 所以前端收到事件后重新 `list` 一定能看到最新结果。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kafka_core::ClusterId;
use kafka_core::topic::{
    CreateTopicReq, DeleteTopicsReq, DescribeTopicsReq, ListOffsetsReq, OffsetRange,
    TopicDeleteResult, TopicDetail, TopicInfo, UpdateTopicConfigReq,
};

use super::ServiceError;
use super::clusters::ClusterService;
use crate::bus::{AppEvent, EventBus, TopicChange};

/// 关闭列表缓存的 TTL 取值。
pub const NO_CACHE: Duration = Duration::ZERO;

#[derive(Debug, Clone)]
pub struct TopicService {
    clusters: ClusterService,
    ttl: Duration,
    cache: Arc<Mutex<HashMap<ClusterId, CachedList>>>,
}

#[derive(Debug, Clone)]
struct CachedList {
    fetched_at: Instant,
    topics: Vec<TopicInfo>,
}

impl TopicService {
    /// `ttl` 为 `NO_CACHE` 时每次 `list` 都直接打驱动。
    pub fn new(clusters: ClusterService, ttl: Duration) -> Self {
        Self {
            clusters,
            ttl,
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn clusters(&self) -> &ClusterService {
        &self.clusters
    }

    pub fn bus(&self) -> &EventBus {
        self.clusters.bus()
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// 让某个集群的列表缓存失效（下次 `list` 必然回源）。
    pub fn invalidate(&self, cluster_id: &ClusterId) {
        let mut guard = lock_cache(&self.cache);
        guard.remove(cluster_id);
    }

    pub fn invalidate_all(&self) {
        let mut guard = lock_cache(&self.cache);
        guard.clear();
    }

    /// 当前仍有效（未过期）的缓存副本；过期条目会被顺手删掉。
    pub fn cached_topics(&self, cluster_id: &ClusterId) -> Option<Vec<TopicInfo>> {
        if self.ttl == NO_CACHE {
            return None;
        }
        let mut guard = lock_cache(&self.cache);
        match guard.get(cluster_id) {
            Some(entry) if entry.fetched_at.elapsed() < self.ttl => Some(entry.topics.clone()),
            Some(_) => {
                guard.remove(cluster_id);
                None
            }
            None => None,
        }
    }

    // ---- 读 --------------------------------------------------------------

    /// 列出 topic。命中未过期缓存时不打驱动。
    pub async fn list(&self, cluster_id: &ClusterId) -> Result<Vec<TopicInfo>, ServiceError> {
        if let Some(cached) = self.cached_topics(cluster_id) {
            return Ok(cached);
        }
        let driver = self.clusters.driver(cluster_id)?;
        let topics = driver.list_topics().await?;
        if self.ttl != NO_CACHE {
            let mut guard = lock_cache(&self.cache);
            guard.insert(
                cluster_id.clone(),
                CachedList {
                    fetched_at: Instant::now(),
                    topics: topics.clone(),
                },
            );
        }
        Ok(topics)
    }

    pub async fn describe(
        &self,
        cluster_id: &ClusterId,
        req: &DescribeTopicsReq,
    ) -> Result<Vec<TopicDetail>, ServiceError> {
        let driver = self.clusters.driver(cluster_id)?;
        Ok(driver.describe_topics(req).await?)
    }

    pub async fn offsets(
        &self,
        cluster_id: &ClusterId,
        req: &ListOffsetsReq,
    ) -> Result<Vec<OffsetRange>, ServiceError> {
        let driver = self.clusters.driver(cluster_id)?;
        Ok(driver.list_offsets(req).await?)
    }

    // ---- 写（成功才发事件）------------------------------------------------

    pub async fn create(
        &self,
        cluster_id: &ClusterId,
        req: &CreateTopicReq,
    ) -> Result<(), ServiceError> {
        let driver = self.clusters.driver(cluster_id)?;
        driver.create_topic(req).await?;
        self.invalidate(cluster_id);
        self.publish_change(cluster_id, &req.name, TopicChange::Created);
        Ok(())
    }

    /// 逐个 topic 发事件；驱动报告失败的条目不发。
    pub async fn delete(
        &self,
        cluster_id: &ClusterId,
        req: &DeleteTopicsReq,
    ) -> Result<Vec<TopicDeleteResult>, ServiceError> {
        let driver = self.clusters.driver(cluster_id)?;
        let results = driver.delete_topics(req).await?;
        let removed: Vec<String> = results
            .iter()
            .filter(|r| r.error.is_none())
            .map(|r| r.name.clone())
            .collect();
        if !removed.is_empty() {
            self.invalidate(cluster_id);
            for name in &removed {
                self.publish_change(cluster_id, name, TopicChange::Deleted);
            }
        }
        Ok(results)
    }

    pub async fn update_config(
        &self,
        cluster_id: &ClusterId,
        req: &UpdateTopicConfigReq,
    ) -> Result<(), ServiceError> {
        let driver = self.clusters.driver(cluster_id)?;
        driver.update_topic_config(req).await?;
        self.invalidate(cluster_id);
        self.publish_change(cluster_id, &req.topic, TopicChange::ConfigChanged);
        Ok(())
    }

    // ---- 内部 ------------------------------------------------------------

    fn publish_change(&self, cluster_id: &ClusterId, topic: &str, change: TopicChange) {
        self.clusters.bus().publish(AppEvent::TopicChanged {
            cluster_id: cluster_id.to_string(),
            topic: topic.to_string(),
            change,
        });
    }
}

/// 缓存锁中毒时继续用（条目只是 `Vec<TopicInfo>`，不存在半改状态）。
fn lock_cache(
    cache: &Arc<Mutex<HashMap<ClusterId, CachedList>>>,
) -> std::sync::MutexGuard<'_, HashMap<ClusterId, CachedList>> {
    cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}
