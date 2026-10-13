# T025 · TopicService

**状态**: 🔵代码完成，待编译验证 | **依赖**: T024 | **改动上限**: 2 文件 / 200 行（实际 3 文件，见结果）

## 目标
topic 相关业务的薄封装：调用驱动 + 发事件 + 简单缓存。

## 只读白名单（最多 3 个）
- `crates/kafka-app/src/services/clusters.rs`（拿 `driver(id)` 的方式）
- `crates/kafka-core/src/topic.rs`（请求/响应类型）
- `crates/kafka-driver/src/driver.rs`（方法签名）

## 要改的文件
- 新建 `crates/kafka-app/src/services/topics.rs`
- 修改 `crates/kafka-app/src/services/mod.rs`

## 必须覆盖
- `list` / `describe` / `create` / `delete` / `update_config` / `offsets` 六个方法，参数含 `ClusterId`
- 写操作成功后：清缓存 + 发 `AppEvent::TopicChanged`
- 缓存：`list` 结果按集群缓存，TTL 由参数给（默认 0 = 不缓存），`invalidate(cluster_id)`
- 未连接 → 直接 `NotConnected`，不发事件
- 事件里带 topic 名，前端可局部刷新

## 明确不做
- 不做分页/排序（前端做）
- 不做重试（T029）

## 验收
- 新建 `crates/kafka-app/tests/svc_topics.rs`：六个方法 + 事件断言 + 缓存失效断言
- `cargo test -p kafka-app --test svc_topics` 全绿

## 结果（完成后填写，≤5 行）
- 产出 3 文件：`services/topics.rs`(~185)、`services/mod.rs`(+4：`pub mod topics;` + `pub use clusters::{ClusterService, ClusterView}; pub use topics::{NO_CACHE, TopicService};`)、`tests/svc_topics.rs`(~300，8 测试)。**偏离 2 文件/200 行上限**。状态：**代码完成，未编译验证**。
- 六个方法：`list/describe/offsets`（只读，不发事件）、`create/delete/update_config`（成功后清缓存 + 发 `TopicChanged`）；全部先 `clusters.driver(id)?` → 未连接直接 `NotConnected` 且零事件。
- 缓存：只缓存 `list`，`ttl == NO_CACHE(Duration::ZERO)` 即完全关闭；`cached_topics()` 顺手删过期条目；`invalidate/invalidate_all`；写操作成功才失效，**失败保留旧缓存且不发事件**（有测试覆盖）。
- `delete` 按驱动逐条结果发事件：`TopicDeleteResult.error == None` 才发 `Deleted`（mock 对未知 topic 返回 `error: Some("unknown topic …")`）。
- 遗留：驱动自身产生的变更（mock pump / 外部客户端）仍靠 T036 事件泵把 `DriverEvent` 转成 `TopicChanged`，届时也应顺带 `invalidate`；缓存目前不区分 internal topic 过滤条件（`describe` 的 `includeInternal` 不进缓存，因为只缓存 `list`）。