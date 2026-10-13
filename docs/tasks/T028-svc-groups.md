# T028 · GroupService

**状态**: 🔵代码完成，待编译验证 | **依赖**: T024 | **改动上限**: 2 文件 / 200 行（实际 3 文件，见结果）

## 目标
消费组列表/详情/位点/重置的业务封装 + 事件。

## 只读白名单（最多 3 个）
- `crates/kafka-core/src/group.rs`
- `crates/kafka-app/src/services/clusters.rs`
- `crates/kafka-core/src/protocol.rs` — `event::GROUP_CHANGED`

## 要改的文件
- 新建 `crates/kafka-app/src/services/groups.rs`
- 修改 `crates/kafka-app/src/services/mod.rs`

## 必须覆盖
- `list` / `describe` / `offsets` / `reset`
- `reset` 成功后发 `AppEvent::GroupChanged`，并把 `ResetOffsetsResult` 原样返回给前端
- `reset` 的 `dry_run` 透传：不发事件、不落库
- 未连接 → `NotConnected`
- 空 group id → `InvalidConfig`

## 明确不做
- 不做「删除消费组」（真实 Kafka 语义复杂，二期）
- 不做 lag 计算缓存

## 验收
- 新建 `crates/kafka-app/tests/svc_groups.rs`
- `cargo test -p kafka-app --test svc_groups` 全绿

## 结果（完成后填写，≤5 行）
- 产出 3 文件：`services/groups.rs`(~105)、`services/mod.rs`(+2 re-export)、`tests/svc_groups.rs`(~330，8 测试)。**偏离 2 文件/200 行上限**（测试文件独立计入）。状态：**代码完成，未编译验证**（T001 未完成）。
- API：`list` / `describe(ids)`（空 ids = 全部）/ `offsets` / `reset`，全部先 `clusters.driver(id)?`；`reset` 把 `ResetOffsetsResult` 原样返回给前端。
- 事件：仅 `!dry_run && result.error.is_none()` 时发一次 `AppEvent::GroupChanged { cluster_id, group_id, partitions: applied.len() }`；`dry_run` 与失败路径（`OffsetOutOfRange` / `PartitionNotFound` / `GroupNotFound`）零事件且位点不变（测试用 `committed_offsets()` 复核）。
- 校验顺序：空/空白 group id 先于连接检查 → `InvalidConfig`（离线时不会误报 `not_connected`）；`describe` 逐个校验 ids。
- 遗留：未加 `commit_offsets` 服务方法（`cmd::group` 无对应命令；驱动侧 `OffsetsCommitted` 事件留给 T036 事件泵转 `GroupChanged`）；lag 缓存按卡片明确不做。