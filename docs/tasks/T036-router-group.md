# T036 · Router：`group.*`（mock.* 已拆分为 T084/T085）

**状态**: ⬜未开始 | **依赖**: T033 | **改动上限**: 3 文件 / 250 行

## 目标

注册 4 个 `group.*` 命令（list/describe/offsets/resetOffsets），复用 T033 helper 与模板，
接 `GroupService`。
（裁定：原卡片的 `mock.*` 拆为 T084（kafka-driver 下行接口）+ T085（桥接 mock.* handler），
因为 trait 改动落在 kafka-driver，与本卡 3 文件上限冲突。）

## 只读白名单（最多 3 个源文件，别读更多）

- `crates/webview-bridge/src/handlers.rs` — helper 与 handler 模板（T033 产物）
- `crates/kafka-app/src/services/groups.rs` — GroupService 方法签名
- `crates/kafka-core/src/group.rs` — `GroupInfo/GroupDetail/ListGroupOffsetsReq/ResetOffsetsReq/ResetOffsetsResult` 字段

## 预先钉死的接口事实（不必去读其它文件）

- cmd 常量：`GROUP_LIST` `GROUP_DESCRIBE` `GROUP_OFFSETS` `GROUP_RESET_OFFSETS`（wire `group.resetOffsets`）。
- GroupService（`app.groups()`，`Clone`）：
  - `async list(&ClusterId) -> Result<Vec<GroupInfo>, ServiceError>`
  - `async describe(&ClusterId, &[String]) -> Result<Vec<GroupDetail>, ServiceError>`（ids 空 = 全部）
  - `async offsets(&ClusterId, &ListGroupOffsetsReq) -> Result<Vec<GroupOffsetEntry>, ServiceError>`
  - `async reset(&ClusterId, &ResetOffsetsReq) -> Result<ResetOffsetsResult, ServiceError>`
- payload 形状（局部 struct flatten）：
  - `group.list`：`{clusterId}`
  - `group.describe`：`{clusterId, ids:[String]}`（ids 缺省 = 空数组）
  - `group.offsets`：`{clusterId, ...ListGroupOffsetsReq 字段}`（req 含 `groupId`、`withLag` 等，camelCase）
  - `group.resetOffsets`：`{clusterId, ...ResetOffsetsReq 字段}` → data=ResetOffsetsResult（camelCase Serialize）
- 空 groupId → `invalid_config`（service 先于连接检查，钉死断言）。
- mock 集群夹具同 T033。

## 要改的文件

- 新建 `crates/webview-bridge/src/handlers_group.rs` — `register_group(app: &kafka_app::App, router: &Router)`
- 新建 `crates/webview-bridge/tests/router_group.rs`
- 修改 `crates/webview-bridge/src/lib.rs` — 加 mod

## 必须覆盖

1. 4 个命令全部用 `cmd::` 常量注册。
2. 端到端（经 `router.handle`）：connect mock 集群 → `group.list` 返回（Mock 预置组，断言含预置 groupId）→
   `group.describe`（指定 ids）字段对 → `group.offsets`（`withLag:true`）有条目 →
   `group.resetOffsets`（`dryRun:false`）→ data `dryRun:false`、`applied` 非空。
3. `dryRun:true` 的 reset：data `dryRun:true`，且**不**发 `GROUP_CHANGED` 事件（用 bus 订阅断言）。
4. 错误路径：未连接 `group.list` → `not_connected`；空 groupId `group.offsets` → `invalid_config`；
   未知 groupId `group.describe` → 错误响应（kind 以驱动语义为准，断言 `ok:false`）。
5. 测试数 ≥ 5。

## 明确不做（防止范围膨胀）

- 不做 `mock.*`（T084/T085）；不做事件泵；不修改 kafka-app / kafka-core / kafka-driver。

## 验收

- `cargo test -p webview-bridge` 全绿（新增测试 ≥ 5）
- `cargo clippy -p webview-bridge --all-targets -- -D warnings` 零告警
- `cargo check --workspace` 通过

## 结果（完成后填写，≤5 行）
-
