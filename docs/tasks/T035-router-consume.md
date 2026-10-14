# T035 · Router：`consume.*`

**状态**: ✅完成 | **依赖**: T033 | **改动上限**: 3 文件 / 250 行

## 目标

注册 6 个 `consume.*` 命令（subscribe/unsubscribe/pause/resume/seek/list），
复用 T033 `handlers.rs` 的 helper 与 handler 模板，接 `ConsumeService`。

## 只读白名单（最多 3 个源文件，别读更多）

- `crates/webview-bridge/src/handlers.rs` — helper 与 handler 模板（T033 产物）
- `crates/kafka-app/src/services/consume.rs` — ConsumeService 方法签名 + `SubscriptionInfo`（Serialize camelCase）
- `crates/kafka-core/src/message.rs` — `SubscribeReq` / `SubscriptionId` 字段

## 预先钉死的接口事实（不必去读其它文件）

- cmd 常量：`CONSUBSCRIBE_START`（wire `consume.subscribe`）`CONSUBSCRIBE_STOP`（wire `consume.unsubscribe`）
  `CONSUME_PAUSE` `CONSUME_RESUME` `CONSUME_SEEK` `CONSUME_LIST`。
- ConsumeService（`app.consume()`，注意它 **不是** `Clone` 之外——它是 `#[derive(Debug)]` + 手写 `Clone`，可 clone）：
  - `async start(&ClusterId, SubscribeReq) -> Result<SubscriptionInfo, ServiceError>`
  - `async stop(&SubscriptionId) -> Result<bool, ServiceError>`（幂等：第二次 `false`）
  - `pause(&SubscriptionId) / resume(&SubscriptionId) -> Result<(), ServiceError>`（同步）
  - `seek(&SubscriptionId, topic: impl Into<String>, partition: u32, offset: i64) -> Result<(), ServiceError>`（同步）
  - `list() -> Vec<SubscriptionInfo>`（同步）
- `SubscribeReq`（camelCase serde + Default）：`subscriptionId`（缺省自动生成 UUID）、`topic`、`partitions`、
  `start`、`follow`、`maxMessages`、`filter`、`maxBytesPerPartition`。
- payload 形状：
  - `consume.subscribe`：`{clusterId, ...SubscribeReq 字段}`（局部 struct flatten）→ data=SubscriptionInfo
  - `consume.unsubscribe`：`{subscriptionId}` → data=bool
  - `consume.pause/resume`：`{subscriptionId}` → data=null
  - `consume.seek`：`{subscriptionId, topic, partition, offset}` → data=null
  - `consume.list`：`{}` → data=[SubscriptionInfo]
- `SubscriptionId` 是 `pub struct SubscriptionId(pub String)`，serde transparent。
- mock 集群夹具同 T033。

## 要改的文件

- 新建 `crates/webview-bridge/src/handlers_consume.rs` — `register_consume(app: &kafka_app::App, router: &Router)`
- 新建 `crates/webview-bridge/tests/router_consume.rs`
- 修改 `crates/webview-bridge/src/lib.rs` — 加 mod

## 必须覆盖

1. 6 个命令全部用 `cmd::` 常量注册。
2. 端到端（经 `router.handle`）：connect mock 集群 → `topic.create` + `produce.send` 造数据 →
   `consume.subscribe`（`follow:false`, `start:{"type":"earliest"}`（adjacently-tagged 实际 wire 形状，见下方偏差）, `maxMessages` 有界）→ data 里 `subscriptionId` 非空、
   `state` 为 running → 等消息经总线送达（`app.bus().subscribe()` 收 `MessageReceived`，或轮询
   `consume.list` 的 `delivered`）→ `consume.unsubscribe` → `true`，再 unsubscribe → `false`。
3. `consume.pause` → `state` 变 `paused`（用 `consume.list` 断言）；`consume.resume` → `running`；
   `consume.seek` 对运行中订阅 ok（data:null）。
4. 错误路径：未知 subscriptionId 的 pause → 错误响应（kind 以 `running_handle` 实际语义为准，断言 `ok:false` 即可）；
   未连接集群 subscribe → `not_connected`；`consume.subscribe` 空 topic → `invalid_config`。
5. 测试数 ≥ 6；涉及等待的用 `tokio::time::timeout` 兜底，不许裸 sleep 死等。

## 明确不做（防止范围膨胀）

- 不做节流/背压（T060/T061）；不做事件泵（T037）；不接 group/mock；不修改 kafka-app / kafka-core。

## 验收

- `cargo test -p webview-bridge` 全绿（新增测试 ≥ 6）
- `cargo clippy -p webview-bridge --all-targets -- -D warnings` 零告警
- `cargo check --workspace` 通过

## 结果（完成后填写，≤5 行）
- 新建 `handlers_consume.rs`（`register_consume`，6 命令全部 `cmd::` 常量）+ `tests/router_consume.rs`（6 个 e2e）；`lib.rs` 仅加 mod。
- `SubscribePayload{clusterId, #[serde(flatten)] SubscribeReq}`：缺省 `subscriptionId` 由 `SubscribeReq::default()` 生成 UUID（R6），桥接不造 id。
- 偏差：`OffsetStart` 是 adjacently-tagged serde，wire 实为 `{"type":"earliest"}` 而非卡面的 `start:"earliest"`（不改 kafka-core，测试按实际形状）。
- 验证：`cargo test -p webview-bridge` 29 通过（23 旧 + 6 新）；clippy `--all-targets -D warnings` 零告警；`cargo check --workspace` 通过。
