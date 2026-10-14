# T085 · Router：`mock.*`（reset / seed / faults）

**状态**: ✅完成 | **依赖**: T084 | **改动上限**: 3 文件 / 250 行

## 目标

注册 3 个 `mock.*` 命令，经 T084 的 `as_any` 接缝 downcast 到 `MockKafkaDriver`
操作其控制面。给开发/演示用的 Mock 开关一个 wire 入口。

## 只读白名单（最多 3 个源文件，别读更多）

- `crates/webview-bridge/src/handlers.rs` — helper 与 handler 模板（T033 产物）
- `crates/kafka-driver/src/mock/driver.rs` — `reset()` / `pin_rng_seed()` / `faults_handle()` / `faults()`
- `crates/kafka-driver/src/mock/fault.rs` — `FaultInjection` 字段（camelCase serde + Default）

## 预先钉死的接口事实（不必去读其它文件）

- cmd 常量：`MOCK_RESET`（`mock.reset`）`MOCK_SEED`（`mock.seed`）`MOCK_FAULTS`（`mock.faults`）。
- `ClusterService::driver(&ClusterId) -> Result<DynDriver, ServiceError>` 拿活驱动；
  `driver.as_any().and_then(|a| a.downcast_ref::<MockKafkaDriver>())` 拿 mock 控制面；
  downcast 失败（真实驱动）→ `ErrorKind::Unsupported`，message 带 clusterId。
- 控制面：
  - `async reset()` → data=null
  - `pin_rng_seed(seed: u64)`（同步）→ data=null；payload `{clusterId, seed}`
  - `mock.faults`：payload `{clusterId}` → 读：`faults().await` → data=FaultInjection（camelCase）；
    payload `{clusterId, injection:{...FaultInjection 字段}}` → 写：`*faults_handle().write() = injection` → data=写入后的读回。
- `FaultInjection` 是 `Serialize + Deserialize + Default`（camelCase），可直接进 payload。
- mock 集群夹具同 T033。

## 要改的文件

- 新建 `crates/webview-bridge/src/handlers_mock.rs` — `register_mock(app: &kafka_app::App, router: &Router)`
- 新建 `crates/webview-bridge/tests/router_mock.rs`
- 修改 `crates/webview-bridge/src/lib.rs` — 加 mod

## 必须覆盖

1. 3 个命令全部用 `cmd::` 常量注册。
2. 端到端（经 `router.handle`）：connect mock 集群 → `mock.faults` 读默认（`failConnect:false` 等）→
   `mock.faults` 写 `{injection:{failProbability:1.0, failMessage:"boom"}}` → 读回一致 →
   `produce.send` 得到注入错误（响应 ok:true 但 `errorCount>0`，或错误响应——以护栏语义为准，断言 `results[i].error` 含 "boom"）→
   `mock.reset` → 再读 faults 回默认。
3. `mock.seed`：`{clusterId, seed:42}` → data=null；再 seed 一次不同值也 ok（幂等覆盖）。
4. 错误路径：未连接 → `not_connected`；payload 缺 seed → `invalid_config`。
5. 测试数 ≥ 4。

## 明确不做（防止范围膨胀）

- 不做 snapshot 落盘接线（T038）；不改 kafka-driver（接缝已由 T084 提供）；不做事件泵。

## 验收

- `cargo test -p webview-bridge` 全绿（新增测试 ≥ 4）
- `cargo clippy -p webview-bridge --all-targets -- -D warnings` 零告警
- `cargo check --workspace` 通过

## 结果（完成后填写，≤5 行）
- 新建 `handlers_mock.rs`（108 行）+ `tests/router_mock.rs`（4 个 e2e 测试），`lib.rs` 加 mod；`handlers.rs` 未动。
- `handler()` 裁定：**保留**——`register_with`（handlers.rs:41）实际在调用它，T033 评审「无调用者」不实。
- 验证：`cargo test -p webview-bridge` 40 全绿（36 旧 + 4 新）；clippy `--all-targets -D warnings` 零告警；`cargo check --workspace` 通过。
- 偏差：`mock.reset` 会清空 topics，测试在 reset 后重建 topic 再验证连接存活；`FaultsPayload` 需 `Default` derive（serde 容器级 default 要求）。
