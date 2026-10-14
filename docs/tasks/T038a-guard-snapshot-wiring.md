# T038a — GuardedDriver 接入 + as_any 转发 + Mock 快照存取（SQLite slot）

状态: ⏸　依赖: T084, T085　验证: `cargo test -p kafka-app` + `cargo test --workspace`　类型: 自动

拆分裁定 R10: 原 T038 拆为 T038a(本卡,kafka-app 接线,自动验证) + T038b(main.rs 装配,手动)。

## 目标

1. `ClusterService::connect` 用 `GuardedDriver` 包裹 `driver_for` 产出的驱动（超时/重试护栏真正生效）。
2. `GuardedDriver` 转发 `as_any` 到内层驱动（T084 接缝；否则护栏后 `mock.*` 全部失效）。
3. Mock 快照按 T079 裁定接 SQLite slot：connect 前恢复、disconnect 前保存。

## 只读白名单

读改: `crates/kafka-app/src/driver_guard.rs`、`crates/kafka-app/src/services/clusters.rs`、`crates/kafka-app/tests/*`（新文件 `guard_snapshot.rs` 或扩展现有）。
只读: `kafka-driver/src/driver.rs`、`kafka-driver/src/mock/driver.rs`、`kafka-app/src/store/settings.rs`、`kafka-app/src/lib.rs`。禁止改其他文件。

## 预先钉死的接口事实

- `GuardedDriver`（driver_guard.rs:146）：`impl KafkaDriver for GuardedDriver`（:273）目前**未**覆写 `as_any`，走 trait 默认 `None`。构造：`GuardedDriver::with_policy(driver, policy)`（:162 区）；若无 Default policy 构造则用 `RetryPolicy::default()`（核实源码）。
- `MockKafkaDriver::snapshot_json(&self) -> Result<String, DriverError>`（async，mock/driver.rs:115）；`restore_json(&self, json) -> Result<(), DriverError>`（async，:129）。restore 只换数据、不伪造 connected（T079）。
- Store 快照 API（settings.rs:153-158，同步）：`save_mock_snapshot(&ClusterId, &str)` / `load_mock_snapshot(&ClusterId) -> Option<String>` / `clear_mock_snapshot`。服务层走 `store_task(&self.store, …)`（clusters.rs 已有用法，spawn_blocking）。
- `connect`（clusters.rs:195）：`let driver = driver_for(&cfg);`（:219）→ `driver.connect(&cfg).await`。`disconnect`（:237）。

## 行为契约（必须覆盖）

1. `GuardedDriver::as_any` → `self.inner.as_any()`（转发内层）。
2. connect：`driver_for` 后先包 GuardedDriver；若 `as_any` 下降到 `MockKafkaDriver`：`load_mock_snapshot` 有值 → `restore_json`（失败仅 warn，继续空态连接）；**在 `driver.connect` 之前**恢复。
3. disconnect：清槽/断开前，若 mock → `snapshot_json` → `save_mock_snapshot`（失败仅 warn，不使 disconnect 失败）。
4. 快照存取走 `store_task`（spawn_blocking），不阻塞异步。

## 测试（≥4，新测试文件）

1. 护栏转发：connect 后 `clusters.driver(id)` 下降 `as_any` 达 `MockKafkaDriver`（如 `faults_handle().write()` 可触达）。
2. 恢复：预存含 topic 的快照 JSON → connect → 经 as_any 读驱动状态见快照 topic。
3. 保存：connect → mock.seed（经接缝）→ disconnect → `load_mock_snapshot` 有值且含 seed 的 topic。
4. 坏 JSON 快照 → connect 仍成功（warn），驱动为空态 mock。
5. 既有全绿：`cargo test --workspace`（护栏包裹不得破坏 webview-bridge 路由 e2e 与 app_smoke）。

## 验证命令

`cargo test -p kafka-app`；`cargo test --workspace`；`cargo clippy -p kafka-app --all-targets -- -D warnings`；rustfmt 仅本卡改动文件（逐文件）。

预算（R8 口径）：2 个代码文件 + 测试文件豁免。
