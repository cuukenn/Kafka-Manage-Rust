# T017 · Mock 数据落盘（snapshot JSON 保存/恢复）

**状态**: ⬜未开始 | **依赖**: T002 | **改动上限**: 3 文件 / 250 行

## 目标
让 Mock 测试数据重启后还在（需求 2.2「Mock 数据持久化」），先做文件 JSON，不引入 SQLite。

## 只读白名单（最多 3 个）
- `crates/kafka-driver/src/mock/driver.rs` — 已有 `snapshot_json()` / `restore_json()` / `reset()` / `seed()`
- `crates/kafka-driver/src/mock/state.rs` — `MockState` 是否 `Serialize/Deserialize`
- `crates/kafka-core/src/cluster.rs` — `ClusterConfig`（决定持久化路径放哪）

## 要改的文件
- 新建 `crates/kafka-driver/src/mock/persist.rs`：`MockPersistence { path }`，提供 `save(&MockState)` / `load()` / `clear()`，原子写（先写 `.tmp` 再 rename）
- 修改 `crates/kafka-driver/src/mock/mod.rs`：`pub mod persist;`
- 新建 `crates/kafka-driver/tests/mock_persist.rs`

## 必须覆盖
- save → 新进程内 load → topic/消息/位点完全一致
- 文件不存在 → `load()` 返回 `Ok(None)`（不是错误）
- 文件损坏（写垃圾） → 返回 `DriverError` 且带可读 message，不 panic
- 原子写：保存过程中不产生半截文件（断言最终只有目标文件，`.tmp` 已清理）

## 明确不做
- 不做 SQLite 持久化、不做自动定时保存（业务层 T022/T024 再接线）
- 不改 `MockKafkaDriver` 的 trait 行为

## 验收
- `cargo test -p kafka-driver --test mock_persist` 全绿（用 `tempfile`，需加到 `kafka-driver` 的 dev-dependencies）
- `cargo clippy -p kafka-driver` 无新增告警

## 结果（完成后填写，≤5 行）
- 新增 `mock/persist.rs`：`MockPersistence { path }` 提供 `save`（`.tmp` + rename 原子写）/ `load`（缺文件 `Ok(None)`，损坏 → `DriverError::other` 可读 message）/ `clear`（幂等）。
- 新增 `tests/mock_persist.rs`（5 测试）：全新 driver 实例 round-trip 全等、缺文件 `Ok(None)`、垃圾文件 typed error、无 `.tmp` 残留 + 覆写、不存在目录保存不 panic。
- `tempfile = "3"` 加入根 workspace.dependencies + kafka-driver dev-deps。
- `cargo test -p kafka-driver` 62/62 全绿；`cargo clippy -p kafka-driver --all-targets -- -D warnings` EXIT=0。