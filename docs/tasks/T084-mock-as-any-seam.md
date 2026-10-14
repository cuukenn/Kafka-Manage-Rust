# T084 · Mock 控制接缝：`KafkaDriver::as_any`（为 T085 `mock.*` 铺路）

**状态**: ✅完成 | **依赖**: T032 | **改动上限**: 3 文件 / 250 行

## 背景（为什么拆这张卡）

T036 原计划同时接 `group.*` + `mock.*`。`mock.*`（reset/seed/faults）需要拿到
**具体类型** `MockKafkaDriver`（`reset()` / `pin_rng_seed()` / `faults_handle()` 都不在
`KafkaDriver` trait 上），而桥接层手里只有 `DynDriver = Arc<dyn KafkaDriver>`。
trait 改动落在 kafka-driver，与 T036 的 3 文件上限冲突 → 拆出本卡（控制器裁定，见 ledger）。

## 目标

给 `KafkaDriver` trait 加一个**默认实现**的下行接缝：

```rust
/// Mock 控制接缝：具体类型 downcast。真实驱动返回 `None`。
fn as_any(&self) -> Option<&dyn std::any::Any> { None }
```

- 默认 `None` → 真实驱动 / `PlaceholderRealDriver` / `GuardedDriver` 零改动即可编译。
- `MockKafkaDriver` 覆写为 `Some(self)`。
- `GuardedDriver`（kafka-app，目前只在测试里包装驱动）**本卡不改**；T038 接线护栏时再转发（登记在 ledger）。

## 只读白名单（最多 3 个源文件，别读更多）

- `crates/kafka-driver/src/driver.rs` — trait 定义 + `PlaceholderRealDriver` + `driver_for`
- `crates/kafka-driver/src/mock/driver.rs` — `MockKafkaDriver`（impl KafkaDriver 段落附近加覆写）
- `crates/kafka-app/src/driver_guard.rs` — 只确认 `GuardedDriver` 编译不受影响（不修改）

## 要改的文件

- 修改 `crates/kafka-driver/src/driver.rs` — trait 加默认方法（含 doc 注释）
- 修改 `crates/kafka-driver/src/mock/driver.rs` — 覆写 `as_any`
- 新建 `crates/kafka-driver/tests/conformance.rs` — 契约测试（见下）

## 必须覆盖

1. trait 默认方法 `as_any`，doc 注释写明用途与「真实驱动必须 None」。
2. `MockKafkaDriver` 覆写返回 `Some(self)`。
3. `tests/conformance.rs`（Mock/Real 语义对齐契约测试的家，后续卡片继续往里加）：
   - `driver_for(&mock_cfg)` 的 `DynDriver`：`as_any().and_then(|a| a.downcast_ref::<MockKafkaDriver>())` 是 `Some`；
   - `driver_for(&real_cfg)`（非 mock，feature off 时是 `PlaceholderRealDriver`）：`as_any()` 是 `None`；
   - 断言 downcast 后可调用 `faults_handle()`（证明接缝真的可达控制面）。
4. 不改任何现有行为：`cargo test -p kafka-driver` 原 69 个测试全绿 + 新契约测试。

## 明确不做（防止范围膨胀）

- 不改 `GuardedDriver`（T038 再转发）；不加其它 mock 方法到 trait；不接桥接 handler（T085）。

## 验收

- `cargo test -p kafka-driver` 全绿（69 + 新契约测试）
- `cargo clippy -p kafka-driver --all-targets -- -D warnings` 零告警
- `cargo check -p kafka-app` 通过（默认实现保证零改动可编译）

## 结果（完成后填写，≤5 行）
- `KafkaDriver::as_any` 默认 `None`（doc 注明真实驱动必须 `None`），`MockKafkaDriver` 覆写为 `Some(self)`；`GuardedDriver` 未改，转发留 T038。
- 新增 `crates/kafka-driver/tests/conformance.rs`（契约测试之家）：3 个 tokio 测试——mock 驱动可 downcast、非 mock 驱动 `as_any()` 为 `None`、downcast 后可取 `faults_handle()`。
- 验证：`cargo test -p kafka-driver` 72 passed（原 69 + 新 3）；`cargo clippy -p kafka-driver --all-targets -- -D warnings` 零告警；`cargo check -p kafka-app` 通过。
- Windows 未出现 LNK1104 / Access denied 抖动，无需 `--release` 复跑。
- 改动 3 个源文件（+11 / +7 / 新建 64 行）+ 2 个文档，未触碰 webview-bridge 与 driver_guard。
