# T078 · Mock `rng()` 未被 seed 固定（每次都混入 `now_ms`）

**状态**: ✅完成 | **依赖**: T015 | **改动上限**: 3 文件 / 250 行

## 目标
`MockKafkaDriver::rng()`（`crates/kafka-driver/src/mock/driver.rs` ~:133-136，本批 C3 之前为 ~:126-129）
每次调用都做 `Rng::new(counter ^ (now_ms() as u64).rotate_left(17))`——`seeded()`/`request_seed()`
只决定是否加载夹具，并不给 roll 播种。T015 的「固定 seed」验收因此只靠 0.0/1.0 两个极端概率
才能通过；任何 0<p<1 的未来测试（阶段 5 的 T062 重连退避、故障抖动）按构造就是非确定的。
修法：加一个 seed-pinning 入口（setter），pin 住后跳过 `now_ms()` 混入，让同一概率序列可复现；
未 pin 时保持现状。

## 只读白名单（最多 3 个）
- `crates/kafka-driver/src/mock/driver.rs` — `rng()`、`seed` 字段、`seeded()`/`request_seed()`
- `crates/kafka-driver/src/mock/fault.rs` — `Rng`、`FaultInjection::check`/`latency_ms` 的概率路径
- `crates/kafka-driver/tests/mock_faults.rs` — 现有「固定 seed」测试的写法与 0.0/1.0 极端断言

## 要改的文件
- 修改 `crates/kafka-driver/src/mock/driver.rs`（pin 入口，跳过 now_ms 混入）
- 修改 `crates/kafka-driver/tests/mock_faults.rs`（或新建测试）

## 必须覆盖
- pin 后两次独立驱动实例（同 seed）得到完全相同的多次 roll 序列
- 0<p<1 的中段概率故障序列在 pin 下可复现（证明不再是「碰运气」）
- 未 pin 时行为与现状一致（不破坏既有测试）

## 明确不做（防止范围膨胀）
- 不改 `FaultInjection` 的概率语义与字段含义
- 不动 `restore_json`/快照范围（那是 T079）

## 验收
- `cargo test -p kafka-driver` 全绿
- 新增测试证明：pinned seed 下 0<p<1 的故障序列可复现
- `cargo clippy -p kafka-driver --all-targets -- -D warnings` 零告警

## 结果（完成后填写，≤5 行）
- `MockKafkaDriver::pin_rng_seed(seed)`：重置 roll 计数器 + `rng_pinned` 置位；`rng()` pin 时只用计数器派生，跳过 `now_ms` 混入；未 pin 路径逐字节不变。
- 新增 2 测试（`mock_faults.rs`）：p=0.5 中段失败序列、1..=49ms 延迟抖动序列——各在两个同 seed 独立实例上完全复现，且断言两种结果都出现（非极端值）。
- RED 证据：pin 之前 3ms 时钟间隔即让两次同调度的 0.5 序列分叉（`EQUAL=false`）；未 pin 的 scratch 复跑 fix 后仍分叉 → 未 pin 行为不变。
- `cargo test -p kafka-driver` 68/68 全绿；`cargo clippy -p kafka-driver --all-targets -- -D warnings` 零告警。
- 未动 `FaultInjection` 语义、`restore_json`/快照范围（T079）。
