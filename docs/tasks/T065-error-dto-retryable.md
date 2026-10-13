# T065 · 修 `ErrorDto::new` 硬编码 `retryable: false`（与 `is_retryable` 分类漂移）

**状态**: ✅完成 | **依赖**: T057（编译打通后才现形） | **改动上限**: 3 文件 / 250 行

## 目标

`tests/bus.rs:150`（`error_event_keeps_the_dto_shape_the_ui_expects`）失败：
`json["payload"]["error"]["retryable"]` 得到 `false`，期望 `true`。

根因：`crates/kafka-core/src/protocol.rs:91-99` 的 `ErrorDto::new(kind, message)`
把 `retryable` **写死为 `false`**，而 `ErrorKind::NotConnected` / `Timeout` /
`Connection` / `Broker` 按 `DriverError::is_retryable()`（`error.rs:173`）是可重试的。
`from_driver()` 走的是 `error.is_retryable()`，所以同一个错误码经两条路径进 wire 会给出
**相反**的 `retryable`（`protocol.rs:222` 的测试就是 `true`）。前端据此决定
「重试按钮」是否可用 → 这是可观测的行为缺陷，不是测试写错。

## 权威与取舍

- 细粒度分类的真源是 `DriverError::is_retryable()`（它区分 `Rebalance`）。
- `ErrorKind` 比 `DriverError` 粗：`Rebalance(_) → ErrorKind::Group`，而
  `GroupNotFound(_) → ErrorKind::Group` 也映射到 `Group`。
  → **不能**把 `Group` 整体算作可重试，否则「组不存在」会被前端建议重试。
- 因此 `ErrorKind::is_retryable()` 取**公共可重试子集**
  `{NotConnected, Connection, Timeout, Broker}`，
  再让 `DriverError::is_retryable()` = `self.kind().is_retryable() || Rebalance`
  —— 两套分类收敛成一个来源，且**行为集合与改动前完全一致**（不碰 driver_guard 的语义）。

## 要改的文件

- 修改 `crates/kafka-core/src/error.rs`（新增 `ErrorKind::is_retryable()`；
  `DriverError::is_retryable()` 改为委托 + `Rebalance` 例外）
- 修改 `crates/kafka-core/src/protocol.rs`（`ErrorDto::new` 用 `kind.is_retryable()`；
  新增内联测试 `error_dto_new_uses_the_retry_classification`）

## 必须覆盖

| # | 位置 | 现象 | 修法 |
|---|---|---|---|
| 1 | `protocol.rs:95` | `retryable: false` 写死 | `retryable: kind.is_retryable()` |
| 2 | `error.rs:173` | 分类只存在于 `DriverError` | 新增 `ErrorKind::is_retryable()`，`DriverError` 委托它 |
| 3 | `protocol.rs` tests | 无回归锁 | 内联测试：`NotConnected/Timeout/Connection/Broker → true`，`InvalidConfig/NotFound/Storage/Auth → false` |

## 明确不做（防止范围膨胀）

- 不改 `ErrorKind` 的 variant 集合，也不改 `kind()` 映射（`Rebalance → Group` 保持）
  → 若要拆出 `ErrorKind::Rebalance`，那是 T081（错误映射审计）的范围
- 不改 `driver_guard` 的重试策略与 `Idem` 判定
- 不给 `ErrorDto::new` 增加 `retryable` 参数（调用点 4 处，语义上 kind 已足够决定）

## 验收

- `cargo test -p kafka-core` 全绿（含新内联测试）
- `cargo test -p kafka-app --test bus` 全绿
- `cargo test -p kafka-app --test driver_guard` 全绿（证明重试语义未变）

## 结果（完成后填写，≤5 行）
- 改了 2 个文件：`kafka-core/src/error.rs`（新增 `ErrorKind::is_retryable()` =
  {NotConnected, Connection, Timeout, Broker}；`DriverError::is_retryable()` 改为委托 + `Rebalance` 例外）
  + `kafka-core/src/protocol.rs`（`ErrorDto::new` 用 `kind.is_retryable()`；新增内联回归测试）。
- RED→GREEN：`error_dto_new_uses_the_retry_classification` 先 FAIL（protocol.rs:234）→ 修后 `cargo test -p kafka-core` 5/5。
- 验收：`--test bus` 9/9、`--test driver_guard` 15/15（重试语义未变，集合与改前相同）。
- 遗留（不在本卡）：`Rebalance` 与 `GroupNotFound` 共用 `ErrorKind::Group` → 拆细属 T081。
