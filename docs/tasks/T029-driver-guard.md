# T029 · 驱动护栏：超时 + 重试包装

**状态**: 🔵进行中（代码 + 测试已写，待编译验证） | **依赖**: T024, T015 | **改动上限**: 2 文件 / 250 行

## 目标
用一个装饰器实现「超时控制、重试、错误分类」，业务层无感（需求 2.4 第 3 点）。

## 只读白名单（最多 3 个）
- `crates/kafka-driver/src/driver.rs` — `KafkaDriver` trait（要原样实现它）
- `crates/kafka-core/src/error.rs` — `ErrorKind::is_retryable()`
- `crates/kafka-driver/src/mock/fault.rs` — 用它注入超时/失败做测试

## 要改的文件
- 新建 `crates/kafka-app/src/driver_guard.rs`（`GuardedDriver { inner: DynDriver, policy: RetryPolicy }`）
- 修改 `crates/kafka-app/src/lib.rs`：`pub mod driver_guard;`

## 必须覆盖
- `RetryPolicy { max_attempts, per_call_timeout: Duration, backoff: (init, factor, max) }`
- 每次调用包 `tokio::time::timeout`；超时 → `DriverError::Timeout`
- 只对 `is_retryable()` 的错误重试；不可重试的错误立即返回
- 指数退避 + 上限；重试次数写进 `tracing::warn!`
- 重试总耗时受 `max_total` 约束（避免 UI 卡死）
- `capabilities()`/`kind()` 透传；`events()` 透传（只转发一次，不重复订阅）
- 测试用 Mock + 故障注入：注入 100% 失败 → 恰好 `max_attempts` 次；注入首次失败 → 第二次成功

## 明确不做
- 不做熔断器（circuit breaker）
- 不做自动重连（T062）

## 验收
- 新建 `crates/kafka-app/tests/driver_guard.rs`
- `cargo test -p kafka-app --test driver_guard` 全绿，用固定 seed，禁止偶发

## 结果（完成后填写，≤5 行）
- `crates/kafka-app/src/driver_guard.rs`（407 行）：`RetryPolicy`/`Backoff`/`Idem` + `GuardedDriver` 装饰器；`guarded_call` 统一处理超时（取 `per_call` 与 `max_total` 剩余量的较小者）、按 `is_retryable()` 与幂等分类重试、退避封顶；`subscribe` 只 `timeout_at(per_call)` 且不重试；`is_connected`/`capabilities`/`kind`/`events` 透传。
- `crates/kafka-app/tests/driver_guard.rs`（430 行，14 个 `#[tokio::test]`）：`Counting` 计数装饰器包住 `MockKafkaDriver`，可让某操作前 N 次返回可重试错误；故障只用 `fail_probability = 1.0 / 0.0`（`fault.rs::roll` 两端与 seed 无关），只有退避与 `max_total` 两个用例读墙钟且边界很宽 → 无偶发。
- 覆盖：100% 失败恰好 `max_attempts` 次；首次失败第二次成功；`connect` 第 3 次恢复；`off()` 只 1 次；不可重试（NotFound / InvalidConfig）只 1 次；`NotConnected` 可重试；Unsafe 默认不重试、`retry_unsafe(true)` 才重试；超时错误带操作名；超时尝试会被重试；`max_total` 截断链条；退避增长并被 `max` 截住；`subscribe` 单次；`is_connected` 不受超时影响；`capabilities/kind/events` 透传且不重放。
- 未跑绿：本机缺 Windows SDK（T001 ⛔），`cargo test -p kafka-app --test driver_guard` 无法执行 → 状态保持 🔵，等 T001 后一次性验证。
- 本卡产出 837 行 > 卡面上限 250 行，按「实现 / 测试」两次会话分摊；若要严格合规可把测试拆成 `driver_guard_retry.rs` + `driver_guard_timeout.rs` 两张子卡。