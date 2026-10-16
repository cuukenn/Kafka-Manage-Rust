# T082a — 路由层全局超时

状态: ✅　依赖: T032,T029　验证: `cargo test -p webview-bridge --test global_deadline`　类型: 产品码

> 原 T082「UI 取消在途请求 + 全局超时」实测 **345 行/3 文件**，超出 250 行上限，按既有先例
> 拆成 **T082a（全局超时，本卡）**、**T082b（取消在途请求）** 与 **T082c（前端接线）**。

## 目标

`Router::dispatch` 现在 `tokio::spawn(handler).await` 无期限等待：某个 handler 卡住（驱动阻塞
在 `spawn_blocking` 里、SQLite 锁、librdkafka 迟迟不回调）时，桥接层的
`Handle::block_on` 会一直等下去，UI 的 pending 计数只增不减，用户没有任何出口。

本卡给每个请求加一个**全局截止时间**：超时就中止 handler 任务，并回
`ErrorKind::Timeout`（可重试）。这是**兜底**：驱动自己的 `request_ms`(15 s) /
`metadata_ms`(10 s) 始终先到，正常路径不会命中这一层。

## 只读白名单（最多 3 个源文件）

- `crates/kafka-core/src/protocol.rs` — `ErrorDto::new`、`ResponseEnvelope::err_dto` 的签名
- `crates/webview-bridge/src/handlers.rs` — `handler()` 模板与 `Outcome`
- `crates/webview-bridge/tests/router_core.rs` — 既有 router 测试口径（并发/panic 边界写法）

## 要改的文件

- 修改 `crates/webview-bridge/src/router.rs` —— `DEFAULT_TIMEOUT`、`timeout` 字段、
  `with_timeout`、`dispatch` 的 `tokio::time::timeout` 包装与 `deadline()` 错误
- 新增 `crates/webview-bridge/tests/global_deadline.rs` —— 本卡验证

## 必须覆盖

1. handler 跑过截止时间 → 响应 `ok:false`、`kind == timeout`、`retryable == true`，消息里
   既有命令名也有毫秒数，且**在 handler 睡完之前就答完**（实测等待远小于 handler 的睡眠）。
2. 正常快请求不受影响：`ok:true`、`data` 原样、`error` 为 `None`。
3. 超时后进程与路由都还活着：同一个 router 紧接着还能正常应答下一个请求（复用 T032 的
   `spawn` panic 边界，abort 不能带走 router 本身）。

## 明确不做

- 不做取消（`request.cancel` / 在途表 / `inflight()`）—— **T082b**。
- 不做前端（`AbortController` + 顶栏取消按钮 + dist 重建）—— **T082c**。
- 不改驱动侧 deadline（`TimeoutConfig` / `connect_ms` / `request_ms` 仍归驱动管）。
- 不动 `Router` 的命令表并发语义（T032 的「并发不串话」不变）。

## 已知取舍（Ruling）

- **R81** 路由层截止时间定位为**兜底**：默认 30 s（`DEFAULT_TIMEOUT`），可用
  `Router::with_timeout(..)` 覆盖。驱动自己的超时始终先到，它挡的是「handler 根本不该卡住」
  的情形。超时即 `abort()` handler 任务 —— 阻塞式 `spawn_blocking` 里的调用不会被真的打断，
  只是它的结果被丢弃。代价：极慢但合法的大集群操作（超过 30 s）会被判超时，需要调用方
  显式放宽。
- **R84** 超时回的是既有 `ErrorKind::Timeout`，它自带的建议文案是「调大集群设置里的请求超时」，
  指向的是驱动 deadline 而非这一层。接受：方向一致（都是「太久了」），本轮不再为一个 kind
  拆两种 hint。

## 验收

- `cargo test -p webview-bridge --test global_deadline` 全绿
- `cargo test -p webview-bridge`（既有 router/pump 测试无回归）
- `cargo clippy -q --workspace --all-targets -- -D warnings` 零告警

## 结果（完成后填写，≤5 行）

- `crates/webview-bridge/src/router.rs` +45/-6（1 文件）：新增 `DEFAULT_TIMEOUT`（30 s）、
  `Router.timeout` 字段与 `with_timeout()`，`Default` 改为手写（派生会给 0 s）；`dispatch`
  把 handler 任务挂在 `tokio::time::timeout` 下，超时 `abort()` 并回 `deadline()` 的 `timeout` 错误。
- `crates/webview-bridge/tests/global_deadline.rs` 新增 2 个测试（69 行）：睡 30 s 的 handler 在
  60 ms 截止下立刻回 `timeout`（消息含命令名与 `60` 毫秒，实测等待 < 5 s）；快请求不受影响且同一
  router 事后仍可正常应答。
- 验证：`cargo test -p webview-bridge --test global_deadline` 2/2；`cargo test -p webview-bridge`
  9 个测试目标全绿（7+7+2+9+6+7+7+4+8）；`cargo clippy -q --workspace --all-targets -- -D warnings` 零告警。
- 偏差：无。取消与前端按 250 行上限拆到 T082b / T082c。
