# T082 — UI 取消在途请求 + 全局超时

状态: ✅　依赖: T032,T029　验证: `cargo test -p webview-bridge --test cancel_inflight`　类型: 产品码

## 目标

给桥接层加两道防阻塞护栏（阶段 7「超时/取消/防阻塞」）：

1. **全局超时**：`Router::dispatch` 目前 `tokio::spawn(handler).await` 无期限等待。某个
   handler 卡住（驱动阻塞、SQLite 锁、librdkafka 未回调）会把 `Handle::block_on` 里的
   整条请求链拖死，UI 的 pending 计数只增不减。改为每个请求跑在一个**截止时间**下，
   超时就中止 handler 任务并回 `ErrorKind::Timeout`。
2. **取消在途请求**：UI 需要一个「不等了」的出口。新增内建命令 `request.cancel`
   （payload `{id}`），按信封 `id` 找到在途 handler 任务并 `abort()`，被取消的请求回
   `ErrorKind::Cancelled`。

前端接线（AbortController + 顶栏取消按钮）按 250 行上限拆到 **T082b**。

## 只读白名单（最多 3 个源文件）

- `crates/kafka-core/src/protocol.rs` — `cmd` 常量、`ErrorDto::new`、`ResponseEnvelope::err_dto`
- `crates/webview-bridge/src/handlers.rs` — `handler()` 模板（确认内建 handler 的写法）
- `crates/webview-bridge/tests/router_core.rs` — 既有 router 测试口径（并发/panic 边界的写法）

## 要改的文件

- 修改 `crates/kafka-core/src/protocol.rs` —— `cmd` 增 `REQUEST_CANCEL`
- 修改 `crates/webview-bridge/src/router.rs` —— 截止时间 + 在途表 + `cancel()` + 内建 `request.cancel`
- 新增 `crates/webview-bridge/tests/cancel_inflight.rs` —— 本卡验证

## 必须覆盖

1. handler 跑过截止时间 → 响应 `ok:false`、`kind == timeout`、`retryable == true`，消息里
   既有命令名也有毫秒数，且**在 handler 睡完之前就答完**（实测等待远小于 handler 的睡眠）。
2. 正常快请求不受截止时间影响：`ok:true`、`data` 原样、结束后在途表归零。
3. 在途请求可被 `request.cancel` 取消：取消响应 `{id, cancelled:true}`，原请求立刻回
   `kind == cancelled`（不重试），且**不等到 handler 睡满**。
4. 取消只作用于被点名的那个：邻居请求仍在途，需要它自己再取消一次才结束。
5. 取消「未知 id」「已答完的 id」都返回 `cancelled:false`（不报错，幂等语义）；截止时间
   触发过的 id 同样已被清出在途表。
6. `request.cancel` 缺 `id` → `invalid_config`，消息点名缺的是哪个字段。
7. 在途表不泄漏：`ok` / `timeout` / `cancel` 三种结束方式走过之后 `inflight() == 0`。

## 明确不做

- 不做前端（`AbortController`、顶栏取消按钮、dist 重建）—— T082b。
- 不改驱动侧 deadline（`TimeoutConfig` / `connect_ms` / `request_ms` 仍在驱动里管，
  本卡的截止时间是**兜底**，不替代它们）。
- 不动 `Router` 的命令表并发语义（T032 已定的「并发不串话」不变）。
- 不给取消加权限验证 / 只允许取消自己的请求（单进程内嵌 UI，无此边界）。
- 不动事件泵（T037）与总线（T023）。

## 已知取舍（Ruling）

- **R81** 路由层截止时间定位为**兜底**：默认 30 s（`DEFAULT_TIMEOUT`），可用
  `Router::with_timeout(..)` 覆盖。驱动自己的超时（`request_ms` 15 s、`metadata_ms` 10 s）
  始终先到，正常路径不会命中这一层；它挡的是「handler 根本不该卡住」的情形。超时即
  `abort()` handler 任务 —— 阻塞式 `spawn_blocking` 里的调用不会被真的打断，只是它的
  结果被丢弃。代价：极慢但合法的大集群操作（超过 30 s）会被判超时，需要调用方显式放宽。
- **R82** `request.cancel` 是 **`Router::new()` 内建注册**的命令，按信封 `id` 寻址，
  命中就 `abort()` 并回 `{id, cancelled:true}`，原请求回 `ErrorKind::Cancelled`；
  未命中回 `cancelled:false` 而**不是**报错（「已经不用取消了」不是错误）。
  `cancel()` 同时是 `Router` 的公开方法，测试与将来的批量取消共用一条路径。代价：
  内建命令可以被后续 `register("request.cancel", ..)` 覆盖（命令表 last-wins，T032 语义）；
  另外「取消」只丢弃结果，不保证外部副作用回滚。
- **R83** 信封 `id` 为空（客户端没给）的请求**不进在途表**：没有可寻址的名字，既不能
  取消也不会在表里留痕。代价：这样发出来的请求只能靠截止时间兜底。
- **R84** 超时回的是既有 `ErrorKind::Timeout`，它自带的建议文案是「调大集群设置里的
  请求超时」，指向的是驱动 deadline 而非这一层。接受：方向一致（都是「太久了」），
  本轮不再为一个 kind 拆两种 hint。

## 验收

- `cargo test -p webview-bridge --test cancel_inflight` 全绿
- `cargo test -p webview-bridge`（既有 router/pump 测试无回归）
- `cargo test --workspace`、`cargo clippy -q --workspace --all-targets -- -D warnings` 零告警

## 结果（完成后填写，≤5 行）

- `protocol.rs` +3（`cmd::REQUEST_CANCEL = "request.cancel"`）；`router.rs` +82/-6：`Router` 增
  `inflight` 表与 `timeout`，`dispatch` 把 handler 任务挂在 `tokio::time::timeout` 下并按
  `JoinError::is_cancelled()` 区分「被取消」与「panic」，新增 `with_timeout` / `inflight()` /
  `cancel()` 与内建 `request.cancel`；`tests/cancel_inflight.rs` 新增 7 个测试。
- 语义：`ok` / `timeout` / `cancelled` 三种结束都会清出在途表；`request.cancel` 对未知或已结束
  的 id 回 `cancelled:false`（幂等），缺 `id` 回 `invalid_config`；空 `id` 的信封不入表（R83）。
- 验证：`cargo test -p webview-bridge --test cancel_inflight` 7/7；`cargo test -p webview-bridge`
  全绿；`cargo test --workspace` 全绿；`cargo clippy -q --workspace --all-targets -- -D warnings` 零告警。
- 偏差：本卡只做后端与测试；前端 `AbortController` + 顶栏取消按钮拆到 T082b（250 行上限）。
