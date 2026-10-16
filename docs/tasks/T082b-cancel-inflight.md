# T082b — 取消在途请求

状态: ✅　依赖: T082a　验证: `cargo test -p webview-bridge --test cancel_inflight`　类型: 产品码

> 原 T082「UI 取消在途请求 + 全局超时」实测 345 行/3 文件超限，按 250 行上限拆成
> **T082a（全局超时）✅**、**T082b（本卡，取消）**、**T082c（前端接线）**。

## 目标

用户需要一个「不等了」的出口。本卡给 `Router` 加在途表：每个请求按信封 `id` 记住 handler
任务的 `AbortHandle`，新增内建命令 `request.cancel`（payload `{id}`）按 id 中止它，被取消的
请求回 `ErrorKind::Cancelled`（不可重试）。同时公开 `Router::cancel(id)` 与
`Router::inflight()`：前者给测试与将来的批量取消共用，后者给 T082c 的取消按钮计数。

## 只读白名单（最多 3 个源文件）

- `crates/kafka-core/src/protocol.rs` — `cmd` 常量模块、`ErrorDto::new`
- `crates/webview-bridge/src/handlers.rs` — `handler()` 模板（内建 handler 的写法）
- `crates/webview-bridge/tests/router_core.rs` — 既有 router 测试口径

## 要改的文件

- 修改 `crates/kafka-core/src/protocol.rs` —— `cmd::REQUEST_CANCEL`
- 修改 `crates/webview-bridge/src/router.rs` —— 在途表 + `cancel()` + `inflight()` + 内建命令
- 新增 `crates/webview-bridge/tests/cancel_inflight.rs` —— 本卡验证

## 必须覆盖

1. 在途请求可被 `request.cancel` 取消：取消响应 `{id, cancelled:true}`，原请求立刻回
   `kind == cancelled`（`retryable == false`），**不等到 handler 睡满**。
2. 取消只作用于被点名的那个：邻居仍在途（`inflight() == 1`），需要它自己再取消一次才结束。
3. 取消「未知 id」「已答完的 id」「已被截止时间收走的 id」都返回 `cancelled:false`（幂等，不报错）。
4. `request.cancel` 缺 `id` → `invalid_config`，消息点名缺的是哪个字段。
5. 在途表不泄漏：`ok` / `timeout` / `cancel` 三种结束方式走过之后 `inflight() == 0`，且没有
   `id` 的信封不入表（R83）。

## 明确不做

- 不做前端（`AbortController`、顶栏取消按钮、dist 重建）—— **T082c**。
- 不改 T082a 的截止时间语义。
- 不做权限校验 /「只允许取消自己的请求」（单进程内嵌 UI，无此边界）。
- 不动事件泵（T037）与总线（T023）。

## 已知取舍（Ruling）

- **R82** `request.cancel` 是 **`Router::new()` 内建注册**的命令，按信封 `id` 寻址，命中就
  `abort()` 并回 `{id, cancelled:true}`，原请求回 `ErrorKind::Cancelled`；未命中回
  `cancelled:false` 而**不是**报错（「已经不用取消了」不是错误）。`Router::cancel()` 是同一条
  路径的公开入口。代价：内建命令可被后续 `register("request.cancel", ..)` 覆盖（命令表
  last-wins，T032 语义）；取消只丢弃结果，不保证外部副作用回滚。
- **R83** 信封 `id` 为空（客户端没给）的请求**不进在途表**：没有可寻址的名字，既不能取消
  也不会在表里留痕。代价：这样发出来的请求只能靠 T082a 的截止时间兜底。

## 验收

- `cargo test -p webview-bridge --test cancel_inflight` 全绿
- `cargo test -p webview-bridge`（既有 router/pump 测试无回归）
- `cargo test -q --workspace` 全绿、`cargo clippy -q --workspace --all-targets -- -D warnings` 零告警

## 结果（完成后填写，≤5 行）

- `crates/kafka-core/src/protocol.rs` +3（`cmd::REQUEST_CANCEL = "request.cancel"`）；`router.rs`
  增在途表 `Inflight = Arc<Mutex<HashMap<String, AbortHandle>>>`、`track()` / `lock_inflight()` /
  `inflight()` / `cancel()` 与内建 `register_cancel()`，`dispatch` 在 `spawn` 后 `track(&id, task.abort_handle())`，
  按 `JoinError::is_cancelled()` 区分「被取消」（回 `cancelled`）与「panic」（回 `internal`）。
- `crates/webview-bridge/tests/cancel_inflight.rs` 新增 3 个测试（合并后 4 个覆盖点）：取消在途
  请求并保住邻居、`cancelled:false` 的三种情形 + 缺 id 报错、在途表不泄漏（含空 id 不入表）。
- 验证：`--test cancel_inflight` 3/3；`cargo test -q --workspace` 273 passed / 45 个测试目标全 ok；
  `cargo clippy -q --workspace --all-targets -- -D warnings` 零告警。
- 偏差：原计划 5 个测试，为守住 250 行上限把「取消邻居」并进第一个测试、把「缺 id」并进第三个，
  覆盖点不变；最终 **245 行/3 文件**。
- 前端（AbortController + 顶栏取消按钮 + dist 重建）留 **T082c**。
