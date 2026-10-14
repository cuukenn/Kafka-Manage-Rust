# T032 · Router 内核：envelope 解析 + 命令表 + 假 App 单测

**状态**: ✅完成 | **依赖**: T030 | **改动上限**: 3 文件 / 250 行

## 目标

在 `crates/webview-bridge`（现为空壳）建立 **传输无关** 的 Router 内核，给桥接层一个可单测的调度核心：

```
raw JSON string/Value → parse RequestEnvelope → 命令表查 cmd → async handler(payload) → ResponseEnvelope
```

- 命令表：`HashMap<String, Handler>`；`Handler` = `Arc<dyn Fn(serde_json::Value) -> BoxFuture<'static, Result<serde_json::Value, ErrorDto>>>`（object-safe，`Send + Sync`）。
- handler 错误类型 = `kafka_core::dto::ErrorDto`（kind/message/retryable，T065 已修 retryable 语义）。真实 App 各域的 `From<DriverError>/From<StorageError>` 转换属 T033+，本卡只做内核 + 假 handler。
- 本卡不接触 tao/wry、不接触真实 `App`、不做事件泵（T037）。

## 只读白名单（最多 3 个）

- `crates/kafka-core/src/dto.rs` — `RequestEnvelope` / `ResponseEnvelope` / `ErrorDto` 的字段与 serde 形状（camelCase wire）
- `crates/kafka-core/src/protocol.rs` — `cmd::*` 命名域常量（`cluster.* topic.* produce.* consume.* group.* config.* mock.*`）
- `crates/kafka-app/src/lib.rs` — T030 `App` 门面 API 面（只确认未来接线形态，不 import 使用）

## 要改的文件

- 新建 `crates/webview-bridge/src/router.rs`（内核），`src/lib.rs` 加 `pub mod router;`
- 新建 `crates/webview-bridge/tests/router_core.rs`（假 handler 单测）
- 修改 `crates/webview-bridge/Cargo.toml`（加 kafka-core 依赖 + tokio/serde_json dev-deps；版本走根 workspace 约定）

## 必须覆盖

1. 合法 envelope（`{"id","cmd","payload"}`）解析成功 → 命中注册 handler → 响应 `{id, ok:true, data}`，payload 原样透传（假 handler 回显即可）
2. 未知 cmd → 错误响应：`ok:false`，`error.kind` 明确（用 `ErrorKind` 里语义最近的可重试=false 的 kind；若无贴切 kind，卡片裁定用 `ErrorKind::Internal` 并在 message 里带 cmd 名），`id` 回显
3. 非法 envelope（缺 `id` / 缺 `cmd` / 非 JSON / `payload` 缺失按 `null` 处理）→ 错误响应不 panic；缺 id 时响应 `id` 用空串（wire 契约：id 恒存在）
4. handler 返回 `Err(ErrorDto)` → 响应 `{id, ok:false, error:{kind,message,retryable}}` 三字段齐全，retryable 由 ErrorDto 决定（不在 Router 里重新判定）
5. 命令表注册语义：同 cmd 重复注册 = 覆盖（后注册生效），测试锁住
6. 异步并发：两个不同 handler 的在途请求可并发完成（假 handler 用 oneshot/延迟释放控制完成顺序，响应 id 各归各，不串扰）

## 明确不做（防止范围膨胀）

- 不做 tao/wry、JS 注入、`external/ipc` 传输（T031/T038）
- 不 import `kafka-app`（真实 App 接线属 T033-T036）
- 不做事件泵 / 节流批量（T037）
- 不做 `config.*` / `mock.*` 具体命令（T036）

## 验收

- `cargo test -p webview-bridge` 全绿（测试数 ≥ 6）
- `cargo clippy -p webview-bridge --all-targets -- -D warnings` 零告警
- `cargo check --workspace` 通过

## 结果（完成后填写，≤5 行）
- 实现：`Router`（`register`/`handle`，`Arc<RwLock<HashMap<String, Handler>>>`，Clone+Send+Sync）；两段式解析（JSON→字段，缺 id→`""`，缺 cmd→invalid_config，缺 payload→null）；未知 cmd→`internal`（不可重试，message 带 cmd 名）；handler 错误原样透传。
- 测试：`tests/router_core.rs` 6 个测试与卡片 6 条必须覆盖 1:1（含 oneshot 反序释放的并发不串扰测试）；`cargo test -p webview-bridge` 全绿、clippy `-D warnings` 零告警、`cargo check --workspace` 通过。
- 备注：envelope 实际定义在 `kafka-core/src/protocol.rs`（卡片写的 dto.rs 是笔误）；新增净行数 250（含 Cargo.toml/lib.rs），Cargo.lock +1 为自动产物。
