# 通信协议

- 请求（JS→Rust）：`{ "id", "cmd", "payload" }`；响应 `{ "id", "ok", "data" }` 或 `{ "id", "ok": false, "error": { "kind", "message", "retryable", "hint"? } }`。
- 事件（Rust→JS）：条目 `{ "event", "payload", "ts" }`，**必须批量**——满 200 条立即 flush，否则首个事件后 50ms flush（`webview-bridge/src/pump.rs`；窗口 ready 前发布的事件泵看不到）。
- 命令域（常量在 `kafka-core/src/protocol.rs`）：`cluster.*` `topic.*` `produce.*` `consume.*` `group.*` `config.*` `mock.*` `request.cancel`。
- 事件名：`connection.state` `message.received` `subscription.state` `subscription.done` `topic.changed` `group.changed` `produce.result` `log.line` `error`。
- 事件载荷形状 = Rust 结构体的 camelCase 形态，例如 `message.received` 就是 `{ subscriptionId, message }`（`message` 是原始 `MessageEvent`，不是 `MessageDto`）。
- 前端调用走 `frontend/src/bridge/rpc.ts`（`fetch('/rpc')` + 在飞计数 + `request.cancel` 取消）。
