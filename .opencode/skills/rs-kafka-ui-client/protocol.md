# 通信协议

- 请求（JS→Rust）：`{ "id", "cmd", "payload" }`；响应 `{ "id", "ok", "data" }` 或 `{ "id", "ok": false, "error": { "kind", "message", "retryable" } }`。
- 事件（Rust→JS）：`{ "event", "payload" }`，**必须批量节流 ≤50ms/200 条**。
- 命令命名域：`cluster.*` `topic.*` `produce.*` `consume.*` `group.*` `config.*` `mock.*`。
