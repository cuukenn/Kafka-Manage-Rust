# Workspace 与各 crate 职责

- `Cargo.toml`：workspace（resolver 3、edition 2024、rust-version 1.85）；`workspace.dependencies` 统一版本；release profile = `opt-level=s + lto=thin + strip + codegen-units=1 + panic=abort`。
- `crates/kafka-core`：`cluster/topic/message/group/error/dto/protocol` 模型、`ErrorKind`（每个 kind 一份可读 hint）、命令与事件常量、前后端协议 DTO。`dto.rs::MessageDto` 是**给 UI 看**的消息形态。
- `crates/kafka-driver`：`KafkaDriver` trait、`MockKafkaDriver`（`MockState` / 订阅 pump / `FaultInjection` / `active_pumps()` 活泵计数）、`real/`（`--features real` 门控，`default=[]`）。`tests/`：mock 套件 + `conformance.rs` + 门控的 `real_conformance.rs`；`benches/mock_throughput.rs`（criterion 吞吐基准）；`examples/leak_probe.rs`（长跑查内存/句柄泄漏）。
- `crates/kafka-app`：`store`(SQLite)、`bus`(事件总线)、`services/{clusters,topics,produce,consume,groups}`、`driver_guard`(超时 + 重试护栏)。
- `crates/webview-bridge`：tao+wry 桥接，命令路由（`handlers_*`）、事件 pump、静态资源。
- `crates/main-bin`：入口（bin-only，`mod glue; mod logging;`），启动装配（日志、本地数据目录、App、Router IPC、事件 pump）；`--features real` 透传给下层。
