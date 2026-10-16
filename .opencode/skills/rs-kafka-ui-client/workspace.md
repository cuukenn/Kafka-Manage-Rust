# Workspace 与各 crate 职责

- `Cargo.toml`：workspace（resolver 3，edition 2024，rust-version 1.85）；`workspace.dependencies` 统一版本；release 用 `opt-level=s + lto=thin + panic=abort`。
- `crates/kafka-core`：`cluster/topic/message/group/error/dto/protocol` 模型、`ErrorKind` 错误、前后端协议 DTO（消息用 base64 + utf8 双通道，勿把 `Vec<u8>` 直接 serde 成数字数组）。
- `crates/kafka-driver`：`KafkaDriver` trait、`MockKafkaDriver`/`MockState`/`MockPump`/`FaultInjection`；`real/` 用 `--features real` 门控（`default=[]`）。
- `crates/kafka-app`：`store`(SQLite)、`bus`(事件总线)、`services/*`、`driver_guard`(超时 + 重试护栏)。
- `crates/webview-bridge`：tao+wry 桥接，命令路由、事件 pump、静态资源。
- `crates/main-bin`：入口，启动装配（日志、本地数据目录、App、Router IPC、事件 pump）。
