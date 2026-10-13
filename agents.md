# agents.md — Kafka-Manage-Rust（rs-kafka-ui-client）

本文件供编码代理（opencode 等）在修改本项目时参考，是常驻的项目约定。

## 项目概述

跨平台 Kafka 可视化客户端。后端 Rust 业务 + 系统 WebView UI，事件驱动 NIO（Tokio）。

## 架构（依赖单向向下，改动不得打破方向）



```
frontend/  (Vue3+TS+Tailwind)

&#x20;  ↓ JSON-RPC 请求-响应 / 事件推送(Rust→JS, 节流≤50ms/200条)

crates/webview-bridge   tao+wry: 命令路由 · 事件 pump · 静态资源

&#x20;  ↓ Arc\<dyn KafkaDriver> (#\[async\_trait])

crates/kafka-app        ClusterService · Topic/Produce/Consume/Group · EventBus · SQLite Store

&#x20;  ↓

crates/kafka-driver     MockKafkaDriver(默认) · RealKafkaDriver(feature="real", rdkafka)

&#x20;  ↓

crates/kafka-core       类型/错误/协议DTO (无 IO 依赖)
```

## Workspace 与各 crate 职责



* `Cargo.toml`：workspace（resolver 3，edition 2024，rust 1.85），`workspace.dependencies` 统一版本，release 用 `opt-level=s + lto=thin + panic=abort`

* `crates/kafka-core`：`cluster/topic/message/group/error/dto/protocol` 模型、`ErrorKind` 错误、前后端协议 DTO（消息用 base64 + utf8 双通道，勿把 `Vec<u8>` 直接 serde 成数字数组）

* `crates/kafka-driver`：`KafkaDriver` trait、`MockKafkaDriver`/`MockState`/`MockPump`/`FaultInjection`；`real/` 用 `--features real` 门控（`default=[]`）

* `crates/kafka-app`：`store`(SQLite)、`bus`(事件总线)、`services/*`、`driver_guard`(超时 + 重试护栏)

* `crates/webview-bridge`、`crates/main-bin`：桥接与入口

## 驱动抽象（核心约束）



* `KafkaDriver` trait 方法用 `#[async_trait]`（直接 `async fn` 的 trait **不是 object-safe**，无法 `Arc<dyn KafkaDriver>`）

* 消费订阅返回 `MessageStream = BoxStream + StreamControls`（pause/resume/seek/stop 控制通道），流要 `'static` 才能跨任务移动

* Mock 与 Real 语义必须对齐：offset 越界、ISR/leader、错误码映射；契约测试放 `kafka-driver/tests/conformance.rs`

## 通信协议



* 请求（JS→Rust）：`{ "id","cmd","payload" }`；响应 `{ "id","ok","data" }` 或 `{ "id","ok":false,"error":{"kind","message","retryable"} }`

* 事件（Rust→JS）：`{ "event","payload" }`，**必须批量节流 ≤50ms/200 条**

* 命令命名域：`cluster.* topic.* produce.* consume.* group.* config.* mock.*`

## 开发约定



* **全异步**：一切 Kafka IO 放 Tokio 任务，UI 主线程零阻塞；Rust→JS 推送必须节流

* **Mock 优先**：业务层测试全用 Mock；Real 只在 `--features real`

* **错误处理**：用 `thiserror` 定义 `ErrorKind`，每个 kind 要有可读 hint

* **DTO**：消息字节走 base64 + utf8 双通道，避免 `Vec<u8>` 序列化成数字数组撑爆 UI

* **前端资源**：用 `include_dir` 嵌入 `frontend/dist`

* **改动边界**：单次改动最多改 **3 个文件**、新增 ≤ **250 行**；超出即拆分

## 构建 / 测试命令



```
cargo check -p kafka-core -p kafka-driver

cargo test  -p kafka-driver --test mock\_lifecycle

cargo test  --workspace

cargo check -p kafka-driver --features real
```

## 环境约束



* **工具链已通（2026-10-13）**：Windows SDK `10.0.26100` 已安装（`winget install --id Microsoft.WindowsSDK.10.0.26100 --exact`），`C:\\Program Files (x86)\\Windows Kits\\10` 下 `Lib`/`Include`/`bin` 齐全，rustc 经注册表自动发现，无需配 PATH。此前「缺 SDK 导致任何 cargo 命令都失败」的阻塞已解除。

* 工具链：rustc 1.99.0 / cargo 1.99.0 / VS Build Tools 2022（MSVC 14.44.35207）/ Windows SDK 10.0.26100 / edition 2024 / rust-version 1.85。

* 已知代码现状：`kafka-core` 编译通过；`kafka-driver` 与 `kafka-app` 尚未编译过，缺陷登记在 `docs/TASKS.md` 的缺陷卡区（T052 已修、T053 待修）。

* 注意：本仓库 `rust-analyzer` 的诊断可能是过期缓存，引用前先核实实际源码。