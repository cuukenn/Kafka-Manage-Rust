---
name: rs-kafka-ui-client
description: rs-kafka-ui-client（Kafka-Manage-Rust）项目约定——架构、crate 职责、驱动抽象、通信协议、开发约定、构建命令、工具链。修改本仓库任何代码前先读对应模块。
---

# rs-kafka-ui-client 项目约定

跨平台 Kafka 可视化客户端：Rust 业务 + 系统 WebView UI，事件驱动 NIO（Tokio）。

本 skill 是本仓库常驻项目约定，供编码代理修改代码时参考。各模块**高内聚、低耦合**，相互独立，按需单独读取：

| 模块 | 职责 | 何时读 |
|---|---|---|
| [architecture](architecture.md) | 分层架构与依赖方向 | 改动前了解模块边界 |
| [workspace](workspace.md) | workspace 配置 + 各 crate 职责 | 新增/修改 crate 时 |
| [driver-abstraction](driver-abstraction.md) | KafkaDriver trait 核心约束 | 动 driver 或加方法时 |
| [protocol](protocol.md) | IPC 请求/响应/事件/命令域 | 改桥接或前端接口时 |
| [conventions](conventions.md) | 开发约定（异步/mock/错误/DTO/边界） | 每次改动前 |
| [build-test](build-test.md) | 构建与测试命令 | 编译/验证时 |
| [toolchain](toolchain.md) | 工具链与环境 | 环境相关问题 |

**最高优先级硬约束（必守）**：单次改动最多改 **3 个文件**、新增 **≤250 行**，超出即拆分。
