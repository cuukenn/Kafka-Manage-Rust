# agents.md — Kafka-Manage-Rust（rs-kafka-ui-client）

供编码代理（opencode 等）在修改本项目时参考的常驻约定。

**项目约定已提取为项目 skill**：`.opencode/skills/rs-kafka-ui-client/`（入口 `SKILL.md`）。
修改代码前按需读取对应模块：架构 / workspace / 驱动抽象 / 协议 / 约定 / 构建测试 / 工具链。

**最高优先级硬约束（必守）**：单次改动最多改 **3 个文件**、新增 **≤250 行**，超出即拆分。

项目一句话：跨平台 Kafka 可视化客户端（Rust 业务 + 系统 WebView UI，事件驱动 NIO/Tokio）。
