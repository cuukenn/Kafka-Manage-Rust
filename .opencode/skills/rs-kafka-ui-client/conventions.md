# 开发约定

- **全异步**：一切 Kafka IO 放 Tokio 任务，UI 主线程零阻塞；Rust→JS 推送必须节流。
- **Mock 优先**：业务层测试全用 Mock；Real 只在 `--features real`。
- **错误处理**：用 `thiserror` 定义 `ErrorKind`，每个 kind 要有可读 hint。
- **DTO**：消息字节走 base64 + utf8 双通道，避免 `Vec<u8>` 序列化成数字数组撑爆 UI。
- **前端资源**：用 `include_dir` 嵌入 `frontend/dist`。
- **改动边界**：单次改动最多改 **3 个文件**、新增 **≤250 行**；超出即拆分。
