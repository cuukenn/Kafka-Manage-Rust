# 开发约定

- **全异步**：一切 Kafka IO 放 Tokio 任务，UI 主线程零阻塞；Rust→JS 推送必须批量（见 protocol）。
- **Mock 优先**：业务层测试全用 Mock；Real 只在 `--features real`，真集群测试还需环境变量门控（`KAFKA_CONFORMANCE_BOOTSTRAP`）。
- **错误处理**：用 `ErrorKind`（`thiserror`）+ 每个 kind 一份可读 hint；前端会显示 hint，新增 kind 时把 hint 与穷尽测试一起补上。
- **字节字段**：`dto.rs::MessageDto` 把每个字节字段发两份——`value`（尽力 UTF-8，给显示）+ `valueB64`（base64，可精确往返/下载），并带 `valueIsUtf8` / `size` / `truncated`。注意 `MessageEvent`（如 `message.received`）与 `ProduceReq` 走的是**原始 `Vec<u8>` → JSON 数字数组**，别把大 payload 塞进显示路径。
- **前端资源**：用 `include_dir` 嵌入 `frontend/dist`；改前端要 `npm run build`，产物变化后 `git add -f frontend/dist`。
- **风格**：按手写风格（4 空格缩进、约 100 列）；**不要跑 `cargo fmt`**（无 `rustfmt.toml`，代码非 rustfmt-clean）。
- **改动边界**：单次改动最多改 **3 个文件**、新增 **≤250 行源码**；超出即拆分（文档不计入）。
