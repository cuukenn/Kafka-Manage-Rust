# 驱动抽象（核心约束）

- `KafkaDriver` trait 方法用 `#[async_trait]`——直接 `async fn` 的 trait **不是 object-safe**，无法 `Arc<dyn KafkaDriver>`。
- 消费订阅返回 `MessageStream = BoxStream + StreamControls`（pause/resume/seek/stop 控制通道）；流要 `'static` 才能跨任务移动。
- Mock 与 Real 语义必须对齐：offset 越界、ISR/leader、错误码映射；契约测试放 `kafka-driver/tests/conformance.rs`。
