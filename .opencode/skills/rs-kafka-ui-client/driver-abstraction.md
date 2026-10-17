# 驱动抽象（核心约束）

- `KafkaDriver` trait 方法用 `#[async_trait]`——直接 `async fn` 的 trait **不是 object-safe**，无法 `Arc<dyn KafkaDriver>`（`type DynDriver = Arc<dyn KafkaDriver>`）。
- 消费订阅返回 `MessageStream`：内部 `BoxStream<'static, Result<MessageEvent, DriverError>>` + `StreamControls`（pause / resume / seek / **stop**）。流必须 `'static` 才能跨任务移动；`next()`/`id()`/`controls()`/`into_stream()` 都是**固有**方法（不必引 `futures::StreamExt`，拖干用 `while let Some(ev) = stream.next().await`）。
- 订阅默认 `follow: true`（追尾、流不结束）；`follow: false` + `MaxMessages` 才是有界读——写测试时别把无界流当 `b.iter` 的迭代体。
- Mock 与 Real 语义必须对齐：offset 越界、ISR / leader、错误码映射。契约测试：`kafka-driver/tests/conformance.rs`（mock 侧）与 `tests/real_conformance.rs`（真集群，需 `KAFKA_CONFORMANCE_BOOTSTRAP` 且要 `-- --ignored`）。
- `MockKafkaDriver::active_pumps()` 是活订阅泵计数：订阅结束后没有归零就是泄漏。长跑查法见 `examples/leak_probe.rs`（配合 `scripts/leak-check.sh` 从进程外采样 RSS 与 fd）。
