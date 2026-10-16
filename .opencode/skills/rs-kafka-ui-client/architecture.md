# 架构

依赖**单向向下**，改动不得打破方向：

```
frontend/  (Vue3+TS+Tailwind)
  ↓  JSON-RPC 请求-响应 / 事件推送(Rust→JS, 节流≤50ms/200条)
crates/webview-bridge   tao+wry: 命令路由 · 事件 pump · 静态资源
  ↓  Arc<dyn KafkaDriver> (#[async_trait])
crates/kafka-app        ClusterService · Topic/Produce/Consume/Group · EventBus · SQLite Store
  ↓
crates/kafka-driver     MockKafkaDriver(默认) · RealKafkaDriver(feature="real", rdkafka)
  ↓
crates/kafka-core       类型/错误/协议DTO (无 IO 依赖)
```

- 各层只允许依赖其下方层，**禁止反向依赖**（如 `kafka-app` 不得依赖 `webview-bridge`）。
- `kafka-core` 无任何 IO 依赖，是全仓最底层。
