# 架构

依赖**单向向下**，改动不得打破方向：

```
frontend/  (Vue3 + TS + Tailwind 4)
  ↓  JSON-RPC 请求-响应 / 事件推送(Rust→JS, 批量: 满 200 条或首个事件后 50ms)
crates/webview-bridge   tao+wry: 命令路由 · 事件 pump · 静态资源
  ↓  Arc<dyn KafkaDriver> (#[async_trait])
crates/kafka-app        ClusterService · Topic/Produce/Consume/Group · EventBus · SQLite Store · driver_guard
  ↓
crates/kafka-driver     MockKafkaDriver(默认) · RealKafkaDriver(feature="real", vendored rdkafka)
  ↓
crates/kafka-core       类型/错误/协议DTO (无 IO 依赖)
```

- 各层只允许依赖其下方层，**禁止反向依赖**（如 `kafka-app` 不得依赖 `webview-bridge`）。
- `kafka-core` 无任何 IO 依赖，是全仓最底层。
- `crates/main-bin` 在最上方装配这一切（runtime / App / Router IPC / 日志 / 本地数据目录），是入口而非被依赖层。
- UI 默认是**编译期嵌入**的 `frontend/dist`；设 `KAFKA_UI_DEV_DIR` 则改从磁盘读，只改前端时免重编译 Rust。
