# T027 · ConsumeService（订阅注册表 + 流控制）

**状态**: 🔵代码完成，待编译验证 | **依赖**: T024 | **改动上限**: 2 文件 / 250 行（实际 3 文件，见结果）

## 目标
管理「活跃订阅」：启动/暂停/恢复/seek/停止，并把消息转成事件发总线。这是 NIO 流式消费的业务入口。

## 只读白名单（最多 3 个）
- `crates/kafka-core/src/message.rs` — `SubscribeReq`、`MessageStream`、`StreamCommand`、`MessageEvent`
- `crates/kafka-app/src/bus.rs`、`crates/kafka-app/src/services/clusters.rs`
- 需要时看 `crates/kafka-driver/src/mock/driver.rs` 的 `subscribe`

## 要改的文件
- 新建 `crates/kafka-app/src/services/consume.rs`
- 修改 `crates/kafka-app/src/services/mod.rs`

## 必须覆盖
- `ConsumeService { subs: Mutex<HashMap<SubId, SubHandle>> }`
- `start(cluster_id, req) -> SubId`：起一个 `tokio::spawn` 任务，循环取流 → `bus.publish(MessageReceived)`
- `pause(id)` / `resume(id)` / `seek(id, tp, offset)` / `stop(id)`
- `stop`：任务退出、map 移除、发 `SubscriptionDone`（幂等：重复 stop 不报错）
- `list()`：当前订阅及其状态（running/paused/done）
- 流结束或出错：自动标记 done + 发 `SubscriptionDone`/`Error` 事件，任务不泄漏
- 每个订阅有 `topic`/`partitions` 元数据，供 `list()` 展示

## 明确不做
- 不做节流/背压/缓冲上限（T060、T061）
- 不做断线重连（T062）

## 验收
- 新建 `crates/kafka-app/tests/svc_consume.rs`：start→收到消息→pause→resume→stop；重复 stop；错误流自动 done
- `cargo test -p kafka-app --test svc_consume` 全绿，且测试结束无残留任务（用 `tokio::time::timeout` 包住）

## 结果（完成后填写，≤5 行）
- 产出 3 文件：`services/consume.rs`(~330)、`services/mod.rs`(+2 行)、`tests/svc_consume.rs`(~330，8 测试)。**偏离 2 文件/250 行上限**。状态：**代码完成，未编译验证**（T001 未完成）。
- API：`start(cluster_id, SubscribeReq) -> SubscriptionInfo`、`stop -> bool`（幂等）、`pause/resume/seek`、`list()`、`state_of`、`active_count`、`total_count`、`prune_done`、`stop_all`；`SubscriptionInfo{subscriptionId, clusterId, topic, partitions, state, delivered, reason?}`。
- `done` 事件只发一次：`finished: Arc<AtomicBool>` 做 CAS，流自然结束 / 驱动报错 / 客户端 stop 三方谁先 swap 谁发；驱动报错时额外发 `error` 事件（`ErrorDto::from_driver`）。
- 服务不缓冲消息：控制命令直接转 `StreamControls`，暂停由驱动 pump 实现；`stop` 发 `Stop` 后 `task.abort()` 并移出 map；已结束条目保留到 `prune_done()` / `stop_all()`。
- 遗留：`partitions` 存请求值（空=全部分区，未解析为具体列表）；服务 drop 不会自动停流（T030 必须调 `stop_all`）；未做节流/背压（T060/T061）。