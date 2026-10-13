# T024 · ClusterService + 驱动注册表

**状态**: 🔵代码完成，待编译验证 | **依赖**: T021, T023 | **改动上限**: 3 文件 / 250 行（实际 4 文件，见结果）

## 目标
把「本地集群配置」和「活的驱动实例」管起来：连接、断开、切换、列集群。

## 只读白名单（最多 3 个）
- `crates/kafka-driver/src/driver.rs` — `driver_for(cfg)`、`DynDriver`
- `crates/kafka-app/src/store/clusters.rs`、`crates/kafka-app/src/bus.rs`
- `crates/kafka-core/src/cluster.rs` — `ClusterConfig`/`ClusterInfo`

## 要改的文件
- 新建 `crates/kafka-app/src/services/mod.rs`、`crates/kafka-app/src/services/clusters.rs`
- 修改 `crates/kafka-app/src/lib.rs`：`pub mod services;`

## 必须覆盖
- `ClusterService { store, bus, drivers: Mutex<HashMap<ClusterId, DynDriver>> }`
- `list()`：本地配置 + 每个集群的连接状态（`connected`/`disconnected`/`connecting`）
- `connect(id)`：取配置 → `driver_for` → `connect()` → 存进 registry → 发 `ConnectionStateChanged`
- `disconnect(id)`：调 `driver.disconnect()` → 从 registry 移除 → 发事件
- `driver(id)`：未连接返回 `DriverError::NotConnected`
- `save`/`delete` 走 store；删除已连接集群时先断开
- 并发 `connect` 同一集群两次：只建立一个驱动实例（用 `tokio::sync::Mutex` 或 `OnceCell` 语义）

## 明确不做
- 不做重试/超时（T029）、不做自动重连（T062）
- 不做 Mock 快照落盘接线（T030 冒烟时再接，或另立卡）

## 验收
- 新建 `crates/kafka-app/tests/svc_clusters.rs`（全用 `mock=true` 的 `ClusterConfig`）
- `cargo test -p kafka-app --test svc_clusters` 全绿

## 结果（完成后填写，≤5 行）
- 产出 4 文件：`services/mod.rs`(~68：`ServiceError{Driver,Storage}` + `kind/is_retryable/to_dto` + `From<ServiceError> for DriverError` + `pub(crate) store_task`)、`services/clusters.rs`(~250)、`lib.rs`(+1)、`tests/svc_clusters.rs`(~215，10 测试)。**偏离 3 文件/250 行上限**（额外加了统一错误类型与阻塞包装）。状态：**代码完成，未编译验证**（T001 未完成）。
- 单飞语义：`slots: Mutex<HashMap<ClusterId, Arc<Slot>>>`，`Slot { gate: tokio::Mutex<()>, driver: std::Mutex<Option<DynDriver>>, state: std::Mutex<ConnectionState> }`；`connect`/`disconnect` 取 `gate` 后二次检查 → 并发 connect 只建一个驱动实例、只发一对状态事件；驱动掉线后下次 connect 会换新实例。
- 所有 SQLite 调用经 `store_task(&Store, closure)`（`spawn_blocking`）→ 不占 async 线程（T020 遗留已清）。`Store` 是 `Send + Sync`（`Mutex<Connection>: Sync`），故 `&Store` 可跨 await。
- 事件：`Connecting → Connected` / `Connecting → Error(reason)` / `Disconnected`；`save` 不发事件；`delete` 先断开再删行并 `forget_recent`；`remember` 改为 await（best-effort 写失败只 warn），保证 connect 返回后 recent 一致。
- 遗留：Mock 快照在 connect 时自动恢复未接线（留给 T030）；`save` 改名/改配置不会更新已连接驱动的既有配置；`ClusterView` = `config` flatten + `state` + `driver`("mock"/"real"/null)。