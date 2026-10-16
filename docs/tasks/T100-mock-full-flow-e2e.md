# T100 — Mock 全流程脚本化 E2E 测试

状态: ⏸　依赖: T063,T050　验证: `cargo test -p main-bin --test e2e_mock_full_flow` + `cargo test --workspace`　类型: 测试（产品码预期 0）

## 裁定 R30（管辖本卡）
T100 = **装配后 router 的端到端（全链路）脚本化测试**，测试行数豁免。必须从**唯一的 rpc 闭包**入口进入（parse → 命令表 → handler → 服务层 → 事件总线回推 → 序列化响应），不得直接调用 handler 函数或服务方法，从而钉住「前后端之间」的跨域契约：命令名、载荷形状、响应形状、事件名与顺序、失败不污染全局状态。

## 为什么需要这张卡
阶段 2–5 已为每张命令表写了单元/域内测试（`webview-bridge/tests/router_cluster.rs`、`router_consume.rs`、`router_topic_produce.rs`、`router_group.rs`、`router_mock.rs`、`router_core.rs`、`event_pump.rs`，以及 `main-bin/tests/bridge_glue.rs`），但**没有任何一条测试把「用户完整旅程」串起来跑过装配后的全链**。本卡补上这条基线，并作为后续真实 broker 组（T070+）的对照。

## 位置与复用设施（已核实，勿重复推导）
- 新文件：`crates/main-bin/tests/e2e_mock_full_flow.rs`。
  - 原因：只有 `main-bin` 装配了全部六张命令表与 `App`（`webview-bridge` 只是 handler 集合，无装配后的 App）。
  - 复用 `bridge_glue.rs` 的同一手法：`#[allow(dead_code)] #[path = "../src/glue.rs"] mod glue;`，然后 `use glue::Assembly;`。
- `Assembly { pub rt: Runtime, pub app: App, pub router: Router }`；`Assembly::new(store: Store) -> anyhow::Result<Self>`（in-memory store 用 `Store::open_in_memory()`）。
- `Assembly::rpc(&self) -> RpcHandler` = `Arc<dyn Fn(String) -> String + Send + Sync>`，内部 `handle.block_on(router.handle(raw))` —— **这就是 WebView 实际走的路径**。
- 请求信封 `{id,cmd,payload}`（`payload` 缺省为 `null`）；响应 `{id,ok,data?,error?,elapsedMs?}`。
- 事件：`asm.app.bus().subscribe() -> BusReceiver`（`recv().await` / `try_recv()`）；`AppEvent::event_name()` / `payload_json()`。测试可在自己的 runtime 上以 `asm.rt.block_on(async { tokio::time::timeout(.., rx.recv()).await })` **有界等待**事件。
- mock 集群夹具（照 `bridge_glue.rs::mock_config`）：
  `ClusterConfig { id: ClusterId::new(), name, bootstrap_servers: vec!["mock".into()], mock: true, ..Default::default() }`，经 `cluster.save`（请求体即 `ClusterConfig` JSON）保存，再 `cluster.connect {"clusterId": id}`。
- mock 泵计数（可选断言）：`cluster.driver(&id)` 得 `DynDriver` → `as_any().downcast_ref::<MockKafkaDriver>()` → `state_handle().read().await.active_pumps()`。

## 流程脚本（至少覆盖以下 11 步，全部经 rpc 闭包）
1. `cluster.list`（空）→ `cluster.save`(mock) → `cluster.list` 恰 1 行，`state == "disconnected"`。
2. `cluster.connect {clusterId}` → 事件 `connection.state`，`state == "connected"`。
3. `topic.create {clusterId,name,numPartitions,replicationFactor}` → `topic.list` 含该 topic → 事件 `topic.changed`（`change == "created"`）。
4. `produce.send {clusterId, requests:[{topic, messages:[...]}]}` → `data.okCount == n`，`data.results[]` 含 `offset`/`partition`/`timestamp`（钉住结果形状）。
5. `consume.subscribe {clusterId, topic, start, follow:true}` → 收 `message.received`（`message` 的 value 字节与 produce 输入一致）＋ `subscription.state == "running"`。
6. `consume.pause {subscriptionId}` → `subscription.state == "paused"`；`consume.resume` → `"running"`；`consume.seek {subscriptionId,topic,partition,offset}` → ok 且不 panic。
7. `consume.unsubscribe {subscriptionId}` → `subscription.done` **恰好一次** ＋ `subscription.state == "done"`。
8. `group.list {clusterId}` → ok（mock 返回数组）。
9. `topic.delete {clusterId, names:[topic]}` → 事件 `topic.changed`（`deleted`）→ `topic.list` 不含该 topic。
10. `cluster.disconnect {clusterId}` → `connection.state == "disconnected"`；`cluster.delete {clusterId}` → `cluster.list` 空。
11. 收尾不变量：`asm.app.consume().active_count() == 0`；在 `cluster.delete` **之前**断言 `active_pumps() == 0`（删除后无法再取 driver）。

## 断言纪律
- 唯一入口是 rpc 闭包（保证走全链 parse→table→handler→serialize）。
- 事件用**有界等待**（`timeout(2s, …)` 或等价轮询截止），禁无界 `sleep` 死等；每条断言带上下文（打印实际响应/事件）。
- 允许拆成 1–3 个 `#[test]`（如「装配与集群生命周期」「topic+produce+consume 全链」「清理与不变量」），但必须是**确定性**的，重复运行稳定。
- 禁 `#[ignore]`；禁为过测而放宽断言。

## 边界与预算
- **产品码 0 文件 / 0 行**（预期）；测试文件行数豁免。
- 不得改动 `Assembly`/`glue.rs`/任何 handler 表/既有测试。若确需暴露一个已存在的 `pub` 访问器，≤1 文件 ≤20 行并在报告中说明理由。
- 无新依赖、无新命令名、无新事件名、不碰 `frontend/**`。

## 验证
- `cargo test -p main-bin --test e2e_mock_full_flow`（全绿）
- `cargo test --workspace`（基线 259 → 报精确新总数）
- `cargo clippy --workspace --all-targets -- -D warnings`（零警告）
- 已知环境 flake：LNK1104 / `os error 5`（杀软）→ 用 `--release` 重跑并说明。
