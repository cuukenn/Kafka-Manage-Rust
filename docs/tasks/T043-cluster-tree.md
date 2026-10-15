# T043 — 集群树（topic→partition）+ 连接状态徽标

状态: ⏸　依赖: T042　验证: `npm run build` + cargo 全绿 + 冒烟；窗口手动项并入批量清单　类型: 前端

## 目标

在外壳侧栏（导航下方）加**集群树**：已连接集群 → topics → partitions 懒加载展开，节点带连接状态徽标（复用 T041 的 state 徽标样式）。

1. **树结构**：根=已保存集群（`cluster.list`），展开已连接集群 → `topic.list` 列 topics；展开 topic → `topic.describe` 列 partitions（partition 序号 + leader + ISR 数，按 DTO 实际字段）。未连接集群节点置灰、展开时提示先连接。
2. **懒加载**：仅在展开时拉取子层；结果缓存于 store，节点刷新按钮（或折叠再展开）强制重拉。
3. **事件驱动**：`connection.state` 变化更新集群节点徽标与可展开性；topic 相关事件（若 protocol.rs 有 topic.created/deleted 类事件名，以 grep 为准）使对应集群的 topics 缓存失效。
4. **选中联动**：点 topic 节点 → ui store 记录选中（`selectedTopic`），T044+ 的 topic 表格消费它；本卡只需存状态并在视图区显示占位标题。

## 线事实（实现前必须窄 grep 核实，不得照抄本卡）

- `topic.list` / `topic.describe` payload 与响应 DTO：grep `crates/kafka-core/src/protocol.rs` 的 TOPIC_ 常量与 `TopicDetail`/partition 行字段 + `crates/webview-bridge/tests/router_topic*.rs` 用例。
- topic 相关事件名：grep protocol.rs 事件常量区。
- 复用 T042 已建立的 store/视图约定（读 T042 落地后的 store/clusters.ts、views/ClustersView.vue 小改即可）。

## 边界与预算

- src 文件 ≤3（建议 `components/TreeView.vue`、`store/tree.ts`、接线改 `Shell.vue` 或 ui.ts 单源），新增 ≤250 行；CSS/配置/dist 豁免；无新依赖；**不改 Rust**；dist 重建并提交。
- 树不做拖拽/虚拟滚动（YAGNI，规模小）。

## 验证

1. `npm run build`；2. `cargo build -p main-bin`；3. `cargo test --workspace` 239 全绿；4. clippy 零；5. 冒烟 8s + CloseMainWindow 干净退出。
窗口手动项（批量清单）：连接 mock 集群→树展开见 topics→展开 topic 见 partitions→断开后节点置灰。
