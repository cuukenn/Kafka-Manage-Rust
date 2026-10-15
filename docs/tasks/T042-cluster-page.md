# T042a — 集群页：列表 + 连接/断开/删除 + 导航接线

状态: ⏸　依赖: T041　验证: `npm run build` + `cargo build -p main-bin` + 冒烟（前端卡不跑 cargo test，见 R23）　类型: 前端
拆分说明: 原 T042 超上下文预算拆两卡（R22，先例 R10）。编辑弹窗在 T042b。

## 范围（只做这些）

1. **集群列表视图**：进入集群视图时 `cluster.list` 拉取，表格列：名称 / bootstrap / Mock 标记 / 连接状态(state 徽标，复用 T041 样式) / driver / 操作。
2. **行操作**：连接 `cluster.connect` / 断开 `cluster.disconnect` / 删除 `cluster.delete`（简单 confirm）。操作结果刷新行；错误行内错误条（toast 留给 T049）。
3. **事件驱动刷新**：`connection.state` 事件触发列表重拉（合并去抖，顺手修 T041 refresh-race minor）。
4. **导航接线**：currentView 增加 'clusters'（优先 store/ui.ts 数据驱动单源）。视图区暂以占位按钮「新建/编辑（T042b）」收尾。

## 线事实（窄 grep，limit≤15，禁读整 Rust 文件）

- `cluster.*` 命令名：grep protocol.rs `CLUSTER_`；payload：grep router_cluster.rs `json!` 或按 key 逐个 grep。
- ClusterView 行形状：grep kafka-app services/clusters.rs `ClusterView` -A 12。
- 事件名 connection.state 已由 T041 核实，复用 ui.ts 订阅路径。

## 边界与预算

src 文件 ≤3（建议 views/ClustersView.vue、store/clusters.ts、ui.ts 接线），≤250 行；CSS/配置/dist 豁免；无新依赖；**不改 Rust**；dist 重建提交（git add -f）。

## 验证（R23：前端卡精简）

1. `npm run build`；2. `cargo build -p main-bin`（嵌入新 dist）；3. 冒烟 8s + CloseMainWindow 干净退出。
（无 Rust 改动则不跑 cargo test/clippy——审查者以 diff 确认 Rust 零改动。）
