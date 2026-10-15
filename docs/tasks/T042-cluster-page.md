# T042 — 集群页 + 集群编辑弹窗（Mock 开关、SASL/SSL 字段）

状态: ⏸　依赖: T041　验证: `npm run build` + cargo 全绿 + 冒烟；窗口手动项并入用户批量清单　类型: 前端

## 目标

把外壳的视图区填上第一个真实页：**集群管理**。

1. **集群列表**：进入集群视图时 `cluster.list` 拉取（返回 `ClusterView` 行：camelCase 展开的 config 字段 + `state` + `driver`，T041 已核实）。表格列：名称 / bootstrap / Mock 标记 / 连接状态(state 徽标) / driver / 操作。
2. **操作**：每行 连接 / 断开 / 删除 / 编辑；顶部「新建集群」。连接状态变化经 `connection.state` 事件刷新（复用 T041 的订阅路径）。
3. **编辑弹窗（新建/编辑共用）**：name、bootstrapServers（逗号分隔输入）、**Mock 开关**、clientId、securityProtocol 选择；SASL 区（mechanism/username/password/saslProtocol）、SSL 区（caPem/caPath/certPem/keyPem/verifyCertificate/verifyHostname）。保存走 `cluster.save`（payload = ClusterConfig 本体，camelCase，见 T039 卡探针）。
4. 删除前确认（简单 confirm 即可）；错误经 RpcError 显示（toast 缺位前用行内错误条，T049 统一）。

## 线事实（实现前必须窄 grep 核实，不得照抄本卡）

- `cluster.*` 全部命令名与 payload 形状：grep `crates/kafka-core/src/protocol.rs` 的 `CLUSTER_` 常量 + `crates/webview-bridge/tests/router_cluster.rs` 用例。
- `ClusterConfig` 完整字段与 serde 形状：`crates/kafka-core/src/cluster.rs`（camelCase + default；SASL/SSL 是 Option 嵌套）。
- 事件名以 protocol.rs 为准（T041 已核实 `connection.state`）。

## 边界与预算

- src 新文件 ≤3（建议 `views/ClustersView.vue`、`components/ClusterModal.vue`、`store/clusters.ts`），新增 ≤250 行；CSS/配置/dist 豁免；无新依赖；**不改 Rust**；dist 重建并提交。
- 导航入口：T041 的 ui store currentView 增加 'clusters' 路由项（改 Shell.vue/App.vue 计入 3 文件预算——若超，允许把导航项定义挪进 store/ui.ts 单源）。
- 弹窗表单直接映射 ClusterConfig；未填字段留默认（serde default 兜底），不做过度校验。

## 验证

1. `npm run build`；2. `cargo build -p main-bin`；3. `cargo test --workspace` 239 全绿（不改 Rust）；4. clippy 零；5. 冒烟 8s + CloseMainWindow 干净退出。
窗口手动项（用户批量清单）：新建 mock 集群→连接→徽标变 connected→断开→删除。
