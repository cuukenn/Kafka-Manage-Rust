# T048a — 消费组页：列表 + 详情（成员/位点/lag）

状态: ⏸　依赖: T044　验证: npm build + cargo build + 冒烟（R23）　类型: 前端
拆分说明: R22 口径预拆；位点重置弹窗在 T048b。

## 范围（只做这些）

1. **GroupsView.vue**：当前集群的消费组表格——组名/状态/成员数（列以 wire DTO 为准，grep 核实）。数据源 `group.list {clusterId}`（命令名 grep protocol.rs GROUP_ 常量核实）。
2. **行点击详情**（页内展开或右侧面板，勿开新页）：成员表（memberId/clientId/host/分配分区，按 DTO）+ 位点表（topic/partition/offset/lag，lag=端 offset−消费 offset，若 wire 直接给 lag 用 wire 的）+ 订阅信息（若 DTO 有）。
3. **刷新**：进入拉一次 + 手动刷新按钮；无组事件推送（事件只有 connection.state/topic.changed/message.received/subscription.*——不监听组事件）。
4. 空态/错误条与既有页一致；导航「消费组」项（ui store 单源）。

## 线事实（窄 grep，limit≤15）

- GROUP_ 命令常量：grep protocol.rs；payload 键：grep router_group.rs 按命令逐个；DTO 字段：grep kafka-app services/groups.rs 或 kafka-core group.rs 结构体（camelCase serde 已确立）。
- 详情命令名（group.describe? group.members? group.offsets?）以 grep 为准，勿猜。

## 边界与预算

src 文件 ≤3（GroupsView.vue + 可选 GroupDetail 组件 + store/groups.ts），≤250 行；CSS/配置/dist 豁免；无新依赖；**不改 Rust**；dist 重建提交。

## 验证（R23）

npm build + cargo build -p main-bin + 冒烟。窗口手动项（批量）：mock 集群 group.list 见预置组（T036 测试用 seed 组 checkout-service/analytics-etl——注意那是测试 seam，wire 空 mock 无预置组；手动项=先 consume.subscribe 产生组? mock 订阅是否注册组以 wire 为准——若无法产生组，空态即验证）。
