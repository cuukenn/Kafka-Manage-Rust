# T046a — 消费面板：订阅 + 实时消息表格

状态: ⏸　依赖: T045　验证: npm build + cargo build + 冒烟（R23）　类型: 前端
拆分说明: R22 口径预拆；暂停/恢复/seek 控制在 T046b（须先核实 wire 存在）。

## 范围（只做这些）

1. **订阅表单**（views/ConsumeView.vue）：topic 下拉（当前集群 topic.list）；起点选择 earliest/latest/指定 offset（数字输入，仅指定时启用）——起点线形状是相邻标签 `{"type":"earliest"}` / `{"type":"offset","offset":N}`（以 grep 为准）；follow 开关。
2. **订阅**：`consume.subscribe` → 返回 subscriptionId（存组件/store 状态）；重复订阅防护（先退订再订或禁用按钮）。
3. **实时表格**：消息经事件推送到达（**事件名与 payload 必须 grep protocol.rs 事件常量核实**，勿猜）；表格列：partition/offset/key/value(utf8 可空则显示 base64 或字节提示)/timestamp；新消息插入顶部或底部（择一，滚动稳定）；上限 500 行截断（防内存）。
4. **退订**：页面离开/手动「退订」→ `consume.unsubscribe {clusterId, subscriptionId}`（防泄漏订阅）。
5. 入口：导航「消费」项（ui store 单源）。

## 线事实（窄 grep，limit≤15）

- CONSUBSCRIBE_/CONSUME_ 命令常量：grep protocol.rs；subscribe/unsubscribe payload：grep router_consume.rs 按 key 逐个；OffsetStart 相邻标签形状：grep kafka-core/src/message.rs `OffsetStart` -A 8。
- 消息推送事件名+payload：grep protocol.rs `event::` 区 + handlers_consume.rs 发送处（limit 15）。

## 边界与预算

src 文件 ≤3（ConsumeView.vue、可能 useConsume 小组件、ui.ts 接线），≤250 行；CSS/配置/dist 豁免；无新依赖；**不改 Rust**；dist 重建提交。

## 验证（R23）

npm build + cargo build -p main-bin + 冒烟。窗口手动项（批量）：T045 发的消息在订阅(earliest)后出现在表格；离开页面后 Rust 日志无泄漏订阅（unsubscribe 生效）。
