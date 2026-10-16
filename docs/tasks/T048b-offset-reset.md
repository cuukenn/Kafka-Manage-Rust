# T048b — 位点重置弹窗（按分区改 offset）

状态: ⏸　依赖: T048a　验证: npm build + cargo build + 冒烟（R23）　类型: 前端

## 第一门（写 UI 前）

核实 wire 是否有位点提交/重置命令：grep protocol.rs GROUP_ 常量 + handlers_group.rs 注册处（limit≤15）。若无（只有读命令），BLOCKED 报告说明，控制器登记后端缺口卡先补 Rust——**不要造命令**。

## 范围（若 wire 存在）

1. 详情位点表行内「重置」→ 弹窗（复用 ClusterModal 样式骨架）：每分区一行 offset 数字输入 + 特殊值 latest/earliest（若 wire 支持哨兵以 grep 为准，否则数字）。
2. 提交 payload 严格按 grep（命令名 + 键名 + 每分区结构）；成功→重拉详情；失败行内错误。
3. 危险操作确认（弹窗内二次确认条，不用 window.confirm——T044a 审查教训）。

## 边界与预算

src 文件 ≤2（ResetOffsetsModal.vue 新 + GroupsView/GroupDetail 改），≤250 行；CSS/配置/dist 豁免；无新依赖；**不改 Rust**；dist 重建提交。

## 验证（R23）

npm build + cargo build -p main-bin + 冒烟。窗口手动项（批量）：重置到 0 → 重订阅 earliest 见重放（或位点表变化）。
