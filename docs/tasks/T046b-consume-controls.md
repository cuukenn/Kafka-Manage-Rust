# T046b — 消费面板：暂停/恢复/seek 控制

状态: ⏸　依赖: T046a　验证: npm build + cargo build + 冒烟（R23）　类型: 前端

## 第一门（写 UI 前）

核实 wire 是否有消费流控制命令：grep protocol.rs `CONSUBSCRIBE_`/`CONSUME_` 常量 + handlers_consume.rs 注册处。若只有 subscribe/unsubscribe（pause/resume/seek 无 wire 命令），**不要造命令**：BLOCKED 报告说明，控制器登记后端缺口卡先补 Rust。

## 范围（若 wire 存在）

1. 订阅激活时显示控制条：暂停/恢复按钮（状态徽标）、seek 到指定 offset（数字输入+确认，复用 OffsetStart 形状）。
2. 控制命令 payload 以 grep router_consume.rs 用例为准；失败行内错误条。
3. 暂停期间已到达消息保留在表格；恢复后继续追加。

## 边界与预算

src 文件 ≤2（改 ConsumeView.vue + 可能小组件），≤250 行；CSS/配置/dist 豁免；无新依赖；**不改 Rust**；dist 重建提交。

## 验证（R23）

npm build + cargo build -p main-bin + 冒烟。窗口手动项（批量）：follow 订阅→暂停→produce 新消息→表格不增长→恢复→增长→seek 到 0→重放。
