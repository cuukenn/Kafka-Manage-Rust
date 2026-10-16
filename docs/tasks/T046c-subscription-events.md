# T046c — 消费徽标同步：订阅状态事件

状态: ⏸　依赖: T046b　验证: npm build + cargo build + 冒烟（R23）　类型: 前端（小卡）
来源: T046b 审查 Important（wire 已有 `subscription.state`/`subscription.done` 事件，前端未监听）。

## 范围（只做这些）

1. ConsumeView.vue 订阅激活期间监听 `subscription.state` 与 `subscription.done`（事件名以 grep protocol.rs:51-52 常量为准）。
2. payload 形状 grep handlers_consume.rs 发送处核实（勿猜）；state 事件驱动徽标 running/paused 翻转；done 事件→表格停止追加、控制条置灰、提示「订阅已结束」（有界/follow 读完自动停的服务器端真相）。
3. 监听器随组件卸载清理（与既有 events.ts on() 清理模式一致）。

## 边界与预算

仅改 ConsumeView.vue（+events.ts 若需），≤60 行；CSS/配置/dist 豁免；无新依赖；**不改 Rust**；dist 重建提交。

## 验证（R23）

npm build + cargo build -p main-bin + 冒烟。窗口手动项（批量）：follow=false 有界订阅读完 → 徽标自动变「已结束」无需操作。
