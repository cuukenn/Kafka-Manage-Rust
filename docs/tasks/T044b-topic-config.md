# T044b — Topic 配置编辑

状态: ⏸　依赖: T044a　验证: npm build + cargo build + 冒烟（R23）　类型: 前端

## 范围

1. 行内「配置」入口：拉取 topic 配置条目（若 wire 有 `topic.config` 类命令——**必须先 grep protocol.rs TOPIC_/CONFIG_ 常量核实**；若配置条目在 `topic.describe` 的 TopicDetail 里则读那里）。
2. 编辑弹窗或行内编辑：key/value 列表增删改 → 保存走核实到的 wire 命令（grep router_topic*.rs 用例定 payload 形状）。
3. 若 wire 根本不支持 topic 配置修改（只有 describe 只读），**不要造命令**：返回 BLOCKED 说明，控制器登记后端缺口卡（T0xx）先补 Rust。

## 边界与预算

src 文件 ≤2（改 TopicsView.vue + 新 TopicConfigModal.vue），≤250 行；CSS/配置/dist 豁免；无新依赖；**不改 Rust**；dist 重建提交。

## 验证（R23）

npm build + cargo build -p main-bin + 冒烟。窗口手动项（批量）：改一个配置项→重开弹窗见新值。
