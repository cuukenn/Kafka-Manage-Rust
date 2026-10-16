# T047b — 消费表格搜索过滤

状态: ⏸　依赖: T047　验证: npm build + cargo build + 冒烟（R23）　类型: 前端（小卡）
来源: R25——原 T047 计划含「搜索过滤」，实现卡改口径后拆出本卡补齐。

## 范围（只做这些）

1. ConsumeView 表格上方过滤条：文本框 + 开关（匹配 value / 匹配 key / 大小写敏感）——语义对齐 kafka-core `SearchFilter`（query/match_value/match_key/case_sensitive，grep message.rs:191 区核实一次即可）。
2. **客户端过滤**已到达的 ≤500 行表格（computed 派生显示行；不改订阅、不发 wire 命令——wire SearchFilter 属订阅期过滤，若 grep 发现 subscribe 已支持 filter 键，仅登记事实，不在本卡接线）。
3. 过滤激活时显示「N/500」计数；清空恢复。

## 边界与预算

仅改 ConsumeView.vue（可抽 filter.ts），≤50 行；CSS/配置/dist 豁免；无新依赖；**不改 Rust**；dist 重建提交。

## 验证（R23）

npm build + cargo build -p main-bin + 冒烟。窗口手动项（批量）：订阅后输入 query 只见匹配行；勾选 key 匹配切换。
