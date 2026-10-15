# T044a — Topic 表格 + 创建/删除

状态: ⏸　依赖: T043　验证: npm build + cargo build + 冒烟（R23）　类型: 前端
拆分说明: R22 口径预拆，配置编辑在 T044b。

## 范围（只做这些）

1. **Topic 表格视图**（views/TopicsView.vue）：当前集群（ui store 选集群，复用 clusters store）的 `topic.list` 表格：名称 / internal 标记 / 其余按 TopicInfo 实际字段（grep 核实）。树里点 topic 节点 → 视图区落到本页并高亮选中行（selectedTopic）。
2. **创建弹窗**：name、numPartitions、replicationFactor（serde default 兜底，不过度校验）→ `topic.create`。
3. **删除**：行内删除 + confirm → `topic.delete`；成功后列表与树缓存失效（tree store 已有 invalidateCluster 可复用则复用，窄读 tree.ts 接口即可）。
4. 错误行内错误条；`topic.changed` 事件触发列表刷新（复用 T043 的去抖/去重模式，窄读 tree.ts 模式而非整文件）。

## 线事实（窄 grep，limit≤15）

- TOPIC_ 命令名：grep protocol.rs；create/delete payload：grep router_topic*.rs 按 key 逐个；TopicInfo 字段：grep kafka-core/src/topic.rs `pub `（limit 20）。

## 边界与预算

src 文件 ≤3（TopicsView.vue、TopicModal.vue、ui.ts/接线），≤250 行；CSS/配置/dist 豁免；无新依赖；**不改 Rust**；dist 重建提交。

## 验证（R23）

npm build + cargo build -p main-bin + 冒烟 8s 干净退出。
窗口手动项（批量）：连接→建 topic→表格出现→树同步→删除→消失。
