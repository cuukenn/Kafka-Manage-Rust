# T045 — 生产面板（key/value/headers/partition + JSON 编辑器）

状态: ⏸　依赖: T044　验证: npm build + cargo build + 冒烟（R23）　类型: 前端

## 范围

1. **生产视图**（views/ProduceView.vue）：目标 topic 下拉（当前集群 topic.list）；key/value 输入（UTF-8 文本；**线形状是字节数组**——前端把字符串转 `Array.from(new TextEncoder().encode(s))`，key 可空）；partition 可选数字；headers 键值行增删。
2. **JSON 编辑器模式**：value 区可切「文本 / JSON」；JSON 模式 = textarea + `JSON.parse` 校验（红字提示，不引第三方编辑器），通过后按格式化后的紧凑 JSON 发送。
3. **发送**：`produce.send`（批量形状 `{clusterId, requests:[{topic, messages:[...]}]}`，R6 既定）；结果区逐条显示 offset/partition 或错误（部分失败 UX 同 T044a）。
4. 入口：导航加「生产」项（ui store 单源）。

## 线事实（窄 grep，limit≤15，不得照抄本卡）

- ProduceMessage/ProduceReq 字段与 serde 形状：grep crates/kafka-core/src/message.rs `pub `（Vec<u8> 是**数字数组**线形状，无 base64 自定义 serde——核实有无例外）。
- MessageHeader 形状：grep 同文件 `MessageHeader` -A 6。
- 响应逐条回执形状：grep router_topic_produce.rs `offset`（limit 10）。

## 边界与预算

src 文件 ≤3（ProduceView.vue、可能 ProduceForm 小组件、ui.ts 接线），≤250 行；CSS/配置/dist 豁免；无新依赖；**不改 Rust**；dist 重建提交。

## 验证（R23）

npm build + cargo build -p main-bin + 冒烟。窗口手动项（批量）：建 topic→生产 2 条（1 条 JSON 模式）→回执见 offset→消费面板验证到达（T046 后）。
