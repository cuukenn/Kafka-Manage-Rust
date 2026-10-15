# T039 — 端到端冒烟（Mock）：连接→列 topic→发消息→收事件

状态: ⏸　依赖: T031, T038b　验证: 手动清单全过（用户执行）　类型: 手动

本卡是**用户手动验收清单**；代理只准备清单并据用户反馈更新状态。全部条目通过才记 ✅。

## 前置

`cargo build -p main-bin` 成功；数据目录（`%LOCALAPPDATA%\rs-kafka-ui-client` 或 dirs 等价物）可写。

## 手动清单（在窗口 devtools Console 逐条执行）

1. **窗口**：`cargo run -p main-bin` → 窗口打开，显示占位页 `shell OK`，标题 `rs-kafka-ui-client`；F12/右键可开 devtools。（覆盖 T031 🔶 项）
2. **RPC 通路**（注意：Windows 上 wry 把 `app://` 页面重映射为同源 `http://app.localhost`，`fetch('app://…')` 会被 Chromium 拒绝——必须用**相对路径**）：
   `await fetch('/rpc',{method:'POST',body:JSON.stringify({id:'1',cmd:'cluster.list',payload:{}})}).then(r=>r.text())`
   → 返回 `{"id":"1","ok":true,...}`。（✅ 用户实测 2026-10-15）
3. **坏输入**：`await fetch('/rpc',{method:'POST',body:'{bad'}).then(r=>r.json())` → `ok:false` 且 `error.kind` 为 parse 类。
4. **连接 Mock 集群**（先定义 helper，后续条目直接复用）：
   ```js
   const rpc=(id,cmd,payload)=>fetch('/rpc',{method:'POST',body:JSON.stringify({id,cmd,payload})}).then(r=>r.json());
   await rpc('2','cluster.save',{id:'smoke-demo',name:'demo',bootstrapServers:['mock'],mock:true})
   await rpc('3','cluster.connect',{clusterId:'smoke-demo'})   // → ok:true
   await rpc('4','cluster.list',{})                            // demo 显示已连接
   ```
5. **建 topic+列 topic**：
   ```js
   await rpc('5','topic.create',{clusterId:'smoke-demo',name:'smoke-test',numPartitions:1,replicationFactor:1})
   await rpc('6','topic.list',{clusterId:'smoke-demo'})        // 出现 smoke-test
   ```
   注：`mock.seed` 只固定 RNG 种子（故障注入序列可复现），**不**播种预置 topic；全新 mock 集群连接后 `topic.list` 为空（fixture 播种仅测试内可用）。
6. **发消息**（value 是字节数组，`[104,105]`="hi"）：
   ```js
   await rpc('7','produce.send',{clusterId:'smoke-demo',requests:[{topic:'smoke-test',messages:[{value:[104,105]}]}]})
   // → ok:true, data.okCount=1, data.results[0].offset 有值
   ```
7. **收事件**（`follow:true` 保持订阅流；占位页钩子会 `console.log("[bridge]", batch)`）：
   ```js
   await rpc('8','consume.subscribe',{clusterId:'smoke-demo',topic:'smoke-test',start:{type:'earliest'},follow:true})
   await rpc('9','produce.send',{clusterId:'smoke-demo',requests:[{topic:'smoke-test',messages:[{value:[121,121]}]}]})
   ```
   → Console 出现 `[bridge]` 批量数组（含刚发的消息，base64+utf8 双通道）；批量节流生效（多条时一次数组 ≤200）。
8. **故障注入**：
   ```js
   await rpc('10','mock.faults',{clusterId:'smoke-demo',injection:{failProbability:1.0,failMessage:'boom'}})
   await rpc('11','produce.send',{clusterId:'smoke-demo',requests:[{topic:'smoke-test',messages:[{value:[120]}]}]}) // → ok:false（或 results[].error='boom'）
   await rpc('12','mock.reset',{clusterId:'smoke-demo'})       // 恢复
   ```
9. **快照持久（T038a）**：基于第 5 步创建的 topic：关窗 → 再开 → 连接同一集群，不重新 `topic.create`，`topic.list` 仍有该 topic（SQLite slot 恢复）。
10. **干净退出**：关窗后进程退出无挂起；stderr 日志见 pump/App 关闭记录；再启动正常。
    （✅ 控制器代验 2026-10-15：WM_CLOSE→`bridge shut down cleanly`→10s 内自然退出未强杀；用户可复核）

## 记录

用户逐条反馈后：`docs/TASKS.md` T039 行 → ✅/❌；失败项登记缺陷卡（T0xx）进 `docs/TASKS.md`。
