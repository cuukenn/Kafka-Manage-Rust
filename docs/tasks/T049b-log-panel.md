# T049b — 日志面板（事件/RPC 环形缓冲视图）

状态: ⏸　依赖: T049a　验证: npm build + cargo build + 冒烟（R23）　类型: 前端

## 范围（只做这些）

1. **logs 存储**（store/logs.ts）：环形缓冲 500 条 `{ts, level, source:'event'|'rpc'|'ui', text}`；`record()`；清空。
2. **来源接线**：
   - events：events.ts 若无通配订阅则加 `onAll(cb)`（+≤10 行，R21 口径）——所有推送批次记一条（事件名+关键字段摘要，勿记整包 JSON）。
   - rpc：rpc.ts 在响应/异常处记 `{cmd, ok, kind?}` 摘要（+≤10 行）。
3. **LogView.vue**：导航「日志」；表格 时间/级别/来源/内容；级别过滤（全部/error/warn/info）；清空按钮；新条目顶部插入，500 截断。

## 边界与预算

src 文件 ≤4（logs.ts、LogView.vue 新；events.ts、rpc.ts 改），≤200 行；Shell 导航接线豁免口径同前卡；CSS/配置/dist 豁免；无新依赖；**不改 Rust**；dist 重建提交。

## 验证（R23）

npm build + cargo build -p main-bin + 冒烟。窗口手动项（批量）：订阅消费后日志面板出现 message.received 摘要行；坏输入 RPC 出现 error 行；清空生效。
