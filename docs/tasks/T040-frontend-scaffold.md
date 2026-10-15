# T040 — 前端资源骨架：Vite+Vue3+TS+Tailwind 脚手架 + rpc/events 桥模块

状态: ⏸　依赖: T039 ✅　验证: `npm run build` + `cargo run -p main-bin` 显示 UI 无 404（用户手动）　类型: 前端脚手架

## 目标

`frontend/` 建立 Vite + Vue3 + TS + Tailwind 工程（PLAN.md 钉死；依赖经本地 npm 安装，运行时零 CDN），构建输出 `frontend/dist/`（入库，R17）。桥接层两个模块 + 一个最小 App 证明 RPC/事件全链路。

## 必须覆盖

1. **脚手架**：`frontend/package.json`（scripts: dev/build）、`vite.config.ts`（build.outDir=dist，base 相对或 `/`——页面从 http://app.localhost 同源加载，资源 URL 必须同源可达）、tsconfig、Tailwind 生效（任一元素用 tailwind class 且样式正确）。
2. **`src/bridge/rpc.ts`**：`rpc(cmd, payload): Promise<data>`；POST `/rpc`（**相对路径**，R14）；请求 `{id,cmd,payload}`，id 自增/uuid；`ok:false` 时抛 `RpcError{kind,message,retryable}`；类型 camelCase 对齐 kafka-core DTO。
3. **`src/bridge/events.ts`**：`index.html` 内联脚本先定义 `window.__bridgeEvents = b => (window.__bridgeQueue ??= []).push(b)`（Rust 可能在模块加载前推批）；events.ts 加载后**drain 队列**再挂订阅者 API（`on(event, handler)`），信封 `{event,payload,ts}`，批量数组逐条分发。
4. **App.vue（最小）**：挂载时 `rpc('cluster.list',{})` 渲染结果（ok → 列表/空态文案；err → kind+message）；一个「重试」按钮；显示一个 tailwind 样式元素。外壳/导航留给 T041。
5. **构建嵌入链**：`npm run build` → `frontend/dist/`（入库）→ `cargo build -p main-bin` 的 include_dir 自动嵌入新 dist；`cargo run -p main-bin` 窗口显示 Vue 应用（不再是占位页）。
6. **回归**：`cargo test --workspace` 全绿——若有测试断言占位页文本（"shell OK"），同步更新该测试（测试文件豁免）。

## 读改

`frontend/**`（新，豁免）、`frontend/dist/**`（构建产物入库，豁免）、`docs/**`；若第 6 条触发：相关测试文件（豁免）。**不改 Rust 源码**。

## 预算（R16）

脚手架/配置（package.json、vite.config.ts、tsconfig*、tailwind/postcss 配置、main.ts、index.html）不计预算；src 功能代码文件 ≤3（rpc.ts、events.ts、App.vue）、新增 ≤250 行。

## 验证

- 自动：`npm install && npm run build` 成功；`cargo build -p main-bin` 成功；`cargo test --workspace` 全绿；控制器冒烟（窗口进程存活、stderr 无 error）。
- 手动（用户）：`cargo run -p main-bin` → 显示 Vue 应用；devtools Network **无 404**；cluster.list 结果渲染；tailwind 样式生效。

## 偏差与风险

- Tailwind 大版本（v3 config 式 vs v4 CSS 式）以当前 npm 实况为准；允许一次窄域文档查询。
- dist 入库带来 churn（R17 已接受）。
- T090 被本卡吸收（R18）：验收后 T090 行记 ✅(absorbed)。
