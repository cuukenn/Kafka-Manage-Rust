# T041 — 应用外壳：布局 / 导航 / 主题切换 / 状态栏

状态: ⏸　依赖: T040　验证: 手动（用户窗口）+ 自动（build/回归/冒烟）　类型: 前端

## 目标

把 T040 的最小 App 升级为可导航的应用外壳：左侧导航 + 顶栏状态 + 主题切换；为 T042–T049 各页预留挂载位。**不写 vue-router**（R19：导航 = UI store 里的 view 状态切换，YAGNI）。

## 必须覆盖

1. **布局**：左侧栏（导航项：集群 / 消费组 / 日志——对应页 T042/T048/T049 未建前显示占位视图）+ 顶栏（应用名、连接状态徽标、主题切换钮）+ 主区（当前 view）。Tailwind 实现，深浅两态都成立。
2. **导航**：`src/store/ui.ts`（reactive store：currentView、theme）；导航点击切 view；当前项高亮。
3. **主题**：深/浅切换即时生效（html class 或 data 属性 + Tailwind dark:）；经 `config.set`/`config.get`（T033 已有 wire 命令，键名如 `ui.theme`）持久化——重启窗口后主题保持。
4. **状态栏徽标**：挂载时 `cluster.list` 判定是否有已连接集群（连接态字段以 DTO 实况为准）；并经 events.ts 订阅集群相关事件（事件名以 kafka-core `protocol.rs` events 常量实况为准，实现者窄域 grep 确认）刷新徽标：未连接/已连接(集群名)。
5. **回归**：`npm run build` 成功；`cargo build -p main-bin` + `cargo test --workspace` 全绿（不改 Rust）；冒烟窗口存活+干净退出。

## 读改

`frontend/src/**`（App.vue 重构 + store/ui.ts + 外壳组件）、`frontend/dist/**`（重建入库）、本卡/docs。**不改 Rust**。

## 预算（R16/R19）

src 功能代码文件 ≤3（如 App.vue、store/ui.ts、components/Shell.vue——组件粒度自定但文件数≤3）、新增 ≤250 行；无 router 库、无新依赖（除非 Tailwind dark mode 需要一行配置，配置文件豁免）。

## 验证

- 自动：build/回归/冒烟（同上）。
- 手动（用户）：导航切换有视图变化；主题切换即时+重启保持；跑过 T039 连接流程后徽标显示已连接（或事件驱动刷新）。

## 偏差与风险

- 集群连接态字段/事件名以实况为准——卡片不猜，实现者 grep 核实并在报告注明。
- 若 T040 选了 IIFE 打包，多文件组件仍走 Vite 构建，无额外影响。
