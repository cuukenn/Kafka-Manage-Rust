# T083b — 日志面板导出

状态: ✅　依赖: T083a　验证: `npm run build` + 手动　类型: 产品码

> 原 T083「日志导出与轮转」含前后端两半，按 250 行上限拆成 **T083a（落盘与轮转）✅** 与
> **T083b（本卡，面板导出）**。

## 目标

T083a 把日志写进了本机文件，但用户手上能带走的东西还只有屏幕上的 500 行。
本卡给日志面板加一个「导出」按钮：把**当前筛选后**的行按**最旧在前**写成一个 `.txt`，
用既有的 Blob + `a[download]` 机制下载（T047 已在消费页用过同一条路，WebView2 的下载
UI 本来就是清单里的手动项）。

## 只读白名单（最多 3 个源文件）

- `frontend/src/views/LogView.vue` — `filter` / `rows` / `fmtTime` / 现有按钮
- `frontend/src/utils/render.ts` — `downloadJson` 的 Blob 下载写法
- `frontend/src/store/logs.ts` — `LogEntry` 的字段（`ts` / `level` / `source` / `text`）

## 要改的文件

- 修改 `frontend/src/utils/render.ts` —— 抽出 `downloadText(filename, text, mime)`，`downloadJson` 改为委托
- 修改 `frontend/src/views/LogView.vue` —— 「导出」按钮 + `exportLogs()` + 时间戳文件名

## 必须覆盖

1. 导出的是**当前筛选后**的行：切到 `error` 再导出，文件里只有 error 行。
2. 顺序**最旧在前**：面板最新在上，日志文件要顺着时间读。
3. 每行 `[<ISO 时间>] <LEVEL> <source> <text>`，文件以换行结尾；文件名
   `kafka-logs-<YYYYMMDD-HHmmss>.txt`。
4. 没有可导出的行时按钮禁用（不生成空文件）。
5. `downloadJson` 行为不变：消费页「导出 JSON」仍是缩进 JSON、`application/json`。

## 明确不做

- 不写 Rust 侧落盘、不弹系统保存对话框（沿用 Blob 下载，与 T047 一致）。
- 不导出 T083a 的 `logs/app.log` 原文（那是进程日志，本卡导的是面板视图）。
- 不改 `store/logs.ts` 的环形缓冲与级别过滤语义（T049b/T080 已收口）。
- 不给导出加裁剪/分片（500 行上限本身就很小）。

## 已知取舍（Ruling）

- **R90** 导出「用户看到的」而不是「全部」：`rows` 就是筛选结果，导出与屏幕一一对应，
  用户不必再猜。代价：想导全量必须先把级别切回「全部」。
- **R91** 时间用 `toISOString()`（UTC、带日期）而不是面板上的 `toLocaleTimeString`：
  面板是「现在发生了什么」用本地时分秒就够，文件是「事后对时间线」，需要无歧义的
  全量时间戳。代价：文件里的时间与屏幕上的列看起来不同（UTC vs 本地）。
- **R92** 复用 `downloadJson` 的 Blob + `a[download]` 路径（抽成 `downloadText` 共享）：
  不再新增一套下载机制，也不动 Rust 侧。代价：下载位置与「是否弹下载 UI」由 WebView2
  决定，仍然只能靠清单人工确认。

## 验收

- `npm run build`（Node 22）通过，dist 重建并 `git add -f`
- `npx tsc --noEmit` 无**新增**错误（仓库既有的 `main.ts → ./App.vue` 一条不算）
- 手动：日志面板点「导出」→ 下载得到 `kafka-logs-*.txt`，内容与面板（含筛选）一致、
  最旧在前（登记清单第 26 条）

## 结果（完成后填写，≤5 行）

- `render.ts` +8/-3：抽 `downloadText(filename, text, mime = 'text/plain')`（Blob + `a[download]`），
  `downloadJson` 改为委托它，行为与改动前逐字一致（消费页「导出 JSON」未受影响）。
- `LogView.vue` +25：新增「导出」按钮（`rows.length === 0` 时禁用）与 `exportLogs()` / `stamp()`；
  导出 `rows`（**当前筛选**，R90）反转成**最旧在前**（R91），每行 `[ISO] LEVEL source text`、
  结尾换行，文件名 `kafka-logs-<YYYYMMDD-HHmmss>.txt`。
- 验证：`npm run build`（Node 22.23.3，48 modules，`dist/assets/index-CatKWlYJ.js` 174.23 kB）通过，
  `git add -f frontend/dist`；`npx tsc --noEmit` 仅剩仓库**既有**的 `src/main.ts(6,17)`
  `Cannot find module './App.vue'`（无 SFC shim / 未装 vue-tsc，见 T081b 记录），新增代码零报错。
- 预算：33 行 / 2 文件。手动项登记为 `STAGE4-manual-checklist.md` 第 26 条。
