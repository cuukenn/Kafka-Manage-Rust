# T081b — 前端接上 `error.hint`

状态: ✅　依赖: T081　验证: `npm run build` + 手动　类型: 产品码

## 目标

T081 把 `ErrorDto.hint` 补成了穷尽表，但审计发现它**到不了用户眼前**：`crates/kafka-core`
会把它序列化进 `error` 对象，而 `frontend/src/bridge/rpc.ts` 在构造 `RpcError` 时只取
`kind`/`message`/`retryable`，`hint` 当场丢掉；22 个 `catch` 点都只显示 `e.message`。
本卡把 hint 接上，让 T081 的建议真正可见。

## 只读白名单（最多 3 个源文件）

- `frontend/src/bridge/rpc.ts` — `RpcErrorBody` / `RpcError` / `rpcInner`
- `frontend/src/store/toast.ts` — toast 文本渲染（只读，确认多行可显示）
- `crates/kafka-core/src/protocol.rs` — `ErrorDto`（只读，确认线上字段名 `hint`）

## 要改的文件

- 修改 `frontend/src/bridge/rpc.ts` —— `RpcErrorBody` 与 `RpcError` 带上 `hint`，
  显示文案 = `message` + 换行 + `hint`

## 必须覆盖

1. `error.hint` 存在时，`RpcError.hint` 拿到它，且 **`RpcError.message` 里也包含它**
   —— 22 个 catch 点只读 `message`，这样一处改动就覆盖全部错误展示位。
2. `error.hint` 缺失时，`message` 与改动前**逐字一致**（不凭空多出一行空提示）。
3. 非信封类失败（`network`/`http`/`protocol` 三个本地构造点）不受影响，`hint` 为 `undefined`。
4. `npm run build` 通过，dist 重建并 `git add -f`。

## 明确不做

- 不给 Toast/表格加第二个 DOM 节点或样式（保持「一行文本」的既有渲染口径）。
- 不改 Rust 侧 hint 表（T081 已收口）；不改 `retryable` 的展示（本轮无「重试按钮」）。
- 不动 `kind`/`retryable` 的既有含义。

## 已知取舍（Ruling）

- **R79** hint 拼进 `message` 而不是新增字段给每个调用点分别渲染：22 个 `catch` 点写的是
  `e.message`，逐个改成双行渲染是 22 处改动且极易漏。代价：`message` 变成「两行显示串」，
  不再是纯粹的 driver 原文；结构化字段仍在 `RpcError.hint` 上单独保留，将来要分开渲染不用再回头改。
- **R80** 分隔符用换行 `\n`：Toast 与日志面板本来就有 `whitespace-pre-wrap`，表格里的 `<p>`
  会把换行折成空格，两者都不会出现多余的 `\n` 字样。代价：同一句话在不同位置的行数与折行不一致。

## 验收

- `npm run build` 通过（Node 22），dist 重建
- `cargo test -p kafka-core` 仍全绿（本卡不碰 Rust）
- 手动：让一次请求失败（例如未连接就刷新 topic 列表），错误文案出现第二行建议
  （登记进 `STAGE4-manual-checklist.md`）

## 结果（完成后填写，≤5 行）

- `frontend/src/bridge/rpc.ts` +17/-2（1 文件）：`RpcErrorBody` 增 `hint?: string`；`RpcError` 增
  `readonly hint?: string`，显示文案 = `message` + `\n` + `hint`（无 hint 时与改动前逐字一致）；
  `rpcInner` 把 `body.error.hint` 传进构造函数。
- 效果：22 个只读 `e.message` 的 catch 点（Toast、各视图的 inline 错误条）与日志面板的
  `rpc failed …` 摘要行，**一处改动全部带上建议**；`RpcError.hint` 仍单独保留字段备将来分元素渲染。
- 验证：`npm run build` 通过（Node 22.23.3，48 modules，dist/assets/index-Cl33y5Dg.js 172.31 kB），
  `git add -f frontend/dist`；`cargo test -p kafka-core` 9/9 仍全绿（本卡不碰 Rust）。
- 偏差/发现：本仓库**没有类型检查脚本**，`npx tsc --noEmit` 会报一条**既存**错误
  `src/main.ts(6,17): Cannot find module './App.vue'`（无 SFC shim、未装 vue-tsc），与本次改动无关；
  新增代码零 TS 报错。`npm run build`（vite，只剥类型）仍是当前唯一门禁。
- 手动项登记为 `STAGE4-manual-checklist.md` 第 23 条。
