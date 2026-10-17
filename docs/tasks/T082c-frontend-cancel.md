# T082c — 前端接线：`AbortController` + 顶栏取消按钮

状态: ✅　依赖: T082b　验证: `npm run build` + 手动　类型: 产品码

> 原 T082 超 250 行上限，拆成 **T082a（全局超时）✅**、**T082b（取消后端）✅**、
> **T082c（本卡，前端）**。

## 目标

T082b 把「取消在途请求」做成了 Rust 侧的命令，但前端还没人按那个按钮，而且
`rpc.ts` 的 `fetch` 没有 `signal`：请求一旦发出，页面只能干等。本卡把两半接起来 ——
每个在途请求登记一个 `AbortController`，顶栏在 `pending.count > 0` 时出现「取消」按钮，
按下就 `abort()` 本地 fetch 并向 Rust 补发 `request.cancel`。

## 只读白名单（最多 3 个源文件）

- `frontend/src/bridge/rpc.ts` — `pending` / `rpcInner` / `RpcError`
- `frontend/src/components/Shell.vue` — 顶栏 spinner 与主题按钮
- `crates/webview-bridge/src/router.rs` — `request.cancel` 的收发形状（只读）

## 要改的文件

- 修改 `frontend/src/bridge/rpc.ts` —— 在途表 + `cancelInflight()` + `AbortSignal` + `cancelled` 错误
- 修改 `frontend/src/components/Shell.vue` —— 顶栏「取消」按钮与提示 toast

## 必须覆盖

1. 每个请求用**自己的信封 id** 登记 `AbortController`，请求结束（成功/失败/被取消）都要
   从表里摘掉，不泄漏。
2. `cancelInflight()` 逐个 `abort()`，并向 Rust 补发 `request.cancel`（payload `{id}`），
   返回丢掉的数量；补发是 best-effort，失败不影响本地取消。
3. 被 abort 的 `fetch` 抛 `AbortError` → 归类成 `kind === 'cancelled'`（不可重试），
   而不是误报成 `network` 失败。
4. 顶栏只在 `pending.count > 0` 时显示「取消」；按下后 toast 报出丢掉的数量。
5. `npm run build` 通过，dist 重建并 `git add -f`。

## 明确不做

- 不改 Rust 侧（T082a/T082b 已收口）；不做「单个请求取消」的每行按钮。
- 不取消事件泵/订阅类长连接（`consume.subscribe` 有自己的 `unsubscribe` 命令）。
- 不给 `RpcError` 加 `retry` 按钮（本轮不做重试 UI）。

## 已知取舍（Ruling）

- **R85** 取消要**两处一起做**：本地 `abort()` 让页面立刻脱身，`request.cancel` 让 Rust 侧
  真的停手。只做前者（旧行为）Rust 仍在跑，只做后者页面仍在等。代价：两次请求共用一次
  用户动作，若 Rust 侧 `request.cancel` 因故没到，处理器会跑完（结果被丢弃）。
- **R86** 取消是**全量**的（顶栏一个按钮丢掉所有在途请求），不做逐请求取消。RPC 面板的
  28 个调用点都是「一个视图一次刷新」的粒度，全量已经够用，而逐请求取消要给每个
  `rpc()` 调用点传句柄。代价：批量刷新时想只留一个也做不到。

## 验收

- `npm run build` 通过（Node 22），dist 重建
- `npx tsc --noEmit` 无**新增**报错（仓库既有的 `src/main.ts` 找不到 `App.vue` 一条不算，见 T081b）
- 手动：触发一次慢请求（大 topic 列表 / 未连接的集群），顶栏出现「取消」，
  按下后 spinner 归零并提示「已取消 N 个在途请求」（登记进 `STAGE4-manual-checklist.md`）

## 结果（完成后填写，≤5 行）

- `frontend/src/bridge/rpc.ts` +46/-1：新增在途表 `inflight: Map<id, AbortController>` 与
  `cancelInflight()`（逐个 `abort()` + 补发 `request.cancel`，返回数量）；`rpcInner` 拆成
  「登记 id/controller → `rpcSend(id, cmd, payload, signal)` → `finally` 摘表」；`fetch` 带
  `signal`，`AbortError` 归类为 `RpcError('cancelled', ..)` 而非 `network`（R85/R86）。
- `frontend/src/components/Shell.vue` +19/-1：`onCancel()` 调 `cancelInflight()` 并 toast
  报数；顶栏 spinner 旁在 `pending.count > 0` 时出现琥珀色「取消」按钮。
- 验证：`npm run build` 通过（Node 22.23.3，48 modules，`dist/assets/index-aSBBEGd_.js` 173.39 kB），
  `git add -f frontend/dist`；`npx tsc --noEmit` 仍只有既有的 `App.vue` 一条，无新增。
- 预算 65 行/2 文件。手动项登记为 `STAGE4-manual-checklist.md` 第 24 条。
