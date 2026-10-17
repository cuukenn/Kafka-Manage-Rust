# T106 — 切 tab 不再中断消费订阅

状态: ✅　依赖: 无　验证: 手动（npm run build + 切 tab 观察订阅仍在）　类型: 产品码

## 目标

用户报「订阅切换 tab 就不订阅了」：在消费页订阅、切到别的 tab 再回来，订阅已经没了。

## 根因（确定性因果链）

`frontend/src/components/Shell.vue` 的主视图区把各视图串在 `ui.currentView` 的 `v-if / v-else-if` 上，
切走 `consume` 就**卸载** `ConsumeView`；而它在 `onBeforeUnmount`（`ConsumeView.vue:291`）里
`void unsubscribe()`，原注释写着「leave page -> no leaked subscription」。于是「看一眼别的 tab」
被当成了「离开页面」，订阅被前端主动取消——等价于用户点了「取消订阅」。

## 只读白名单（最多 3 个源文件）

- `frontend/src/store/ui.ts` — `currentView` 路由（无 vue-router，R19）
- `frontend/src/bridge/events.ts` — handler 注册/注销与批次分发
- `frontend/src/views/ConsumeView.vue` — 订阅生命周期

## 要改的文件

- 修改 `frontend/src/components/Shell.vue` — `ConsumeView` 移出 `v-if` 链，改由 `<KeepAlive>` 包裹
- 修改 `frontend/src/views/ConsumeView.vue` — 增加 `onActivated`，订正 `onBeforeUnmount` 注释

## 必须覆盖

1. 切到别的 tab 再切回，订阅仍在（不再产生 `consume.unsubscribe`）。
2. 重新进入消费页时主题列表仍然刷新（`onActivated` → `loadTopics`），避免 KeepAlive 导致的列表陈旧。
3. 应用关闭时订阅依然卸掉（`onBeforeUnmount` 只在 teardown 触发，`unsubscribe` 保留）。

## 明确不做

- 不改后端/router 的订阅语义：`consume.unsubscribe` 仍然是真的取消。
- 不给其他视图加 KeepAlive：它们靠 `onMounted` 重新拉数据，常驻会让列表变陈旧（另案）。
- 不做多订阅并存的后台管理面板。

## 已知取舍（Ruling）

- **R115** 用 `<KeepAlive>` 而不是把订阅状态提到 store：视图缓存是最小改动，代价是订阅寿命
  与组件寿命绑定；将来做「多订阅并存」必须重构成 store。
- **R116** 用 `onActivated` 刷新主题列表：KeepAlive 让 `onMounted` 只跑一次，不加的话在主题页
  新建的 topic 切回消费页会看不见。代价是首次进入多跑一次 `loadTopics`（幂等只读）。
- **R117** 不把 KeepAlive 泛化到其他视图：本卡只修「订阅被误杀」这一条，泛化要一并引入
  激活刷新，超出 3 文件/250 行预算。

## 验收

- `npm run build`（Node 22 / vite 7）通过
- 改动 ≤250 行且 ≤3 个源文件
- 人工：订阅 → 切 tab → 切回，行数仍在增长；关闭应用后后端无残留订阅（T063 的 teardown 仍生效）

## 结果（完成后填写，≤5 行）

- `Shell.vue` **+7/−1**：`ConsumeView` 从 `v-if` 链中取出，改由 `<KeepAlive>` 包裹（切 tab 只 deactivate，
  不再卸载）——这就是本卡的修复本体。
- `ConsumeView.vue` **+9/−2**：新增 `onActivated(() => void loadTopics())`（补偿 KeepAlive 让
  `onMounted` 只跑一次）；`onBeforeUnmount` 注释订正为「只在 app teardown 触发」，`unsubscribe` 保留。
- 预算 **16 行 / 2 个源文件**（`frontend/dist` 的 3 个生成物：`index-CZoamonp.js` +9、旧的
  `index-CsaT7qns.js` −9、`index.html` 1/1，不计预算）。
- 验证：`npm run build`（Node v22.23.3 / vite 7.3.7）→ `48 modules transformed`、
  `dist/assets/index-CZoamonp.js` 176.98 kB、`✓ built in 621ms`。未改 Rust ⇒ 未跑 cargo 门禁。
- 偏差：本机无法自动点按 GUI，「切 tab 订阅仍在」的观感确认写入清单第 31 条；R115 的代价
  （订阅寿命 = 组件寿命）已如实记在取舍里。
