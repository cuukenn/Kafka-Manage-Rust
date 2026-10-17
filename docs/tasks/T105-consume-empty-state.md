# T105 — 消费页空态：不再默认内部主题，并说明「已订阅但没有数据」

状态: ✅　依赖: 无　验证: 手动（`npm run build` + 真 Kafka 订阅观察）　类型: 产品码

## 目标

用户报「真 Kafka（`--features real`）订阅后一行数据都没有」。排查后确认**数据通路全对**：
驱动层（mock/real）、router、bus、前端事件桥、前端 RPC 桥逐层实测通过，真集群一致性测试
（T077）与 mock E2E（T100）全绿，前端吞首批消息的 `subId` 竞态也被实测否定（真驱动下
`message.received` 一律晚于 RPC 回复）。

空列表的真因是**界面没有说实话**：

- 该 broker 上除探针主题外只有一个**空的** `__consumer_offsets`（50 分区、0 条消息），
  没有任何用户自建且带数据的主题；
- Consume 页在主题列表变化时**默认选中列表第一个**，而 `topic.list` 的顺序不保证字母序，
  很容易落在内部主题上；
- 对一个空主题用 earliest + follow 订阅，**正确地**得到 0 行——但界面只显示「暂无消息」，
  与「功能坏了」在视觉上完全一样。

本卡只改消费页的两处表达，不动任何通路逻辑：

- **默认不选内部主题**：主题列表变化时优先选第一个不以 `__` 开头的主题；全是内部主题时才退回第一个。
- **空态说实话**：把空列表分成三种情形说清楚——未订阅（引导去订阅）、已订阅但该 topic 暂时为空
  （说明「新消息会实时出现」）、过滤后无匹配；并对 `__*` 主题点明「这是 Kafka 内部 topic，通常没有业务数据」。

## 只读白名单（最多 3 个源文件）

- `crates/kafka-driver/src/real/consumer.rs` — earliest/latest 起读语义（空主题必然 0 行）
- `crates/webview-bridge/src/pump.rs` — 事件批次延迟（有数据时必然晚于回复到达，故守卫不会吞消息）
- `frontend/src/bridge/rpc.ts` — `topic.list` / `consume.subscribe` 的线上形状

## 要改的文件

- 修改 `frontend/src/views/ConsumeView.vue` — 新增 `internalTopic` 计算属性、改写 `loadTopics()`
  的默认选中逻辑、把空态拆成三个分支

## 必须覆盖

1. 主题列表变化时默认选中**第一个非 `__` 开头**的主题；列表全是内部主题时才退回第一个。
2. 空态三分支：`!subId` → 引导（沿用 `EmptyState`）；`filterActive` → 「无匹配消息」；
   已订阅且无数据 → 说明文案，且在 `internalTopic` 为真时追加内部主题提示。
3. 不改任何线上命令：`loadTopics()` 仍只发 `topic.list`，订阅仍只发 `consume.subscribe`。

## 明确不做

- 不改驱动/router/bus 的任何行为——经实测数据通路无缺陷。
- 不隐藏、不禁止选择内部主题：运维确实需要偶尔看一眼 `__consumer_offsets`，只是不把它当默认。
- 不做「一键生成示例数据」按钮（`mock.seed` 只固定 RNG、不播种主题，见 T039；真集群造数据是独立特性）。
- 不改 `frontend/dist`（生成物，随构建更新，不计预算）。

## 已知取舍（Ruling）

- **R113** 内部主题不作为默认选择，但**保留可见可选**：默认值只是便利，隐藏主题会剥夺运维的边角能力；
  代价是用户仍可能手动选中 `__consumer_offsets` 并看到十六进制记录，故必须由空态文案兜住。
- **R114** 空态文案按「未订阅 / 已订阅无数据 / 过滤无匹配」三分支表达，而不是统一显示「暂无消息」：
  同样一行文字背后是三种完全不同的原因，含混显示会让「功能正常」看起来像「功能损坏」——这正是本卡的起因。

## 验收

- `npm run build`（Node 22）通过，无新增告警
- 主题列表含内部主题时，默认选中落在用户主题上
- 订阅空主题时界面说明「暂时是空的」而非只显示「暂无消息」
- 预算：≤250 行 / ≤3 个源文件（`frontend/dist` 不计）

## 结果（完成后填写，≤5 行）

- 只改 `frontend/src/views/ConsumeView.vue`：**+17/−2、1 个源文件**；`frontend/dist`（1 改 1 删 1 增）是构建产物，按约定不计预算。
- 新增 `internalTopic` 计算属性；`loadTopics()` 改为「优先第一个非 `__` 主题，全是内部主题才退回第一个」；
  空态拆成 `!subId`（引导）/ `filterActive`（无匹配）/ 已订阅无数据（说明 + 内部主题提示）三分支。
- 验证：Node **v22.23.3** 下 `npm run build` 通过（`48 modules transformed`，`dist/assets/index-CsaT7qns.js` 174.59 kB，652 ms）；未改 Rust，故未跑 cargo 门禁。
- 诚实备注：本机无法自动点按 GUI，界面观感留给 `docs/tasks/STAGE4-manual-checklist.md` 第 30 条手工确认；有数据的主题订阅照旧实时显示（真驱动实测：回复 111.2 ms，3 条消息 117.05 ms 到达）。
