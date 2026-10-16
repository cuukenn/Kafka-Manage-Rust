# T048a 报告 — 消费组页：列表 + 页内详情（成员/位点/lag）

状态: ✅ 完成　提交: `b5d3fad`（父 `07aa023`）

## 交付

- `frontend/src/views/GroupsView.vue`（新建，227 行）：
  - 组表：组名/状态徽章/成员数/协议类型/分配器/来源（isLocal→本地订阅/外部），列与 GroupInfo wire DTO 一致。
  - 行点击 → 页内详情面板（表格下方，非新页）：成员表（memberId/clientId/clientHost/分配分区 `topic-N` 列表）+ 提交位点表（topic/partition/offset/lag，lag 直接取 wire 值，null 显示「—」）+ 协调者 ID 与「订阅分配见成员表」提示（DTO 无独立订阅列表，订阅信息以成员 assignments 呈现）。
  - 进入拉一次（onMounted: refreshClusters + group.list）+ 手动「刷新」按钮；**未监听任何组事件**（按卡片口径；protocol.rs 虽有 `event::GROUP_CHANGED` 常量，但卡片明确不监听组事件，未发明监听器）。
  - 空态/错误条与 ClustersView 同款样式；未连接集群时提示先连接。
  - 「重置位点」按钮：disabled 占位（title=T048b），未实现任何重置逻辑。
- `frontend/src/components/Shell.vue`（+6/-4）：groups 占位 section 替换为 `<GroupsView v-else-if="ui.currentView === 'groups'" />` + import + 注释更新。导航「消费组」已存在于 NAV（ui store 单源，无需改）。
- dist 重建提交（hash swap：`index-BpeR6ZSt.js` → `index-Y8Bj_ela.js`）。

## 线事实（窄 grep 核实）

- 命令常量（kafka-core/src/protocol.rs L34-37）：`group.list` / `group.describe` / `group.offsets` / `group.resetOffsets`；事件常量 `group.changed` 存在但本卡不监听。
- payload 键（webview-bridge/tests/router_group.rs）：`group.list {clusterId}`；`group.describe {clusterId, ids?}`（ids 可省略=全部组）→ `GroupDetail[]`（`data[0].info.groupId` 断言）；`group.offsets {clusterId, groupId, withLag:true}` → 条目数组（按 partition 排序断言）。
- DTO（kafka-core/src/group.rs，camelCase serde）：
  - GroupInfo: groupId, state(snake_case: empty|stable|preparing_rebalance|completing_rebalance|dead|unknown), memberCount, protocolType, partitionAssignor, coordinatorId|null, isLocal。
  - GroupDetail: {info, members[]}; GroupMember: memberId, clientId, clientHost, assignments: TopicPartition[]。
  - GroupOffsetEntry: topic, partition, offset, metadata, committedAt|null, **lag: Option<i64>（wire 直给，端 offset−消费 offset 由后端计算）** → 前端不自行算 lag。

## 验证（R23）

1. `npm run build`：✓ 38 modules，dist 产物 `index-Y8Bj_ela.js` 150.52 kB。
2. `cargo build -p main-bin`：✓ Finished dev profile（4.67s；无 Rust 改动，仅重编）。
3. 冒烟 8s：✓ closedWindow=True exited=True，stderr 空、无 ERROR。
4. git show --stat HEAD：仅 2 个 src 文件 + 3 个 dist 文件；**零 Rust 文件**。

## 预算

src 2 文件（GroupsView.vue 227 行 + Shell.vue 净 +6 行）≈ 233 新增行 ≤250；无新依赖；无 store 文件（状态内聚在视图内，与 ConsumeView 模式一致）。

## 偏差与备注

- 卡片手动项提及「mock 预置组 checkout-service/analytics-etl」——那是测试 seam；wire 空 mock 无预置组，空态即首屏正常验证路径（已按此实现，未伪造 seed 步骤）。
- 工作区存在外来未提交改动 `agents.md`（未触碰、未提交）；`.opencode/` 未跟踪（未触碰）。
- 详情面板选择集群 = 首个 connected 集群（导航 setView 会清空 selectedTopic，故树选择不适用）。

## 疑虑

- `group.changed` 事件常量存在于 protocol.rs——若 T048b/后续要事件驱动刷新，可加监听；本卡按卡片明确口径未加。
