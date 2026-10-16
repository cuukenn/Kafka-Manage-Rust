# T077 — Mock/Real 行为对齐测试架（`#[ignore]` + 环境变量门控）

状态: ✅　依赖: T071–T076　验证: `cargo test -p kafka-driver -- --ignored`　类型: 产品码

## 目标

把 mock 与 real 两个驱动放到同一段脚本上跑一遍，要求它们给出同样的答案；再把**已经知情并接受**
的差异（R30/R33 等）单独钉住，让「悄悄改掉一个差异」必须留下痕迹。这样 Stage 6 收尾后，
`real` 与整个 UI 赖以开发的 `mock` 参考实现之间，没有未记录的漂移。

## 只读白名单

- `crates/kafka-core/src/topic.rs`
- `crates/kafka-core/src/message.rs`
- `crates/kafka-core/src/group.rs`

## 要改的文件

1. `crates/kafka-driver/tests/real_conformance.rs` —— 新建：脚本化 transcript + 漂移钉子
2. `docs/TASKS.md` —— 状态列与快照

## 必须覆盖

- 一段共享脚本（`transcript`）依次跑完：`cluster_info` → `create_topic` → 重复建 → `describe_topics`
  → `produce`（每个分区各一条）→ `list_offsets`（Earliest/Latest）→ `subscribe` 有界读回
  → `update_topic_config`（set / unset）→ 六种拒绝 → `delete_topics` → 删后 `describe`
- 两个驱动逐行相等：`assert_eq!(real_lines, mock_lines)`
- 拒绝路径逐条比对**错误类别**：空主题名、空消息批、ghost 主题、ghost 组、无 groupId 的 commit、
  越界起点 → `InvalidConfig / InvalidConfig / NotFound / NotFound / Unsupported / OffsetOutOfRange`
- 第二条测试 `documented_drifts_between_mock_and_real_hold`：R30（mock 有 clusterId/controller，
  real 恒为 `None`）、R33（`list_topics` 的 `configs`：mock 有、real 恒空）
- 门控：`#[ignore]` + 环境变量 `KAFKA_CONFORMANCE_BOOTSTRAP`；未设时打印跳过说明并返回

## 明确不做

- 不做 SASL/SSL 的对齐（需要第二套凭据环境，留给需要它的卡；T076 的手动清单已覆盖）
- 不比对 mock 侧独有的控制面（faults / fixtures / 事件流 / 快照持久化）
- 不比对 `ClusterInfo.latency_ms`、`PartitionDetail.leader_epoch`、`committed_at` 等
  real 侧结构上就取不到的字段（R55–R58）

## 已知取舍（Ruling）

- **R69**：只比对**错误类别**（`DriverError::kind()`），不比消息文本与出错字段名。理由：文本来自
  librdkafka 与 mock 两套实现，逐字相等既无意义也做不到。代价：错误信息文案漂移不会被这条测试抓到
  （那属于 T081 的错误提示审计）。
- **R70**：`transcript` 里每条消息都**强制指定分区**。理由：mock 是轮询、librdkafka 是粘性分区器，
  不强制则同一条脚本会落进不同分区，`latest` 从 `[1,1,1]` 变成 `[0,0,3]`——那是分区器差异，不是契约
  差异。代价：分区器的行为差异本身没有被这条测试覆盖。
- **R71**：real 侧结构上取不到的字段（clusterId/controller/leaderEpoch/committedAt/latency）一律**不进
  transcript**，知情且接受的差异改由第二条测试**显式断言**，而不是从比对里悄悄删掉。代价：多一条测试；
  收益是「差异」变成可断言的事实而不是注释。
- **R72**：环境变量未设时测试**跳过并通过**（打印说明后 `return`），不失败。理由：`--ignored` 也要能在
  没有 broker 的机器上跑通，否则门控就成了硬依赖。代价：忘记设变量时会得到 2 个绿的假象——`--nocapture`
  能看见 `skipping T077: …`，卡片与索引都记了这条。

## 验收

```
KAFKA_CONFORMANCE_BOOTSTRAP=localhost:9092 \
  cargo test -p kafka-driver --features real --test real_conformance -- --ignored
# → 2 passed

cargo test -p kafka-driver          # 默认不跑：72 passed, 2 ignored
cargo clippy -p kafka-driver --features real --all-targets -- -D warnings   # 零告警
```

## 结果

- 两个 `#[ignore]` 测试在 Docker dev broker（`localhost:9092`）上实测通过：transcript **23 行逐字相等**
  （`create: ok` / `create twice: AlreadyExists` / `partitions: 3` / `replicas: 1` /
  `retention.ms: 60000`→`120000`→`absent` / `produced: 3` / `delivery errors: 0` /
  `earliest: [0, 0, 0]` / `latest: [1, 1, 1]` / `read back: ["p0","p1","p2"]` /
  `blank topic: InvalidConfig` / `ghost topic: NotFound` / `no group id: Unsupported` /
  `past the end: OffsetOutOfRange` / `after delete: NotFound` …）。
- R30/R33 钉子测试同样实跑通过（real `clusterId`/`controllerId` 为 `None`；real `list_topics().configs` 为空
  而 mock 非空）。
- 跑完 broker 上无 `t077-*` 残留（脚本自带清理），默认 mock 套件仍 72/72、新测试计为 ignored。
- 新增 249 行 / 1 文件（≤250）；`clippy --all-targets -- -D warnings` 零告警。Stage 6 就此收尾。
