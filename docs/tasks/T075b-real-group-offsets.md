# T075b — real：消费组位点与滞后（list_group_offsets）

状态: ✅　依赖: T075a　验证: 真集群手动清单　类型: 产品码

## 目标

读出一个消费组的**已提交位点**，并在要求时补上滞后（`log end − committed`）。

## 只读白名单（最多 3 个源文件）

- crates/kafka-driver/src/mock/driver.rs（`list_group_offsets` 语义基线）
- crates/kafka-core/src/group.rs（`GroupOffsetEntry` / `ListGroupOffsetsReq` 契约）
- crates/kafka-driver/src/real/consumer.rs（`watermarks` 复用）

## 要改的文件

- `crates/kafka-driver/src/real/group.rs`
- `crates/kafka-driver/src/real/consumer.rs`（抽出 `consumer_config_for(cfg, group_id)`、放开 `watermarks` 可见性）
- `crates/kafka-driver/src/real/mod.rs`

## 必须覆盖

- 空 `group_id` → `invalid("groupId")`；组不存在 → `GroupNotFound`（沿用 T075a 的 `fetch_group_list` 探测）。
- 目标分区：`topics` 非空 = 这些主题的全部**健康**分区（主题不存在 → `TopicNotFound`）；为空 = 集群内全部**非内部**主题的全部分区（与 R46 同一取舍）。
- 位点读取：用 `group.id = req.group_id` 的一次性 `BaseConsumer` 调 `committed_offsets`（阻塞，走 `spawn_blocking`），只保留**真的提交过**的分区（`Offset::Offset(_)`；`Offset::Invalid` 表示没提交过，丢掉）。
- `with_lag`：对这些分区取一次 `watermarks`，`lag = max(high − offset, 0)`。
- 位点读取**只查不消费**：不 `poll`、不入组、不提交。

## 明确不做

- 不做 reset / commit（T075c）。
- 不返回 `committed_at`（见 R58）。
- 不补 real 侧自动化测试（T077 统一做）。

## 已知取舍（Ruling）

- **R58**：`committed_at` 恒为 `None`。librdkafka 的 OffsetFetch 回包里没有提交时间戳（`TopicPartitionListElem` 只有 topic/partition/offset/metadata）。代价：UI 位点表的「提交时间」一栏在 real 下显示 `—`。
- **R59**：取水印失败**不**让整个调用失败，而是把所有 `lag` 置 `None` 并 `warn` 一条。主题在提交之后被删掉就会这样，而 mock 对「主题未知」也回 `None`，两边语义对齐。代价：真的取不到水印时只在日志里留痕。
- **R60**：空 `topics` 展开成集群内**全部**分区，再由「有没有提交」筛一遍。librdkafka 没有「列出某组的全部提交位点」的接口（只有 admin API 才按组列举，rdkafka 0.39 未包装），只能先给全量分区再筛（与 R46 同源）。代价：大集群上这一次调用会带一大坨分区。

## 验收

1. `cargo check -p kafka-driver --features real` 通过。
2. `cargo clippy -p kafka-driver --features real --all-targets -- -D warnings` 零告警。
3. `cargo test -p kafka-driver`（mock 回归）72/72 通过。
4. Docker 单节点真集群跑临时 example：① 用 `kafka-console-consumer.sh` 提交位点后，`list_group_offsets` 读回的 offset 与 `kafka-consumer-groups.sh --describe` 一致；② `with_lag` 打开时 lag 与 broker 侧 LAG 列一致，关闭时全为 `None`；③ `topics` 过滤只回被点名的主题；④ 不存在的组 → `GroupNotFound`、不存在的主题 → `TopicNotFound`、空 `group_id` → `InvalidConfig`；⑤ 没提交过的空组回空列表（不是错误）。

## 结果

- 改动：`real/group.rs` +127、`real/consumer.rs` +11（抽出 `consumer_config_for(cfg, group_id)`、放开 `watermarks` 可见性）、`real/mod.rs` +6，共 144 行/3 文件，未超上限。
- 验证：`cargo check -p kafka-driver --features real`、`cargo clippy -p kafka-driver --features real --all-targets -- -D warnings`（零告警）、`cargo test -p kafka-driver`（mock 回归）72/72 全绿。
- 真集群（Docker `rs-kafka-ui-dev`，主题 `t075b-demo` 3 分区）：`--reset-offsets --to-earliest` 写入位点后补长日志，读回 p0 `offset=0 lag=3`、p1/p2 `offset=0 lag=1`，与 `kafka-consumer-groups.sh --describe` 的 LAG 列 3/1/1 完全一致；`with_lag=false` 三个 lag 全 `None`；`topics` 过滤回 3 行、名字存在但该组没提交过位点的主题回 0 行（不报错）。
- 真集群错误路径：ghost 组 → `GroupNotFound`、ghost 主题 → `TopicNotFound`、空白组名 → `InvalidConfig{field:"groupId"}`。
- Ruling：R58（`committed_at` 恒 `None`）、R59（取水印失败把所有 `lag` 置 `None` 并 `warn`）、R60（空 `topics` 展开成全集群非内部主题分区再筛）。
