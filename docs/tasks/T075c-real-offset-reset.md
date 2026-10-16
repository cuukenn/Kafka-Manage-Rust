# T075c — real：reset_offsets / commit_offsets

状态: ✅　依赖: T075b　验证: 真集群手动清单　类型: 产品码

## 目标

把位点写回去：`commit_offsets`（提交指定分区的位点）与 `reset_offsets`（按最早 / 最新 / 指定 offset / 时间戳算出目标位点，支持 `dry_run` 只算不写）。

## 只读白名单（最多 3 个源文件）

- crates/kafka-driver/src/mock/driver.rs（`reset_offsets` / `commit_offsets` 语义基线）
- crates/kafka-core/src/group.rs（`ResetSpec` / `ResetOffsetsResult` / `CommitOffsetsReq` 契约）
- crates/kafka-driver/src/real/consumer.rs（`watermarks` / `at_or_after` 复用）

## 要改的文件

- `crates/kafka-driver/src/real/group.rs`
- `crates/kafka-driver/src/real/consumer.rs`（放开 `at_or_after` / `to_i32` 可见性）
- `crates/kafka-driver/src/real/mod.rs`

## 必须覆盖

- `commit_offsets`：`group_id` 缺失或空白 → `Unsupported`（与 mock 同一条）；`offsets` 为空 → `invalid("offsets")`；用 `group.id = group_id` 的一次性 consumer 以 `CommitMode::Sync` 提交，逐条带上 `metadata`。
- `reset_offsets`：`req.validate()`；组不存在 → `GroupNotFound`；目标分区 = `partitions` 非空取之，否则 `topics` 非空的全部健康分区（主题不存在 → `TopicNotFound`），否则**该组已提交位点的分区**（复用 T075b 的 `offsets`）。
- 显式 `partitions` 也要校验：主题不存在 → `TopicNotFound`、分区不存在 → `PartitionNotFound`（否则会落到水印层报出一个含混的传输错误）。
- 目标位点：`Earliest` → low、`Latest` → high、`ToOffset(v)` 越界 → `OffsetOutOfRange`、`ToTimestamp(ts)` → 复用 `at_or_after`。
- `dry_run = true` 只算不写；`applied` 逐条回目标位点（`metadata` 空、`timestamp` / `leader_epoch` 为 `None`）。
- 写入用 `CommitMode::Sync`，避免「返回了但还没落」的假成功。
- 全部写回都**只提交、不消费**：不 `poll`、不入组。

## 明确不做

- 不做 `committed_at`（R58 已定）。
- 不实现 rdkafka 0.39 未包装的 `IncrementalAlterConfigs` / `delete_records` 类接口。
- 不补 real 侧自动化测试（T077 统一做）。

## 已知取舍（Ruling）

- **R61**：`commit_offsets` **不**做范围校验（不查主题是否存在、不比对高低水位）。Kafka 的提交只是一个存下来的数字，越界提交本身合法；mock 的范围检查是 mock 自己的守卫。代价：real 下能提交一个越界位点而 mock 会挡住，T077 对拍时要认这条漂移。
- **R62**：`reset_offsets` 要求组已存在（与 mock 一致），因此真实集群上**不能**用这条路给全新组播种位点。代价：想给新组播种得先在别处把组建出来；好处是 real 与 mock 语义一致。查组存在复用 T075a 的 `fetch_group_list` 探测。
- **R63**：`ToTimestamp` 解析不到更晚的消息时回落到 `high`（与 `list_offsets` 的 R48 同源），否则 `dry_run` 会显示 `None`、真写时又落到另一个值。

## 验收

1. `cargo check -p kafka-driver --features real` 通过。
2. `cargo clippy -p kafka-driver --features real --all-targets -- -D warnings` 零告警。
3. `cargo test -p kafka-driver`（mock 回归）72/72 通过。
4. Docker 单节点真集群跑临时 example：① `commit_offsets` 提交后，`kafka-consumer-groups.sh --describe` 读到的 CURRENT-OFFSET 与提交值一致，且 `metadata` 被 broker 记下；② `reset_offsets` 的 `Earliest`/`Latest`/`ToOffset`/`ToTimestamp` 四种 spec 各写出与 broker 现场一致的位点；③ `dry_run=true` 只回目标值、broker 侧位点不变；④ `ToOffset` 越界 → `OffsetOutOfRange`；⑤ 不存在的组 → `GroupNotFound`；⑥ 缺 `group_id` / 空 `offsets` / 空主题名的错误路径。

## 结果

- 改动：`real/group.rs` +197、`real/mod.rs` +14、`real/consumer.rs` +2（放开 `at_or_after` / `to_i32`），共 213 行/3 文件，未超上限；另删掉了已无人调用的 `require_unsupported`（T075c 之后 trait 方法已全部落地）。
- 验证：`cargo check -p kafka-driver --features real`、`cargo clippy -p kafka-driver --features real --all-targets -- -D warnings`（零告警）、`cargo test -p kafka-driver`（mock 回归）72/72 全绿。
- 真集群（Docker `rs-kafka-ui-dev`，主题 `t075c-demo` 3 分区、水位 3/1/2）：① `commit_offsets` 提交 1/0/2 后 broker 自己的 CURRENT-OFFSET 正是 1/0/2，且 `metadata` 回读为 `from-driver`；② 四种 spec 写出的位点与现场一致——`Earliest` 0/0/0、`Latest` 3/1/2、`ToTimestamp(0)` 0/0/0、`ToTimestamp(max)` 3/1/2（回落 high）、`ToOffset(1)` 1/1/1；③ `dry_run` 回 `[0,0,0]` 而 broker 仍停在 1/0/2。
- 错误路径：`ToOffset(99)` → `OffsetOutOfRange{low:0,high:3}`；ghost 组 → `GroupNotFound`；显式分区的不存在主题 / 分区 → `TopicNotFound` / `PartitionNotFound`；缺 `group_id` → `Unsupported`；空 `offsets` → `InvalidConfig{field:"offsets"}`；空白组名 → `InvalidConfig{field:"groupId"}`。
- Ruling：R61（commit 不做范围校验）、R62（reset 要求组已存在）、R63（`ToTimestamp` 解析不到回落 `high`）；`capabilities()` 本卡再开 `offset_reset` / `offset_commit`。
