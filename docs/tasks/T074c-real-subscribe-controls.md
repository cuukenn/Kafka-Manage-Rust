# T074c — real：subscribe 投递成形与控制（filter/max_messages + pause/resume/seek）

状态: 🔵进行中　依赖: T074b　验证: 真集群手动清单　类型: 产品码

## 目标

给 T074b 的流补上「投递成形」与「过程控制」：客户端过滤、条数上限，以及 `pause`/`resume`/`seek` 三条控制命令真正生效且不丢消息。

## 只读白名单（最多 3 个源文件）

- crates/kafka-driver/src/real/consumer.rs（T074b 已落地：`pump`/`run`/`stop_requested`）
- crates/kafka-driver/src/mock/pump.rs（控制语义基线）
- crates/kafka_core/src/message.rs（`SearchFilter::matches`）

## 要改的文件

- `crates/kafka-driver/src/real/consumer.rs`

## 必须覆盖

- `filter`：逐条客户端过滤，复用 `kafka_core::message::SearchFilter::matches`；**被过滤掉的不计入 `max_messages`**（与 mock 一致）。
- `max_messages`：投递到 N 条即结束流。
- `Pause`：暂停投递（不再 `poll`），暂停期间产生的消息在 `Resume` 后补到，不丢。
- `Resume`：恢复投递。
- `Seek`：对**已分配**的分区重定位；未订阅的分区忽略；`offset` 取 `max(0)`；`seek` 成功后清除该分区的「已读完」标记，否则有界读会立刻误判结束。
- **竞态**：`pause`/`stop` 可能在泵阻塞于 `poll` 时到达——`poll` 返回后必须再查一次命令，`pause` 期间取回的那条消息要暂存（`held`）并在恢复后投递，`stop` 时丢弃。
- **不开** `server_side_filter` 能力位：真实 Kafka 没有服务端过滤，本卡是客户端过滤。

## 明确不做

- 不入消费组、不提交位点（消费组相关落在 T075）。
- 不补 real 侧自动化测试（T077 统一做）。
- 不调 librdkafka 的 `rd_kafka_pause_partitions`（见 R53）。

## 已知取舍（Ruling）

- **R53**：`pause` = 停止 `poll`，不调 `rd_kafka_pause_partitions`。跳过 `poll` 已经停止投递，且**不会丢掉**已取回队列里的消息；用 librdkafka 的 pause 还得额外维护一份分区清单，收益不抵复杂度。代价：暂停期间 broker 侧可能再推进一小段（受队列上限约束）；且控制命令在**下一次泵观察**时生效——`stop` 已发出但泵正好从 `poll` 取回一条消息时，该条消息可能仍会投递（实测可见），mock 的语义同样是「其后不再投递」。
- **R54**：`poll` 返回后发现 `pause` 已到达时，把这条消息暂存在 `held`（至多一条，因为暂停后不再 `poll`），恢复后优先投递。**为什么**：不这样做，这一条会被直接丢弃——实测「暂停期间 produce 2 条、resume 后只收到 1 条」正是这个 bug（当时写成 `held.take().filter(|_| !paused)`，`take` 已经把值拿走了）。代价：多一个 `Option<MessageEvent>` 状态。

## 验收

1. `cargo check -p kafka-driver --features real` 通过。
2. `cargo clippy -p kafka-driver --features real --all-targets -- -D warnings` 零告警。
3. `cargo test -p kafka-driver`（mock 回归）72/72 通过。
4. Docker 单节点真集群跑临时 example：① `max_messages=1` 只收到 1 条随后流结束；② `filter` 只放行匹配的消息且不计入上限；③ `pause` 后 produce 2 条，暂停期间一条都收不到，`resume` 后 2 条都到；④ `seek` 回 0 后重放同一条；⑤ 未订阅分区的 `seek` 被忽略。

## 结果

（待填：改动行数/文件数、三条命令的验证命令与结果、真集群逐项证据、Ruling 记录）
