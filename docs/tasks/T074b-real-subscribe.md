# T074b — real：subscribe 流核心（有界/追尾 + stop）

状态: ✅　依赖: T074a　验证: 真集群手动清单　类型: 产品码

## 目标

把 `subscribe` 接到真集群：按 `SubscribeReq` 的起始位点 `assign` 若干分区，把消息泵成 `MessageStream`。`follow=false` 读到分区末尾即结束，`follow=true` 一直等新消息；`Stop` 结束流；读者丢弃流时泵自己退出。

## 只读白名单（最多 3 个源文件）

- crates/kafka-driver/src/real/consumer.rs（T074a 已落地：`consumer_config`、`watermarks`、`at_or_after`）
- crates/kafka-driver/src/real/mod.rs
- crates/kafka-driver/src/mock/pump.rs（流语义基线）

## 要改的文件

- `crates/kafka-driver/src/real/consumer.rs`：`real_subscribe` + 订阅规划 + 阻塞泵 + 消息映射
- `crates/kafka-driver/src/real/mod.rs`：`subscribe` 改委派

## 必须覆盖

- 起始位点四种：`Earliest`/`Latest` 取水印，`Offset(n)` 越界回 `DriverError::OffsetOutOfRange`，`Timestamp(ts)` 复用 T074a 的时间点解析。
- `req.partitions` 为空 → 该主题全部分区；给了不存在的分区 → `DriverError::PartitionNotFound`（`SubscribeReq::targets` 不过滤，必须自己校验）。
- 主题不存在 → `DriverError::TopicNotFound`；空 topic → 沿用 `SubscribeReq::validate()`。
- `follow=false`：读完全部分区即结束流；`follow=true`：读到末尾后继续等。
- `Stop` 结束流；读者丢弃流时泵退出，不留后台空转。
- `MessageEvent` 字段：key/value/headers/`timestamp` 与 `timestamp_type`（`CreateTime`/`LogAppendTime`/`Unknown`），`leader_epoch` 恒为 `None`。

## 明确不做

- **`pause`/`resume`/`seek` 落在 T074c**：本卡只认 `Stop`，其余命令会被读走并忽略（命令通道保持存活，所以前端调 `pause()` 不会拿到 `Cancelled`，只是暂时不生效）。
- **`filter` 与 `max_messages` 落在 T074c**：本卡把 `SubscribeReq` 的这两个字段原样忽略。
- 不入消费组、不提交位点，消费组相关落在 T075。
- 不补 real 侧自动化测试（T077 统一做）。
- `max_bytes_per_partition` 与 mock 一样先忽略（属于 `cfg.extra` 逃生通道）。

## 已知取舍（Ruling）

- **R49**：驱动用 `assign` 而不是 `subscribe`，因此不入组、不提交位点。代价：真实消费组的 lag/reset 语义要等 T075 单独实现。
- **R50**：消费者配置一个合成 `group.id`（`rs-kafka-ui-reader`）。**为什么**：librdkafka 的 `rd_kafka_assign` 对没有 `group.id` 的客户端直接失败（`rd_kafka_cgrp_get` 返回 NULL → `"Local: Unknown group"`），实测第一次真集群跑就撞上了。代价：若有人靠 `extra` 覆盖成真实组名，理论上存在被误认为在组内的观感；实际不入组、不提交，broker 侧看不到该组。
- **R51**：有界读靠 `enable.partition.eof` 的 `PartitionEOF` 事件判定「分区已读完」，与 mock 的「一 tick 无新消息即结束」等价。代价：依赖该事件；若 `cfg.extra` 把 `enable.partition.eof` 关掉，`follow=false` 会退化成一直等。
- **R52**：librdkafka 的 `PartitionEOF` 只带分区号不带主题，正好一对一（一次订阅只订阅一个主题），所以「读完」集合只按分区号记。

## 验收

1. `cargo check -p kafka-driver --features real` 通过。
2. `cargo clippy -p kafka-driver --features real --all-targets -- -D warnings` 零告警。
3. `cargo test -p kafka-driver`（mock 回归）72/72 通过。
4. Docker 单节点真集群跑临时 example：① produce 3 条（分区 0/1/2 各 1 条）→ `Earliest` + `follow=false` 读回 3 条且内容一致、随后流结束；② `Latest` + `follow=true` 时另起一次 produce → 流里收到新消息；③ `Offset(越界)` → `OffsetOutOfRange`，不存在分区 → `PartitionNotFound`，不存在主题 → `TopicNotFound`；④ `stop` 后流结束；⑤ 丢弃流后驱动仍可用。

## 结果

- 改动：`real/consumer.rs` 增 `real_subscribe`（分区校验 → 水印 → 起始位点 → `assign`）、`Pump` 规划结构、阻塞泵 `pump`/`run`/`stop_requested`、`MessageEvent` 组装 `event_from`/`headers_of`；`mod.rs` 把 `subscribe` 改委派；2 文件 248 行，未超上限。
- 验证：`cargo check -p kafka-driver --features real`、`cargo clippy -p kafka-driver --features real --all-targets -- -D warnings` 均绿；`cargo test -p kafka-driver`（mock 回归）72/72。
- 真集群（Docker `rs-kafka-ui-dev`，主题 `t074b-demo` 3 分区）：① 写入 3 条后 `Earliest` 有界读回 3 条，随后 `<end>`；② `follow=true` 后新 produce 的消息即时到达；③ 越界 `Offset` → `OffsetOutOfRange`、不存在分区 → `PartitionNotFound`、不存在主题 → `TopicNotFound`；④ `stop` 后流结束（`<end>`）；⑤ 读者丢弃流后 `is_connected() == true`，驱动仍可用。
- Ruling：R49（assign 不入组、不提交）、R50（合成 `group.id`，librdkafka 的 assign 必需）、R51（有界读靠 `PartitionEOF`）、R52（`PartitionEOF` 只带分区号）。
- 补充：本卡最初写成「流 + 全套控制」，实测 331 行超 250 上限，按既有先例拆成 T074b（本卡，流核心）与 T074c（控制与投递成形）。
