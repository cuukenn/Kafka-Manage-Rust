# T074a — real：list_offsets（起止位点 + 时间点解析）

状态: ✅　依赖: T073　验证: 真集群手动清单　类型: 产品码

## 目标

把 `list_offsets` 接到真集群：给定 `Vec<TopicPartition>` 与 `OffsetSpec`，逐分区回 `OffsetRange{low, high, resolved_offset}`。分区为空时展开成「所有非内部主题的全部分区」（与 mock 的语义对齐）。

## 只读白名单（最多 3 个源文件）

- crates/kafka-driver/src/real/mod.rs
- crates/kafka-driver/src/real/topic.rs（水印批量取法基线）
- crates/kafka-driver/src/mock/driver.rs（`list_offsets` 语义基线，第 452–489 行）

## 要改的文件

- `crates/kafka-driver/src/real/consumer.rs`（新增）：`consumer_config`、`real_list_offsets`、分区展开、水印批量、时间点解析
- `crates/kafka-driver/src/real/mod.rs`：`mod consumer;` + `list_offsets` 改委派

## 必须覆盖

- 空 `partitions` → 展开为所有非内部主题的全部分区（mock 是「所有主题」，这里剔掉 `__` 前缀）。
- 主题不存在 → `DriverError::TopicNotFound`；分区不存在 → `DriverError::PartitionNotFound`（与 mock 一致）。
- `Earliest` → `resolved_offset = Some(low)`；`Latest` → `Some(high)`。
- `Timestamp(ts)` → `first offset at/after ts`；查不到（librdkafka 回 -1）时退成 `high`，与 mock 的 `first_offset_at_or_after` 一致。
- `low`/`high` 一律来自 `fetch_watermarks`，与 `describe_topics` 同源。
- 顺序与请求一致（`partitions` 非空时逐个对应）。

## 明确不做

- 不做 `subscribe` 流（T074b）。
- 不补 real 侧自动化测试（T077 统一做）。
- 不新增能力位：`DriverCapabilities` 没有 offsets 位。

## 已知取舍（Ruling）

- **R45**：`list_offsets` 的时间点解析必须借一个 consumer（librdkafka 只在 consumer 上暴露 `offsets_for_times`），因此本卡按调用新建一次性 `BaseConsumer`，用完即弃。代价：每次带时间戳的调用多一次客户端构造与一次 metadata 握手；`Earliest`/`Latest` 走 `fetch_watermarks`，不受影响。
- **R46**：空 `partitions` 展开时**剔掉内部主题**。mock 的状态里没有 `__consumer_offsets`，真集群有（本机 50 个分区），照搬「所有主题」会把一次诊断变成 50 次多余往返。代价：与本机对比 mock 时，真集群不会包含内部主题。
- **R47**：`consumer_config` 显式设 `enable.auto.commit=false`（驱动自己 assign 分区、从不入组）与 `enable.partition.eof=true`（T074b 的有界读靠 EOF 判断分区已读完）。`cfg.extra` 仍最后覆盖。
- **R48**：`Timestamp` 查不到时给 `high` 而不是 `None`。librdkafka 用 offset `-1` 表示「没有 ≥ 该时间戳的消息」，而 rdkafka 的 `Offset::from_raw(-1)` 会把它映射成 `Offset::End`，语义上不可区分，故统一按「查不到」处理并对齐 mock 的 `high`。

## 验收

1. `cargo check -p kafka-driver --features real` 通过。
2. `cargo clippy -p kafka-driver --features real --all-targets -- -D warnings` 零告警。
3. `cargo test -p kafka-driver`（mock 回归）72/72 通过。
4. Docker 单节点真集群跑临时 example：① 对 `t071-demo` 指定全部 3 个分区、`Earliest`/`Latest` → `low`/`high` 与 `kafka-get-offsets.sh` 一致；② `Timestamp` 取 0 → `resolved_offset` 落在最早消息处、取未来时间 → 退成 `high`；③ 不存在的主题 → `TopicNotFound`，不存在的分区 → `PartitionNotFound`；④ 空 `partitions` → 展开出 `t071-demo` 的 3 个分区且**不含** `__consumer_offsets`。

## 结果

- 改动：新增 `real/consumer.rs`（`consumer_config` + `real_list_offsets`：分区展开、一次 `spawn_blocking` 批量取水印、按需借一次性 consumer 解析时间戳），`mod.rs` 挂模块并把 `list_offsets` 改委派；2 文件 206 行，未超上限。
- 验证：`cargo check -p kafka-driver --features real`、`cargo clippy -p kafka-driver --features real --all-targets -- -D warnings` 均绿；`cargo test -p kafka-driver`（mock 回归）72/72。
- 真集群（Docker `rs-kafka-ui-dev`，主题 `t071-demo` 3 分区）：① `Earliest`/`Latest` 的 low/high 与 `kafka-get-offsets.sh` 完全一致；② `Timestamp(0)` → `resolved_offset=0`，`Timestamp(未来)` → 退成 `high`；③ 不存在主题 → `TopicNotFound`、越界分区 → `PartitionNotFound`；④ 空 `partitions` → 恰好 3 项且不含 `__consumer_offsets`。
- Ruling：R45（时间戳解析借一次性 consumer）、R46（空请求展开时剔除内部主题）、R47（`enable.auto.commit=false` + `enable.partition.eof=true`）、R48（时间戳查不到退成 `high`）。
