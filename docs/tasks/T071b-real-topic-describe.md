# T071b — real：describe_topics（分区明细 + 起止位点 + 配置）

状态: ⬜　依赖: T071a　验证: `cargo check -p kafka-driver --features real` + 真集群手动清单　类型: 产品码

## 目标
把 `describe_topics` 接通：按 `DescribeTopicsReq` 出分区级明细（leader / 副本 / ISR / 起止位点），
并按「只回被覆盖的配置项」补上 `TopicDetail.configs`。多分区 topic 的位点要**一次** `spawn_blocking`
批量取，不是每分区一次。

真实 Kafka 用 `scripts/kafka/docker-compose.yml`（Docker Desktop，KRaft 单节点，localhost:9092）。

## 只读白名单（最多 3 个源文件）
- `crates/kafka-driver/src/mock/driver.rs` — `describe_topics` 语义参照（空 names = 全部可见；
  点名缺失 → `TopicNotFound`；显式点名 internal 放行）
- `crates/kafka-core/src/topic.rs` — `TopicDetail`/`PartitionDetail`/`DescribeTopicsReq` 字段
- `crates/kafka-driver/src/real/topic.rs` — T071a 落下的 `is_internal`/`infos` 与模块骨架

## 要改的文件
- 修改 `crates/kafka-driver/src/real/mod.rs`
- 修改 `crates/kafka-driver/src/real/topic.rs`

## 必须覆盖
1. `plan(metadata, req)`（**同步**，吃掉 `Metadata` 的所有权，R35）：选 topic 并把需要的东西拷成
   owned `Planned { name, internal, partitions: Vec<PartitionPlan> }`。选择规则与 mock 对齐：
   - `req.names` 空 → 全部可见 topic（`include_internal` 决定是否滤掉 `__` 前缀）；
   - 非空 → 只取点名的；名字不在（或带 `error()`）→ `DriverError::TopicNotFound`；
   - 显式点名 internal topic 放行（不因 `include_internal=false` 被拒）。
2. `assemble(live, planned)`（**异步**）：拉配置 + 位点，组装 `TopicDetail`。`leader_epoch` 恒 `None`
   （librdkafka metadata 不暴露）；`id` 由 `i32` 转 `u32`。
3. 位点：`Client::fetch_watermarks(topic, partition, timeout)` 取 `(low, high)`；**所有分区装在同一个
   `spawn_blocking`** 里循环（R31），一个宽 topic 只付一次线程池往返；结果按 topic → partition 两级 map 存放。
4. 配置：一次 `AdminClient::describe_configs(&[ResourceSpecifier::Topic(name)], &AdminOptions::new())`
   批量拉全部被描述 topic；这是 **async** API（直接 await，不要 `spawn_blocking`）。
   R34：只保留 `!is_default && !is_sensitive` 且 `value.is_some()` 的项，与 mock「只存显式设置过的配置」对齐。
   单个 resource 出错 → `DriverError::other("describe_configs failed: <code>")`。
5. `describe_topics` 在 `mod.rs` 里把两步串起来：`fetch_metadata` → `plan` → `assemble`。

## 明确不做（防止范围膨胀）
- 不做 create/delete/update topic（T072）——本卡只读，`alter_configs` 不碰。
- 不做 `list_offsets`（T074）——`OffsetRange`/`OffsetSpec` 是 T074 的事，本卡只用 watermarks。
- 不改 `driver.rs` / `lib.rs` / `config.rs` / `capabilities()`（无新能力位）。

## 已知取舍（Ruling）
- R36：点名一个**存在但带 `error()`** 的 topic 按 `TopicNotFound` 处理。librdkafka 只给错误码，
  分不出「不存在」与「无权限」；等有真实鉴权场景再细分（代价：SASL 场景下错误文案会偏）。

## 验收
- `cargo check -p kafka-driver --features real` 全绿
- `cargo clippy -p kafka-driver --features real --all-targets -- -D warnings` 无告警
- `cargo test -p kafka-driver`（mock 回归）全绿
- 真集群手动清单：`describe_topics(["t071-demo"])` → 3 分区、`replica_ids=[1]`、`isr_ids=[1]`、
  空 topic 时 `low == high`，生产 1 条后该分区 `high` 变 1；`describe_topics(["nope"])` → `TopicNotFound`；
  空 names → 只出 `t071-demo`；`include_internal=true` → 追加 `__consumer_offsets` 且 `internal=true`；
  断开后 → `NotConnected`

## 结果（完成后填写，≤5 行）
-
