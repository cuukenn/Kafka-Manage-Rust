# T072a — real：create / delete topic

状态: ✅　依赖: T071b　验证: 真集群手动清单　类型: 产品码

## 目标

给 real 驱动接上两个主题写操作：建主题（固定副本因子或显式分区分配）、删主题（逐项汇报，未知主题与内部主题不炸掉整次调用）。配置写入（`update_topic_config`）因 250 行上限拆到 T072b。

## 只读白名单（最多 3 个源文件）

- crates/kafka-driver/src/real/mod.rs
- crates/kafka-driver/src/real/topic.rs
- crates/kafka-core/src/topic.rs（`CreateTopicReq` / `DeleteTopicsReq` / `TopicDeleteResult` 契约）

## 要改的文件

- 新增 `crates/kafka-driver/src/real/admin.rs`
- `crates/kafka-driver/src/real/mod.rs`：`mod admin;`、`create_topic`/`delete_topics` 改为委派、`Live` 增加 `brokers`、`capabilities` 打开 `admin_api`
- `crates/kafka-driver/src/real/topic.rs`：`is_internal` 提到 `pub(super)`（删主题要用同一套内部主题判据）

## 必须覆盖

- `create_topic`：先 `req.validate(live.brokers)`（与 mock 同一处校验，非法请求不发往 broker）；`replica_assignment` 为空走 `TopicReplication::Fixed`，否则走 `Variable`；`configs` 逐条 `set`；`TopicAlreadyExists` → `DriverError::TopicAlreadyExists`，`InvalidPartitions`/`InvalidReplicationFactor`/`InvalidConfig` → `DriverError::invalid`，其余进 `DriverError::Broker`。
- `delete_topics`：结果按请求顺序逐项返回；`__` 前缀主题拒绝删除（文案与 mock 一致）；broker 回 `UnknownTopicOrPartition` 时写 `unknown topic {name}`；空列表直接返回空且不打扰 broker。
- `capabilities()` 打开 `admin_api`（`topic_config_write` 留给 T072b）。
- 新逻辑放在 `pub(super)` 的 `real_*` 方法里，trait 方法只做委派，便于 T077 复用。

## 明确不做

- 不做 `update_topic_config`（T072b）。
- 不实现扩分区（`create_partitions`）与删记录（`delete_records`）——契约里没有对应请求类型。
- 不给 real 驱动接 `events()`（仍是 `None`）：写操作后的刷新由上层显式拉取。
- 不在本卡补 real 侧自动化测试（T077 的 conformance harness 统一做）。

## 已知取舍（Ruling）

- **R38**：`Live` 增加 `brokers`（取自 `connect` 时的 `ClusterInfo`），使建主题的副本因子校验不必额外打一次 metadata；`connect` 内部的一次性探针 `Live` 传 `0`，它只用于 `fetch_metadata`，不参与校验。
- **R40**：删内部主题在客户端就按 `__` 前缀拒绝，不去问 broker。Kafka 允许在停止写入后删除内部主题，而 mock 是一律拒绝；两者取一致的行为，UI 才敢照能力位展示。代价：真集群上确实想删内部主题时，本驱动帮不上忙。

## 验收

1. `cargo check -p kafka-driver --features real` 通过。
2. `cargo clippy -p kafka-driver --features real --all-targets -- -D warnings` 零告警。
3. `cargo test -p kafka-driver`（mock 回归）72/72 通过。
4. 对 Docker 单节点真集群跑临时 example：建 `t072a-demo`（3 分区 rf=1 + `retention.ms`）→ `describe_topics` 核对 → 重复建得到 `AlreadyExists` → 一次删三个名字（正常/不存在/内部）核对逐项结果 → 列表确认主题消失；并用 `kafka-topics.sh --list` 在 broker 侧复核。

## 结果

- 改动：新增 `real/admin.rs`（create/delete + AdminOptions/错误映射），`mod.rs` 加 `mod admin;`、`Live.brokers`、两处委派与 `admin_api` 能力位，`topic.rs` 的 `is_internal` 提为 `pub(super)`；3 文件 193 行，未超上限。
- 验证：`cargo check --features real`、`clippy --features real --all-targets -D warnings` 均绿；`cargo test -p kafka-driver` 72/72。
- 真集群（Docker `rs-kafka-ui-dev`）：建 `t072a-demo` 3 分区 rf=1 带 `retention.ms=123456789` → `describe_topics` 核对一致；重复建 → `AlreadyExists`；一次删 demo/ghost/`__consumer_offsets` 得到「成功 / unknown topic t072a-ghost / 拒绝 internal」；`kafka-topics.sh --list` 侧复核只剩 `t071-demo` 与 `__consumer_offsets`。
- Ruling：R38（`Live.brokers` 省掉一次 metadata 往返）、R40（内部主题客户端一律拒删，与 mock 行为对齐）。
