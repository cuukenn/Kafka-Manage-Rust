# T072b — real：update topic config

状态: ✅　依赖: T072a　验证: 真集群手动清单　类型: 产品码

## 目标

把 `update_topic_config` 接到 `AdminClient::alter_configs`，并处理掉这个 API 的非增量语义：先读回主题当前被覆盖的配置，叠加 `set`、去掉 `unset`，把合并后的全集发出去。

## 只读白名单（最多 3 个源文件）

- crates/kafka-driver/src/real/mod.rs
- crates/kafka-driver/src/real/topic.rs
- crates/kafka-core/src/topic.rs（`UpdateTopicConfigReq` 契约）

## 要改的文件

- `crates/kafka-driver/src/real/admin.rs`：新增 `real_update_topic_config` 与 `config_error`
- `crates/kafka-driver/src/real/mod.rs`：`update_topic_config` 改为委派；`capabilities` 打开 `topic_config_write`
- `crates/kafka-driver/src/real/topic.rs`：`overridden_configs` 提到 `pub(super)`（配置写入要拿它当基线）

## 必须覆盖

- 空 `topic` → `DriverError::invalid("topic", …)`；空 key → `DriverError::invalid("set", …)`；`set` 与 `unset` 都空 → 直接 `Ok(())` 不打扰 broker。
- 写之前先确认主题存在（`fetch_metadata(Some(topic))` + `topic::plan(..., include_internal: true)`），不存在 → `TopicNotFound`。
- 合并基线 = `overridden_configs` 的当前覆盖项；写入时把**全集**发给 `alter_configs`，未提及的参数才不会被重置。
- `unset` 全部生效后合并集为空时仍要正常写入（不得报错）。
- `UnknownTopicOrPartition` → `TopicNotFound`，`InvalidConfig` → `DriverError::invalid("set", …)`，其余进 `DriverError::Broker`。
- `capabilities()` 打开 `topic_config_write`。

## 明确不做

- 不做增量 AlterConfigs：rdkafka 0.39 没有包装 `rd_kafka_IncrementalAlterConfigs`，直接下 `rdkafka_sys` 要走 unsafe FFI，超出本卡预算。
- 不写 `sensitive` 配置（值读不回来），也不碰 broker 级配置（契约里只有主题级请求）。
- 不在本卡补 real 侧自动化测试（T077 统一做）。

## 已知取舍（Ruling）

- **R37**：rdkafka 0.39 的 `alter_configs` 是**非增量** AlterConfigs，文档原文为 “resetting unspecified parameters to their default values”。只发 `set` 会抹掉该主题其它所有覆盖项，因此必须先 `describe_configs` 读回当前覆盖集合、合并后再整体发。代价有二：① 每次配置写入多一次 `DescribeConfigs` 往返；② `sensitive` 覆盖项的值读不回来（恒为 null），若某主题存在敏感覆盖项，本次写入会把它重置为默认——topic 级敏感配置在实践中极罕见，接受并记录。
- **R39**：配置写入前**显式**确认主题存在。实测发现：对不存在的主题，Kafka 在变更集为空时会把 `AlterConfigs` 当成功应答（本例第 5 步原本返回 `Ok`），若靠写入结果反推存在性，UI 会显示一次「假保存成功」。多一次 metadata 往返换一个确定的 `TopicNotFound`，值得。
- **R41**：`unset` 在本驱动是支持的（不同于早先设想的「rdkafka 无法表达 null」）：把该键从合并集里去掉再整体替换，语义就等于回到默认值。

## 验收

1. `cargo check -p kafka-driver --features real` 通过。
2. `cargo clippy -p kafka-driver --features real --all-targets -- -D warnings` 零告警。
3. `cargo test -p kafka-driver`（mock 回归）72/72 通过。
4. 对 Docker 单节点真集群跑临时 example，并在其中直接调 `kafka-configs.sh --describe` 做 broker 侧复核。

## 结果

- 改动：`admin.rs` 新增 `real_update_topic_config`（存在性校验 + 合并后整体替换）与 `config_error`，`mod.rs` 改委派并打开 `topic_config_write`，`topic.rs` 把 `overridden_configs` 提为 `pub(super)`；3 文件 94 行，未超上限。
- 验证：`cargo check --features real`、`clippy --features real --all-targets -D warnings` 均绿；`cargo test -p kafka-driver` 72/72。
- 真集群（Docker `rs-kafka-ui-dev`，主题 `t072b-demo`）：① 一次写两项 → describe 与 `kafka-configs.sh` 均见 `DYNAMIC_TOPIC_CONFIG`；② 只写 `retention.ms` → `cleanup.policy` **保留**（证明非增量替换已被合并抵消）；③ `unset cleanup.policy` → 只剩 `retention.ms`，broker 侧确认；④ `unset` 全部 → 合并集为空仍写成功、配置清零；⑤ 改不存在的主题 → `NotFound`；⑥ 空主题名 → `InvalidConfig`。
- Ruling：R37（合并后整体替换，代价是多一次 DescribeConfigs 且敏感覆盖项会被重置）、R39（写入前显式校验存在性，避免「假保存成功」）、R41（`unset` 可用）。
