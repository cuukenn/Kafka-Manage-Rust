# T075a — real：消费组列表与详情（list / describe）

状态: ✅　依赖: T071b　验证: 真集群手动清单　类型: 产品码

## 目标

把 real 驱动的消费组**元数据**读出来：`list_consumer_groups` 与 `describe_consumer_groups`，含每个成员及其分区分配（本地解码 ConsumerProtocol 的 assignment 字节）。

## 只读白名单（最多 3 个源文件）

- crates/kafka-driver/src/mock/driver.rs（`list_consumer_groups` / `describe_consumer_groups` 语义基线）
- crates/kafka-core/src/group.rs（DTO 契约：`GroupInfo` / `GroupDetail` / `GroupMember` / `GroupState`）
- crates/kafka-driver/src/real/mod.rs（`Live` 脚手架）

## 要改的文件

- `crates/kafka-driver/src/real/group.rs`（新增）
- `crates/kafka-driver/src/real/mod.rs`

## 必须覆盖

- `list_consumer_groups`：一次 `fetch_group_list(None, timeout)`，映射为 `GroupInfo`；空集群回空列表。
- `describe_consumer_groups`：`ids` 为空 = 全部组（与 mock 一致）；给定 id 时**逐一** `fetch_group_list(Some(id))`，取不到即 `DriverError::GroupNotFound`。
- 成员分区分配：`GroupMemberInfo::assignment()` 是 `ConsumerProtocolAssignment` 的序列化字节（`int16` 版本 + `int32` 主题数 + 每主题「`int16` 长度主题名 + `int32` 分区数 + 分区号」+ `bytes` user_data），在本地解码成 `Vec<TopicPartition>`；解不出就回空列表，**不报错**。
- 字段映射：`partition_assignor` ← librdkafka 的 `protocol()`（选定的分配器名，如 `range`）；`protocol_type` ← `protocol_type()`（如 `consumer`）；`member_count` ← 成员数。
- `fetch_group_list` 是同步阻塞调用，走 `spawn_blocking`（R31），且**原生快照（`GroupList`，非 `Send`）在阻塞闭包内解析成 owned 结构后才返回**（R35 同款处理）。
- 只有本卡落地后才把 `capabilities().consumer_groups` 置 `true`（R32）。

## 明确不做

- 不读提交位点 / 滞后（T075b）。
- 不做 reset / commit（T075c）。
- 不用 `rd_kafka_ListConsumerGroups` / `rd_kafka_DescribeConsumerGroups` admin API（rdkafka 0.39 未包装，只有已废弃的 `rd_kafka_list_groups` 可用）。
- 不解析 `metadata()`（订阅态，UI 不展示）。
- 不补 real 侧自动化测试（T077 统一做）。

## 已知取舍（Ruling）

- **R55**：`coordinator_id` 恒为 `None`。librdkafka 的 `GroupInfo` 只暴露 name/state/protocol/protocol_type/members，没有协调者 id（与 R30 同类）。代价：UI 详情页协调者一栏显示 `—`。
- **R56**：`is_local` 恒为 `false`。真实 broker 不认识本驱动合成的读组名（R50），无从判定「这个组是不是本应用自己的订阅」。代价：real 下没有「本地组」高亮。
- **R57**：`fetch_group_list(Some(id))` 对不存在的组回**空列表**而不是错误（librdkafka 文档：错误码只用于传输/超时/部分成功），所以「查不到」以列表为空判定；同理 assignment 解码失败按空分配处理。

## 验收

1. `cargo check -p kafka-driver --features real` 通过。
2. `cargo clippy -p kafka-driver --features real --all-targets -- -D warnings` 零告警。
3. `cargo test -p kafka-driver`（mock 回归）72/72 通过。
4. Docker 单节点真集群跑临时 example：① 消费组列表能列出 `t071-g` 等真实组；② 用 `kafka-console-consumer.sh` 挂一个活跃组成员，`describe` 能读出成员 id / client id / 主机，且 `member_count` 与 `kafka-consumer-groups.sh --describe` 一致；③ `describe` 一个不存在的组回 `GroupNotFound`；④ 空 `ids` 回全部组。

## 结果

- 改动：新增 `real/group.rs`（159 行）+ `real/mod.rs`（+11/−6），共 170 行/2 文件，未超上限；`capabilities().consumer_groups` 本卡置 `true`。
- 验证：`cargo check -p kafka-driver --features real`、`cargo clippy -p kafka-driver --features real --all-targets -- -D warnings` 均绿；`cargo test -p kafka-driver`（mock 回归）72/72。
- 真集群（Docker `rs-kafka-ui-dev`）：① 列表回 2 组——活跃 `t075a-g`（Stable/1 人/`range`）与空闲 `t071-g`（Empty/0 人/空分配器）；② `describe` 解出成员 id/client_id/host 及分配的分区，与 `kafka-consumer-groups.sh --describe` 逐列一致（成员 id、主机、分区完全相同）；③ 不存在的组回 `GroupNotFound`；④ 空 `ids` 回全部组。
- Ruling：R55（`coordinator_id` 恒 `None`）、R56（`is_local` 恒 `false`）、R57（「查不到」以列表为空判定，assignment 解码失败按空分配处理）。
- 说明：位点读取/滞后与 reset/commit 分别落在 T075b / T075c。
