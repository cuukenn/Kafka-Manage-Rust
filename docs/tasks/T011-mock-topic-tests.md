# T011 · Mock 测试：topic CRUD / describe / 配置 / offsets

**状态**: ⬜未开始 | **依赖**: T002 | **改动上限**: 1 文件 / 250 行

## 目标
锁住 Mock 的 topic 管理语义，保证与真实 Kafka 行为一致（前端调试才有意义）。

## 只读白名单（最多 3 个）
- `crates/kafka-core/src/topic.rs` — `CreateTopicReq`/`DeleteTopicsReq`/`DescribeTopicsReq`/`UpdateTopicConfigReq`/`ListOffsetsReq`/`TopicInfo`/`TopicDetail`/`OffsetRange` + `validate()`
- `crates/kafka-driver/src/driver.rs` — 方法签名
- 需要时看 `crates/kafka-driver/src/mock/state.rs` 的 `insert_topic`

## 要改的文件
- 新建 `crates/kafka-driver/tests/mock_topics.rs`

## 必须覆盖
- 建 topic → `list_topics` 能看到，分区数/副本数正确
- 重复建同名 topic → `DriverError::TopicAlreadyExists`
- 非法名（空、含非法字符、>249）/ `replicationFactor` 超过 broker 数 → `InvalidConfig`
- `describe_topics` 返回每个分区的 leader、replicas、isr
- `delete_topics` 返回逐项结果，删除后 `list_topics` 不再包含
- `update_topic_config` 改 `retention.ms` 后 `describe` 能看到新值
- `list_offsets`：空 topic 的 beginning/end 关系正确（earliest==latest）

## 明确不做
- 不改生产代码；缺陷登记到 T016
- 不测 produce/consume

## 验收
- `cargo test -p kafka-driver --test mock_topics` 全绿，测试数 ≥ 8

## 结果（完成后填写，≤5 行）
-