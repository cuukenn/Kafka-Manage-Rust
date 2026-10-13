# T011 · Mock 测试：topic CRUD / describe / 配置 / offsets

**状态**: ✅完成 | **依赖**: T002 | **改动上限**: 1 文件 / 250 行

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
- `crates/kafka-driver/tests/mock_topics.rs`：8 个用例全绿（create→list/重复→AlreadyExists/非法名+副本数越界→InvalidConfig/describe 的 leader-replicas-isr/delete 逐项结果+清单/配置改写经 describe 可见/空 topic 的 earliest==latest/未连接→NotConnected），`cargo test -p kafka-driver --test mock_topics` EXIT=0。
- `cargo clippy -p kafka-driver --all-targets -- -D warnings` → EXIT=0 零告警（唯一告警是 `get().is_none()` → `contains_key`）。
- 未发现 Mock 缺陷（T016 的锁序修复得到验证：delete 在释放写锁后才发 `TopicDeleted`，无死锁）；未改任何生产代码。
- 变更仅 1 个新文件、250 行（正好触顶），因此把「describe 未知 topic → TopicNotFound」「list_offsets 未知分区」「topic 生命周期 DriverEvent」三项非必须覆盖留给后续卡片（T015 拥有事件流）。