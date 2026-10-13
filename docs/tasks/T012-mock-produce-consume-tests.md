# T012 · Mock 测试：produce + subscribe 消息流 + offset 语义

**状态**: ⬜未开始 | **依赖**: T002 | **改动上限**: 1 文件 / 250 行

## 目标
锁住「写内存 → 流式读回」的核心闭环，这是 Mock 模式最有价值的部分。

## 只读白名单（最多 3 个）
- `crates/kafka-core/src/message.rs` — `ProduceReq`/`ProduceResult`/`SubscribeReq`/`MessageEvent`/`MessageStream`/过滤条件
- `crates/kafka-driver/src/driver.rs` — `produce`/`subscribe` 签名
- 需要时看 `crates/kafka-driver/src/mock/pump.rs`（推送节奏）

## 要改的文件
- 新建 `crates/kafka-driver/tests/mock_produce_consume.rs`

## 必须覆盖
- 单条 produce：返回 partition + offset，`key` 相同 → 落同一分区（若实现如此）
- 批量 produce：结果条数 == 请求条数，offset 连续递增
- 指定 partition 发送 → 落在该分区
- headers 往返一致（含 base64/二进制）
- `subscribe` 从 earliest 读：能读到已写入的全部消息，offset 从 beginning 开始
- `subscribe` 从 latest 读：只读新写入的
- 指定 offset / 时间戳起点（若 `SubscribeReq` 支持）
- 先订阅后写入：新消息能被推送到流里（用 `tokio::time::timeout` 包住 `recv`）
- 过滤条件命中/不命中

## 明确不做
- 不改生产代码；缺陷登记 T016
- 不测 pause/resume/seek（T013）
- 测试里禁止 `sleep` 超过 200ms，避免测试变慢

## 验收
- `cargo test -p kafka-driver --test mock_produce_consume` 全绿，测试数 ≥ 8
- 单测总耗时 < 3s

## 结果（完成后填写，≤5 行）
- 新建 `crates/kafka-driver/tests/mock_produce_consume.rs`（249 行，10 个用例，0 ignored）：produce 单条/批量 offset 连续、显式 partition 落地、mock 按 round-robin 路由（key 不固定分区，故卡片「若实现如此」分支不成立，改为锁定 round-robin）、key+二进制 headers 原样往返、earliest/latest/指定 offset/时间戳起点、越界 offset 报 `OffsetOutOfRange`、先订阅后写入能推送、过滤命中/不命中。
- 验证：`cargo test -p kafka-driver --test mock_produce_consume` 10 passed / 0 failed（测试二进制耗时 0.172s）；`cargo clippy -p kafka-driver --all-targets -- -D warnings` 0 warnings；`cargo test --workspace` 全绿。未发现生产缺陷，无 `#[ignore]`；全程无 sleep，靠 `start_paused` + `max_messages` 界定读取。
