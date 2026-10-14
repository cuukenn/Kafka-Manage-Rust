# T034 · Router：`topic.*` + `produce.send`

**状态**: ⬜未开始 | **依赖**: T033 | **改动上限**: 3 文件 / 250 行

## 目标

注册 6 个 `topic.*` + 1 个 `produce.send` 命令，复用 T033 `handlers.rs` 的共享 helper
（`parse_payload` / `to_data` / `cluster_id_of`）与 handler 模板。

## 只读白名单（最多 3 个源文件，别读更多）

- `crates/webview-bridge/src/handlers.rs` — helper 与 handler 模板（T033 产物）
- `crates/kafka-core/src/topic.rs` — 各 Req/Resp DTO 字段（camelCase serde + Default）
- `crates/kafka-core/src/message.rs` — `ProduceReq` / `ProduceResult` 字段

## 预先钉死的接口事实（不必去读其它文件）

- cmd 常量（kafka-core protocol.rs）：`TOPIC_LIST` `TOPIC_DESCRIBE` `TOPIC_CREATE` `TOPIC_DELETE`
  `TOPIC_UPDATE_CONFIG`（wire 名 `topic.updateConfig`）`TOPIC_OFFSETS` `PRODUCE_SEND`。
- TopicService（`app.topics()`）：`list(&ClusterId)`、`describe(&ClusterId,&DescribeTopicsReq)`、
  `create(&ClusterId,&CreateTopicReq)`、`delete(&ClusterId,&DeleteTopicsReq)`、
  `update_config(&ClusterId,&UpdateTopicConfigReq)`、`offsets(&ClusterId,&ListOffsetsReq)`；
  全部 `Result<_, ServiceError>`，错误 `to_dto()`。
- ProduceService（`app.produce()`）：`send_batch(&ClusterId, &[ProduceReq]) -> Result<ProduceReport, ServiceError>`
  （空批次 → `invalid_config`）。`ProduceReport`（Serialize camelCase）：`results[] / okCount / errorCount`。
- payload 形状：`{clusterId, ...req 字段}` — 本卡文件内定义局部 payload struct：
  `#[serde(rename_all="camelCase", default)] struct XPayload { cluster_id: ClusterId, #[serde(flatten)] req: TheReq }`。
- `produce.send` payload：`{clusterId, requests:[ProduceReq...]}` → `send_batch` → data=ProduceReport。
- mock 集群夹具同 T033。

## 要改的文件

- 新建 `crates/webview-bridge/src/handlers_topic.rs` — `register_topic_and_produce(app: &kafka_app::App, router: &Router)`
- 新建 `crates/webview-bridge/tests/router_topic_produce.rs`
- 修改 `crates/webview-bridge/src/lib.rs` — 加 mod

## 必须覆盖

1. 7 个命令全部用 `cmd::` 常量注册（不许手写字符串）。
2. 端到端（全部经 `router.handle(raw_json_str)`）：connect mock 集群 → `topic.create` ok →
   `topic.list` 含新 topic → `topic.describe` 字段对 → `topic.updateConfig` ok →
   `topic.offsets` 返回 `OffsetRange`（low/high）→ `topic.delete` → `topic.list` 不再含。
3. `produce.send` 单条与多条：`okCount/errorCount` 正确、`results.len()==requests.len()`；
   发往不存在的 topic 时逐条 `results[i].error` 有值但响应仍 `ok:true`（部分失败不是调用失败）。
4. 错误路径：未连接 `topic.list` → `not_connected`；`topic.create` 空 name → `invalid_config`；
   payload 类型错（如 partitions 传字符串）→ `invalid_config`。
5. 测试数 ≥ 8。

## 明确不做（防止范围膨胀）

- 不接 consume/group/mock 命令；不做事件泵；不修改 kafka-app / kafka-core。

## 验收

- `cargo test -p webview-bridge` 全绿（新增测试 ≥ 8）
- `cargo clippy -p webview-bridge --all-targets -- -D warnings` 零告警
- `cargo check --workspace` 通过

## 结果（完成后填写，≤5 行）
-
