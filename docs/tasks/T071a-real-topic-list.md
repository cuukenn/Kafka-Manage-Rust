# T071a — real：cluster_info 复用 + list_topics

状态: ✅　依赖: T070　验证: `cargo check -p kafka-driver --features real` + 真集群手动清单　类型: 产品码

## 目标
给 real 驱动做**每次域操作都要用的那层脚手架**，并用第一个只读方法 `list_topics` 验收它：
把「取活会话 + 取元信息」从 T070 的 `connect` 里抽成 `Live` 快照与 `fetch_metadata()`，后续 T071b–T076
全部复用。`describe_topics` 本卡**仍返回 `Unsupported`**（留 T071b）。

真实 Kafka 用 `scripts/kafka/docker-compose.yml`（Docker Desktop，KRaft 单节点，localhost:9092）。

## 只读白名单（最多 3 个源文件）
- `crates/kafka-driver/src/mock/driver.rs` — `list_topics` 语义参照（含 internal topic 照回）
- `crates/kafka-core/src/topic.rs` — `TopicInfo` 字段
- `crates/kafka-driver/src/real/mod.rs` — 复用/additive 改造 T070 的 `Session`

## 要改的文件
- 修改 `crates/kafka-driver/src/real/mod.rs`
- 新建 `crates/kafka-driver/src/real/topic.rs`

## 必须覆盖
1. `Session` 记下 `cfg: ClusterConfig`；新增 `Live { admin, cfg }` 与 `live()`（未连接 → `NotConnected`，
   名字取 driver 记下的 cluster name）。每次操作取一次快照，**不跨 await 持有读锁**。
2. `fetch_metadata(live, Option<&str>)` 抽成共用助手（`spawn_blocking`，R31：librdkafka 同步 API 不得阻塞
   runtime）；`metadata_error` 改成按 `server` 字符串 + `operation` 名构造，T070 的 `connect` 改走
   `fetch_metadata`（错误语义不变：元信息超时 → `Timeout`，其余 → `Connection`）。
3. `list_topics`：`fetch_metadata(None)` → 每个 `MetadataTopic` 出 `TopicInfo`：
   `internal` 用 `__` 前缀判定（librdkafka metadata 不标 internal，只有这一个信号）；
   `partition_count` = 分区数；`replication_factor` = 各分区副本数的最大值；
   带 `error()` 的 topic 跳过；`configs` 本卡留空（R33）。
4. `capabilities()` 仍全 false —— 只读面没有能力位。

## 明确不做（防止范围膨胀）
- 不做 `describe_topics`（T071b，本卡保持 `Unsupported`）、不做 create/delete/update topic（T072）、
  list_offsets（T074）、produce/subscribe（T073/T074）。
- 不补 `cluster_id`/`controller_id`（R30 已裁定：rdkafka 0.39 `AdminClient` 无 `describe_cluster`，
  librdkafka `Metadata` 也不含这两个字段）。
- 不改 `driver.rs` / `lib.rs` / `config.rs`。

## 已知取舍（Ruling）
- R33：`list_topics` 的 `configs` 留空。列表页不渲染配置，填它要为每次刷新多打一轮 `DescribeConfigs`；
  配置由 T071b 的 `describe_topics` 批量拉。代价：与 mock 的 `list_topics` 在该字段上有分歧，
  T077 对齐架若要断言此字段须按本裁定写。
- R35：`MetadataTopic`/`Metadata` 内含 librdkafka 裸指针，**既非 `Send` 也非 `Sync`**，不能跨 await。
  异步函数的「取元信息 → 等待」两段必须在同步阶段先把需要的东西拷成 owned 数据。

## 验收
- `cargo check -p kafka-driver --features real` 全绿
- `cargo clippy -p kafka-driver --features real --all-targets -- -D warnings` 无告警
- `cargo test -p kafka-driver`（mock 回归）全绿
- 真集群手动清单：连 Docker Kafka → `list_topics` 同时出现 `t071-demo`（`internal=false`、
  3 分区、`rf=1`）与 `__consumer_offsets`（`internal=true`、50 分区）；
  断开后 `list_topics` → `NotConnected(<cluster name>)`

## 结果（完成后填写，≤5 行）
- 2 文件 92 行（`real/mod.rs` +51、新建 `real/topic.rs` 41），`check`/`clippy -D warnings` 无告警，mock 72/72 绿。
- 抽出 `Live`（admin + cfg 快照）与 `live()`/`fetch_metadata()`；T070 的 `connect` 改走同一助手，
  错误语义不变（元信息超时 → `Timeout`，其余 → `Connection`）。
- `list_topics` 由 metadata 出概览：`internal` 走 `__` 前缀、`rf` 取各分区副本数最大值、带 `error()` 的 topic 跳过。
- Docker Kafka 实测：`t071-demo` internal=false 3 分区 rf=1、`__consumer_offsets` internal=true 50 分区；
  断开后 `list_topics` → `NotConnected("docker-dev")`。
- 原 T071 计划 295 行超上限，按项目既有后缀卡惯例拆为 T071a/T071b（`describe_topics` 留 T071b）。
