# T073 — real：produce（批量 + acks）

状态: ✅　依赖: T070　验证: 真集群手动清单　类型: 产品码

## 目标

把 `produce` 接到 `rdkafka::producer::FutureProducer`：整批先全部入队、再统一收投递回执，按请求顺序回 `Vec<ProduceResult>`；同时修掉「按主题名做 metadata 探测会把主题创建出来」这个副作用。

## 只读白名单（最多 3 个源文件）

- crates/kafka-driver/src/real/mod.rs
- crates/kafka-driver/src/real/topic.rs
- crates/kafka-driver/src/mock/driver.rs（produce 语义基线）

## 要改的文件

- `crates/kafka-driver/src/real/producer.rs`（新增）：`producer_config`、`real_produce`、入队与回执映射
- `crates/kafka-driver/src/real/mod.rs`：`Session`/`Live` 带上生产器，`connect` 建、`disconnect` 拆，`produce` 改委派
- `crates/kafka-driver/src/real/config.rs`：`allow.auto.create.topics=false`

## 必须覆盖

- 空 topic / 空 messages → 沿用 `ProduceReq::validate()`。
- 主题不存在 → 整包 `DriverError::TopicNotFound`（与 mock 一致），且**不得**在 broker 上把主题创建出来。
- 显式 `partition` 越界 → 单条 `error = Some("unknown topic {topic} partition {p}")`（与 mock 同文案），不打扰 broker。
- 未指定 `partition` → 交给 librdkafka 分区器；结果里的 `partition`/`offset`/`timestamp` 取自投递回执。
- 单条失败不影响同批其它条：`ProduceResult.error` 逐条填。
- `acks=all` + `enable.idempotence=true`（重试不产生重复）。
- 结果顺序 = `req.messages` 顺序。
- `messages` 里的 headers 要真正发出去（`OwnedHeaders`）。

## 明确不做

- 不做事务生产、不做压缩/批量参数调优（属于 `cfg.extra` 逃生通道）。
- 不补 real 侧自动化测试（T077 统一做）。
- `capabilities()` 没有 produce 位可开，本卡不动它。

## 已知取舍（Ruling）

- **R42**：`client_config` 显式设 `allow.auto.create.topics=false`。librdkafka 对 **producer 型**客户端的该默认值是 `true`（`librdkafka/src/rdkafka_conf.c:4412`；`AdminClient` 内部就是 producer 型），所以「按主题名探测存在性」这一步会顺手把主题建出来——实测 T072b 的 `t072b-ghost` 就是这么出现在 broker 上的。代价：依赖 broker 自动建主题的工作流（本 UI 不提供）不再生效；README 里的 `cfg.extra` 仍可覆写。
- **R43**：`delivery.timeout.ms` 取 `request_ms`，并把 `linger.ms` 设为 0（librdkafka 要求 `delivery.timeout.ms >= linger.ms + request.timeout.ms`）。代价：放弃 linger 攒批；批量连贯性由「先全部入队、再收执回」保证。
- **R44**：结果顺序保留 `req.messages` 顺序（不按投递完成顺序回传）：按序入队、入队即失败者记在原位，再 `join_all` 回填到各自的槽位。

## 验收

1. `cargo check -p kafka-driver --features real` 通过。
2. `cargo clippy -p kafka-driver --features real --all-targets -- -D warnings` 零告警。
3. `cargo test -p kafka-driver`（mock 回归）72/72 通过。
4. Docker 单节点真集群跑临时 example：① 批量 3 条（含 key、headers、显式分区）→ 回执 partition/offset 且 `kafka-console-consumer.sh` 侧内容一致；② 对不存在的主题 produce → `TopicNotFound`，且 `kafka-topics.sh --list` 确认**未**新建主题；③ 越界分区 → 单条 error、其余正常；④ 空 topic / 空 messages → `InvalidConfig`。

## 结果

- 改动：新增 `real/producer.rs`（`producer_config` + `real_produce`：整批入队、`join_all` 收回执、按请求顺序回填），`mod.rs` 把生产器挂进 `Session`/`Live`、`connect` 建 `disconnect` 拆并改委派，`config.rs` 关掉 `allow.auto.create.topics`；3 文件 212 行，未超上限。
- 验证：`cargo check -p kafka-driver --features real`、`cargo clippy -p kafka-driver --features real --all-targets -- -D warnings` 均绿；`cargo test -p kafka-driver`（mock 回归）72/72。
- 真集群（Docker `rs-kafka-ui-dev`，主题 `t073-demo`）：① 批量 3 条（key + header + 指定分区 + 自动分区）→ 回执按请求顺序给出 partition 0/0/2 与真实 offset/时间戳，`kafka-console-consumer.sh` 侧见 `h:v`/`k1` 且内容一致；② 不存在的主题 → 整包 `TopicNotFound`，`kafka-topics.sh --list` 确认**未**新建；③ 越界分区 9 → 该条 `unknown topic t073-demo partition 9`，同批 partition 1 那条照常成功；④ 空 topic / 空 messages → `InvalidConfig`。
- 顺带修复：R42 一并消掉了 T072b 的遗留副作用——本卡第 ⑤ 步对不存在主题写配置得到 `NotFound`，且 broker 上**没有**出现 `t073-ghost2`；T072b 期间被误建的 `t072b-ghost` 已删除。
- Ruling：R42（producer 型客户端该默认值是 `true`，metadata 探测会顺手建主题，故显式关闭）、R43（`delivery.timeout.ms` = request_ms 且 `linger.ms` = 0）、R44（结果顺序 = 请求顺序）。
