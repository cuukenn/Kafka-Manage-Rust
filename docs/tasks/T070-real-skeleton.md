# T070 — `real` 模块骨架 + 配置映射 + 连接生命周期

状态: ⬜　依赖: T001　验证: `cargo check -p kafka-driver --features real`　类型: 产品码

## 目标
让 `--features real` 首次编译通过，并把 real 驱动的**连接生命周期**打通：`connect` 建
librdkafka 客户端并拉集群元信息，`cluster_info` 复用同一份元信息，`disconnect` 释放。
其余域方法（topic/produce/consume/group）本卡**先返回 `Unsupported`**，由 T071–T076 逐卡替换，
这样每一卡的验收命令都是同一句 `cargo check --features real`，编译风险最早暴露。

真实 Kafka 用 `scripts/kafka/docker-compose.yml`（Docker Desktop，KRaft 单节点，localhost:9092）。

## 只读白名单（最多 3 个源文件）
- `crates/kafka-driver/src/driver.rs` — trait 签名 / `DriverCapabilities` / `DriverKind`
- `crates/kafka-driver/src/mock/driver.rs` — `connect`/`disconnect`/`cluster_info`/`capabilities` 语义参照
- `crates/kafka-core/src/cluster.rs` — `ClusterConfig`/`ClusterInfo`/`SecurityProtocol`/`TimeoutConfig` 字段

## 要改的文件
- 新建 `crates/kafka-driver/src/real/mod.rs`
- 新建 `crates/kafka-driver/src/real/config.rs`

（`lib.rs` 已有 `#[cfg(feature="real")] pub mod real;`，`driver.rs` 已引用 `real::RealKafkaDriver`，均不改。）

## 必须覆盖
1. `config::client_config(&ClusterConfig) -> Result<ClientConfig, DriverError>`：
   `bootstrap.servers`、`client.id`、`socket.timeout.ms`/`request.timeout.ms`/`socket.connection.setup.timeout.ms`
   （来自 `TimeoutConfig`）、`security.protocol`（`SecurityProtocol::as_str()`，本卡仅 PLAINTEXT/SSL 开关）、
   最后用 `cfg.extra` 覆盖（逃生舱，键值直接 `set`）。
2. `RealKafkaDriver { inner: RwLock<Option<Session>> }`，`Session { cfg, client: Arc<Client<DefaultClientContext>>, info }`；
   `new()` 返回未连接态。
3. `connect`：`cfg.validate()` → 建 `Client`（`ClientConfig::create`，失败映射 `Connection`）→
   真元信息调用放 `tokio::task::spawn_blocking`（librdkafka 同步 API 不得阻塞 runtime）→
   `fetch_metadata(None, timeouts.metadata_ms)` 失败映射 `Timeout`/`Connection` → 组装 `ClusterInfo` 落 Session。
4. `cluster_info` 未连接时 `NotConnected`，否则回放 Session 里的 `ClusterInfo`；`disconnect` 清 Session
   （未连接即 no-op，返回 `Ok`）；`is_connected` 看 Session 是否存在。
5. `ClusterInfo` 组装：`brokers` 来自 `metadata.brokers()`（`node_id`/`host`/`port`）；
   `cluster_id`/`controller_id` 本卡均为 `None`（librdkafka metadata 既不含 cluster id 也不含
   controller id，见 R30；controller 由 T071 的 `AdminClient::describe_cluster` 补）；
   `kafka_version` 走 `cfg.extra` 的 `broker.version` 或 `None`；`latency_ms` 记录本次抓取耗时。
6. 未实现方法统一 `DriverError::unsupported("real driver: <what>")`；`kind()` = `Real`；
   `as_any()` 保持默认 `None`（满足 `conformance.rs::real_driver_offers_no_as_any_seam`）；
   `capabilities()` 本卡返回 `DriverCapabilities::default()`（全 false），由落地的各卡逐位打开
   （R32），避免 UI 提前亮出一个点下去必然 `Unsupported` 的按钮。

## 明确不做（防止范围膨胀）
- 不做 topic/produce/consume/group 的真实实现（T071–T075）。
- 不做 SASL/SCRAM/OAuth（T076）；SSL 本卡只映射 `security.protocol`，证书字段留 T076。
- 不加新 trait 方法、不改 mock、不改 `kafka-core`、不加新依赖。
- 不写 real 的集成测试（T077 建一致性测试架）。

## 验收
- `cargo check -p kafka-driver --features real` 全绿（首次需编译 librdkafka）。
- `cargo test -p kafka-driver`（不带 real）仍全绿。
- `cargo clippy -p kafka-driver --features real --all-targets -- -D warnings` 零新增告警。

## 裁定（执行时确认）
- R30：real 驱动 `ClusterInfo.cluster_id` 与 `controller_id` 本卡允许为 `None`（librdkafka
  metadata 不含二者）；前端把 `None` 渲染为 `—`，不得因此判连接失败。controller 由 T071 的
  `AdminClient::describe_cluster` 补。代价：T070~T071 之间真集群页少显示两个字段。
- R31：real 驱动的同步 librdkafka 调用一律走 `spawn_blocking`；`connect` 的元信息超时用
  `Timeouts::metadata_ms`，不是硬编码。
- R32：`capabilities()` 逐卡打开，本卡全 false；不提前声明尚未实现的域。

## 结果（完成后填写，≤5 行）
- 新增 `real/mod.rs`(213) + `real/config.rs`(35) = **248 行 / 2 文件**（≤250/3 达标；未改 lib.rs/driver.rs/Cargo.toml）。
- `cargo check -p kafka-driver --features real` 绿（macOS arm64：cmake 4.4.4 + pkg-config + OpenSSL 3.5.9，
  `PKG_CONFIG_PATH=/opt/homebrew/opt/openssl@3/lib/pkgconfig`）；`clippy --features real --all-targets -D warnings` 零告警；mock 72 测试全绿。
- 对 Docker Kafka 实测（`scripts/kafka/docker-compose.yml` → localhost:9092，cluster `5L6g3nShT-eMCtK--X86sw`）：
  `connect` 拿到 1 broker(localhost:9092)、`is_connected`/`cluster_info`/`disconnect`/断开后 `NotConnected(名字)` 全对（临时 example 验证后删除）。
- R30 落实：`cluster_id`/`controller_id` 均 `None`——librdkafka `Metadata` 无二者，`AdminClient` 也没有 `describe_cluster`。
- R32 落实：`capabilities()` 本卡全 `false`，由 T071–T076 逐位打开。
