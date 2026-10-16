# T076 — real：SASL / SSL 参数映射（来自 ClusterConfig）

状态: ✅　依赖: T070　验证: 真集群手动清单　类型: 产品码

## 目标

把 `ClusterConfig.auth` / `.ssl` 映射成 librdkafka 属性，让 `connect` 能真的连上
`SASL_PLAINTEXT/PLAIN` 与 `SSL` 两种监听器；并把 `capabilities().sasl` / `.ssl` 打开。

## 只读白名单

- `crates/kafka-core/src/cluster.rs`（`SecurityProtocol` / `SaslMechanism` / `AuthConfig` / `SslConfig`）
- `crates/kafka-core/src/error.rs`
- `docs/tasks/T070-real-skeleton.md`

## 要改的文件

1. `crates/kafka-driver/src/real/config.rs` —— 增加 SASL/SSL 分支（`client_config` 仍是唯一入口）
2. `crates/kafka-driver/src/real/mod.rs` —— `capabilities()` 打开 `sasl` / `ssl`
3. `scripts/kafka/docker-compose.yml` + `scripts/kafka/gen-secrets.sh` —— 追加 `SASL`(9094) 与 `SSL`(9095) 监听器，供真实校验

## 必须覆盖

- `uses_sasl()` 为真：`sasl.mechanism` ← `SaslMechanism::as_str()`、`sasl.username`、`sasl.password`
- `auth.sasl_protocol` 非空：覆盖 `security.protocol`（小写化）——见 R64
- `uses_tls()` 为真：`ssl.ca.pem` / `ssl.ca.location` / `ssl.certificate.pem` + `ssl.key.pem` /
  `enable.ssl.certificate.verification` / `ssl.endpoint.identification.algorithm`
- 证书与私钥必须成对，单边 → `DriverError::invalid("ssl.…")`（R66）
- `cfg.extra` **仍然最后应用**（逃生舱永远是最后一句）
- `capabilities()`：`sasl: true, ssl: true`，其余不动

## 明确不做

- 不做 OAUTHBEARER/GSSAPI 的回调与 token 刷新（只有属性映射，回调留给需要它的卡）
- 只接 PEM（`ca_pem`/`cert_pem`/`key_pem` 内容，或 `ca_path` 路径），不做 JKS/PKCS12 转换
- 不校验密码是否非空：那要由 broker 拒绝，映射层不做语义判断（错密码必须真的连不上，见验收）

## 已知取舍（Ruling）

- **R64**：`auth.sasl_protocol` 按 `security.protocol` 覆盖处理。理由：字段注释写的是
  `PLAIN / SASL_PLAINTEXT`，正是 `security.protocol` 的取值域；librdkafka 没有独立的「SASL
  握手协议」开关。代价：若用户原意是别的，会被这条覆盖——但 `cfg.extra` 仍可再覆盖。
- **R65**：凭据分支按**生效的**协议串判断（`contains("sasl")` / `contains("ssl")`），而不是
  按 `security_protocol` 枚举。第一版按枚举判断，清单第 8 条当场翻车：`securityProtocol=
  plaintext` + `saslProtocol=SASL_PLAINTEXT` 时 `security.protocol` 已是 `sasl_plaintext`，
  但 `sasl.mechanism` 没被写入，librdkafka 只好用默认的 GSSAPI，报
  `No provider for SASL mechanism GSSAPI: recompile librdkafka with libsasl2`。代价：一份带凭据
  的配置切回 PLAINTEXT 时不会写入 SASL 属性（这正是想要的）。
- **R66**：`cert_pem` 与 `key_pem` 必须成对，单边在本地就报 `InvalidConfig`。代价：多一条
  本地校验；收益：不必等到 broker 握手才看到错误。
- **R67**：`ssl` 段缺失时**不写任何 TLS 属性**（保持 librdkafka 的 `verify=true` +
  `endpoint.identification=https` 默认）。注意 `SslConfig::derive(Default)` 会让
  `verify_certificate = false`，与 serde 默认相反，所以这里不能 `unwrap_or_default()`。
  代价：想关闭校验必须在 UI 里显式勾上 SSL 段。
- **R68**：SASL 密码错误经 `fetch_metadata` 报出来的是 `Connection`（librdkafka 原文
  `BrokerTransportFailure (Local: Broker transport failure)`），不是 `Auth`。librdkafka 在
  「认证被拒」后放弃该 broker，元数据请求只能看到传输层失败，拿不到
  `SASL_AUTHENTICATION_FAILED`。代价：用户看到的是「连不上」而不是「密码错」；清单第 3 条
  记录了这一实际输出。真正的认证错误映射留给 T081（错误提示审计）。

## 验收

- `cargo check -p kafka-driver --features real`
- `cargo clippy -p kafka-driver --features real --all-targets -- -D warnings`
- `cargo test -p kafka-driver`（mock 回归，不跑 `real`）
- 真集群手动清单（`examples/real_t076.rs`，跑完即删）：PLAINTEXT 回归 / SASL 正例 /
  SASL 错密码 / SSL 用 `ca.pem` 正例 / SSL 无信任锚 / 关闭校验 / 证书单边

## 结果

- 87 行 / 2 文件（`config.rs` +85，`mod.rs` +2）；`cargo check`、`clippy --all-targets -D warnings` 零告警，mock 72/72。
- dev broker 追加 `SASL://:9094`（SASL_PLAINTEXT/PLAIN，用户 alice/alice-secret）与 `SSL://:9095` 两个监听器，PLAINTEXT:9092 未动；密钥材料由新增的 `scripts/kafka/gen-secrets.sh` 生成（`secrets/` 已 gitignore）。
- 清单 8 条实测（`examples/real_t076.rs`，跑完即删）：PLAINTEXT OK；SASL alice OK；错密码 → `Connection`（R68）；SSL+ca.pem OK；SSL 无信任锚 → 失败；关校验 → OK；证书缺私钥 → `InvalidConfig{field:"ssl.keyPem"}`；`saslProtocol` 覆盖 → OK。
- 第 8 条当场暴露 R65 的缺陷（默认 GSSAPI 报 `No provider for SASL mechanism GSSAPI`），已改为按生效协议串判断。
