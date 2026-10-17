# T103 — README 补「运行」章节 + 修前置块排版

状态: ✅　依赖: 无　验证: 文档　类型: 文档

## 目标

`README.md` 在「构建 / 开发 / 测试」里给了两条 `cargo check`/`cargo test` 与前置条件，
却**从头到尾没有说怎么把程序跑起来**：`cargo run -p main-bin`、页面来源（编译期嵌入的
`frontend/dist` 与 `KAFKA_UI_DEV_DIR` 覆盖）、`--features real` 的开关位置、本地 broker
的端口与凭据，全部只存在于源码里。同一个区块还有排版损坏：Windows 前置的每一行之间夹
一个 `>` 空行、Linux 前置之间夹一个**不带 `>`** 的空行，引用块被切成五段并留下空档；
代码块里的 `#` 还残留着工具转义的 `\#`。

本卡补齐运行方式，并把该区块的排版修平。

## 只读白名单（最多 3 个源文件）

- `crates/main-bin/src/main.rs` — 入口：`PageSource::Embedded` / `DevDir` 的选择
- `crates/webview-bridge/src/shell.rs` — `DEV_DIR_ENV = "KAFKA_UI_DEV_DIR"`、`include_dir!("…/frontend/dist")`
- `crates/kafka-driver/src/driver.rs` — `driver_for()` 的 `cfg.mock` / `--features real` 分支

## 要改的文件

- 修改 `README.md` —— 唯一改动文件

## 必须覆盖

1. 默认路径：`cargo run -p main-bin`（Mock 驱动、UI 编译期嵌入、零外部依赖）。
2. 只改前端：`nvm use 22` + `cd frontend && npm run build` + `KAFKA_UI_DEV_DIR="$PWD/frontend/dist"`。
3. 真 Kafka：`cargo run -p main-bin --features real`，含 macOS 的 `PKG_CONFIG_PATH` / `OPENSSL_DIR`，
   并写明未开特性时返回的是明确错误而不是崩溃。
4. 本地 broker：`docker compose -f scripts/kafka/docker-compose.yml up -d`，9092 / 9094(SASL) / 9095(SSL)。
5. 数据落点：`<本地数据目录>/rs-kafka-ui-client/`（macOS 路径）与轮转日志 `logs/app.log`。

## 明确不做

- 不动任何 Rust / 前端源码，不新增脚本、不改 `ci.yml`。
- 不重写「构建 / 开发 / 测试」与「前置」的**事实内容**，只修排版与被转义的字符。
- 不为 §3 陈旧的关键路径补新叙事（那是有日期的历史记录）。

## 已知取舍（Ruling）

- **R108** 运行说明写在 README 而不是另开 `docs/how-to-run.md`：README 是唯一入口，
  运行方式紧挨着「前置」才找得到；代价是 README 变长，靠小节标题分层。
- **R109** 前置两段保留原有全部事实，仅合并被切碎的行、清掉空档与 `\#` 转义：
  排版修复不改事实，所以不需要重新核对 `ci.yml` 的 apt 清单。

## 验收

- 两个前置块各自是**单段**引用，块内无空档
- `## 运行` 给出 4 条路径，命令与源码一致（环境变量名、特性名、端口、文件路径逐条核过）
- 代码块内无 `\#` 残留

## 结果（完成后填写，≤5 行）

- 仅改 `README.md`（1 文件，docs 不计预算）：新增 `## 运行`，给出 4 条路径（`cargo run -p main-bin`／
  `KAFKA_UI_DEV_DIR` + Node 22 免重编译／`--features real` + macOS openssl 环境／docker compose broker），
  并把两个前置引用合并成单段、清掉代码块里的 `\#` 转义与 4 处标题后多余空行。
- 事实逐条核过源码：`DEV_DIR_ENV = "KAFKA_UI_DEV_DIR"`（shell.rs:34）、
  `include_dir!("$CARGO_MANIFEST_DIR/../../frontend/dist")`（shell.rs:40）、`driver_for()` 的 `cfg.mock`
  分支与未开特性时的 `NOT_BUILT` 文案（driver.rs:126–148）、`ClusterModal.vue` 的「Mock 驱动」勾选框、
  compose 端口 9092/9094/9095 与 `scripts/kafka/secrets/`（`.gitignore:27`）、`APP_DIR_NAME`
  （store/db.rs:20）；未跑门禁（无源码改动，`cargo`/`npm` 不涉及）。
- 偏差：无。未动任何 Rust / 前端源码、未新增脚本。
