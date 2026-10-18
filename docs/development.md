# 开发

面向要改这个仓库的人。只想跑起来看，[README](../README.md) 就够了。

## 前置（按平台）

- **Windows**：VS Build Tools 2022（MSVC）+ Windows SDK（提供 `kernel32.lib` / `libucrt.lib`）。缺 SDK 时任何 `cargo build / test / check` 都会在链接期失败（找不到 `link.exe`）。
- **Linux**：WebKitGTK / GTK 开发包，否则 `webview-bridge` 无法编译：`sudo apt-get install -y libwebkit2gtk-4.1-dev libgtk-3-dev librsvg2-dev libssl-dev pkg-config`；启用真实驱动还需 `cmake` 与 `build-essential`。CI 用的正是这份清单（`.github/workflows/ci.yml`）。
- **macOS**：`cmake`（Homebrew）；真实驱动还需 openssl@3（见「构建真实驱动」）。
- **前端**：Node 22（Vite 7 拒绝更旧的版本）。

## 门禁

```sh
cargo test --workspace                                      # 全仓测试（基线 281 passed）
cargo clippy --workspace --all-targets -- -D warnings        # 必须零告警
cargo build --release -p main-bin                           # 发版二进制
cargo bench -p kafka-driver                                 # criterion 吞吐基准（可选，较慢）
nvm use 22 && cd frontend && npm run build                   # 前端唯一门禁
```

- **不要跑 `cargo fmt`**：仓库没有 `rustfmt.toml`，代码不是 rustfmt-clean，格式化会产生海量无关 diff。风格手工维持（4 空格缩进、约 100 列）。
- 前端只有 `npm run build`，没有 typecheck / lint 脚本。
- `frontend/dist` 被 gitignore，但有**两个文件被追踪**：产物变化后要 `git add -f frontend/dist`。
- `cargo check -p kafka-driver --features real` 只做类型检查、**不链接**，别把它当作 real 能构建的凭据。
- Mock 是默认驱动（`kafka-driver` 的 `default = []`，`real` 为 opt-in），不用 rdkafka 时无需 librdkafka / cmake。

真实集群一致性测试（需可达 broker，默认 `#[ignore]`）：

```sh
KAFKA_CONFORMANCE_BOOTSTRAP=localhost:9092 cargo test -p kafka-driver --features real -- --ignored
```

## 技术栈

- Rust：edition 2024、rust-version 1.85、workspace resolver 3；MIT License
- 异步：Tokio 1.53（full）、tokio-stream、futures、async-trait
- 序列化：serde / serde_json / base64 0.22
- 存储：rusqlite 0.40（bundled SQLite）
- UI：tao 0.37 + wry 0.57（系统 WebView）、include_dir 0.7（前端资源在编译期嵌入二进制）
- 日志：tracing / tracing-subscriber；错误：thiserror 2.0 / anyhow
- Kafka：rdkafka 0.39（仅 `--features real` 启用，需 cmake + C 工具链，内含 vendored librdkafka 2.12.1）
- 基准（dev）：criterion 0.7，见 `crates/kafka-driver/benches/mock_throughput.rs`

版本以根 `Cargo.toml` 的 `[workspace.dependencies]` 为准。

## Workspace 布局

```
Cargo.toml            # workspace + workspace.dependencies 统一版本
crates/
├── kafka-core/       # 公共类型、错误、协议 DTO（无 IO 依赖）
├── kafka-driver/     # KafkaDriver trait + mock/ + real(feature)；tests/ 契约测试、benches/ 吞吐基准、examples/ 长跑探针
├── kafka-app/        # 业务服务、事件总线、SQLite 存储
├── webview-bridge/   # tao+wry 桥接、命令路由、事件 pump
└── main-bin/         # 入口：装配 runtime / App / Router / 日志 / 数据目录
frontend/             # Vue3 + Vite + Tailwind 4 源码（dist 在编译期嵌入）
```

各模块的职责边界、驱动抽象与开发约定写在项目 skill 里：`.opencode/skills/rs-kafka-ui-client/`，改哪个模块读哪一篇。

## 前后端协议

- JS→Rust：请求 - 响应，`{id, cmd, payload}` → `{id, ok:true, data}` 或 `{id, ok:false, error:{kind, message, retryable, hint?}}`
- Rust→JS：单向事件推送，条目 `{event, payload, ts}`，批量 flush（满 200 条，或首个事件后 50ms）
- 命令域：`cluster.* / topic.* / produce.* / consume.* / group.* / config.* / mock.* / request.cancel`
- 事件名：`connection.state`、`message.received`、`subscription.state`、`subscription.done`、`topic.changed`、`group.changed`、`produce.result`、`log.line`、`error`

## 构建真实驱动（OpenSSL 自带，不依赖本机）

`--features real` 现在把 OpenSSL **编进二进制**：根 `Cargo.toml` 的 `rdkafka` 用 `ssl-vendored`
（⇒ `rdkafka-sys/ssl-vendored` ⇒ `openssl-sys/vendored`），`openssl-src` 从源码编出
`libssl.a` / `libcrypto.a` 静态链入；`rdkafka-sys` 的 `config.register_dep("openssl")` 把
vendored 的 root 排在 `CMAKE_PREFIX_PATH` 最前（`cmake` crate `src/lib.rs:565-570`），所以
librdkafka 的 `find_package(OpenSSL)` 用的是自带的那份。

```bash
cargo build --release -p main-bin --features real --locked
otool -L target/release/main-bin | grep -iE 'openssl|libssl|libcrypto'   # 应为空
```

本机实测（2026-10-17，`env -u OPENSSL_DIR -u PKG_CONFIG_PATH`）：非系统依赖**没有任何
Homebrew 路径**，只剩 `/usr/lib/*` 与 `/System/Library/Frameworks/*`；二进制 5.9 MB → 10.0 MB，
编译多花约 2~3 分钟。

> 为什么非改不可：`v0.1.0`~`v0.1.4` 的 macOS 产物里写着
> `/usr/local/opt/openssl@3/lib/libssl.3.dylib`（Intel runner 的 Homebrew 路径）——下载方没有
> 这个路径就起不来，「产物不能直接运行」就是这一条。
> 历史：T104 的 openssl@4 / `_ENGINE_by_id` 链接坑随 vendored 一并失效。

## 本地开发用 broker（可选）

`scripts/kafka/docker-compose.yml` 起的是 `apache/kafka:3.9.1`：宿主机 `localhost:9092`（PLAINTEXT）、`9094`（SASL_PLAINTEXT / PLAIN，`alice` / `alice-secret`）、`9095`（SSL，CA 在 `scripts/kafka/secrets/ca.pem`）。

```sh
cd scripts/kafka && ./gen-secrets.sh && cd ../..   # 生成 TLS 材料，仅首次；secrets/ 不入库
docker compose -f scripts/kafka/docker-compose.yml up -d
```

运行数据放在平台本地数据目录下的 `rs-kafka-ui-client/`（macOS：`~/Library/Application Support/rs-kafka-ui-client/`），内含本地库文件与轮转日志 `logs/app.log`。

## 发布（打 tag）

发布只认 tag，**不需要动 `Cargo.toml`**：

```bash
git tag -a v0.1.5 -m "v0.1.5"
git push origin v0.1.5
```

`release.yml` 在 `v*` 上触发 → 五个平台各自构建（档案名 `名称_版本.平台_架构.格式`，例
`rs-kafka-ui-client_0.1.5.macos_arm64.tar.gz`；解压出来 macOS 是 `rs-kafka-ui-client_0.1.5.app`
——**ad-hoc 签名**（`codesign --force --deep --sign -`，不需要证书；T132a），双击即运行，首次可能仍要右键→打开或先 `xattr -dr com.apple.quarantine`；Windows 是 `rs-kafka-ui-client_0.1.5.exe`；
Linux 是带执行位的裸文件 `rs-kafka-ui-client_0.1.5`）→ 各自上传一个 workflow artifact →
最后一个 `release` job 汇总，**一次性**创建 Release 并挂上全部 5 个产物。

为什么不是五个 job 各传一次 Release：`v0.1.0` 已经证明那样会坏——五个 job 同时创建同一个
Release，`already_exists` 竞态，`generate_release_notes` 还各追加一次，正文里
`**Full Changelog**` 重复了 5 遍（T113）。

`scripts/release.sh <version>` 仍然保留，但**不是发布的前置条件**：它只在你想让
`Cargo.toml` 的 `[workspace.package] version` 跟上 tag 时才需要（纯元数据，构建产物里
没有任何地方读它）。

> 历史：T114 曾加过一个 `version-guard`，要求 tag 与 manifest 一致，结果把 `v0.1.3`、
> `v0.1.4` 两次打 tag 都拦下来了。而全仓 grep `CARGO_PKG_VERSION` **零命中**——它拦的是
> 没人看的东西。T118 已删除该 job。

## 任务与卡片

每个改动一张卡：`docs/tasks/Txxx-*.md`，完成时在 `docs/TASKS.md` 对应行标 ✅ 并在卡尾补 ≤5 行「结果」。含 `手动` / `real` 验证的卡，另在 `docs/tasks/STAGE4-manual-checklist.md` 追加一条。

硬约束：**单次改动最多 3 个文件、新增 ≤250 行源码**（只数源码行；`docs/`、`*.md`、`.github/`、`scripts/`、`frontend/dist/`、`Cargo.lock` 不计），超出就按关注点拆成带字母后缀的卡（如 T074a/b/c）。
