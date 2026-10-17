# Kafka-Manage-Rust（rs-kafka-ui-client）

用 **Rust** 编写的跨平台 **Kafka 可视化客户端**：UI 采用系统原生 WebView（HTML/JS/CSS），后端 Rust 承担业务逻辑与 Kafka 通信，事件驱动 NIO（Tokio）。

## 功能特性

**集群与元信息**

- 多集群配置：增删改查、JSON 导入导出、SQLite 持久化、连接测试、集群元信息
- Topic：列表 / 详情（分区细节）/ 创建 / 删除 / 改配置
- 消费组：列表与成员、分区分配、已提交 offset、位点重置

**消费与生产**

- 消费：按 topic（可限定分区）、起点 `earliest` / `latest` / 指定 offset（协议另支持 timestamp 起点，界面暂未开放）、流式实时订阅、raw 与 JSON 渲染、关键字过滤、暂停 / 恢复、seek、导出与复制
- 生产：单条 / 批量、key、指定分区、headers、投递回执
- 消息表最多保留 500 行（最新在上），长时间订阅不会撑爆界面

**Mock 驱动（默认，零外部依赖）**

- 按集群开关；内存里的生产 - 消费闭环
- 故障注入：连接失败、随机失败（可按操作限定）、超时、leader 下线、broker 错误码
- 人工延迟注入；集群数据快照持久化到 SQLite（经 IPC 提供，界面尚未暴露全部入口）

**安全与界面**

- SASL PLAIN / SCRAM-SHA-256 / 512、SSL
- 响应式布局 + 深浅主题、集群 → topic → 分区导航树、消息表格与实时流面板、JSON 编辑器与预览、loading 与错误 toast、日志面板

## 架构（分层，依赖单向向下）

```
UI 层        frontend/                Vue3 + TS + Tailwind 4（Vite 构建）
桥接层       crates/webview-bridge    tao + wry：命令路由 · 事件 pump · 静态资源
业务层       crates/kafka-app         Tokio 事件驱动：服务 + EventBus + SQLite Store
驱动层       crates/kafka-driver      MockKafkaDriver(默认) · RealKafkaDriver(feature="real", rdkafka)
基础层       crates/kafka-core        类型 / 错误 / 协议 DTO（无 IO 依赖）
入口         crates/main-bin          装配 runtime / App / Router / 日志 / 数据目录
```

前后端通信：JS→Rust 为**请求 - 响应**（`{id, cmd, payload}` → `{id, ok, data}` 或 `{id, ok:false, error}`）；Rust→JS 为**单向事件推送**（条目 `{event, payload, ts}`，批量 flush：满 200 条或首个事件后 50ms）。命令域 `cluster.* / topic.* / produce.* / consume.* / group.* / config.* / mock.* / request.cancel`；事件名 `connection.state`、`message.received`、`subscription.state`、`subscription.done`、`topic.changed`、`group.changed`、`produce.result`、`log.line`、`error`。

## Workspace 布局

```
Cargo.toml            # workspace + workspace.dependencies 统一版本
crates/
├── kafka-core/       # 公共类型、错误、协议 DTO（无 IO 依赖）
├── kafka-driver/     # KafkaDriver trait + mock/ + real(feature)；tests/ 契约测试、benches/ 吞吐基准、examples/ 长跑探针
├── kafka-app/        # 业务服务、事件总线、SQLite 存储
├── webview-bridge/   # tao+wry 桥接、命令路由、事件 pump
└── main-bin/         # 入口：装配 + 前端资源嵌入
frontend/             # Vue3 + Vite + Tailwind 源码（dist 在编译期嵌入）
```

## 技术栈

- Rust：edition 2024、rust-version 1.85、workspace resolver 3；MIT License
- 异步：Tokio 1.53（full）、tokio-stream、futures、async-trait
- 序列化：serde / serde_json / base64 0.22
- 存储：rusqlite 0.40（bundled SQLite）
- UI：tao 0.37 + wry 0.57（系统 WebView）、include_dir 0.7（前端资源在编译期嵌入二进制）
- 日志：tracing / tracing-subscriber
- 错误：thiserror 2.0 / anyhow
- Kafka：rdkafka 0.39（仅 `--features real` 启用，需 cmake + C 工具链，内含 vendored librdkafka 2.12.1）
- 基准（dev）：criterion 0.7，见 `crates/kafka-driver/benches/mock_throughput.rs`

## 构建 / 开发 / 测试

**前置（按平台）**

- **Windows**：VS Build Tools 2022（MSVC）+ Windows SDK（提供 `kernel32.lib` / `libucrt.lib`）。缺 SDK 时任何 `cargo build / test / check` 都会在链接期失败（找不到 `link.exe`）。
- **Linux**：WebKitGTK / GTK 开发包，否则 `webview-bridge` 无法编译：`sudo apt-get install -y libwebkit2gtk-4.1-dev libgtk-3-dev librsvg2-dev libssl-dev pkg-config`；启用真实驱动（`--features real`）还需 `cmake` 与 `build-essential`。CI 用的正是这份清单（见 `.github/workflows/ci.yml`）。
- **macOS**：`cmake`（Homebrew）；真实驱动还需要 openssl@3（原因见「运行」一节）。

```sh
# 提交前的常规门禁
cargo test --workspace                                      # 全仓测试（当前基线 281 passed）
cargo clippy --workspace --all-targets -- -D warnings        # 必须零告警
cargo build --release -p main-bin                           # 发版二进制
cargo bench -p kafka-driver                                 # criterion 吞吐基准（可选，较慢）

# 类型检查 / 单套件
cargo check -p kafka-core -p kafka-driver                   # 不依赖 rdkafka
cargo test -p kafka-driver --test mock_lifecycle

# 前端（Node 22；Vite 7 拒绝更旧的版本）
nvm use 22 && cd frontend && npm run build

# 真实驱动（需可达 broker；详见「运行」一节）
cargo check -p kafka-driver --features real   # 只做类型检查、不链接，别把它当作 real 能构建的凭据
```

Mock 是默认驱动（`kafka-driver` 的 `default = []`，`real` 为 opt-in），不使用 rdkafka 时无需 librdkafka / cmake。

## 运行

默认驱动是 Mock，零外部依赖，跑起来就是一个自带 UI 的桌面窗口（UI 在编译期经 `include_dir` 嵌入二进制，取自 `frontend/dist`）：

```sh
cargo run -p main-bin
```

**只改前端时**（免重编译 Rust）：先构建资源，再用 `KAFKA_UI_DEV_DIR` 让它改从磁盘目录读。前端构建需要 Node 22（Vite 7 拒绝更旧的版本）：

```sh
nvm use 22
cd frontend && npm run build && cd ..
KAFKA_UI_DEV_DIR="$PWD/frontend/dist" cargo run -p main-bin
```

**连真实 Kafka**：构建时打开 `real` 特性（需 `cmake` 与 `openssl@3`），随后在界面里新增集群、取消勾选「Mock 驱动」、填 bootstrap 地址。macOS 上直接用脚本，它会自己找到 keg-only 的 openssl@3：

```sh
brew install cmake openssl@3
./scripts/real-run.sh        # 等价于 cargo run -p main-bin --features real，参数可透传
```

**为什么需要指路**：Homebrew 默认的 `openssl` 已经是 4.x，而 OpenSSL 4 不再导出 ENGINE API，vendored 的 librdkafka 2.12.1 仍在 `rdkafka_ssl.c` 里调用 `ENGINE_*`，于是链接失败：

```
Undefined symbols for architecture arm64:
  "_ENGINE_by_id", referenced from: _rd_kafka_ssl_ctx_init_engine ...
```

openssl@3 是 keg-only 的，pkg-config 默认找不到它。脚本做的事就是这两行环境变量：

```sh
export OPENSSL_DIR="$(brew --prefix openssl@3)"
export PKG_CONFIG_PATH="$OPENSSL_DIR/lib/pkgconfig"
cargo run -p main-bin --features real
```

嫌每次导出麻烦，就把它钉在**机器级**（`~/.cargo/config.toml`）。它只影响本机的 cargo 构建，且 `force` 默认为 `false`——已显式设置的环境变量仍然优先，CI runner 自带的值也不会被覆盖：

```toml
# ~/.cargo/config.toml
[env]
OPENSSL_DIR = "/opt/homebrew/opt/openssl@3"
PKG_CONFIG_PATH = "/opt/homebrew/opt/openssl@3/lib/pkgconfig"
```

这样裸 `cargo run -p main-bin --features real` 就能直接编译运行，不必再记脚本或环境变量。

⚠️ `cargo check` 不链接，所以在上面环境缺失时它照样「通过」——只有 `cargo build` / `cargo run` 会暴露这个链接错误。

没开 `real` 时非 mock 集群不会崩，而是给出明确错误：`real Kafka driver is not compiled in; rebuild with --features real`。

**本地开发用 broker**（可选）：`apache/kafka:3.9.1`，宿主机 `localhost:9092`（PLAINTEXT）、`9094`（SASL_PLAINTEXT/PLAIN，`alice` / `alice-secret`）、`9095`（SSL，CA 在 `scripts/kafka/secrets/ca.pem`）：

```sh
cd scripts/kafka && ./gen-secrets.sh && cd ../..   # 生成 TLS 材料，仅首次；secrets/ 不入库
docker compose -f scripts/kafka/docker-compose.yml up -d
```

运行数据放在平台本地数据目录下的 `rs-kafka-ui-client/`（macOS 为 `~/Library/Application Support/rs-kafka-ui-client/`），内含本地库文件与轮转日志 `logs/app.log`。

## 文档

- [agents.md](agents.md) —— 面向编码代理的协作约定与硬约束
- [.opencode/skills/rs-kafka-ui-client/](.opencode/skills/rs-kafka-ui-client/SKILL.md) —— 项目约定 skill：架构 / 各 crate 职责 / 驱动抽象 / 协议 / 开发约定 / 构建门禁 / 工具链（改代码前按模块读）
- [docs/TASKS.md](docs/TASKS.md) —— 任务索引与全部卡片状态
- [docs/PLAN.md](docs/PLAN.md) —— 计划与阶段划分
- [docs/reports/](docs/reports/) —— 验收与长跑报告（如 T102 内存/句柄泄漏检查）

## License

MIT