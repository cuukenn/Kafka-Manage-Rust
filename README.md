# Kafka-Manage-Rust（rs-kafka-ui-client）

用 **Rust** 编写的跨平台 **Kafka 可视化客户端**：UI 采用系统原生 WebView（HTML/JS/CSS），后端 Rust 承担业务逻辑与 Kafka 通信，事件驱动 NIO（Tokio）。

## 功能特性

* **Kafka 核心**：多集群配置（增删改查 + JSON 导入导出 + SQLite 持久化）、连接测试、集群元信息；topic 列表 / 创建 / 删除 / 改配置 / 分区详情；消费（按 topic / 分区、offset/timestamp/earliest/latest、分页、流式实时订阅、raw/json 格式化、搜索过滤、暂停 / 恢复）；生产（单条 / 批量、key/partition/headers、回执）；消费组（成员与分区分配、offset 重置、已提交 offset 查看）；安全（SASL PLAIN/SCRAM-SHA-256/512、SSL）

* **Mock 专属**：按集群全局开关、自定义 broker/topic/ 分区、内存生产 - 消费闭环、故障注入（连接失败 / 超时 /leader 下线 / 错误码）、延迟注入、Mock 数据快照持久化到 SQLite

* **UI**：响应式 + 深浅主题、集群→topic→分区导航树、消息表格 + 实时流面板、表单弹窗、JSON 编辑器与预览、loading + 错误 toast、日志面板

## 架构（分层，依赖单向向下）

```
UI 层        frontend/                Vue3 + TS + Tailwind（Vite 构建）

桥接层       crates/webview-bridge    tao + wry：命令路由 · 事件 pump · 静态资源

业务层       crates/kafka-app         Tokio 事件驱动：服务 + EventBus + SQLite Store

驱动层       crates/kafka-driver      MockKafkaDriver(默认) · RealKafkaDriver(feature="real", rdkafka)

基础层       crates/kafka-core        类型 / 错误 / 协议 DTO（无 IO 依赖）
```

前后端通信：JS→Rust 为**请求 - 响应**（带 `id` 关联）；Rust→JS 为**单向事件推送**（批量节流 ≤50ms/200 条）。命令命名 `cluster.* / topic.* / produce.* / consume.* / group.* / config.* / mock.*`。

## Workspace 布局

```
Cargo.toml            # workspace + workspace.dependencies 统一版本

crates/

├── kafka-core/       # 公共类型、错误、协议 DTO

├── kafka-driver/     # KafkaDriver trait + mock/ + real(feature)

├── kafka-app/        # 业务服务、事件总线、SQLite 存储

├── webview-bridge/   # tao+wry 桥接、命令路由、事件 pump

└── main-bin/         # 入口：组装 + 前端资源嵌入

frontend/             # Vue3+Vite+Tailwind 源码
```

## 技术栈

* Rust（edition 2024，rust-version 1.85，workspace resolver 3），MIT License

* Tokio 1.53（full）、tokio-stream、futures、async-trait

* serde / serde\_json / base64

* rusqlite 0.40（bundled SQLite）

* tao 0.37 + wry 0.57（WebView UI）、include\_dir 0.7（资源嵌入）

* tracing / tracing-subscriber

* thiserror 2.0 / anyhow

* rdkafka 0.39（`--features real` 才启用，需 cmake + C 工具链）

## 构建 / 开发 / 测试

> **前置（Windows）**：需要 VS Build Tools 2022（MSVC 已装）以及 Windows SDK（含 `kernel32.lib` / `libucrt.lib`）。当前机器缺 SDK，任何 `cargo build/test/check` 都会因链接失败报 "linker `link.exe` not found"。

> **前置（Linux）**：需要 WebKitGTK / GTK 开发包，否则 `webview-bridge` 无法编译：`sudo apt-get install -y libwebkit2gtk-4.1-dev libgtk-3-dev librsvg2-dev libssl-dev pkg-config`；启用真实驱动（`--features real`）还需 `cmake` 与 `build-essential`。CI 用的正是这份清单（见 `.github/workflows/ci.yml`）。

```sh
# 类型检查（kafka-core / kafka-driver，无 rdkafka 依赖）
cargo check -p kafka-core -p kafka-driver

# 单元测试（示例：Mock 生命周期）
cargo test -p kafka-driver --test mock_lifecycle

# 全仓测试
cargo test --workspace

# 启用真实 Kafka 驱动（需 cmake + C 工具链 + 正确的 OpenSSL，见「运行」一节）
cargo check -p kafka-driver --features real   # 只做类型检查、不链接，别把它当作 real 能构建的凭据
```

Mock 是默认驱动（`kafka-driver` 的 `default = []`，`real` 为 opt-in），不使用 rdkafka 时无需 librdkafka/cmake。

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

* [agents.md](agents.md) —— 面向编码代理的协作约定

## License

MIT