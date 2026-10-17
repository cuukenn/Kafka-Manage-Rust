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

> **前置（Windows）**
>
> ：需要 VS Build Tools 2022（MSVC 已装）以及 Windows SDK（含 
>
> `kernel32.lib`
>
>  / 
>
> `libucrt.lib`
>
> ）。当前机器缺 SDK，任何 
>
> `cargo build/test/check`
>
>  都会因链接失败报 "linker 
>
> `link.exe`
>
>  not found"。

> **前置（Linux）**

>

> ：需要 WebKitGTK / GTK 开发包，否则 `webview-bridge` 无法编译：

>

> `sudo apt-get install -y libwebkit2gtk-4.1-dev libgtk-3-dev librsvg2-dev libssl-dev pkg-config`

>

> ；启用真实驱动（`--features real`）还需 `cmake` 与 `build-essential`。CI 用的正是这份清单（见 `.github/workflows/ci.yml`）。

```
\# 类型检查（kafka-core / kafka-driver，无 rdkafka 依赖）

cargo check -p kafka-core -p kafka-driver

\# 单元测试（示例：Mock 生命周期）

cargo test -p kafka-driver --test mock\_lifecycle

\# 全仓测试

cargo test --workspace

\# 启用真实 Kafka 驱动（需 cmake + C 工具链）

cargo check -p kafka-driver --features real
```

Mock 是默认驱动（`kafka-driver` 的 `default = []`，`real` 为 opt-in），不使用 rdkafka 时无需 librdkafka/cmake。

## 文档



* [agents.md](agents.md) —— 面向编码代理的协作约定

## License

MIT