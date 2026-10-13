# rs-kafka-ui-client — 实施方案 v2（已按环境与技术校订更新）

> 项目：Rust 编写、跨平台 Kafka 可视化客户端。UI = 系统原生 WebView（HTML/JS/CSS），
> 后端 Rust 做业务 + Kafka 通信，事件驱动 NIO（Tokio），驱动层 trait 抽象（Real / Mock），
> Mock 优先开发，前期不需要真实 Kafka 环境。

---

## 0. v1 → v2 校订记录（重要）

| 项 | v1 方案 | v2 校订 | 原因 |
|---|---|---|---|
| `wry` | 0.40 | **0.57** | 当前最新稳定（2026-09）；0.40 API 已变（`ipc` 模块、`WebViewBuilder` 签名） |
| 窗口层 | 未提 | **新增 `tao` 0.37** | wry 只是 WebView，不含窗口/事件循环；裸用 wry 必须自己接 tao/win32 |
| `rdkafka` | 0.36 | **0.39**（`cmake-build` + `ssl` feature 名已改） | 0.36 的 `cmake` feature 在新版本叫 `cmake-build` |
| `rusqlite` | 0.29 | **0.40** | bundled SQLite 版本更新 |
| `thiserror` | 1.0 | **2.0** | 错误枚举 derive 语法兼容，但需 2.0 才能配合 `#[error(transparent)]` 新写法 |
| `tokio` | 1.0 | **1.53** | — |
| `KafkaDriver` trait | 直接 `async fn` | **`#[async_trait]`** | trait 里的 `async fn` **不是 object-safe**，无法 `Arc<dyn KafkaDriver>`；必须 async_trait（或手写 `Pin<Box<dyn Future>>`） |
| `subscribe() -> MessageStream` | 未定义 | **`MessageStream { BoxStream + StreamControls }`** | 需要 pause/resume/seek/stop 控制通道，且流要 `'static` 才能跨任务移动 |
| Real 驱动编译 | 默认编译 | **cargo feature `real`，默认关闭** | librdkafka 需要 cmake + C 编译器；关 feature 后 Mock 开发零 C 依赖，Windows/Linux 都能秒编译 |
| 消息字节 → JSON | 未定义 | **DTO 层 base64 + utf8 双通道** | `Vec<u8>` 直接 serde_json 会序列化成数字数组，UI 体积爆炸且不可读 |
| 前端资源嵌入 | `include_str` | **`include_dir` 0.7** | 多文件静态目录（html/js/css/assets） |

**环境实测（2026-10-12，Windows 11 / win32）**

| 依赖 | 状态 |
|---|---|
| Rust | ✅ rustc 1.99.0 / cargo 1.99.0（`C:\Users\cuukenn\.cargo\bin`，已入用户 PATH） |
| WebView2 运行时 | ✅ 154.0.4258.62（wry 可直接用，无需打包内核） |
| Node / npm | ✅ v24.20.0 / 11.19.0（前端 Vite 构建可用） |
| MSVC 链接器（link.exe） | ⏳ VS 2022 Build Tools 安装中（`cargo build`/`cargo test` 需要链接器；`cargo check` 不需要） |
| cmake | ⏳ 仅阶段 6（Real 驱动）需要，届时 `winget install Kitware.CMake` |

---

## 1. 架构（分层，依赖单向向下）

```
┌──────────────────────────────────────────────────────────┐
│ UI 层  frontend/  Vue 3 + TS + Tailwind（Vite 构建）      │
│   集群树 / topic 列表 / 消息表格 / 实时流 / 弹窗 / 日志面板 │
└────────────────────────┬─────────────────────────────────┘
                         │ JSON-RPC 请求-响应（JS→Rust, invoke）
                         │ 事件推送（Rust→JS, 节流批推送）
┌────────────────────────▼─────────────────────────────────┐
│ 桥接层  crates/webview-bridge   tao + wry                │
│   命令路由表 · 请求 ID 关联 · 事件 pump · 静态资源加载     │
└────────────────────────┬─────────────────────────────────┘
                         │
┌────────────────────────▼─────────────────────────────────┐
│ 业务层  crates/kafka-app   Tokio 事件驱动                 │
│   ClusterService · TopicService · ProduceService          │
│   ConsumeService(流+节流) · GroupService · EventBus        │
│   Store(SQLite: 集群配置/最近使用/mock 快照) · Retry/Timeout│
└────────────────────────┬─────────────────────────────────┘
                         │ Arc<dyn KafkaDriver>（#[async_trait]）
┌────────────────────────▼─────────────────────────────────┐
│ 驱动层  crates/kafka-driver                               │
│   MockKafkaDriver（内存 + 故障注入 + 延迟注入，默认）      │
│   RealKafkaDriver（rdkafka，feature = "real"）            │
└──────────────────────────────────────────────────────────┘
                         │
              crates/kafka-core：类型 / 错误 / 协议 DTO（无 IO 依赖）
```

### 1.1 Workspace 布局

```
rs-kafka-ui-client/
├── Cargo.toml                  # workspace + workspace.dependencies 统一版本
├── crates/
│   ├── kafka-core/             # 公共类型、错误、前后端协议 DTO（base64 消息）
│   ├── kafka-driver/           # KafkaDriver trait + mock/ + real(feature)
│   ├── kafka-app/              # 业务服务、事件总线、SQLite 存储
│   ├── webview-bridge/         # tao+wry 桥接、命令路由、事件 pump
│   └── main-bin/               # 入口：组装 + 前端资源嵌入
├── frontend/                   # Vue3+Vite+Tailwind 源码 → dist/
├── docs/PLAN.md                # 本文件
└── scripts/                    # 打包脚本（win/mac/linux）
```

### 1.2 前后端通信协议（阶段 3 冻结）

请求（JS → Rust，`window.__ask` / `ipc.postMessage`）：
```json
{ "id": "req-7", "cmd": "topic.list", "payload": { "clusterId": "c1" } }
```
响应（Rust → JS，按 id 回填）：
```json
{ "id": "req-7", "ok": true, "data": [ ... ] }
{ "id": "req-7", "ok": false, "error": { "kind": "timeout", "message": "...", "retryable": true } }
```
事件（Rust → JS，主动推送，**批量节流 ≤ 50ms / 200 条**）：
```json
{ "event": "message.received", "payload": { "subscriptionId": "s1", "messages": [ ... ] } }
{ "event": "connection.state", "payload": { "clusterId": "c1", "state": "disconnected" } }
```
命令命名：`cluster.* / topic.* / produce.* / consume.* / group.* / config.* / mock.*`

---

## 2. 功能清单（保持 v1，补齐实现口径）

**Kafka 核心**：多集群配置（增删改查 + 导入导出 JSON + SQLite 持久化）、连接测试、集群元信息（broker/controller/版本）；topic 列表（分区/副本/ISR/配置）、创建、删除、改配置、分区详情；消费（按 topic/分区、offset/timestamp/earliest/latest、分页、流式实时订阅、raw/json 格式化、搜索过滤、暂停/恢复）；生产（单条/批量、key/partition/headers、回执）；消费组（列表、成员与分区分配、offset 重置 earliest/latest/toOffset/toTimestamp、已提交 offset 查看）；安全（SASL PLAIN/SCRAM-SHA-256/512、SSL 证书、连接/请求超时）。

**Mock 专属**：全局 `mock` 开关（按集群）；自定义 broker/topic/分区；内存生产-消费闭环；故障注入（连接失败、超时、leader 下线、broker 错误码）；延迟注入；mock 数据快照持久化到 SQLite，重启保留。

**UI**：响应式 + 深浅主题；集群→topic→分区导航树；消息表格 + 实时流面板；表单弹窗（建 topic / 发消息 / 重置 offset）；JSON 编辑器与预览；全局 loading + 错误 toast；日志面板（后端事件/请求日志）。

**工程**：事件驱动 NIO（全异步，UI 主线程零阻塞）；分层解耦；超时/重试/自动重连；SQLite 持久化；三平台打包；分级日志可导出；单元测试（mock 层 + 业务层）。

---

## 3. 阶段计划（Mock 优先，前 5 阶段不需要 Kafka）

- [x] **阶段 0** 环境：Rust 1.99 + WebView2 确认；MSVC Build Tools / cmake 补齐
- [ ] **阶段 1** 骨架 + 驱动抽象：workspace、`kafka-core` 类型与错误、`KafkaDriver` trait、**MockKafkaDriver 全实现 + 故障注入**、Real 驱动 feature 桩、mock 单测
- [ ] **阶段 2** 业务层 + 事件总线 + SQLite：`KafkaEvent` broadcast 总线、ClusterService、Topic/Produce/Consume/Group 服务、连接管理（mock/real 切换）、超时+重试、业务层单测（全 mock）
- [ ] **阶段 3** 桥接层：tao+wry 建窗、静态资源加载、JS→Rust 命令路由、Rust→JS 事件推送、协议 JSON schema、桥接层测试
- [ ] **阶段 4** 前端 UI：布局/主题、集群配置页、topic 列表+创建弹窗、生产面板、消费面板（实时表格+JSON 预览）、消费组页、loading/toast/日志面板（全部对接 mock）
- [ ] **阶段 5** NIO 消费流完善：tokio stream 订阅、推送节流、暂停/恢复/seek、断线重连事件、资源释放
- [ ] **阶段 6** RealKafka 完善 + 真机联调：rdkafka 封装（SASL/SSL）、真实集群全功能对齐 mock 语义（需 cmake + Kafka）
- [ ] **阶段 7** 健壮性：全局错误捕获、tracing 日志 + 前端日志面板、超时/取消/防阻塞、边界测试（断网/海量消息/并发）
- [ ] **阶段 8** 打包发布：前端 dist 经 `include_dir` 嵌入、三平台构建脚本、WebView2 缺失引导、README + Mock 使用说明
- [ ] **阶段 9** 集成与性能：mock 全流程回归、真实 Kafka 吞吐压测、内存泄漏检查

---

## 4. 风险与对策（v2 更新）

1. **链接器缺失**：`cargo build`/`test` 需要 MSVC `link.exe`（或 GNU 工具链）。对策：VS Build Tools（后台安装中）；未装好前用 `cargo check` 保证类型正确。
2. **librdkafka 编译**：需要 cmake + C 编译器 + NASM（可选）。对策：`real` feature 默认关闭，阶段 6 再开；CI 用 `cargo test --features real`。
3. **WebView 主线程阻塞**：所有 Kafka IO 必须在 Tokio 任务；Rust→JS 推送必须节流（50ms/200 条批量），否则 UI 卡死。
4. **Mock 与 Real 语义漂移**：Mock 必须实现 offset 越界、ISR/leader 语义、错误码映射；对策：`kafka-driver/tests/conformance.rs` 同一套契约测试跑两个驱动（阶段 6 启用 real 侧）。
5. **Linux 依赖**：webkitgtk 4.1；打包脚本需检测并提示。Windows 需 WebView2 Runtime（本机已具备）。
6. **两种通信模型混淆**：JS→Rust 是请求-响应（有 id 关联），Rust→JS 是单向事件流。前端 store 必须分开处理。

---

## 5. 依赖清单（v2，workspace 统一版本）

```toml
tokio = "1.53"        # full
tokio-stream = "0.1"
futures = "0.3"
async-trait = "0.1"
serde = "1.0"  serde_json = "1.0"  base64 = "0.22"
rusqlite = "0.40"     # bundled
tao = "0.37"  wry = "0.57"
include_dir = "0.7"
tracing = "0.1"  tracing-subscriber = "0.3"  tracing-appender = "0.2"
thiserror = "2.0"  anyhow = "1.0"
uuid = "1.28"  chrono = "0.4"  dirs = "7.0"
rdkafka = { version = "0.39", features = ["cmake-build", "ssl"], optional = true }  # feature "real"
```
