# Kafka-Manage-Rust（rs-kafka-ui-client）

用 **Rust** 编写的跨平台 **Kafka 可视化客户端**：UI 是系统原生 WebView（HTML/JS/CSS），后端 Rust 承担业务逻辑与 Kafka 通信，事件驱动 NIO（Tokio）。

**默认零依赖就能跑**——内置 Mock 驱动，不装 Kafka 也能把整个界面点一遍。

## 功能

- **集群与元信息**：多集群配置（增删改查、JSON 导入导出、SQLite 持久化、连接测试）、topic 列表与详情、创建 / 删除 / 改配置、消费组列表与成员、已提交 offset、位点重置
- **消费**：按 topic（可限定分区）、起点 `earliest` / `latest` / 指定 offset、实时流式订阅、raw 与 JSON 渲染、关键字过滤、暂停 / 恢复、seek、导出与复制
- **生产**：单条 / 批量、key、指定分区、headers、投递回执
- **Mock 驱动**（按集群开关）：内存里的生产 - 消费闭环、故障注入（连接失败 / 随机失败 / 超时 / leader 下线 / broker 错误码）、延迟注入、集群快照持久化
- **安全与界面**：SASL PLAIN / SCRAM-SHA-256 / 512、SSL；深浅主题、集群 → topic → 分区导航树、消息表格与实时流面板、JSON 编辑器、日志面板
- 消息表最多保留 500 行（最新在上），长时间订阅不会撑爆界面

## 快速开始

```sh
cargo run -p main-bin          # Mock 驱动，零外部依赖，直接开窗
```

界面里「新增集群」保持勾选 **Mock 驱动**即可开始点。

**连真实 Kafka**（macOS 用脚本，它会自己找到 keg-only 的 openssl@3）：

```sh
brew install cmake openssl@3
./scripts/real-run.sh          # = cargo run -p main-bin --features real，参数可透传
```

然后在界面里新增集群 → 取消勾选「Mock 驱动」→ 填 bootstrap 地址。

**只改前端**（免重编译 Rust，需 Node 22）：

```sh
nvm use 22 && cd frontend && npm run build && cd ..
KAFKA_UI_DEV_DIR="$PWD/frontend/dist" cargo run -p main-bin
```

**想要一个本地 broker**（可选，`apache/kafka:3.9.1`，带 PLAINTEXT / SASL / SSL 三种监听）：

```sh
cd scripts/kafka && ./gen-secrets.sh && cd ../..   # 仅首次生成 TLS 材料
docker compose -f scripts/kafka/docker-compose.yml up -d
```

运行数据落在平台数据目录下的 `rs-kafka-ui-client/`（macOS：`~/Library/Application Support/rs-kafka-ui-client/`），含本地库与轮转日志 `logs/app.log`。

## 架构

```
UI 层        frontend/                Vue3 + TS + Tailwind 4（Vite 构建）
桥接层       crates/webview-bridge    tao + wry：命令路由 · 事件 pump · 静态资源
业务层       crates/kafka-app         Tokio 事件驱动：服务 + EventBus + SQLite Store
驱动层       crates/kafka-driver      MockKafkaDriver(默认) · RealKafkaDriver(feature="real", rdkafka)
基础层       crates/kafka-core        类型 / 错误 / 协议 DTO（无 IO 依赖）
入口         crates/main-bin          装配 runtime / App / Router / 日志 / 数据目录
```

依赖单向向下；前后端用 JSON-RPC（请求 - 响应 + 单向事件推送），细节见 [docs/development.md](docs/development.md)。

## 文档

- [docs/development.md](docs/development.md) —— 构建与门禁、技术栈、crate 职责、IPC 协议、`--features real` 的 openssl 坑、本地 broker
- [.opencode/skills/rs-kafka-ui-client/](.opencode/skills/rs-kafka-ui-client/SKILL.md) —— 项目约定 skill，改代码前按模块读
- [agents.md](agents.md) —— 面向编码代理的协作约定与硬约束
- [docs/TASKS.md](docs/TASKS.md) —— 任务索引与卡片状态；[docs/PLAN.md](docs/PLAN.md) —— 计划与阶段划分
- [docs/reports/](docs/reports/) —— 验收与长跑报告（如 T102 内存 / 句柄泄漏检查）

## License

MIT
