# T080 — tracing → `log.line` 事件 → 前端日志面板

状态: ✅　依赖: T037,T049　验证: 手动 + `cargo test -p main-bin`　类型: 产品码

## 目标

进程里的 `tracing` 事件除了照旧进 stderr，还作为 `AppEvent::LogLine` 发到应用总线，
由既有事件泵（T037）批量推给前端；日志面板（T049b）把 `log.line` 渲染成
「级别 + target + message」的行。至此 Rust 侧发生的事在 UI 里可见，不必再开终端。

## 只读白名单（最多 3 个源文件）

- `crates/kafka-app/src/bus.rs` — `AppEvent::LogLine`、`EventBus::publish/subscribe`
- `crates/webview-bridge/src/pump.rs` — `EventPump::spawn` / `EventSink` / `PumpHandle::shutdown`
- `frontend/src/store/logs.ts` — `record()`、`onAll` 摘要行、`LogSource`

## 要改的文件

- 新建 `crates/main-bin/src/logging.rs` —— 订阅器安装 + `BusLogs` layer + `LogBridge` + 单测
- 修改 `crates/main-bin/src/main.rs` —— `logging::install()` 取代内联 fmt 初始化；装配后 `attach`
- 修改 `frontend/src/store/logs.ts` —— `log.line` 专用分支与新来源 `rust`

## 必须覆盖

1. `tracing::info!(target:"kafka_app::store", path=.., "local store opened")` → 恰好一条
   `AppEvent::LogLine`：`level=="info"`、`target=="kafka_app::store"`、`message` 含正文与字段值。
2. 级别小写：`warn`/`error` 得到 `"warn"`/`"error"`（`tracing` 的 `Level` 是大写常量）。
3. attach 之前的事件只进 stderr：不 panic、不发总线事件。
4. 端到端（Rust 半程）：装 layer → `EventPump::spawn` → 发一条 info → `shutdown()` 冲刷残余批 →
   假 sink 收到的批 JSON 里含 `"event":"log.line"` 与消息正文。

## 明确不做

- 不做日志落盘/轮转（T083）；不改前端面板的级别过滤与 500 环形口径；
  不改事件泵批量口径（≤200 条 / 50ms）；不新增依赖。

## 已知取舍（Ruling）

- **R73** layer 用 `Arc<OnceLock<EventBus>>` 槽位：订阅器必须在 `main` 第一行安装，而总线要到
  `Assembly` 把 `App` 建出来才存在。代价：`Assembly::open_default/new` 里那句 `local store opened`
  与 `router ready` 发生在 attach 之前，只进 stderr，UI 面板看不到启动最初两行；`ready_hook` 的
  `event pump online` 在 attach 之后，能看到。把 attach 挪进 `glue.rs` 能补上这两行，但会多动一个文件。
- **R74** 一条 `tracing` 事件压成一行文本：`message` 正文在前，其余字段（`path=…`、`topic=…`）接在后面。
  线上形状（`level`/`target`/`message` 三个字符串）是 T037 钉死的，不为多字段扩协议。
  代价：结构化字段退化成文本，前端无法按字段过滤。
- **R75** 级别不做二次过滤：`RUST_LOG` 放进来什么就发什么。默认 `info`，前端 500 条环形 + 级别过滤
  兜底。代价：`RUST_LOG=debug` 时总线流量与面板噪声一起上升（广播通道有界，慢订阅者只会 Lagged）。

## 验收

- `cargo test -p main-bin` 全绿（新增 ≥4 个单测）
- `cargo clippy -p main-bin --all-targets --features real -- -D warnings` 零告警
- `cargo check --workspace` 通过
- 前端 `npm run build` 通过，dist 重建提交
- 手动：启动应用，日志面板出现 `rust` 来源的 info 行（如 `event pump online`），级别过滤与清空照常

## 结果（完成后填写，≤5 行）

- 新建 `logging.rs`（214 行，含 4 个单测）：`install()` 装 `registry().with(EnvFilter).with(BusLogs).with(fmt::layer)`；
  `BusLogs` 把每条事件压成 `AppEvent::LogLine { level, target, message }`（`Fields` visitor，正文在前、其余字段接后）。
- `main.rs`（+9/-9）：`logging::install()` 取代内联 fmt 初始化；`Assembly` 拆包后 `logs.attach(app.bus().clone())`。
- 前端 `logs.ts`（+27/-2）：`log.line` 走专用分支渲染成「target message」，新增来源 `rust`（`LogView.vue` 无需改）。
- 验证：`cargo test -p main-bin` 4+6+1 全绿（4 个新单测含「tracing→layer→总线→事件泵→假 sink」端到端）；
  `cargo clippy -p main-bin --all-targets --features real -- -D warnings` 零告警；`cargo check --workspace` 通过；
  `npm run build`（Node 22.23.3）通过，dist 重建。手动项登记为 `STAGE4-manual-checklist.md` 第 22 条。
- 偏差：首次 clippy 报 `ok().expect()`（`ok_expect` lint），改为 `let _ = slot.set(..)`；本机 Node 16 跑不动 Vite 7，
  经 nvm 装 22.23.3（环境变更，非代码）。
