# T038b — main.rs 启动装配：日志、数据目录、App、Router IPC、事件泵、窗口

状态: ⏸　依赖: T031, T033–T037, T085, T038a　验证: 自动部分见下；整体 🔶待手动验证（用户）　类型: 装配+手动

拆分裁定 R10: 本卡为 T038 的装配半边；kafka-app 接线（护栏/快照）已由 T038a 完成。

## 目标

`cargo run -p main-bin` 打开窗口并具备完整桥：JS 可经 `app://localhost/rpc` POST 调 `router.handle`，Rust 事件经 EventPump 批量推给 `window.__bridgeEvents`。

## 只读白名单

读改: `crates/main-bin/src/main.rs`、`crates/main-bin/src/glue.rs`（新）、`crates/webview-bridge/src/shell.rs`（扩展）、`crates/main-bin/Cargo.toml`（加 `dirs`，豁免）、`frontend/dist/index.html`（加 `__bridgeEvents` 钩子脚本，豁免）、`crates/main-bin/tests/*`（新，豁免）、docs。
只读: `handlers*.rs`（register_* 签名）、`pump.rs`、`router.rs`、`kafka-app/src/lib.rs`、`store/db.rs`。禁止改 handlers*/pump/router/App。

## 预先钉死的接口事实

- shell.rs 现状：`run_window(WindowOptions{title,page})`（:173）内部自建 EventLoop，webview 私有，无 IPC/事件挂点——本卡扩展 shell。
- `EventPump::spawn(bus: &EventBus, sink: Arc<dyn EventSink>) -> PumpHandle`；`EventSink::push_batch(&self, batch_json: &str)`（pump.rs:25）；`PumpHandle::shutdown()` async。
- `Router`（Clone）`.handle(raw: &str) -> ResponseEnvelope`；register 函数：`register_cluster`/`register_config`（handlers.rs）、`register_topic_and_produce`、`register_consume`、`register_group`、`register_mock`。
- `App::builder().store(Store).build()`；`Store::open(path)`（db.rs:40）；`App::shutdown()` async；`app.bus()`。
- main-bin 已依赖 kafka-core/app/driver/webview-bridge/tokio/tracing/tracing-subscriber/anyhow；workspace 有 `dirs = "7.0"`（本卡给 main-bin 加上）。
- tao 0.37 `EventLoopProxy<UserEvent>`（`EventLoop::create_proxy`，核实 API）；wry 0.57 `with_custom_protocol` 闭包同步、`WebView::evaluate` 须主线程、`with_devtools()`。

## 钉死的扩展 API（shell.rs）

- `pub enum UserEvent { Batch(String) }`（shell 拥有；run 循环收到即 `webview.evaluate(event_script(json))`）。
- `pub type RpcHandler = Arc<dyn Fn(String) -> String + Send + Sync>`（同步；glue 内 `handle.block_on`）。
- `pub trait BridgeGuard: Send { fn on_close(self: Box<Self>); }`（CloseRequested 时调用：关泵+App::shutdown）。
- `pub fn run_window_with(opts: WindowOptions, rpc: RpcHandler, on_ready: Box<dyn FnOnce(&Window, &EventLoopProxy<UserEvent>) -> Box<dyn BridgeGuard> + Send>) -> anyhow::Result<()>`；`run_window` 保持原签名（= 无桥版本，T031 测试不破）。
- 协议路由：`app://localhost/index.html` 等 GET → 现有资源服务；`POST app://localhost/rpc` → body 进 `rpc` → 200 `text/plain`（响应体=router 响应 JSON）；其余 → 404。
- `pub fn event_script(batch_json: &str) -> String`（纯函数）：`window.__bridgeEvents && window.__bridgeEvents(<batch_json>)`。
- devtools 开启（`with_devtools`）。

## main.rs / glue.rs 装配

1. `tracing_subscriber` fmt + EnvFilter（默认 info）。
2. 数据目录 `dirs::data_local_dir()/rs-kafka-ui-client`（create_dir_all），`Store::open(dir/db.sqlite)`。
3. `App::builder().store(...).build()`；`Router::new` + 六个 register_*。
4. tokio multi-thread Runtime 持有；`rpc` 闭包 = router.clone + `Handle::block_on(router.handle(&raw))` → serde_json 字符串。
5. `on_ready`：`EventPump::spawn(app.bus(), Arc::new(WrySink{proxy}))`；返回 BridgeGuard（PumpHandle+App+Handle）。
6. `WrySink`（shell.rs）：`push_batch` → `proxy.send_event(UserEvent::Batch(json))`，失败仅 warn。
7. 窗口关闭 → `on_close`：block_on(pump.shutdown + app.shutdown) → run_window_with 返回 Ok。

## 自动测试（main-bin/tests/bridge_glue.rs，无窗口）

1. rpc 闭包：App+router 装配 → 喂 `cluster.list` 请求 JSON → 返回响应 JSON `ok:true`。
2. rpc 坏输入 → 响应 `ok:false, kind:"parse"`（或 malformed 实际 kind）。
3. `event_script` 输出为合法 JS 表达式且原样内嵌 batch JSON。
4. register_mock 经 glue 装配可用（mock.reset ok）。
（WrySink/窗口运行时行为无法无头测试 → ⚠️ 手动。）

## 手动验证清单（用户，🔶）

`cargo run -p main-bin` → 窗口打开显示占位页 → F12 devtools console：
`fetch('app://localhost/rpc',{method:'POST',body:JSON.stringify({id:'1',cmd:'cluster.list',payload:{}})}).then(r=>r.text()).then(console.log)` → `{"id":"1","ok":true,...}`；
再 `cluster.save`(mock 配置)+`cluster.connect`+`mock.seed`+`topic.list` 见预置；`produce.send` 后 console 出现 `__bridgeEvents` 批量事件。

## 验证命令

`cargo test -p main-bin`；`cargo test --workspace`；`cargo clippy -p main-bin -p webview-bridge --all-targets -- -D warnings`；`cargo build -p main-bin` + 冒烟（起 6s 存活后 kill）。rustfmt 仅改动文件。

预算（R8 口径）：3 代码文件（main.rs/glue.rs/shell.rs）；Cargo.toml、index.html、测试豁免。

## 结果（2026-10-14，实现完成）

- 装配完成：main.rs + glue.rs（日志/数据目录/App/Router 六表/tokio 多线程 runtime/事件泵/ShutdownGuard）+ shell.rs 扩展（UserEvent/RpcHandler/BridgeGuard/WrySink/run_window_with/event_script/POST /rpc 路由/devtools）+ index.html `__bridgeEvents` 钩子。
- 自动验证全绿：`cargo test -p main-bin` 6 测试；workspace 全绿（kafka-app 100 / kafka-driver 72 / webview-bridge 54=52+2 新增 shell 单测）；clippy `-D warnings` 零告警；build + 冒烟（进程存活 7s 后 kill，stderr 干净，store/router/pump INFO 正常）。
- 整体 🔶待手动验证：用户按上方手动清单验证窗口打开、devtools RPC 探测、produce.send 后 `__bridgeEvents` 批量事件。
