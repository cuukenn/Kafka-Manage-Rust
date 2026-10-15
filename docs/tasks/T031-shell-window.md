# T031 — tao+wry 窗口启动 + 静态资源加载（dev/embedded）

状态：✅（2026-10-15 用户经 T039 条目1 实测：窗口打开显示占位页+devtools 可用） ｜ 依赖：T030 ✅ ｜ 验证：手动（窗口打开显示占位页）+ 自动（check/clippy/纯逻辑单测）

## 目标

webview-bridge 新增 `shell` 模块：tao 事件循环 + wry WebView + 静态资源两种来源
（embedded 占位页 via include_dir；dev 目录覆盖 via 环境变量）。main-bin 的 main.rs
改为调用 shell 打开窗口。**手动验证：`cargo run -p main-bin` 窗口打开显示占位页。**

## 要改的文件（代码 3 个，R8 口径；配置/资源豁免）

- `crates/webview-bridge/src/shell.rs`（新建）
- `crates/webview-bridge/src/lib.rs`（+1 `pub mod shell;`）
- `crates/main-bin/src/main.rs`（重写为调用 shell，~20 行）
- 豁免：`crates/webview-bridge/Cargo.toml`（+`include_dir.workspace = true`）、
  `frontend/dist/index.html`（新建占位页资源）

## 只读白名单

- 根 `Cargo.toml`（tao = "0.37"、wry = "0.57"、include_dir = "0.7"）
- `crates/webview-bridge/Cargo.toml`、`crates/main-bin/Cargo.toml`

## 预先钉死的接口事实

- tao 0.37 / wry 0.57 / include_dir 0.7（workspace.dependencies，均已声明；
  webview-bridge 已依赖 tao+wry，main-bin 已依赖 webview-bridge）。
- 钉死 API 面：
  - `pub struct WindowOptions { pub title: String, pub page: PageSource }`
  - `pub enum PageSource { Embedded, DevDir(std::path::PathBuf) }`
  - `pub fn run_window(opts: WindowOptions) -> anyhow::Result<()>` —
    在主线程创建 `tao::event_loop::EventLoop` 并**阻塞**运行（Windows 要求主线程）；
    窗口 1280×800，标题取 `opts.title`；窗口关闭 → `ControlFlow::Exit` → 返回 Ok。
- Embedded：`include_dir!("$CARGO_MANIFEST_DIR/../../frontend/dist")` 编译期嵌入；
  占位页 `frontend/dist/index.html` 内容钉死要素：`<title>rs-kafka-ui-client</title>`、
  可见文本 `shell OK`、`<div id="app"></div>`（阶段 4 替换点）。
- DevDir：main-bin 读环境变量 `KAFKA_UI_DEV_DIR`，存在→`DevDir`，否则 `Embedded`。
- 资源协议：wry 自定义协议（如 `app://`）；`DevDir` 从磁盘目录解析文件并推断
  content-type（.html/.js/.css/.svg/.png/.ico/.json），**必须拒绝目录逃逸**
  （`..`/绝对路径/解析后不在根内 → 404）。Embedded 从 include_dir 读同一路径集。
  具体 wry builder 写法由实现者按 wry 0.57 文档定（可 webfetch docs.rs/wry/0.57）。
- shell 模块**不得**依赖 kafka-app/kafka-driver 类型；不接 Router IPC、不起 App、
  不起 EventPump——那些是 T038 装配。
- 纯逻辑（content-type 推断、路径解析/逃逸拒绝、include_dir 读取）拆成可测函数，
  单测不需要真实窗口。

## 必须覆盖

1. 单测：content-type 推断表；路径逃逸拒绝（`..`、绝对路径、`%2e%2e`）；
   Embedded 能读出 index.html 且含 `shell OK`；DevDir 读盘命中/未命中→404。≥5 个。
2. `cargo check -p webview-bridge -p main-bin` 通过（tao/wry 在现有工具链可编译）。
3. `cargo clippy -p webview-bridge -p main-bin --all-targets -- -D warnings` 零。
4. 手动（用户）：`cargo run -p main-bin` 窗口打开显示占位页；
   `KAFKA_UI_DEV_DIR=frontend/dist cargo run -p main-bin` 从磁盘服务同一页。

## 验证命令

```
cargo check -p webview-bridge -p main-bin
cargo test  -p webview-bridge
cargo clippy -p webview-bridge -p main-bin --all-targets -- -D warnings
```

## 风险与备注

- Windows 需 WebView2 运行时（Win10/11 一般自带）；若手动运行因缺运行时失败，
  如实报告，不要 vendoring loader。
- rustfmt 只许 `rustfmt --edition 2024 <file>` 逐文件，禁 cargo fmt/模块递归。
- 已知 AV 抖动 LNK1104/os error 5 → `--release` 复跑并注明。

## 结果（≤5 行，完成后填）

- `shell.rs`：tao 0.37 `run_return` + wry 0.57 `app://` 自定义协议；纯逻辑单测 5 个（content-type 表、`..`/绝对/`%2e%2e` 逃逸拒绝、Embedded 读 index.html 含 `shell OK`、DevDir 命中/404、build_response 404）。
- `cargo test -p webview-bridge` 52 绿（47 旧 + 5 新）；check / clippy `-D warnings` / `build -p main-bin` 全过。
- 冒烟：`target\debug\main-bin.exe` 后台启动 6s 存活无 panic（stderr 空），已 kill；窗口可见性留用户手动验证。
- 偏差：`EventLoop::run` 返回 `!`，为满足「关闭→返回 Ok」改用 `run_return`；工作区遗留 handlers.rs/router.rs 纯格式化改动（非本任务）未纳入提交。

