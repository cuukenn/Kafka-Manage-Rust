# T091 — Windows WebView2 运行时检测与首启提示

状态: ✅　依赖: T090　验证: 手动 + `cargo test -p webview-bridge`　类型: 产品码

## 目标

Windows 上 wry 必须有 **WebView2 运行时**才能建窗口。今天没有运行时的话，用户在
`WebViewBuilder::build` 处拿到的只有一句 `.context("failed to create WebView (is the
WebView2 runtime installed?)")` —— 既没说清缺什么、也没给下载地址，窗口没出现就退出，
用户不知道该装什么。

本卡加一道**启动前置检查**：窗口创建之前先看运行时在不在，不在就打印一条点名
「WebView2 Runtime」+ 下载链接的警示、尽力打开浏览器到下载页，然后以非零码退出，
替掉原来那句猜谜式的 context。

## 只读白名单（最多 3 个源文件）

- `crates/webview-bridge/src/shell.rs` — `run_window` / `run_window_with` 的窗口创建路径
- `crates/webview-bridge/src/lib.rs` — 模块表
- `crates/main-bin/src/main.rs` — 确认 `?` 会把退出码透出去

## 要改的文件

- 新建 `crates/webview-bridge/src/webview2.rs` —— 检测 + 提示 + `preflight()` + 单测
- 修改 `crates/webview-bridge/src/lib.rs` —— `pub mod webview2;`
- 修改 `crates/webview-bridge/src/shell.rs` —— `run_window` / `run_window_with` 开头各一行 `preflight()?`

## 必须覆盖

1. 从目录里认出生效版本：给一棵临时树，其中含 `msedgewebview2.exe` 的版本目录取**版本号最大**
   的那个（`100.0.0` 胜 `9.0.0`，即数字比较而非字典序）。
2. 目录不存在、或只有不带 exe 的同名字录/散文件 → `None`（不是「假装装了」）。
3. `KAFKA_UI_FAKE_WEBVIEW2_MISSING=1` 时 `is_available()` 为假、`preflight()` 返回 `Err`，
   且错误文本里含下载地址（本机 macOS 上唯一能走到的「缺失」路径）。
4. 非 Windows 且未强制缺失 → `is_available()` 为真，`preflight()` 为 `Ok`（macOS/Linux 行为不变）。

## 明确不做

- 不读注册表、不调 WebView2 的 `GetAvailableCoreWebView2BrowserVersionString`：那要引
  `webview2-com`（Windows-only 依赖，本机编不了也测不了）。按文件系统判断够了。
- 不做 tao 弹窗：tao 0.37.1 **没有** dialog API（`grep -rn MessageDialog` 在 tao 源码里零命中），
  要弹窗就得自己引 `windows` crate —— 为一个提示引 Windows-only 依赖不划算。
- 不在 macOS/Linux 上做任何实际检查（它们由系统 WebView 提供，没有「装没装」这回事）。
- 不自动下载/安装运行时（只打开下载页，用户自己装；静默安装一个系统组件不该由 UI 应用做）。
- 不改 `main.rs`（`preflight()` 的 `?` 已经能透出非零退出码与文本）。

## 已知取舍（Ruling）

- **R93** 检测走**文件系统**（`%ProgramFiles(x86)%\Microsoft\EdgeWebView\Application\<版本>\msedgewebview2.exe`）
  而不是注册表 / WebView2 API：能把整条判定逻辑做成纯函数，在这台 macOS 上跑真单测；
  代价是若用户把运行时装到了非标准位置会漏报（那时 wry 原本的 context 仍在，只是回到猜谜）。
- **R94** 「首启提示」= **stderr 警告 + 尽力打开下载页 + 非零退出**，不是模态对话框：
  本机没法编译任何 Windows-only 弹窗代码，而日志（T080/T083a）与退出码是可验证的。
  代价：用户会看到终端输出而不是弹窗（GUI 双击启动时看不到，但下载页仍会自动打开）。
- **R95** 判定入口是 `shell.rs` 里 `run_window` / `run_window_with` 的第一行：
  这里才是「窗口建不出来」的地方，`main.rs` 不重复判断；代价是两条路径各加一行（顺序维护）。
- **R96** `KAFKA_UI_FAKE_WEBVIEW2_MISSING` 这个环境变量只为**手动验证**存在：
  本卡 verify 是「手动」，而作者在 macOS 上，没有它就没法亲眼看到缺失分支与提示文本。

## 验收

- `cargo test -p webview-bridge` 全绿（新增 4 个单测）
- `cargo clippy -q -p webview-bridge --all-targets -- -D warnings` 零告警
- `cargo test -q --workspace` 无回归
- 手动（登记进 `STAGE4-manual-checklist.md`）：`KAFKA_UI_FAKE_WEBVIEW2_MISSING=1 cargo run -p main-bin`
  → stderr 出现点名 WebView2 与下载地址的警示、浏览器打开下载页、进程非零退出；
  不带该变量启动则照常出窗口。

## 结果（完成后填写，≤5 行）

## 结果（完成后填写，≤5 行）

- 新增 `crates/webview-bridge/src/webview2.rs`（212 行含 4 个单测）：`installed_version_in(root)` 从
  `<root>/<version>/msedgewebview2.exe` 里挑**数值最大**的版本（`parse_version` 按 `.` 切 `u32`，
  `120.0` > `9.0`），`runtime_root()` 只在 Windows 取 `ProgramFiles(x86)`/`ProgramFiles`（R93）。
- `lib.rs` +1（`pub mod webview2;`）；`shell.rs` +5：`run_window` 与 `run_window_with` 首行各加
  `crate::webview2::preflight()?;` —— 失败时先 `tracing::warn!` 带下载地址、再尽力打开下载页、
  最后 `bail!` 让 `main` 以非零码退出（R94/R95）；原有两条 `"failed to create WebView …"` 串保留。
- 验证：`cargo test -p webview-bridge` 全绿（4 个新单测：数值选最新 / 无 exe 不算 / 只认点分数字 /
  强制缺失翻转可用性并使 preflight 失败）；`cargo clippy -p webview-bridge --all-targets -- -D warnings`
  零告警；`cargo check --workspace` 通过；`cargo test --workspace` 281 通过无回归。
- 偏差：无。预算 218 行/3 文件（`main.rs` 无需改动 —— `?` 已把退出码与文案带出去）。
