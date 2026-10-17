# T093 — Linux webkitgtk 依赖说明 + README

状态: ✅　依赖: T092　验证: 文档　类型: 文档

## 目标

`webview-bridge` 依赖 tao/wry，在 Linux 上要链 WebKitGTK/GTK；缺开发包时用户只会看到
一段难读的 `pkg-config`/链接报错，不知道缺什么。`README.md` 目前只有**前置（Windows）**
（VS Build Tools + Windows SDK），没有 Linux 一节。本卡把 T092 里 CI 实际安装的那份
apt 清单搬进 README，形成同一份「前置依赖」说明。

## 只读白名单（最多 3 个源文件）

- `.github/workflows/ci.yml` — Linux 步骤实际 apt 安装的那份清单（唯一事实来源）
- `README.md` — 既有「构建 / 开发 / 测试」小节与「前置（Windows）」
- `Cargo.toml` — 确认 `webview-bridge` 的 tao/wry 依赖与 `real` 特性位置

## 要改的文件

- 修改 `README.md` —— 在「前置（Windows）」之后、代码块之前插入「前置（Linux）」

## 必须覆盖

1. 新增的「前置（Linux）」紧邻既有「前置（Windows）」，两节并排，读者不必翻页找。
2. apt 包名与 `ci.yml` **逐字一致**：`libwebkit2gtk-4.1-dev`、`libgtk-3-dev`、
   `librsvg2-dev`、`libssl-dev`、`pkg-config`。
3. 点名 `--features real` 额外需要 `cmake` 与 `build-essential`（rdkafka 的构建依赖）。
4. 说明「缺包时表现」：编译 `webview-bridge` 会失败，而不是沉默。

## 明确不做

- 不写 `scripts/` 下的安装脚本（README 一句 `apt-get install -y …` 足够）。
- 不覆盖非 apt 发行版（Fedora/Arch 等只在前文口径之外，需要时再单开卡）。
- 不教怎么装 Rust 工具链（README 已有构建小节）。
- 不改 CI（T092 已收口）。

## 已知取舍（Ruling）

- **R101** Linux 前置依赖只写「开发者机器上编译」这一档：README 面向从源码构建的人，不涉及
  发布产物（`.deb`/AppImage）各自的运行时依赖。代价：分发打包那一步的依赖清单仍不在此处。

## 验收

- `README.md` 中「前置（Linux）」存在且紧邻「前置（Windows）」
- 包名清单与 `.github/workflows/ci.yml` 的 `apt-get install` 行逐字一致
- 文件仍是合法 Markdown（`git diff --stat README.md` 只增不减既有结构）

## 结果（完成后填写，≤5 行）

- `README.md` 只在「前置（Windows）」与代码块之间插入一段「前置（Linux）」引用块（+16 行）：
  说明缺 WebKitGTK/GTK 开发包会让 `webview-bridge` 编译失败，给出
  `sudo apt-get install -y libwebkit2gtk-4.1-dev libgtk-3-dev librsvg2-dev libssl-dev pkg-config`，
  并注明 `--features real` 另需 `cmake` 与 `build-essential`。
- 包名清单**逐字复制**自 T092 的 `.github/workflows/ci.yml`（Linux 步骤），两处不再各写一份。
- 验证：读回 `README.md` 确认新块位置与内容；`git diff --stat README.md` 显示只增 16 行、无删除；
  Markdown 结构未变（前后代码块与「文档」小节照旧）。
- 本卡纯文档（无源文件改动），不占 250 行预算；CI 侧无需改动。
- 偏差：无。
