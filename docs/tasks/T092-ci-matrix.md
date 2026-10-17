# T092 — CI 矩阵（win/linux/macos）+ 产物上传

状态: ✅　依赖: T090　验证: CI 绿　类型: 配置

## 目标

仓库里**只有一个**工作流 `.github/workflows/release.yml`，且只在打 `v*` tag 时触发：平时
任何一次 push 都不跑测试，三个平台的编译情况要到发版那一刻才知道。本卡补一个**每次 push /
PR 都跑**的 CI：win/linux/macos 三平台矩阵，装依赖 → 构建前端 → `cargo test --workspace`
→ clippy → 编译 release 二进制 → 用 `actions/upload-artifact` 传产物。

顺带修一个既存缺陷：`release.yml` 用 `node-version: 20`，而 Vite 7 要求
Node ≥20.19 / ≥22.12（本机实测 Node 16 与 20 早期版本都会被拒），发版那一刻的前端构建会挂在
`npm run build`。改为 22。

## 只读白名单（最多 3 个源文件）

- `.github/workflows/release.yml` —— 既有矩阵/打包/上传口径
- `Cargo.toml` —— workspace 成员、edition/rust-version、release profile
- `frontend/package.json` —— `npm ci` / `npm run build` 两个脚本

## 要改的文件

- 新增 `.github/workflows/ci.yml` —— push/PR 触发的三平台矩阵
- 修改 `.github/workflows/release.yml` —— `node-version: 20` → `22`

## 必须覆盖

1. **三平台矩阵**：`ubuntu-22.04` / `macos-14` / `windows-latest`，`fail-fast: false`
   （一个平台挂掉不掩盖另外两个的结果）。
2. **先构建前端再碰 Rust**：`main-bin` 用 `include_dir!("$CARGO_MANIFEST_DIR/../../frontend/dist")`
   在编译期嵌入资源，`frontend/dist` 只有两个文件被强制入库，所以每个平台都必须先
   `npm ci && npm run build`，否则 `cargo test --workspace` 会在 `main-bin` 上编译失败。
3. **Linux 依赖齐备**：`libwebkit2gtk-4.1-dev` / `libgtk-3-dev` / `librsvg2-dev` +
   `libssl-dev` + `pkg-config`（tao/wry 在 Linux 上走 gtk3 + webkit2gtk-4.1，rdkafka 走 openssl）。
4. **三道门禁**：`cargo test --workspace --locked`、`cargo clippy --workspace --all-targets -- -D warnings`、
   `cargo build --release -p main-bin`。
5. **产物上传**：`actions/upload-artifact@v4` 收 `target/release/main-bin`（Windows 是 `.exe`），
   `if-no-files-found: error` —— 路径写错要当成失败，而不是静默传个空包。
6. **真驱动可编译**：单独一个 Ubuntu job 跑 `cargo check -p kafka-driver --features real`，
   把「`--features real` 只在开发者本机验证过」这条从人肉记忆变成门禁（rdkafka 需 cmake + C 工具链，
   bundled 构建要几分钟，所以不放进三平台矩阵）。

## 明确不做

- **不在本机跑 CI**：这是 macOS/arm64 单机，没有 GitHub runner；验证靠「YAML 语法/结构自查 +
  本地等价命令已全绿」，这一点在「结果」里如实写明，不假装 CI 已绿。
- 不发版：tag 触发的打包仍归 `release.yml`，本卡只补 push/PR 门禁。
- 不装 `libayatana-appindicator3-dev`：工程里没有托盘（`tao` 的 tray 未启用）。
- 不做交叉编译/多架构 release：`release.yml` 已覆盖 linux aarch64 与 macos x64。
- 不引入 `cargo-nextest` / `cargo-deny` / 覆盖率上传等额外工具链。

## 已知取舍（Ruling）

- **R97** CI 与 release **分成两个工作流**而不是把 push 触发塞进 `release.yml`：后者带
  `permissions: contents: write` 与 `softprops/action-gh-release`，不该在每次 push 都拿到
  写权限。代价：两处都要维护「构建前端 → 编译 `main-bin`」这段。
- **R98** 用 `ubuntu-22.04` 而不是 `ubuntu-latest`：webkit2gtk-4.1 在 22.04 上确实有包，
  与 `release.yml` 保持一致，也避免 `latest` 悄悄滚到新 LTS 时整条流水线突然变色。
  代价：22.04 会先于 `latest` 结束支持，将来要成对升级。
- **R99** `--locked` 只加在 `cargo test` 上：它守住「提交的 `Cargo.lock` 能与清单自洽」，
  而 clippy / release 构建复用同一份锁文件，不必重复。代价：如果谁手动改了依赖没更新锁，
  报错点在测试步骤而非依赖步骤。
- **R100** 真驱动门禁只放一个 `cargo check`（Ubuntu），不跑它的测试：`real_conformance`
  是 `#[ignore]` + 需要真 broker（`KAFKA_CONFORMANCE_BOOTSTRAP`），CI 里没有 Kafka 容器。
  代价：「预发级」验证仍只在开发者本机 + Docker broker 上做（T077 清单）。

## 验收

- `.github/workflows/ci.yml` 的结构自查：三平台矩阵、Linux 依赖、前端构建、三道门禁、产物上传、
  `--features real` 单独的 check job 全部在位；`fail-fast: false`；`permissions: contents: read`。
- YAML 可被解析：`python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml'))"`。
- 本地等价命令已全绿：`cargo test -q --workspace`、`cargo clippy -q --workspace --all-targets -- -D warnings`、
  `cargo build --release -p main-bin`、`cd frontend && npm ci && npm run build`（Node 22）。
- `release.yml` 的 `node-version` 为 `22`。
- 手动/无法在本机验证的部分写进「结果」：CI 是否真绿要等推上去后才能确认。

## 结果（完成后填写，≤5 行）

- 新增 `.github/workflows/ci.yml`：`test` 任务在 `ubuntu-22.04`/`macos-14`/`windows-latest` 上开矩阵
  （`fail-fast: false`），依次装 Linux 依赖 → Node 22 → `npm ci && npm run build` → `cargo test
  --workspace --locked` → `cargo clippy --workspace --all-targets -- -D warnings` →
  `cargo build --release -p main-bin` → `actions/upload-artifact@v4`（`if-no-files-found: error`）；
  另有 `real-check` 任务只在 Ubuntu 上跑 `cargo check -p kafka-driver --features real`。
- 修改 `.github/workflows/release.yml` 第 55 行 `node-version: 20` → `22`：顺带修掉发布流水线的
  **实际缺陷** —— Vite 7 要求 Node ≥20.19/≥22.12，原来打 tag 时前端构建必然失败。
- 验证：`ruby -ryaml` 解析两个 workflow（本机 `python3` 无 yaml 模块，改用它）；本地等价门禁全绿 ——
  `cargo test -q --workspace` 281 passed、`cargo clippy -q --workspace --all-targets -- -D warnings`
  零告警、`cargo build --release -p main-bin` 成功（24.29 s，4094960 B）、Node 22 下
  `npm ci && npm run build` 产出与已提交的 dist **逐字节相同**（所以无需 `git add -f`）。
- 诚实说明：CI 自身无法在本机（macOS/arm64）运行，行 4 的「CI 绿」要等第一次 push 才能坐实；
  本卡能给到的是 YAML 可解析 + 三条门禁在本机通过 + workflow 结构自检（R97–R100）。
- 偏差：无。
