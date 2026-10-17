# T112 — CI 也真实编译 real 驱动（共用 real-deps 复合作）

状态: ✅　依赖: T110　验证: 手动（YAML 解析 + 与 release.yml 共用同一份）　类型: 配置

## 目标

`ci.yml` 的 `real-check` 一直只跑 `cargo check -p kafka-driver --features real`——**只做类型检查、不链接**。于是 T110/T111 那类「real 编不过」的问题，CI 一个都拦不住，只能等打 tag 时在 release 里暴露（v0.1.1 就是这样丢掉 4/5 个产物的）。

本卡把 CI 升级为**真的链接构建**，并把三平台各自准备依赖的那段逻辑抽成复合作 `.github/actions/real-deps/`，供 `ci.yml` 与 `release.yml` 共用——一处维护，且保住 R98「CI 与 release 构建同一套环境」的对齐。

## 只读白名单（最多 3 个源文件）

- `.github/workflows/release.yml` —— 依赖准备步骤的原文（要被抽出去的那三段）
- `.github/workflows/ci.yml` —— `real-check` job 全貌
- `crates/main-bin/Cargo.toml` —— `real` feature 的传递关系

## 要改的文件

- 新增 `.github/actions/real-deps/action.yml` —— 三平台依赖准备的复合作
- 修改 `.github/workflows/ci.yml` —— 用复合作 + 真实链接构建
- 修改 `.github/workflows/release.yml` —— 三段内联步骤换成复合作

## 明确不做

- 不改 `rdkafka` / `openssl` 依赖规格（T111 已定案）
- 不改五平台矩阵、产物命名与 action 版本
- 不改 `release.yml` 里两段打包逻辑
- 不解决「5 job 并发建 release」与 tag↔`Cargo.toml` 护栏（T109 已列的后续卡）
- 不在本卡打 tag

## 已知取舍（Ruling）

- **R133** 抽复合作而不是在 `ci.yml` 里再抄一份：三平台依赖清单必须同源（R98），复制会漂移；代价是多一个 `.github/actions/` 目录与一层 `uses:` 间接。
- **R134** `real-check` 改用 `cargo build -p main-bin --features real --locked`（真实链接）而不是 `cargo check`：T110/T111 的问题**全部只在链接期暴露**，只 check 等于没测；代价是 CI 时长增加（要编 librdkafka），并需要完整的 Linux 系统依赖。
- **R135** 复合作只装依赖、不负责构建：`ci.yml` 与 `release.yml` 的构建命令不同（前者无 `--target`，后者带），塞进复合作就要给它传参、反而更难读；代价是两处各写一条 cargo 命令。

## 验收

- `.github/actions/real-deps/action.yml`、`ci.yml`、`release.yml` 三份 YAML 均可解析
- `release.yml` 步序与替换前一致（12 步）
- 依赖清单只存在于复合作一处（`grep` 确认两 workflow 里不再有 apt/brew/OpenSSL 步骤）

## 结果（完成后填写，≤5 行）

- 新增 `.github/actions/real-deps/action.yml`（37 行、3 step）：Linux 装全清单、macOS 装 keg-only openssl@3 并导出 `OPENSSL_DIR`/`PKG_CONFIG_PATH`、Windows 按镜像真实布局（

`C:\Program Files\OpenSSL` → `lib\` 下递归找 `libcrypto.lib`）导出三个 `OPENSSL_*` 并把 `bin` 加入 PATH；找不到即 `throw`。
- `ci.yml`：`real-check` 用该复合作，并把 `cargo check -p kafka-driver --features real` 换成 **`cargo build -p main-bin --features real --locked`** ⇒ 链接期问题从此在 CI 就能拦住。
- `release.yml`：原三段内联依赖步骤整体换成一处 `uses: ./.github/actions/real-deps`；`ruby -ryaml` → `YAML OK, steps=12`（checkout → real-deps → 提取版本 → 工具链 → 缓存 → Node → 前端 → 编译 → 准备产物 → 打包×2 → 发布）。
- 诚实备注：仍是**静态校验**——本机无法执行 GitHub Actions，真正的验证要等下一条带 `--features real` 的 CI 跑起来（`ci.yml` 的 push/PR 触发即可，不必打 tag）。


## 必须覆盖

1. `ci.yml` 的 real-check 从 `cargo check`（不链接）升为**真实链接构建**：`cargo build -p main-bin --features real --locked`。
2. 三平台依赖准备只留一份实现（`.github/actions/real-deps/`），`ci.yml` 与 `release.yml` 都 `uses:` 它 —— 保住 R98 的「同一套环境」。
3. 复合作必须覆盖 T110/T111 查明的三处前置：Linux 的 webkit2gtk/gtk/rsvg/libssl/cmake 全清单、macOS 的 keg-only openssl@3 + 两个环境变量、Windows 按镜像真实布局（`C:\Program Files\OpenSSL`，`lib\` 下递归找 `libcrypto.lib`）写 `OPENSSL_*` 并把 `bin` 加进 PATH。
4. 两文件改完后 YAML 均可解析，且 `release.yml` 的步序不变（依赖准备仍在构建之前）。