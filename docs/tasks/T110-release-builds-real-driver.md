# T110 — 发布产物内置 real 驱动

状态: ✅　依赖: T109　验证: 手动（`cargo build --release -p main-bin --features real` + 产物字符串取证 + YAML 解析）　类型: 配置

## 目标

`release.yml` 一直只跑 `cargo build --release -p main-bin`，**没有 `--features real`**。Mock 是
默认驱动（`kafka-driver` 的 `default = []`），于是打出来的发布产物是「Mock-only」的：用户下载后
接真实集群会得到明确错误 `real Kafka driver is not compiled in; rebuild with --features real`。
发布产物必须两种驱动都带。

## 只读白名单（最多 3 个源文件）

- `crates/main-bin/Cargo.toml` —— `real = ["kafka-app/real", "kafka-driver/real"]` 的 feature 定义
- `crates/kafka-driver/src/driver.rs` —— `PlaceholderRealDriver` 与 `NOT_BUILT` 文案（取证用的字符串）
- `.github/workflows/ci.yml` —— T093 那份 Linux 依赖清单（release 已在 T109 复用）

## 要改的文件

- 修改 `.github/workflows/release.yml`

## 必须覆盖

1. 构建命令带 `--features real`（并保留 T109 加的 `--locked`）。
2. 每个平台都拿得到 OpenSSL 开发包：Linux 由已有的 apt 清单覆盖（`libssl-dev` + `cmake`）；
   macOS 显式钉 keg-only 的 `openssl@3`；Windows 定位带 `libcrypto.lib` 的安装根目录。
3. Windows 产物把 `libcrypto-3-x64.dll` 与 `libssl-3-x64.dll` 与 exe 一起打包 —— real 驱动是
   **动态**链接 OpenSSL 的，缺 DLL 的话用户侧启动即失败。
4. 打包统一先收进 `staging/`，三个平台共用同一个暂存目录。

## 明确不做

- **不改成 `ssl-vendored`**：vendored 能让 Windows 免发 DLL、也免去找 OpenSSL，但它会让
  release 产物的链接方式与开发者本机（系统 openssl@3）分叉，而 R98「CI 与 release 构建同一
  镜像」的那层一致性更值钱。
- 不改产物命名、不改 `softprops/action-gh-release` 的版本与参数。
- 不动 T109 留下的两个后续项：5 job 并发建 release 的竞态、tag ↔ `Cargo.toml` 版本护栏。
- 不顺手把 `ci.yml` 的 `real-check`（现在是 `cargo check`，**不链接**）升级成链接构建 —— 这是
  真正能防回归的一步，但属于另一张卡（见「已知取舍」R129 的诚实备注）。

## 已知取舍（Ruling）

- **R127** 用系统 OpenSSL 而非 `ssl-vendored`：Linux 的 apt 清单已含 `libssl-dev`、Windows
  镜像自带 3.6.5、macOS 一条 brew 即可，成本最低；代价是 Windows 必须随包发 DLL，且换了
  runner 镜像就要复核一次。
- **R128** macOS 上主动 `brew install openssl@3` 并导出 `OPENSSL_DIR` / `PKG_CONFIG_PATH`，
  不依赖镜像的默认发现：镜像只列出 `openssl 1.1.1w`，而 keg-only 的 3.x 不保证被 pkg-config
  命中（T104 在本机踩到的正是这一类：默认 openssl 已是 4.x，ENGINE API 消失）。
- **R129** Windows 找不到 OpenSSL 时 **throw 而不是降级**：宁可让 workflow 红着失败，也不产出
  「能下载、一启动就崩」的产物。同理缺 DLL 直接 `throw`。代价：镜像布局变化会让 release 失败
  （这是期望行为，不是故障）。附带诚实备注：`ci.yml` 的 `real-check` 只做类型检查不链接，所以
  这条链路的回归**不会**被 CI 拦住，只会被重新打 tag 时发现。

## 验收

- `ruby -ryaml` 解析 `release.yml` 通过
- 本机跑通 runner 的同一条命令（去掉 `--target`）：`cargo build --release -p main-bin --locked --features real` 退出码 0
- 产物里**不含** `real Kafka driver is not compiled in`；而**不带** features 构建时该字符串存在
  （证明判据有区分度）

## 结果（完成后填写，≤5 行）

- `release.yml`：构建命令加 `--features real`（保留 `--locked`）；新增 macOS / Windows 两个
  OpenSSL 环境步骤（macOS：`HOMEBREW_NO_AUTO_UPDATE=1 brew install openssl@3` + 导出
  `OPENSSL_DIR`/`PKG_CONFIG_PATH`；Windows：找带 `libcrypto.lib` 的根目录，找不到 `throw`）；
  打包改为先入 `staging/`，Windows 额外 `Copy-Item` 两个 DLL 再 `Compress-Archive staging\*`，
  其余平台 `tar -czf … -C staging`。
- 取证（本机，`PKG_CONFIG_PATH`/`OPENSSL_DIR` 指向 openssl@3）：`cargo build --release -p main-bin
  --locked --features real` → `Finished release profile in 21.30s`、**exit 0**，产物 5 843 360 字节；
  `strings` 查不到占位文案 ⇒ real 驱动确已编入。反向对照：不带 features 构建（7.56s）后占位文案
  **存在** ⇒ 判据有效。`ruby -ryaml` → `release.yml: YAML OK`。
- 偏差：`release.yml` **仍未在任何 runner 上执行过**（`git tag` 为空）。macOS 的 brew 路径、
  Windows 的 OpenSSL 目录与 DLL 名（按 OpenSSL 3.x 的 Windows 分发惯例取 `libcrypto-3-x64.dll`
  / `libssl-3-x64.dll`）属**未在 runner 上验证**的部分，首次打 tag 才能兑现。
