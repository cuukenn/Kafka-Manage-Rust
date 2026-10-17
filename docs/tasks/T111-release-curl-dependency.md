# T111 — 修复 release 的 4/5 平台失败：librdkafka 需要 curl 头

状态: ✅　依赖: T110　验证: 手动（Docker 复现 + 本机 --locked 构建）　类型: 产品码

## 目标

T110 给发布构建加了 `--features real` 之后，`v0.1.1` 的 release **只有 macOS arm64 一个产物**：
另外四个 job 全红（Linux 两个 exit 101、Windows 在我写的 OpenSSL 探测步 exit 1）。
本卡查清 Linux 的 101 并把修法落到依赖规格上，让五个平台重新都能出产物。


## 只读白名单（最多 3 个源文件）

- `crates/kafka-driver/Cargo.toml` —— `real` feature 的依赖链
- `.github/workflows/release.yml` —— 五平台矩阵与 OpenSSL 前置
- `~/.cargo/registry/.../rdkafka-sys-4.10.0+2.12.1/build.rs` —— 只读外部依赖，定位根因

## 要改的文件

- 修改 `Cargo.toml`（根）—— rdkafka 的 workspace 依赖加 `"curl"` feature
- 修改 `Cargo.lock` —— 自动新增 `curl-sys`（生成物）

## 必须覆盖

1. **复现**：容器里只装 workflow 原本那份 apt 清单、跑 `--features real`，得到与 runner 一致的失败（`curl/curl.h: No such file or directory` → `CARGO_EXIT=101`）。
2. **守卫定位**：指出要 curl 头的不是 `WITH_CURL`，而是 `#ifdef WITH_OAUTHBEARER_OIDC`（`rdkafka_conf.c:59-61`），而 rdkafka-sys `build.rs:274` 无条件 `WITH_SASL_OAUTHBEARER=1` ⇒ 与 `WITH_CURL=0` 矛盾。
3. **修复可用**：同一容器把 rdkafka features 改成 `["cmake-build", "ssl", "curl"]`（**不装任何 curl 开发包**）后构建成功（`CARGO_EXIT=0`）。
4. **不破坏本机**：macOS 上 `--features real` 构建成功，随后 `--locked` 构建也成功（`Cargo.lock` 已含 `curl-sys`）。

## 明确不做

- 不在 CI 里装 curl 开发包（见 R130）。
- 不给 rdkafka 开 `ssl-vendored`（与 T110/R127 口径一致，仍用系统 OpenSSL）。
- 不改产物命名、action 版本与五平台矩阵。
- 不动 T109 已列的后续项（tag ↔ `Cargo.toml` 版本护栏、5 job 并发建 release）。
- 不在本卡打 tag。

## 已知取舍（Ruling）

- **R130** 用 `curl` feature 让 `curl-sys` 提供 curl 头与库，而不是让每个 runner 自己装curl开发包：
  该头文件是**三个平台都必需**的（`WITH_OAUTHBEARER_OIDC` 无条件成立），交给依赖能一次盖住所有平台，
  也不依赖 runner 预装什么。代价：新增 `curl-sys`（系统无 libcurl 时它自己编译一份），
  `--features real` 首次构建更慢。
- **R131** Windows 仍按镜像实际安装方式定位 OpenSSL（根目录 `C:\Program Files\OpenSSL`，
  在 `lib\` 下递归找 `libcrypto.lib`），不引入 vcpkg：镜像用的是 slproweb 安装器，目录形状以它为准。
  代价：将来镜像换安装器就要跟着改。
- **R132** 只把「让 real 编得过」做到底（依赖 + Windows OpenSSL/DLL），
  不顺手把 CI 的 `real-check` 从 `cargo check` 升为链接构建 —— 那是独立一卡。

## 验收

- 容器复现：原清单失败（`curl/curl.h: No such file or directory`）；加 `curl` feature 后 `CARGO_EXIT=0`。
- 本机：`--features real` 构建成功，随后 `--locked` 构建也成功。
- `Cargo.lock` 只新增 `curl-sys`（+16 行）。

## 结果（完成后填写，≤5 行）

- 根 `Cargo.toml` 第 56 行 rdkafka 依赖加 `"curl"` feature（`["cmake-build","ssl","curl"]`），
  `Cargo.lock` +16 行、**只新增 `curl-sys`** —— 全卡唯一的源码行改动就这一处。
- 本机（macOS）：`cargo build --release -p main-bin --features real` → `Finished release ... 25.12s`；
  紧接着 `--locked` → `Finished release ... 0.11s`（lock 与 --locked 一致，发布用 --locked 不会失败）。
- Docker 复现（ubuntu:24.04 arm64，只装 workflow 原本那份 apt 清单）：原样构建 `CARGO_EXIT=101`
  且报 `rdkafka_conf.c:60:10: fatal error: curl/curl.h: No such file or directory`；
  仅把 features 换成带 `curl` 后两次构建均 `CARGO_EXIT=0`。
- 同一卡内还修掉 Windows 侧两处：OpenSSL 根目录改按 `include\openssl\ssl.h` 识别、
  库目录 `lib\` 下递归找 `libcrypto.lib`；DLL 打包改为 glob `libcrypto-*.dll` / `libssl-*.dll`。
- 诚实备注：**尚未打 tag**，真实验收要等下一次发布（如 v0.1.2）；Windows/macOS runner 上的
  新步骤仍未经运行。
