# T119 — 产物要能直接跑：OpenSSL 自带 + 名称-版本号-架构-格式

状态: ✅　依赖: T110,T112　验证: 手动（本机 `otool -L` + YAML 解析 + 上游源码取证）　类型: 产品配置 + CI 配置

## 目标

用户报的两件事：**(1) 下载下来的产物不能直接运行**；**(2) 命名没有规范**，要
`名称-版本号-架构-格式`。

(1) 的根因：产物链的是 **runner 上的 Homebrew OpenSSL**。用户看到的
`/usr/local/opt/openssl@3/lib/libssl.3.dylib` 就是 Intel runner 上的路径。本机用同一条命令
（`--features real`，`cmake-build`）构建出来的是：

```
$ otool -L target/release/main-bin | grep -iE 'openssl|libssl|libcrypto'
/opt/homebrew/opt/openssl@3/lib/libssl.3.dylib
/opt/homebrew/opt/openssl@3/lib/libcrypto.3.dylib
```

非系统依赖只有这两个，其余全是 `/usr/lib/*` 与 `/System/Library/Frameworks/*`。
⇒ 五个产物**只在装了 Homebrew openssl@3 的机器上能启动**，别人下载下来就是「缺库」。

## 改法与取证

`rdkafka` 的 `ssl` → `ssl-vendored`（⇒ `rdkafka-sys/ssl-vendored` ⇒ `openssl-sys/vendored`），
OpenSSL 由 `openssl-src` 从源码编成静态库链进二进制。链路逐段核实过：

- `rdkafka-sys 4.10.0`：`ssl-vendored = ["ssl", "openssl-sys/vendored"]` ⇒ 不必再并列写 `ssl`。
- `grep -rn 'feature = "ssl"' rdkafka-0.39.0/src` → **0 命中** ⇒ rdkafka 自己的 `ssl` 只是转发，
  换掉不丢功能。
- `openssl-sys 0.9.117` `build/main.rs:49-57`：`vendored` 分支在 `find_openssl()` **最前**，
  只有 `OPENSSL_NO_VENDOR` 非 `0` 才回落系统库 ⇒ 设了 `OPENSSL_DIR` 也照样走 vendored。
- `rdkafka-sys/build.rs:275` `config.register_dep("openssl")` + `cmake-0.1.58 src/lib.rs:565-570`：
  注册的 dep 读 `DEP_OPENSSL_ROOT` 并**先于**系统 `CMAKE_PREFIX_PATH` 塞进去 ⇒ librdkafka 的
  `find_package(OpenSSL)` 用的是自带那份（`openssl-src` 只产 `.a`，所以必然是静态）。
- Windows 能编：`openssl-src src/lib.rs:109-122` 的 `is_nasm_ready()` 跑 `where nasm`，缺失时
  `configure.arg("no-asm")`（`lib.rs:287-297`）；两个 runner 镜像都带 Perl
  （Windows2025 `5.42.0`、macos-15 `5.44.0`），镜像**未列 NASM**，所以这条回落是关键。

## 只读白名单（最多 3 个源文件）

- `Cargo.toml` —— 改动前的 `rdkafka` 那一行（`ssl` + `curl`）
- `.github/actions/real-deps/action.yml` —— 改动前的三平台依赖准备
- `.github/workflows/release.yml` —— 改动前的命名与 Windows DLL 步

## 要改的文件

- 修改 `Cargo.toml` —— `rdkafka` 的 `"ssl"` → `"ssl-vendored"`（`"curl"` 保留，T111 加的）
- 修改 `.github/actions/real-deps/action.yml` —— 只剩 Linux 步；删 macOS/Windows 两段；
  不再装 `libssl-dev`，补 `perl`
- 修改 `.github/workflows/release.yml` —— 命名改 target 三元组；删 Windows 的 DLL 拷贝步

## 必须覆盖

1. 产物名 = `名称-版本号-架构-格式`，架构栏用完整 target 三元组（用户选定的形态）：
   `rs-kafka-ui-client-<ver>-x86_64-unknown-linux-gnu.tar.gz`、
   `…-aarch64-unknown-linux-gnu.tar.gz`、`…-x86_64-pc-windows-msvc.zip`、
   `…-x86_64-apple-darwin.tar.gz`、`…-aarch64-apple-darwin.tar.gz`。五者天然唯一，
   不再出现「macos 叫 arm64、linux 叫 aarch64」的同物两名。
2. 产物**自包含**：`otool -L` 里不得再出现 openssl/libssl/libcrypto。
3. Windows 包内不再放 `libcrypto-*.dll` / `libssl-*.dll`（静态链进 exe 了）。
4. `real-deps` 复合动作只剩 Linux 一步；`ci.yml` 与 `release.yml` 仍共用它（R98 同源要求）。
5. 活文档同步：`agents.md`、skill 的 `toolchain` / `build-test`、`docs/development.md`
   的 openssl 节 —— 历史卡片（T104/T110/T112）不改，它们是记录。

## 明确不做

- 不在 macOS 上做 ad-hoc 签名 / 公证（需要 Apple 开发者证书，是另一件事；本次只解决「缺库」）
- 不把 tar/zip 改成带顶层目录的布局（用户没要求，且会改变既有解压习惯）
- 不引入 `curl-static`（`curl` 特征走系统 libcurl，runner 预装 `libcurl4-openssl-dev`，
  产物链的是 `/usr/lib/libcurl.4.dylib` 这类系统库，不是 keg-only 路径）
- 不删 `scripts/real-run.sh`（它仍能用，只是钉的 `OPENSSL_DIR` 被忽略）

## 已知取舍（Ruling）

- **R148**：用 vendored OpenSSL，而不是「随包发 dylib + `install_name_tool` 改 `@rpath`」。
  后者要在产物里改 Mach-O 的加载路径，还要处理 macOS 的 library validation 与 quarantine；
  静态进去是一次性解决。代价：本机二进制 5.9 MB → 10.0 MB，编译多 2~3 分钟 × 5 平台。
- **R149**：架构栏用完整 target 三元组，而不是 `x86_64` / `aarch64`。后者会让 linux 与 macos 的
  aarch64 产物**同名撞车**，且 macos 侧历史上写作 `arm64` —— 同物两名。代价：文件名变长。
- **R150**：Linux 不再装 `libssl-dev`。装了也用不到（vendored 的 root 排在
  `CMAKE_PREFIX_PATH` 最前），留着只会让「产物又链了系统 OpenSSL」这种回归重新变得不可见；
  宁可让它在 CMake 配置阶段就红。

## 验收

- `Cargo.toml` 的 `rdkafka` 含 `ssl-vendored`，`Cargo.lock` 出现 `openssl-src`
- 本机 `cargo build --release -p main-bin --features real`（且 `env -u OPENSSL_DIR
  -u PKG_CONFIG_PATH`）后 `otool -L` **无** openssl/libssl/libcrypto
- `release.yml` 的五个产物名 = 上面五条，`ruby -ryaml` 可解析、`build` 仍 12 步
- `real-deps` 复合动作 1 步；`ci.yml` 仍可解析
- 最终判据（GitHub Release 侧）：下次 tag 出来的五个产物名符合规范，且 macOS/Linux 下载方
  不再需要 Homebrew / 系统 OpenSSL 就能启动

## 结果（完成后填写，≤5 行）

- 本机验证：`env -u OPENSSL_DIR -u PKG_CONFIG_PATH cargo build --release -p main-bin
  --features real` → 2m33s 成功，`otool -L` 非系统依赖**零** openssl；5 863 072 → 9 982 400 B。
- `Cargo.toml` 1 行源码改动（`ssl` → `ssl-vendored`）；`Cargo.lock` +10 行（不计入预算）。
- `.github/`：`release.yml` 命名改 target 三元组 + 删 DLL 步 + 删死变量 `ARCH`；
  `real-deps` 从 3 步缩到 1 步。
- 文档：`agents.md`、skill `toolchain`/`build-test`、`docs/development.md` 的 openssl 节同步改写。
- 真正的判据在 GitHub Release 上：待 v0.1.5 的 tag 触发后看五个产物。
