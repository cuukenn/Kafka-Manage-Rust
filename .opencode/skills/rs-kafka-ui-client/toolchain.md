# 工具链与环境

- 版本：rustc / cargo **1.99.1**、edition 2024、rust-version 1.85、resolver 3。

## macOS（darwin / arm64）

- 需要 `cmake`（Homebrew）。
- **`--features real` 的 OpenSSL 自 T119 起是自带的**：根 `Cargo.toml` 用 `rdkafka` 的 `ssl-vendored`（⇒ `rdkafka-sys/ssl-vendored` ⇒ `openssl-sys/vendored`），OpenSSL 由 `openssl-src` 从源码编成**静态库**链进二进制。产物不再引用 `/opt/homebrew/opt/openssl@3/lib/libssl.3.dylib`；`OPENSSL_DIR` / `PKG_CONFIG_PATH` 不再需要 —— vendored 在 `openssl-sys` 的 `find_openssl()` 里排在最前，只有显式设 `OPENSSL_NO_VENDOR=1` 才会回到系统库。
  - 代价：首次编译多花 2~3 分钟编 OpenSSL；本机 release 二进制从 5.9 MB 涨到 10.0 MB。
  - 历史（T104 的坑，随 vendored 一并失效）：Homebrew 默认 `openssl` 是 **openssl@4**，不再导出 ENGINE API，vendored librdkafka 仍调用 `_ENGINE_by_id` ⇒ 链接失败（退出码 101）；当时靠钉 keg-only 的 openssl@3 绕过。
- 前端需要 **Node 22**（vite 7；宿主默认 Node 16 会被拒）；`nvm use 22`。

## Windows

- VS Build Tools 2022（MSVC 14.44.35207）+ Windows SDK `10.0.26100`（`winget install --id Microsoft.WindowsSDK.10.0.26100 --exact`）；`Lib` / `Include` / `bin` 齐全后 rustc 经注册表自动发现，无需配 PATH。

## 注意

- 本仓库 `rust-analyzer` 诊断可能是过期缓存，引用前先核实实际源码。
