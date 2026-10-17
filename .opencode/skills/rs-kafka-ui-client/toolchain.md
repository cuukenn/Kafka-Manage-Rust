# 工具链与环境

- 版本：rustc / cargo **1.99.1**、edition 2024、rust-version 1.85、resolver 3。

## macOS（darwin / arm64）

- 需要 `cmake`（Homebrew）。
- **`--features real` 必须钉住 openssl@3**：Homebrew 的默认 `openssl` 已是 **openssl@4**，而 OpenSSL 4 **不再导出 ENGINE API**；vendored librdkafka 仍调用 `_ENGINE_by_id` 等符号 ⇒ 链接失败 `Undefined symbols … _ENGINE_by_id`（退出码 101）。
  - 首选：`./scripts/real-run.sh build -p main-bin --features real`（自动定位 keg-only 的 openssl@3）。
  - 或机器级 `~/.cargo/config.toml` 的 `[env]` 钉住 `OPENSSL_DIR` / `PKG_CONFIG_PATH`（`force` 默认 false，只填未设置的变量，不覆盖显式值与 CI）。
- 前端需要 **Node 22**（vite 7；宿主默认 Node 16 会被拒）；`nvm use 22`。

## Windows

- VS Build Tools 2022（MSVC 14.44.35207）+ Windows SDK `10.0.26100`（`winget install --id Microsoft.WindowsSDK.10.0.26100 --exact`）；`Lib` / `Include` / `bin` 齐全后 rustc 经注册表自动发现，无需配 PATH。

## 注意

- 本仓库 `rust-analyzer` 诊断可能是过期缓存，引用前先核实实际源码。
