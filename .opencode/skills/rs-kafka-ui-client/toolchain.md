# 工具链与环境

- 工具链：rustc 1.99.0 / cargo 1.99.0 / VS Build Tools 2022（MSVC 14.44.35207）/ Windows SDK 10.0.26100 / edition 2024 / rust-version 1.85。
- Windows SDK `10.0.26100` 需安装（`winget install --id Microsoft.WindowsSDK.10.0.26100 --exact`）；`Lib`/`Include`/`bin` 齐全后 rustc 经注册表自动发现，无需配 PATH。
- 注意：本仓库 `rust-analyzer` 诊断可能是过期缓存，引用前先核实实际源码。
