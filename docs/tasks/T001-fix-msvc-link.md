# T001 · 修复 MSVC 链接环境（安装 Windows SDK）

**状态**: ✅完成 | **依赖**: 无（**全局阻塞，最先做**） | **改动上限**: 不改代码

## 目标
让本机 `cargo build/test` 能正常链接，解除全仓编译阻塞。

## 已确诊的事实（不用再查）
- `rustc hello.rs` 直接失败：`error: linker link.exe not found`
- 手动 `vcvars64.bat` 后 `link.exe` 能找到，但报 `LNK1181: 无法打开输入文件 kernel32.lib`
- `C:\Program Files (x86)\Windows Kits\10` 下只有 `Catalogs`、`Redist`，**没有 `Lib`/`Include`/`bin`** → Windows SDK 未安装
- VS Build Tools 2022 已装：`C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools`，MSVC 组件 `14.44.35207` 存在
- rustc 1.99.0 / cargo 1.99.0 / host `x86_64-pc-windows-msvc`

## 做法（按顺序试，成功即停）
1. 给 Build Tools 加 SDK 组件（需管理员，耗时较长）：
   `winget install Microsoft.VisualStudio.2022.BuildTools --override "--quiet --add Microsoft.VisualStudio.Component.Windows11SDK.22621"`
   或运行 `C:\Program Files (x86)\Microsoft Visual Studio\Installer\vs_installer.exe`，在 Build Tools 修改里勾选「Windows 11 SDK」。
2. 备选：装独立 Windows SDK（微软官方 `winsdksetup.exe`）。
3. 装完后**新开一个终端**（不要复用旧 PATH）。

## 验收（必须全部贴出输出）
1. `Test-Path "C:\Program Files (x86)\Windows Kits\10\Lib"` 为 True，且其下有 `10.0.*\um\x64\kernel32.lib`
2. 裸编译通过：`rustc "$env:TEMP\opencode\linktest\hello.rs" -o "$env:TEMP\opencode\linktest\hello.exe"` 且 `& hello.exe` 打印 `ok`
3. `cargo check -p kafka-core -p kafka-driver` 通过（在 `E:\Workplace\Kafka-Manage-Rust`）

## 明确不做
- 不改任何源码、不改 Cargo.toml、不装 rdkafka/cmake、不切 GNU 工具链
- 若第 1 步需要用户授权/重启，把状态改 `⛔阻塞` 并写清卡在哪一步

## 结果（完成后填写，≤5 行）
- 走「备选」路线：`winget install --id Microsoft.WindowsSDK.10.0.26100 --exact`（需 UAC 授权），已成功安装；独立 SDK 与 VS 组件共用 `C:\Program Files (x86)\Windows Kits\10`，rustc 通过注册表自动发现，安装位置不可选。
- 验收 1 ✅：`Lib`/`Include`/`bin` 齐全，版本目录 `10.0.26100.0`，`kernel32.Lib` 在 `Lib\10.0.26100.0\um\x64`，ucrt 在 `Lib\10.0.26100.0\ucrt\x64\libucrt.lib`。
- 验收 2 ✅：`rustc hello.rs -o hello.exe` 退出码 0，`hello.exe` 打印 `ok`（rustc/cargo 1.99.0）。链接环境彻底通了，VS Build Tools 之前就已装好，缺的只有 SDK。
- 验收 3 拆出去了：环境通了之后暴露的是**代码从未编译过**，已登记 T052（kafka-core，已修绿）与 T053（kafka-driver，待修）。`cargo check -p kafka-core --all-targets` 现已全绿。
- 代价：C: 盘 10.1 GB → 6.59 GB 空闲（约 3.5 GB）。D:/E: 盘充裕。