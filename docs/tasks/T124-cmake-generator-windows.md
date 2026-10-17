# T124 — Windows 的 CMake 生成器：不让 crate 去猜 VS 版本

状态: ✅　依赖: T120,T121,T122,T123　验证: 手动（annotation 正文 + 本机 CMake 复现 + 上游源码取证）　类型: CI 配置

## 目标

v0.1.5 第四轮（run 39055727993）：四个平台全绿，Windows 仍红在「编译 release 二进制」。
T123 的 perl 修复**已生效**（`[notice] OPENSSL_SRC_PERL=C:\Strawberry\perl\bin\perl.exe`，
perl 的报错整段消失），失败点往前推到 rdkafka-sys 的 CMake 配置阶段：

```
CMake Error: Could not create named generator Visual Studio 18 2026
```

## 取证

- 那串生成器名是 `cmake` crate 自己起的：`cmake-0.1.58 src/lib.rs:962-980` 的
  `visual_studio_generator()` 把 `find_vs_version() → VsVers::Vs18` 映射成
  `"Visual Studio 18 2026"`；runner 上的 CMake 不认这个名字。
- 本机复现（CMake 4.4.4）：`cmake -G "Visual Studio 18 2026" -Thost=x64 -Ax64` →
  第一行就是 `CMake Error: Could not create named generator Visual Studio 18 2026`，
  后面跟着整份 Generators 清单 —— 与 annotation 里那堆 `Kate - MinGW Makefiles` /
  `Sublime Text 2 - NMake Makefiles` 完全同形。⇒ 确认失败形态。
- `cmake` crate 在 MSVC 下只有两条安全路径（`src/lib.rs:649-652`）：VS 生成器，或
  `NMake Makefiles` / `NMake Makefiles JOM`；选后者时它**不会**再塞 `-Thost=x64 -Ax64`
  （宿主架构由 vcvars 决定，正是我们要的）。
- 生成器可以被 env 显式指定：`src/lib.rs:510-513` `self.generator.or(getenv_target_os("CMAKE_GENERATOR"))`。
- 单配置生成器不会丢优化：crate 会按 Rust 的 profile 推 `CMAKE_BUILD_TYPE`
  （`src/lib.rs:110-117`、`695+`）并 `-D` 进去 ⇒ release 仍是 `Release`。
- `nmake` 由 T120 的 `ilammy/msvc-dev-cmd@v1` 保证存在（与 T121 已钉住的 `link.exe` 同目录）。

## 要改的文件

- 修改 `.github/workflows/release.yml` —— 新增第 9 步「CMake 生成器（Windows）」：
  `"CMAKE_GENERATOR=NMake Makefiles"` 写进 `GITHUB_ENV`；`where.exe nmake` 取不到就当场
  `throw`（把「msvc-dev-cmd 到底生效没有」变成显式检查）；`::notice::` 报
  `CMAKE_GENERATOR` + `cmake --version` + nmake 的实际路径
- 修改 `.github/workflows/release.yml` —— 失败上报从 `tail -n 25 build.log` 改成
  `grep -niE "cmake error|could not create named generator|^error|error:|panicked|could not|not found|generator|nmake|perl reported"`
  再取尾部 25 行

## 明确不做

- 不升级 / 打补丁 `cmake` crate（它是 rdkafka-sys 的 build-dependency，映射表不在本仓）
- 不猜 VS 生成器的正确名字（`Visual Studio 18 2025`? `Visual Studio 18 2026`? —— 猜对也是暂时对，
  runner 镜像一更新又漂）
- 不引入 Ninja（镜像是否预装 ninja 未确认；nmake 是已被 msvc-dev-cmd 保证的那一个）
- 不动 Linux/macOS 的构建路径（四个平台这轮全绿）

## 已知取舍（Ruling）

- **R159**：生成器名用 env 显式钉成 `NMake Makefiles`，而不是等 `cmake` crate 认识 runner 上的 VS。
  本仓控制不了 crate 的映射表，而 NMake 是 CMake 与 crate 都明确支持的通道
  （`src/lib.rs:649-652`）。代价：NMake Makefiles 是单配置生成器，比 VS 生成器慢，
  且依赖 vcvars 已把宿主架构设对（由 msvc-dev-cmd 保证）。
- **R160**：失败上报改成按关键字挑行。上一轮 CMake 把整份 generator 清单打在日志最后，
  尾部 25 行全是噪音，真正的 `CMake Error:` 在更前面 —— 一整轮都看不到根因。
  代价：关键字漏了仍可能看不到，但至少不再被「尾部」这个形状绑架。

## 验收

- `release.yml` YAML 可解析，`build` 15 步，Windows 侧顺序为
  MSVC 开发环境 → CMake 生成器 → 钉住链接器 → 编译
- 下一轮 Windows 的 annotation 应出现 `[notice] CMAKE_GENERATOR=NMake Makefiles | cmake <版本> | nmake=<路径>`
- 若仍失败，annotation 里应出现带行号的 `CMake Error:` 原文（而不是 generator 清单）

## 结果（完成后填写，≤5 行）

- 本机复现确认了失败形态（CMake 4.4.4 对未知生成器输出的正是 annotation 里那堆清单）。
- `release.yml`：+1 步（CMake 生成器）+ 失败上报改成 grep 挑行；`.github/` 改动，**0 行源码**。
- 判据在下一轮：Windows 是否过、以及 `CMAKE_GENERATOR` 的 notice 是否出现。
