# T126 — 产物名改成 `名称_版本.平台_架构.格式`，解压出来的文件带上版本号

状态: ✅　依赖: T119,T113　验证: 手动（YAML 解析 + 五平台名单模拟）　类型: CI 配置

## 目标

用户看完 v0.1.5 的产物后说：「解压产物名称不对，应该为名称_版本.产物平台格式」，并明确
「目前 macos 的明确不对」。追问确认：档案名要区分架构，用 `平台_架构` 的写法。

v0.1.5 实际长这样（T119 的 target 三元组命名）：

```
档案    rs-kafka-ui-client-0.1.5-aarch64-apple-darwin.tar.gz
解压出来 rs-kafka-ui-client            ← 没有版本号、没有平台，macOS 连格式段都没有
```

本卡把两层都改成用户要的形态。

## 只读白名单（最多 3 个源文件）

- `.github/workflows/release.yml` —— 改动前的 matrix 行、`env` 块、`准备产物文件` / 两个打包步
- `docs/tasks/T119-self-contained-artifacts.md` —— 被本卡改掉命名那一半的卡（自包含部分不动）
- `docs/tasks/T125-release-v015-acceptance.md` —— 记录了旧命名的验收卡

## 要改的文件

- 修改 `.github/workflows/release.yml` —— matrix 的 `arch` 值改 `x64`/`arm64`、`env` 补回 `ARCH`、
  `OUTBIN` 与两个打包步的命名
- 修改 `docs/development.md` —— 发布节的命名描述
- 修改 `docs/tasks/STAGE4-manual-checklist.md` —— 第 37 条（旧命名断言已成假命题）

## 必须覆盖

1. 档案名 = `名称_版本.平台_架构.格式`：
   `rs-kafka-ui-client_0.1.5.windows_x64.zip`、`…_0.1.5.linux_x64.tar.gz`、
   `…_0.1.5.linux_arm64.tar.gz`、`…_0.1.5.macos_x64.tar.gz`、`…_0.1.5.macos_arm64.tar.gz`。
2. 解压出来的可执行文件 = `名称_版本`（Windows 多一个 `.exe`）：
   `rs-kafka-ui-client_0.1.5` / `rs-kafka-ui-client_0.1.5.exe`。
3. 五者仍然唯一（`linux_x64` 与 `macos_x64` 不撞名），`upload-artifact` 的 name 与 `ASSET` 同源。
4. 构建本身不动：`--target "$TARGET"` 仍用完整三元组，`cp "target/${TARGET}/release/${BIN}"` 不变。

## 明确不做

- 不再用 target 三元组做档案名（三元组继续留在构建命令里，只是不再出现在产物名上）
- 不改 tar/zip 的目录布局（仍然是解压即得文件，不套一层目录）
- 不给 macOS 造 `.app` bundle（若「macOS 的格式」指的是 `.app`，那是另一张卡）
- 不动 T119 的自包含结论与 T113 的单点发布

## 已知取舍（Ruling）

- **R162**：档案名用 `平台_架构`（`x64` / `arm64`）而不是 target 三元组。三元组对机器友好、天然唯一，
  但用户要的是「一眼看懂的平台+架构」。代价：`x64`/`arm64` 不是 Rust 的官方拼写（`x86_64`/`aarch64`），
  只能在 matrix 里显式写死，且档案名与 `--target` 不再字面一致。
- **R163**：解压出来的可执行文件带版本号。代价：启动命令变成 `./rs-kafka-ui-client_0.1.5`，
  脚本/文档不能硬编码一个永久路径。换来的是「解压出来的东西自己说明它是哪个版本」——
  这正是用户抱怨的那一层。

## 验收

- `ruby -ryaml` 可解析，`build` 仍 15 步
- 模拟出的五个档案名与两个解压文件名符合上面第 1、2 条
- 无残留的旧命名用法（`$env:TARGET` 只应出现在构建/拷贝，不出现在命名）
- 最终判据：下次 tag 出来的 Release 上，五个 asset 名与解压后的文件名都对

## 结果（完成后填写，≤5 行）

- `release.yml`：matrix `arch` → `x64`/`arm64`；`env` 补 `ARCH`；`OUTBIN="${APP_NAME}_${VERSION}"`
  （Windows 加 `.exe`）；两个打包步改成 `${APP_NAME}_${VERSION}.${PLATFORM}_${ARCH}`。
- 模拟输出：`rs-kafka-ui-client_0.1.5.macos_arm64.tar.gz -> rs-kafka-ui-client_0.1.5` 等五条，
  `YAML OK jobs=build,release build_steps=15`。
- 规划提交：`.github/` 与 `docs/`，**0 行源码**。
- 真正的验收在下次 tag：Release 上的 asset 名与解压后的文件名。
