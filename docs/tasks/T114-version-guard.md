# T114 — 版本护栏：tag 必须等于 Cargo.toml 的 workspace 版本

状态: ✅　依赖: T113　验证: 手动（YAML 解析 + 护栏逻辑干跑）　类型: 配置

## 目标

`release.yml` 提取版本时只做 `VERSION="${GITHUB_REF_NAME#v}"`，**从不校验**这个数字是否
等于产物里编进去的版本。于是打 `v1.2.3` 这种与 `Cargo.toml` 不符的 tag，会顺顺当当地
发出一批「文件名写着 1.2.3、`--version` 却报 0.1.0」的产物——而且没人会在 CI 里被拦住。

这是 T109 列的遗留项。本卡在 `build` 之前加一个 `version-guard` job：tag 去掉 `v` 后
必须等于根 `Cargo.toml` 的 `[workspace.package] version`，否则在第一个 job 就失败。

## 只读白名单（最多 3 个源文件）

- `.github/workflows/release.yml` —— 改动前全文（`build` 的版本提取步）
- `Cargo.toml` —— `[workspace.package] version` 的位置与写法
- `docs/tasks/T109-release-workflow-fixes.md` —— 本项的出处

## 要改的文件

- 修改 `.github/workflows/release.yml` —— 新增 `version-guard` job，并给 `build` 加 `needs`

## 必须覆盖

1. 新增 job `version-guard`（`runs-on: ubuntu-22.04`、`permissions: contents: read`）——
   不跑构建，只做一次字符串比较。
2. 从 `Cargo.toml` 的 `[workspace.package]` 段里取 `version`，马甲不变（不解析 TOML 全部
   内容，只在该段内取第一行 `version`）。
3. tag（去 `v`）与它不一致时 `::error::` + `exit 1`；两者都打印出来，便于对照。
4. `build` 加 `needs: version-guard` ⇒ 版本不对时五个 runner 一个都不起。

## 明确不做

- 不改版本号本身（本卡只加护栏，不动 `Cargo.toml` 的 `0.1.0`）
- 不做自动改版本 / 自动打 tag（发布仍由人工决定）
- 不校验 tag 与 git 历史 / 分支的关系
- 不在 `build`、`release` 里重复校验

## 已知取舍（Ruling）

- **R139** 用独立 job 而不是塞进 `build` 的第一步：`build` 是 5 路 matrix，
  塞进去就等于同一件事在五个 runner 上各跑一遍、且要到最后一刻才整体失败；
  独立 job 只跑一次、失败即拦住下游。代价是多一个 job 的排队时间（约几秒）。
- **R140** 用 `awk` 在 `[workspace.package]` 段内取 `version`，而不是
  `cargo metadata`：护栏要在**装 Rust 工具链之前**就能跑（越早失败越好），
  `cargo metadata` 会强制先装工具链。代价是它只认这一种写法——将来若版本改到别处
  或改成动态生成，这条要跟着改；取不到时脚本会明确报错而不是静默放过。

## 验收

- 三份 YAML 仍可解析；`release.yml` 的 jobs 为 `version-guard, build, release`
- `build.needs == version-guard`，`version-guard` 只有 2 步（checkout + 校验）
- 干跑：tag 与 `Cargo.toml` 一致时退出 0；不一致时退出 1

## 结果（完成后填写，≤5 行）

- `.github/workflows/release.yml` **+27/−0**（全为 `.github/`，**0 行源码**）：新增 `version-guard` job
  （checkout → `awk` 取 `[workspace.package] version` → 与 `${GITHUB_REF_NAME#v}` 比对，
  不等则 `::error::` + `exit 1`），`build` 加 `needs: version-guard`。
- 验证：`ruby -ryaml` → `YAML OK jobs=version-guard,build,release build.needs=version-guard guard steps=2`；
  `awk -F'"' '/^\[workspace\.package\]/ { f = 1 } f && /^version/ { print $2; exit }' Cargo.toml` → `0.1.0`。
- 干跑护栏：`GITHUB_REF_NAME=v0.1.0` → `OK tag=0.1.0 manifest=0.1.0`；
  `GITHUB_REF_NAME=v9.9.9` → `MISMATCH …` 且 **exit=1**（按设计失败）。
- 诚实备注：仍未被 runner 执行过；下一次打 tag 时才会真跑。

