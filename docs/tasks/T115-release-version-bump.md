# T115 — 发布时同步版本：`scripts/release.sh`，并把工作区版本升到 0.1.3

状态: ✅　依赖: T114　验证: 手动（`scripts/release.sh` 干跑 + 护栏逻辑核对）　类型: 产品码

## 目标

T114 的护栏在 `v0.1.3` 上**按设计开火**了：run `39011500119` 里 job `tag matches Cargo.toml`
在第 3 步失败，`build`×5 与 `publish release` 全部 skipped。原因是 tag 说 `0.1.3`，
而 `[workspace.package] version` 还是 `0.1.0`。

护栏是对的 —— 对照 `v0.1.2`（run `39010921524`，success）：它的产物名是 `…-0.1.2-…`，
而里面的二进制自称 `0.1.0`，正是护栏要抓的漂移。真正缺的是**发布时同步版本的那条路**：
本卡补 `scripts/release.sh`（bump + 刷 lock + commit + tag + push），并把工作区版本升到
`0.1.3`，让这个 tag 可以正常发布。

## 只读白名单（最多 3 个源文件）

- `.github/workflows/release.yml` —— `version-guard` 里读取版本的那段 `awk`
- `Cargo.toml` —— `[workspace.package]` 段
- `docs/tasks/T114-version-guard.md` —— 护栏的约定与取舍

## 要改的文件

- 新增 `scripts/release.sh` —— 一条命令走完 bump → 刷 lock → commit → tag → push
- 修改 `Cargo.toml` —— `version` 由 `0.1.0` 升到 `0.1.3`
- 修改 `Cargo.lock` —— 5 个 workspace 成员的版本（生成物）

## 必须覆盖

1. `release.sh` 的四道前置各自拦住错误：版本号格式、该 tag 已存在、工作树不干净、当前版本已等于目标。
2. bump 只改 `[workspace.package]` 段内的 `version` 行，且**读取用与 `version-guard` 完全相同的 `awk`**（写入与校验互为对照，不会各认一种写法）。
3. bump 之后**必须刷新 `Cargo.lock`**：`release.yml` 用 `--locked`，而每个 workspace 成员把自己的版本写进 lock，不刷新就会以 `cannot update the lock file … because --locked was passed` 失败。
4. `--dry-run` 只打印 `git diff --stat` 后回滚；真跑才 commit + 打注记 tag + push。

## 明确不做

- 不自动决定版本号（没有「按 commit 推断 major/minor/patch」的逻辑）。
- 不放宽 `version-guard` —— 见 R141。
- 不处理**移动已存在的 tag**（要重指得手动 `git tag -f`；本卡对 `v0.1.3` 的处置见「结果」）。
- 不改 CI 结构与 `release.yml` 的任何 job。

## 已知取舍（Ruling）

- **R141** 保留严格护栏，不做「以 tag 为准、构建时把版本盖进去」：产物名（`…-0.1.2-…`）与二进制自称的版本必须一致，`v0.1.2` 已经漂过一次；代价是每次发布要先 bump 版本，这件事由 `release.sh` 承担。
- **R142** 用独立脚本而不是 Makefile / npm script：本仓已有 `scripts/`（`real-run.sh`、`leak-check.sh`）的惯例，且这活儿要混用 `git`、`awk`、`cargo`；代价是多一个入口。
- **R143** `--dry-run` 用 `git checkout --` 回滚而不是在临时目录里跑：`Cargo.lock` 只能由 `cargo` 在真实仓库内生成；代价是干跑也要求工作树干净——而这本来就是发布的前置条件。

## 验收

- 四条错误路径的退出码：无参数 `2`、格式错 `2`、已是该版本 `1`、tag 已存在 `1`
- `--dry-run` 打印 diff，且回滚后 `git status` 干净、`Cargo.toml` 版本不变
- `Cargo.toml` 与 `Cargo.lock` 版本一致，且 `cargo check -p main-bin --locked` 通过（这正是 `release.yml` 用的模式）

## 结果（完成后填写，≤5 行）

- `Cargo.toml` 第 12 行 `0.1.0` → `0.1.3`（**1 行源码**）；`Cargo.lock` 5 个成员的版本同步（+5/−5，生成物）；新增 `scripts/release.sh`（`scripts/` 按约定不计预算）。
- 实测锁文件这条坑：bump 后先跑 `cargo check -p main-bin --locked` → 报 `cannot update the lock file … because --locked was passed`；`cargo update -w --offline` 刷新后 → `Finished dev profile`。
- 四条错误路径实测：无参数 `exit=2`（打印 usage）、`abc` `exit=2`、`0.1.3` `exit=1`（already at）、`0.1.2` `exit=1`（tag 已存在）；另外「工作树不干净」也实测触发过一次（未提交的 `release.sh`）。
- 预算：**1 行源码 / 1 个源文件**。
- 偏差：`v0.1.3` 原本指向 `c6a67dc`（树里是 `0.1.0`），必须重指到一个版本为 `0.1.3` 的提交才能发布 —— 处置见下。

