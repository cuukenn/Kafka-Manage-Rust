# T118 — 撤掉 tag/版本护栏：打 tag 就该能发布

状态: ✅　依赖: T114　验证: 手动（YAML 解析 + `CARGO_PKG_VERSION` 取证）　类型: 配置

## 目标

要的发布流程只有一件事：**打 tag → 触发 → 各平台编译 → 产物上传 Release**。
T114 加的 `version-guard` 把它变成了「打 tag 前先记得改 `Cargo.toml`」，实际后果是
`v0.1.3` 与 `v0.1.4` 两次打 tag 都只在第一个 job 就红、五个 build job 全部跳过
（run 39011500119、39020365165），而用户要的产物一个都没出来。

而它拦住的「漂移」没有任何地方读：

```
grep -rn "CARGO_PKG_VERSION" --include="*.rs" --include="*.vue" --include="*.ts" .
→ 零命中
```

产物名来自 tag，`[workspace.package] version` 只是 crate 元数据。护栏在防一个没人观测的
不一致，代价却是每条发布路径上的手工步骤。本卡删掉它。

## 只读白名单（最多 3 个源文件）

- `.github/workflows/release.yml` —— 改动前全文（`version-guard` job 与 `build.needs`）
- `docs/tasks/T114-version-guard.md` —— 被本卡推翻的那条决策
- `docs/tasks/T113-release-concurrency.md` —— 单点发布的结构，本卡不动

## 要改的文件

- 修改 `.github/workflows/release.yml` —— 删 `version-guard` job 与 `build.needs`
- 修改 `docs/development.md` —— 「发布（打 tag）」一节不再声称需要先 bump 版本

## 必须覆盖

1. `version-guard` job 整体删除，`build` 的 `needs:` 一并去掉。
2. `jobs` 只剩 `build`（五平台矩阵）与 `release`（单点发布），其余结构（real-deps 复合作、
   `--features real --locked` 构建、打包、artifact 汇总）**逐字不动**。
3. `docs/development.md` 的发布节改成「只打 tag 就行」，并说明 `scripts/release.sh`
   从「前置条件」降级为「可选」（只为让 crate 元数据跟上 tag）。
4. `STAGE4-manual-checklist.md` 第 35 条原文断言「不一致时应在第一个 job 就红」，
   已成假命题，必须改写成新的可验证描述。

## 明确不做

- 不改成「保留护栏但降级为 warning」：会红却不影响产物的 job 只会让人习惯性无视红叉
- 不还原 T113 的单点发布（五个 job 各传一次 Release 会重现 `v0.1.0` 的正文重复 5 次）
- 不在构建期用 tag 覆盖 `Cargo.toml` 版本（要么放弃 `--locked`，要么在 runner 里刷锁；
  换来的只是没人读的元数据）
- 不删除 `scripts/release.sh`（见 R147）

## 已知取舍（Ruling）

- **R146**：删掉护栏，而不是让它变成非阻塞提醒。要么拦要么不拦——一个「红着但产物照样出」
  的 job 会训练人忽略红叉，比没有它更糟。代价：`Cargo.toml` 的版本会落后于 tag，成为纯
  元数据（已确认无人读它）。
- **R147**：保留 `scripts/release.sh`，但明确它**不是发布前置条件**。它仍能让想要版本号
  对齐的人一条命令做完 bump+刷锁+提交+打 tag。代价：仓库里留着一个「可以不做」的步骤，
  靠文档说清它为什么可跳过。

## 验收

- `release.yml` YAML 可解析，`jobs` = `build,release`，`build.needs` 为空
- `build` 仍 12 步、`release` 仍 3 步（除 `needs` 外无结构变化）
- `docs/development.md` 的发布节不再要求学生先 bump；第 35 条已改写
- 效果判据：打一个 tag（不碰 `Cargo.toml`）应当直接出产物

## 结果（完成后填写，≤5 行）

- `release.yml`：删掉 26 行 `version-guard` job 与 `build.needs`；`ruby -ryaml` →
  `jobs=build,release build_steps=12 release_steps=3 build_needs=nil`。
- `docs/development.md`：发布节重写为「打 tag 就行」，并记下护栏被删的原因（附 grep 取证）。
- 规划提交：`.github/` 与 `docs/`，**0 行源码**。
- 真正的验收：下次打 tag（不碰 `Cargo.toml`）应直接出 5 个产物。
