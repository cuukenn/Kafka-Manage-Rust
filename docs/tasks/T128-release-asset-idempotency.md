# T128 — 重打同一个 tag 要得到干净的产物（Release 的 asset 不幂等）

状态: ✅　依赖: T113,T126,T127　验证: 手动（YAML 解析 + 本机把清理脚本真跑一遍，DELETE 换成 print）　类型: CI 配置

## 问题（v0.1.5 实测）

用户的重试手段是「删 tag → 重打 tag」。但 **删 tag 并不会删掉 Release**：

| 事实 | 值 |
|---|---|
| Release id | `415989164`（tag 删除后仍是同一个） |
| Release created_at | `2026-10-17T18:30:57Z`（第一次发布） |
| 旧 5 个 asset created_at | `18:30:57Z` |
| 新 5 个 asset created_at | `21:34:38Z` |

⇒ 第二次发布是**往上追加**，Release 上攒了 10 个 asset：5 个旧命名（target 三元组）
+ 5 个新命名（其中一个是错的 `…_0.1.5.x64_x64.zip`）。下载方看到的是 10 个互相矛盾的文件。

## 改法

`release` job 里、`创建 Release` 之前加一步「清空旧 asset」：用 workflow 自带的
`GITHUB_TOKEN` 查 `releases/tags/<tag>`，把该 Release 的每个 asset `DELETE` 掉；
Release 不存在（首次发布，404）就跳过。`permissions` 补 `releases: write`。

## 只读白名单（最多 3 个源文件）

- `.github/workflows/release.yml` —— 改动前的 `release` job（4 步）
- `docs/tasks/T125-release-v015-acceptance.md` —— 记录了「单点发布」那次修复的卡
- `docs/tasks/T126-artifact-naming.md` —— 命名决策（本卡让它可以反复重打）

## 必须覆盖

1. 首次发布（Release 不存在 / 404）不报错、不跳过发布。
2. 重复发布：旧 asset 全部删除，Release 上只剩本次的 5 个。
3. 不删 Release 本身（body / tag 关联保持），只删 assets。
4. 不引入第三方 action，只用 `GITHUB_TOKEN` + REST。

## 明确不做

- 不在本机删（本地没有可用 token，`gh` 未安装）
- 不删 Release、不改 tag 关联、不动 `generate_release_notes`
- 不做「保留上一版产物」的归档（用户要的是干净的一次发布，不是历史留存）

## 已知取舍（Ruling）

- **R166**：**删 tag ≠ 删 Release**（实测：`git push origin :refs/tags/v0.1.5` 删掉了 tag，
  Release 与它的 assets 都活着）。所以「重打 tag 重试」这条路径的幂等性只能由 workflow 自己保证，
  不能指望 GitHub 的连带删除。
- **R167**：用 `GITHUB_TOKEN` 在流水线里删 asset，而不是让人去 UI 手动删。代价：多一次 REST 往返、
  且发布失败时旧产物已被清掉（本来就是要重跑的产物，可接受）。

## 验收

- `ruby -ryaml` 可解析；`release` job 4 步、`permissions` 含 `releases: write`
- 本机把该步脚本真跑一遍（DELETE→print、去掉鉴权头）：能列出当前 Release 的 10 个 asset 并给出 release id
- 最终判据：重打 v0.1.5 后，Release 上**恰好 5 个** asset，且命名与包内形状都对

## 结果（完成后填写，≤5 行）

- 本机 dry-run 输出：`would delete:` ×10 + `cleared 10 old asset(s) from release 415989164`
  ⇒ 脚本的 tag 查询、asset 循环、URL 拼接都对（DELETE 与鉴权头在 CI 里才生效）。
- `release` job：4 步（下载 → 列出 → 清空旧 asset → 创建 Release），`permissions` 加 `releases: write`。
- 单文件 `.github/workflows/release.yml`，**0 行源码**。
- 判据在重打 tag 之后：Release 恰好 5 个 asset。
