# T130 — 发布流水线为什么从 2279ce7 之后就没跑过：`releases: write` 让整份 release.yml 被拒收

状态: ✅　依赖: T113,T128　验证: 手动（用 GitHub 的 workflow schema 本机校验，正/反两个方向都跑过）　类型: CI 配置

## 症状（误导人用的那种）

从 `2279ce7`（最后一次成功建出 Release）之后，tag 触发的构建**一次都没有跑过**：

| run | 触发 | 结论 |
|---|---|---|
| `39082557243` / `39082570174` | 3450c4b 的 push / tag 重建 | completed **failure**，`jobs` 列表 `total_count 0` |
| `39084170776` | 0ad38ed 的 push | 同上 |
| 特征 | | `name` 显示成 `.github/workflows/release.yml`（不是 `name: Release Build`）、`created_at == updated_at`（秒失败）、`head_branch=master`、API 里**没有任何错误文本** |

而 `ci.yml` 一直正常（`39084171592` CI = success ⇒ T129 的测试修复已被 CI 认可）。

## 根因

T128 给 `release` job 的 `permissions` 加了一行 `releases: write`。**GitHub 的 workflow schema 对 `permissions` 是 `additionalProperties: false`，合法 scope 列表里没有 `releases`**（`actions / artifact-metadata / attestations / checks / code-quality / contents / deployments / discussions / id-token / issues / models / packages / pages / pull-requests / repository-projects / security-events / statuses / vulnerability-alerts / copilot-requests`）。

一份被拒收的 workflow 不会报错给你看：GitHub 只留下上面那张表里的痕迹，并且**任何** push 事件都会生成一个 0 job 的失败 run（所以连「tags-only 的 workflow 不该被 branch push 触发」这条都不成立了 —— 这正是我发现异常的入口）。

`ruby -ryaml` 完全解析得过 —— 这就是为什么 T128 的「YAML 校验」门禁放过了它。

## 改法

1. `release.yml`：删掉 `releases: write`，只留 `contents: write`（建 Release、上传 asset、删旧 asset 都归它管）。
2. 新增 `scripts/check-workflows.sh`（离线、只依赖宿主 ruby）：解析 `.github/workflows/*.yml` 与
   `.github/actions/*/action.yml`，查两件事 —— `permissions` 的 scope 必须在白名单里、同一层不得有重复 key。
3. `ci.yml` 新增第一个 job `workflow 自检`，跑上面那个脚本。这样「文件被 GitHub 拒收」这类故障
   至少有一个能读的地方，而不是靠人去猜 0-job run 是什么意思。

## 只读白名单（最多 3 个源文件）

- `.github/workflows/release.yml` —— 改动前的 `release` job（`permissions` 块）
- `.github/workflows/ci.yml` —— 改动前的 `jobs:` 开头与 `permissions` 块
- `docs/tasks/T128-release-asset-idempotency.md` —— 引入这行权限的那张卡

## 必须覆盖

1. `release.yml` 用 GitHub 自己的 schema 校验通过（`json.schemastore.org/github-workflow.json`，0 error）。
2. 门禁脚本**反向**能抓到那份坏文件：对 `git show HEAD:.github/workflows/release.yml` 报
   `ERROR ... permissions scope 非法 -> releases`、exit 1；对修好的工作区 exit 0。
3. `ci.yml` 仍然 schema OK（新 job 不引入新的非法键）。
4. 重打 tag 后 Release Build 真的跑起来（判据在 CI 上，见验收）。

## 明确不做

- 不改 T128 的清空 asset 逻辑本身（它是对的，只是权限写错了）
- 不把完整 schema 校验（联网 + jsonschema）塞进 CI —— 本机一次性跑就够，CI 里只跑离线那两条
- 不动 `build` job 的构建/打包步骤

## 已知取舍（Ruling）

- **R172**：workflow 文件的合法性要自己验。GitHub 拒收一份 workflow 时**不提供任何错误文本**，
  外部痕迹只有：`name` 退化成文件路径、`jobs` 为空、`created_at == updated_at`、连 tags-only 的
  workflow 也会被 branch push 生成一个 run。这条故障的唯一体现是「tag 触发的构建凭空消失」，
  所以把「这份文件合法吗」做成 CI 第一步 + 本机门禁。（完整校验的入口：
  `curl -sL https://json.schemastore.org/github-workflow.json`，注意 YAML 1.1 会把 `on:` 读成 `true`，
  转 JSON 时要改回 `"on"`，否则会误报。）
- **R173**：`permissions` 里没有 `releases` scope —— 建/删 Release 与 asset 一律走 `contents: write`。
  代价：`contents: write` 比想象中的「只给发布权限」宽得多（能写仓库 ref）。在一个只有 release job
  用到它的 workflow 里可接受，但要记住它不是最小权限。

## 顺带修正 T128 的一条前提

「删 tag 会不会删掉 Release」两次观察互相矛盾：T126 那次删 tag 后旧 Release（id 415989164）和它的
asset 都还在（于是攒出 10 个 asset）；这次（3450c4b 之前那次删除）之后 `/releases/tags/v0.1.5` 直接 **404**。
⇒ 结论：这条行为不可依赖，T128 的「发布前显式清空旧 asset」是对的写法；v0.1.5 的 Release 现在不存在，
重打 tag 会新建一个。

## 验收

- 本机：`SCHEMA OK`（release.yml、ci.yml 各 0 error）；门禁脚本正向 exit 0、反向 exit 1 并指名 `releases`
- CI：下一次 push 应当**不再**出现 0-job 的 release.yml run，且 `workflow 自检` job 绿
- Release：重打 tag 后 `build` 五个 job + `publish release` 都跑，Release 上恰好 5 个 asset

## 结果（完成后填写，≤5 行）

- `release.yml` 去掉 `releases: write`；`ci.yml` 加 `workflow 自检` job；新增 `scripts/check-workflows.sh`。
- 本机门禁：正向 `workflows OK`（exit 0）；反向对 HEAD 那份文件报 `permissions scope 非法 -> releases`（exit 1）。
- schema 校验：修好的 release.yml 与 ci.yml 都 `SCHEMA OK`（0 error）；坏的那份 1 error（在 `jobs/release`）。
- 判据在 CI/Release 上：待 push + 重打 tag 后核对。
