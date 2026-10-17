# T113 — release 发布改成「先传 artifact、再单点发布」

状态: ✅　依赖: T112　验证: 手动（YAML 解析 + 结构核对）　类型: 配置

## 目标

`release.yml` 的 `build` job 在五个 runner 上并行跑，**每个 job 各自调用一次
`softprops/action-gh-release`**：五个进程同时对一个还不存在的 release 做「创建 + 上传」，
于是出现两类竞态——先到的创建、后到的撞 `already_exists`（上传可能丢），而
`generate_release_notes: true` 被**五个 job 各追加一次**。

这不是推测：`v0.1.0` 的 release 正文里 `**Full Changelog**: …` 原样重复了 **5 次**，
正好等于 matrix 的行数。

本卡把发布拆成两个阶段：`build` 只管构建并**上传 artifact**（`contents: read`），
新增的 `release` job 汇总全部 artifact 后**只调用一次** `action-gh-release`
（`contents: write`）。产物内容、命名、五平台矩阵都不变。

## 只读白名单（最多 3 个源文件）

- `.github/workflows/release.yml` —— 改动前全文（并发写 release 的三处）
- `.github/workflows/ci.yml` —— 可参照的 job 权限写法
- `docs/tasks/T109-release-workflow-fixes.md` —— 本项目对 release 的结构约定

## 要改的文件

- 修改 `.github/workflows/release.yml` —— 顶部权限收紧、`build` 改为传 artifact、新增 `release` job

## 必须覆盖

1. 顶部 `permissions` 由 `contents: write` 收紧为 `contents: read`（最小权限）。
2. `build` 的最后一个 step 换成 `actions/upload-artifact@v4`（`if-no-files-found: error`），不再碰 release。
3. 新增 `release` job：`needs: build`、`permissions: contents: write`、`download-artifact`（`merge-multiple`）→ 列出产物 → **一次** `action-gh-release`（`files: dist/*`、`fail_on_unmatched_files: true`）。
4. `uses: softprops/action-gh-release` 全文只出现一次（另一处只是注释里提到名字）；五平台矩阵、产物命名、打包逻辑不变。

## 明确不做

- 不改五平台矩阵、产物命名与两段打包命令
- 不改标签触发方式（`on.push.tags: ['v*']` 保持）
- 不加 tag ↔ `Cargo.toml` 版本护栏（T109 已列的后续卡）
- 不改 action 大版本（Node 20 弃用告警另议）
- 不在本卡打 tag

## 已知取舍（Ruling）

- **R136** 用「build 传 artifact → 单个 release job 汇总发布」而不是给 action-gh-release 加重试/加锁：竞态的正解是让**只有一个写者**，重试只是把窗口缩小、正文仍可能被追加多次；代价是产物要经 artifact 中转（7 天保留期、多一次上传下载）。
- **R137** `generate_release_notes: true` 挪到唯一的 release job 里：自动正文仍要有，但它必须只生成一次；代价是正文不再由各平台 job 各自贡献（本来也不该）。
- **R138** 顶部 `permissions` 收紧为 `contents: read`，写权限只下放到 release job：build 的五个 job 其实只需要读仓库和上传 artifact，最小权限；代价是将来若给 build 加别的写入步骤要记得补权限。

## 验收

- `release.yml` YAML 可解析；jobs 恰好为 `build`、`release`
- 顶部 `permissions.contents == read`，release job 里为 `write`
- `uses: softprops/action-gh-release` 全文只有 1 处，且位于 `needs: build` 的 job 内
- build 的步序仍 12 步（最后一步由发布改为上传）

## 结果（完成后填写，≤5 行）

- `release.yml`（`git show --numstat`：+33/−3）：顶部权限 `contents: write` → **`contents: read`**；build 末步 `softprops/action-gh-release@v2` → **`actions/upload-artifact@v4`**（`name`/`path` 均为 `${{ env.ASSET }}`、`if-no-files-found: error`、`retention-days: 7`）；新增 job **`release`**（`needs: build`、`runs-on: ubuntu-22.04`、`permissions: contents: write`）＝ download-artifact@v4（`path: dist`、`merge-multiple: true`）→ `ls -lh dist` → action-gh-release@v2（`files: dist/*`、`generate_release_notes: true`、`fail_on_unmatched_files: true`）。
- `ruby -ryaml`：YAML OK；`jobs=build,release`；顶部 `permissions=read`、release job `permissions=write`、`needs=build`；build steps=12、release steps=3；`grep -c 'uses: softprops/action-gh-release'` = **1**（裸 `grep -c action-gh-release` 是 2，第二处是 T113 自己的注释文字）。
- 效果：五个平台**只会创建一次 release**，`already_exists` 与正文被追加 5 次的问题同时消失；产物名与内容一字未改。
- 诚实备注：仍是静态校验——本机无法执行 Actions，真实验证等下一次打 tag（`STAGE4-manual-checklist.md` 第 34 条）。
