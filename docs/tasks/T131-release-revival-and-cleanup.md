# T131 — 旧 asset 为什么清不掉：GitHub 会把同一 tag 的 Release「复活」

状态: ✅　依赖: T113,T126,T128,T130　验证: 手动（本机把该步脚本抽出来打真 API 做 dry-run + 负路径）　类型: CI 配置

## 现象

`be77dd0` 触发的 Release Build（run `39089989979`）六个 job 全绿，T127/T130 的修复都生效了
（Windows 档案名终于是 `…_0.1.5.windows_x64.zip`），但 Release v0.1.5 上有 **11 个 asset**：

```
5 个  2026-10-17T18:30:57Z   旧 target 三元组命名（T119 时代）
1 个  2026-10-17T21:34:38Z   rs-kafka-ui-client_0.1.5.x64_x64.zip（T126 那次的名错产物）
5 个  2026-10-18T02:27:38Z   这次的新命名（对的）
```

Release **id 415989164 从头到尾没变**，但它的 `created_at` 现在是 `2026-10-18T02:24:38Z` ——
正好是我删 tag 又重建的那一刻（run 在 `02:24:51` 起跑）。⇒ **删 tag 之后 GitHub 把那个 Release 复活了**：
id 不变、`created_at` 变成 tag 重建的时刻、旧 assets 全部回来。

## 关键推论（T128 的清理步为什么没生效）

21:34 那批本来有 5 个（`linux_x64 / linux_arm64 / macos_x64 / macos_arm64 / x64_x64`），现在只剩 `x64_x64`。
消失那四个的名字与这次上传的**完全相同** —— 是 `action-gh-release` 的「同名 asset 先删后传」干的，
不是我的清理步（它若跑成，五个三元组命名的也该一起消失）。

清理步唯一的自洽解释：它当时 `GET /releases/tags/{tag}` 拿到 **404**，于是走了「release 还不存在，无需清理」
那条静默分支。删 tag 与重建 tag 之间 GitHub 的 Release 处于软删/复活状态，按 tag 查会短暂 404，
而 Release 本体和它的 assets 都还在 —— 这正是 T128 依赖的那个接口不可靠的地方。

## 改法（`release.yml` 的清理步，v2）

1. **找 Release 有兜底**：先 `GET /releases/tags/{tag}`；失败就 `GET /releases?per_page=100` 里按
   `tag_name` 匹配。两条路都写清楚来源（`by-tag` / `by-list`）。
2. **删整个 Release，不再逐个删 asset**：`DELETE /repos/{repo}/releases/{id}` —— 一次调用，
   脏 asset 连同 Release 一起没，`action-gh-release` 随后建一个全新的（新 id、恰好 5 个 asset）。
3. **每一步都留 annotation**（T120 的通道）：查到了哪个 release、几个 asset、名字列表；
   删除成功/失败（失败带 HTTP 码与响应前 300 字符）。静默跳过这条分支也发一条 notice。

## 只读白名单（最多 3 个源文件）

- `.github/workflows/release.yml` —— 改动前的清理步（T128 版）
- `docs/tasks/T128-release-asset-idempotency.md` —— 被本卡修正前提的那张卡
- `docs/tasks/T130-invalid-workflow-permissions.md` —— 同一批发布流水线缺陷的另一半

## 必须覆盖

1. 同一个 tag 反复重打，Release 上**恰好 5 个** asset（旧的、命名错的都不留）。
2. 清理步的结论在 annotations 里看得见，不再靠「asset 数量」反推。
3. 首次发布（没有 Release）时仍然安全跳过。
4. 清理失败不阻断发布（发一条 `::error::` 后继续）—— 产物比干净的名字更要紧，但失败必须可见。

## 明确不做

- 不删 tag、不删分支、不动历史提交
- 不做「保留旧 Release 只删 asset」—— 已证明该接口路径不可靠，且逐个删没有收益
- 不给 `action-gh-release` 加 `overwrite`（它的同名覆盖只覆盖同名，坏名字不会自己消失）

## 已知取舍（Ruling）

- **R174**：不能假设「删 tag 会删掉 Release」。GitHub 会把同一 tag 的 Release 复活（id 不变、
  `created_at` 变成 tag 重建时刻、旧 assets 全回来），而 `GET /releases/tags/{tag}` 在删/建之间会短暂 404。
  所以幂等要按列表找 Release、并删整个 Release。代价：Release 的 id 与 URL 每次重打都会变（旧下载链接失效）。
- **R175**：流水线里任何「静默跳过」的分支都必须留下 annotation 证据。T128 那次清理没跑，
  日志里一个字都看不到，我只能从 asset 的 `created_at` 反推 —— 这是 T120 那条教训第二次兑现。

## 验收

- `./scripts/check-workflows.sh` → `workflows OK`（T130 的门禁）
- 本机把该步脚本从 YAML 里抽出来、去掉鉴权头、把 DELETE 换成打印，打真 API：
  `::notice::found release 415989164 (by-tag) with 11 asset(s): …` + `DRY-RUN would DELETE release 415989164 (11 assets)`
- 负路径（带假 token）：`::notice::lookup by tag failed: HTTP 401` → `::error::list releases failed: HTTP 401`
  → `::notice::no release for v0.1.5 yet - nothing to clean`，脚本正常退出（不会把 job 弄红）
- 最终判据：重打 tag 后 Release v0.1.5 恰好 5 个 asset，且 annotations 里有清理步的 notice

## 结果（完成后填写，≤5 行）

- 清理步 v2 落地：by-tag 失败 ⇒ by-list 兜底；删整个 Release；结论全部走 annotation。
- 本机 dry-run 打真 API 认到 release 415989164 与 11 个 asset；假 token 的负路径也走通（不弄红 job）。
- 顺带纠正 T128 的前提：GitHub 会复活同一 tag 的 Release（见卡内时间线）。
- `.github/workflows/release.yml` 单文件改动 + 本卡，**0 行源码**。
- 重打 tag 后实测（`77504ab`）：Release Build run `39091136645` 六个 job 全绿，Release v0.1.5 上**恰好 5 个**
  asset（原来那 11 个脏的没了）；Windows 档案名带 `windows`；macOS 两个 4648066 / 4515879 B（内含 `.app`）。
  CI run `39091096753` 五 job 全绿，含新加的「workflow 自检」。
