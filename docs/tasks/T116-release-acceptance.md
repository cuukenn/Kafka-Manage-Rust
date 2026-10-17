# T116 — release 端到端验收（v0.1.3）

状态: ✅　依赖: T111,T113,T114　验证: 手动（Actions API + Releases API）　类型: 文档

## 目标

T111/T113/T114 三张卡的承诺都写着「等下一次打 tag 才算验收」。`v0.1.3` 已打出并跑完，
本卡把验收结果钉下来，并落定 `STAGE4-manual-checklist.md` 第 32/34/35 条。

## 只读白名单（最多 3 个源文件）

- `.github/workflows/release.yml` —— 本次运行的 job 与步骤
- `Cargo.toml` —— `[workspace.package] version`
- `docs/tasks/T115-release-version-bump.md` —— 这次 tag 重指的出处

## 要改的文件

- 新增本卡，并补 `docs/TASKS.md` 与 `docs/tasks/STAGE4-manual-checklist.md`（全为文档，无源码）

## 必须覆盖

1. run `39011872955`（v0.1.3 @ `5ff7a10`）**7/7 job success**：version-guard → build×5 → release。
2. Release `v0.1.3` 五个产物齐全，命名 `rs-kafka-ui-client-0.1.3-<platform>-<arch>.<ext>`。
3. 正文 `**Full Changelog**` 只出现 **1 次**（对照 v0.1.2 = 5 次、v0.1.0 = 5 次）。
4. Linux 两个 job 绿 ⇒ T111 的 curl 头修复在 runner 上成立（v0.1.1 只有 2 个产物）。

## 明确不做

- 不改任何 workflow 与源码
- 不删除 v0.1.0 / v0.1.1 / v0.1.2 的历史 release
- 不再重指任何 tag

## 已知取舍（Ruling）

- **R144** 验收证据取 Actions/Releases API 与资产大小，**不下载产物本体**：本机 `github.com`
  不可达（`curl` 返回 `000`，只有 `api.github.com` 通）。DLL 一项因此改由 workflow 逻辑背书——
  打包步骤在 `libcrypto-*.dll` / `libssl-*.dll` 找不到时会 `throw`，而 `windows-x86_64` job 是绿的。
- **R145** 结果写进本卡与清单，**不回改** T111/T113/T114 的卡：那三张记的是「改动 + 当时无法验证」，
  回改会让历史失真。

## 验收

见「结果」表。

## 结果（完成后填写，≤5 行）

- **run `39011872955`（v0.1.3 @ `5ff7a10`）：7/7 job success** —— `tag matches Cargo.toml`、`macos-x86_64`、
  `macos-arm64`、`linux-x86_64`、`linux-aarch64`、`windows-x86_64`、`publish release`。这是本项目**首次**
  一个 release 全平台跑通（v0.1.0 无 real 驱动、v0.1.1 只剩 2 个产物、v0.1.2 正文重复 5 次）。
- 产物（Releases API，`draft=false`）：五个齐全——`linux-aarch64` 2 663 397 B、`linux-x86_64` 2 740 824 B、
  `macos-arm64` 2 503 243 B、`macos-x86_64` 2 671 164 B、`windows-x86_64.zip` 5 641 464 B；
  命名一律 `rs-kafka-ui-client-0.1.3-<platform>-<arch>.<ext>`。对照 v0.1.0 的 1.7 MB 量级 ⇒ 体积增幅与
  「真的链进了 librdkafka + OpenSSL」一致（T110）。
- 正文：**`**Full Changelog**` 恰好 1 次**（v0.1.2 与 v0.1.0 都是 5 次）⇒ T113 的单写者发布成立。
- 未能直接验证的一项（诚实备注）：Windows zip **内部**是否含 `libcrypto-*.dll` / `libssl-*.dll`。
  本机 `github.com` 不可达（`curl -sIL` → `000`，只有 `api.github.com` 通），产物本体下载不下来。
  只由 workflow 逻辑背书：打包步骤缺 DLL 即 `throw`，而该 job 是绿的（R144）。
- 因此 `STAGE4-manual-checklist.md` 第 32/34/35 条可视为**已由本次运行兑现**，仅 DLL 一项仍留人工确认。
