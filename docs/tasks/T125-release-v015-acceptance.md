# T125 — v0.1.5 发布验收：五平台产物 + 单点发布 + 命名规范

状态: ✅　依赖: T119,T120,T121,T122,T123,T124　验证: Actions API + Release API　类型: 验收

## 目标

确认用户要的那一条端到端成立：**打 tag → 触发 → 各平台编译 → 产物上传 Release**，
并且产物满足 T119 定的两件事：命名 `名称-版本号-<target 三元组>.<格式>`、自包含。

## 五轮迭代（每一轮的失败点都被下一轮修掉）

| 轮 | run | 结论 | 失败点 → 修它的卡 |
|---|---|---|---|
| 1 | 39049237261 | 4 平台绿 / Windows 红 | `openssl-src` 在 MSVC 上找不到 nmake 环境 → **T120**（`ilammy/msvc-dev-cmd@v1` + annotation 通道） |
| 2 | 39053986277 | 同上 | rustc 用了 Git 的 MSYS `link.exe` → **T121**（`CARGO_TARGET_..._LINKER` 钉死 MSVC） |
| 3 | 39054672571 | 同上 | `perl ./Configure` 缺 `Locale::Maketext::Simple` → **T122**（装 Strawberry perl） |
| 3′ | 39055249047 | 同上，报错逐字未变 | PATH 前置被 Git Bash 盖掉 → **T123**（`OPENSSL_SRC_PERL` 显式指定） |
| 4 | 39055727993 | 同上 | `Could not create named generator Visual Studio 18 2026` → **T124**（`CMAKE_GENERATOR=NMake Makefiles`） |
| 5 | **39057524891** | **六 job 全 success** | — |

## 最终结果（Release API 实测）

```
id 415989164  name v0.1.5  published_at 2026-10-17T18:30:58Z  draft False  prerelease False
assets 5   body 88 chars   'Full Changelog' ×1   tar.gz 4 + zip 1

rs-kafka-ui-client-0.1.5-x86_64-unknown-linux-gnu.tar.gz     4 797 271
rs-kafka-ui-client-0.1.5-aarch64-unknown-linux-gnu.tar.gz    5 084 085
rs-kafka-ui-client-0.1.5-x86_64-pc-windows-msvc.zip          4 455 586
rs-kafka-ui-client-0.1.5-x86_64-apple-darwin.tar.gz          4 515 310
rs-kafka-ui-client-0.1.5-aarch64-apple-darwin.tar.gz         4 647 399
```

- 命名：五者唯一，架构栏是完整 target 三元组 ⇒ 不再有「macos 写 arm64、linux 写 aarch64」的同物两名。
- 单点发布（T113）成立：正文只有一份 changelog，没有 `already_exists` 竞态。
- 压缩后 4.4~5.1 MB / 个，与「OpenSSL 静态编进去、本机未压缩 10 MB」的量级相符。

## 还没闭环的（不在本卡范围）

- `ci.yml` 的 `ubuntu-22.04` 「测试」步仍红（与 release 无关，长期存在；本轮 T120 已给它加了 annotation 通道，下次能拿到正文）。
- macOS 产物**未签名/未公证**：`otool` 层面自包含了，但 Gatekeeper 的 quarantine 是另一件事，需要 Apple 证书。
- 「下载后能不能直接跑」的最终判据是人工的，登记在 `STAGE4-manual-checklist.md` 第 **37** 条。

## 已知取舍（Ruling）

- **R161**：验收只认 API 的结构事实（job 结论、asset 名单与大小、正文里 changelog 的次数），
  不去猜网页。本机 `github.com` 不通、job 日志匿名 404，API 是唯一可靠的取证面。

## 结果（完成后填写，≤5 行）

- `v0.1.5` 已发布：六 job 全绿、5 个产物、命名符合规范、正文不重复。
- 用户要的发布流程现在字面成立：`git tag -a v0.1.6 && git push origin v0.1.6` 就出产物。
- Windows 那条链（T120→T124）逐层闭环，每层都由上一轮的 annotation 证明生效。
- 遗留：`ci.yml` 的 ubuntu 测试红、macOS 未签名、产物可运行性待人工确认（清单第 37 条）。
