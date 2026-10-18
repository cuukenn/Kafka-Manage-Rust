# T133 — `v0.1.5` 定稿验收：tag → 五平台 → Release 整条链

状态: ✅　依赖: T111–T132b　验证: 用户手动（原话「产物正常、验证已通过」）+ Actions / Release API　类型: 验收

## 目标

用户从 v0.1.1 一路反馈到 v0.1.5，要的东西一句话：「打 tag、触发、各个环境编译产物上传 release 资源」，
且产物**能直接运行**、命名有规范（名称 版本号 架构 格式）。本卡记录这条链的定稿形态与用户的终判。

## 定稿形态（实测，不是推断）

- `git tag -a v0.1.5 && git push origin v0.1.5` → Release Build run **`39093061289`**，
  `windows-x64` / `linux-x64` / `linux-arm64` / `macos-arm64` / `macos-x64` / `publish release` **六 job 全绿**。
- Release 上**恰好 5 个** asset，命名 `名称_版本.平台_架构.格式`：

```
rs-kafka-ui-client_0.1.5.linux_x64.tar.gz     4 797 276
rs-kafka-ui-client_0.1.5.linux_arm64.tar.gz   5 084 089
rs-kafka-ui-client_0.1.5.macos_x64.tar.gz     4 602 977
rs-kafka-ui-client_0.1.5.macos_arm64.tar.gz   4 649 088
rs-kafka-ui-client_0.1.5.windows_x64.zip      4 455 599
```

- 包内形态：macOS 是 ad-hoc 签名的 `.app`（`Contents/{Info.plist,_CodeSignature,MacOS/<名称>_<版本>}`）、
  Windows 是单个 `.exe`（OpenSSL 静态编进二进制，不带 DLL）、Linux 是带执行位的裸文件。
- CI run **`39093029727`** 五 job 全绿，含 T130 新增的「workflow 自检」。
- 该 tag 重复打是幂等的：清理步先删整个旧 Release，所以 Release 上永远是这 5 个，不再累积。

## 六轮迭代（每一轮 CI 都是绿的，坏的是产物形态）

| 轮 | 用户看到的症状 | 根因 | 卡 |
|----|----------------|------|----|
| 1 | Windows 平台段写成 `x64`（`x64_x64.zip`） | `msvc-dev-cmd` 写的 `Platform=x64` 顶掉 job 的 `PLATFORM`（Windows 环境变量不分大小写） | T127 |
| 2 | macOS 解压出来没有格式段、双击打不开 | 裸二进制不是 Finder 认的可执行格式 | T127 |
| 3 | 重打 tag 后 Release 攒了 11 个 asset | 删 tag 不删 Release，GitHub 会把同一 tag 的 Release 复活；清理步又因 `GET /releases/tags/{tag}` 短暂 404 而静默跳过 | T131 |
| 4 | macOS 报「已损坏，无法执行」 | 可执行文件带 linker 的 ad-hoc 签名、bundle 自己从未被签 | T132a |
| 5 | （阻止发布）release.yml 整份被 GitHub 拒收、0 个 job | `permissions` 里写了不存在的 `releases` scope（T128 埋的） | T130 |
| 6 | 命名从 target 三元组改成 `平台_架构` | 用户要「一眼看懂的平台+架构」 | T126 |

## 用户验收（逐字）

「产物正常、验证已通过、进行收尾」

## 已知取舍（Ruling）

- **R177**：**CI 绿 ≠ 产物可用**。上面六轮里 CI 每轮都是绿的，坏的正是产物形态本身
  （重复 changelog、DLL 依赖、`x64_x64`、未签名的 `.app`、11 个 asset）。CI 能证明的只是
  「结构正确 + 签名自洽 + 上传成功」；「下载下来能不能跑」这一关只能由人跑一次。
  代价：每个发布形态的改动都要占用用户一次手动验收（本卡是这一轮的最后一次）。

## 明确不做

- 代码签名 / 公证（需要 Apple 开发者证书；用户明确「证书等不需要管」）——ad-hoc 是没证书时的上限
- 不动版本号：`v0.1.5` 就是定稿，后续功能改动另开版本

## 结果（完成后填写，≤5 行）

- 用户终判：「产物正常、验证已通过、进行收尾」—— 清单第 37 条通过。
- 六 job + `publish release` 全绿（`39093061289`），Release 恰好 5 个 asset，命名与包内形态均符合定稿。
- 六轮缺陷链全部闭环：T126 → T127 → T130 → T131 → T132a → T132b。
- 遗留（有意不做）：签名/公证。R177：CI 绿不等于产物可用，终判是人跑一次。
