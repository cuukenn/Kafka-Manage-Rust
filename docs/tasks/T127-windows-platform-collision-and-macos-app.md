# T127 — Windows 的 `x64_x64`，与 macOS「没有格式、双击跑不了」

状态: ✅　依赖: T119,T120,T126　验证: 手动（YAML 解析 + 本机把 staging/tar 两步真跑一遍 + `plutil -lint`）　类型: CI 配置

## 两个缺陷（都来自 v0.1.5 的实测产物）

1. **`rs-kafka-ui-client_0.1.5.x64_x64.zip`** —— 平台段本该是 `windows`，结果也是 `x64`。
   原因：`ilammy/msvc-dev-cmd@v1` 会往环境写 `Platform=x64`，而 **Windows 环境变量不区分大小写**，
   把 job `env:` 里的 `PLATFORM=windows` 顶掉了。Linux/macOS 不受影响（那里 `PLATFORM` 没被别的东西占用）。
2. **macOS 解压出来是裸文件 `rs-kafka-ui-client_0.1.5`**（没有格式段）—— 用户原话
   「不带格式无法直接运行」，追问后确认「macos 一样」。Finder 认不出可执行格式 ⇒ 双击打不开。

## 改法

1. 不再读 `$PLATFORM`：需要平台名的地方一律写 `${{ matrix.platform }}`，由 runner 在脚本执行前
   替换掉 —— shell 根本看不到这个变量，也就没有撞名机会。job `env:` 里的 `PLATFORM` 一并删除。
2. macOS 产物包成 **`.app`**（Finder 能直接双击的格式）：
   `staging/<名称>_<版本>.app/Contents/{Info.plist, MacOS/<名称>_<版本>}`，tar 打的是这个目录。
   Linux 保持裸二进制（`./名字` 就是它的运行方式），Windows 保持 `.exe`。

## 只读白名单（最多 3 个源文件）

- `.github/workflows/release.yml` —— 改动前的「准备产物文件」与两个打包步
- `docs/tasks/T126-artifact-naming.md` —— 被本卡修正的命名决策（R162/R163 仍成立）
- `docs/tasks/T125-release-v015-acceptance.md` —— 记录着 `x64_x64` 那次实测的卡

## 要改的文件

- 修改 `.github/workflows/release.yml` —— 删 `env.PLATFORM`；「准备产物文件」改成按
  `${{ matrix.platform }}` 分支并生成 `.app`；两个打包步改用 `${{ matrix.platform }}` 并按
  `$BUNDLE` 决定 tar 的对象

## 必须覆盖

1. Windows 档案名的平台段是 `windows`，不是 `x64`。
2. macOS 档案解压出来是 `rs-kafka-ui-client_<ver>.app`，里面有 `Contents/Info.plist`
   与 `Contents/MacOS/rs-kafka-ui-client_<ver>`（可执行位保留）。
3. Linux 档案不变：解压出来是带执行位的裸文件。
4. 档案名仍是 `名称_版本.平台_架构.格式`（T126 的形态不动）。

## 明确不做

- 不做代码签名 / 公证（需要 Apple 开发者证书）。`.app` 解决的是「双击打不开」，
  不解决 Gatekeeper 的 quarantine —— 未签名的 `.app` 首次打开仍需右键→打开。
- 不给 Linux 做 AppImage（用户没要求，且会引入新的打包工具链）
- 不改 Windows/Linux 的包内布局

## 已知取舍（Ruling）

- **R164**：环境变量名字也算接口。`PLATFORM` 这种通用名在 Windows 上会被第三方 action 写的
  同名（不分大小写）变量顶掉，且**不报错**；一律用 `${{ matrix.platform }}` 让 runner 替换，
  把「谁的 PATH/环境赢」这种竞争从设计里删掉。
- **R165**：macOS 发 `.app` 而不是裸文件。代价：多一个 Info.plist 与目录层级，且未签名时
  Gatekeeper 仍会拦；收益：Finder/双击这条最常见的运行路径通了。

## 验收

- `ruby -ryaml` 可解析，`build` 仍 15 步
- 本机把「准备产物文件 + tar 打包」两步脚本按 macos / linux 两个分支真跑一遍：
  macos 出 `.app`（`tar -tvf` 里可执行位是 `rwxr-xr-x`；`plutil -lint` = OK），
  linux 出裸文件（`rwxr-xr-x`）
- 最终判据：下一次 tag 的 Release 里，Windows 档案名带 `windows`，macOS 档案解压出 `.app`

## 结果（完成后填写，≤5 行）

- 本机真跑两个分支的 staging+tar：macos → `rs-kafka-ui-client_0.1.5.app/Contents/{Info.plist,MacOS/…}`
  （`plutil -lint` OK，plist 解析出 CFBundleExecutable=`rs-kafka-ui-client_0.1.5`）；
  linux → 裸文件 `rs-kafka-ui-client_0.1.5`（`rwxr-xr-x`）。
- 中途踩到一次 YAML 破坏：heredoc 的 plist 写在第 1 列会终止 block scalar，缩进到块内 10 空格后正常。
- `.github/workflows/release.yml` 单文件改动，**0 行源码**。
- 真正的判据在 Release 上：待重跑后核对五个档案名与 macOS 包内形状。
