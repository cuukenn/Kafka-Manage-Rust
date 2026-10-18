# T132a — macOS 的 `.app` 在 CI 里做 ad-hoc 签名（治「已损坏，无法执行」）

状态: ✅　依赖: T127　验证: 手动（本机复现诊断 + 真跑签名的 staging/tar 两步 + 负路径取真实退出码）　类型: CI 配置

## 用户报告

「1、macos产物执行以损坏、无法执行。」「2、证书等不需要管。」—— 即：excluded 掉证书/公证，
但仍要解决双击打不开。

## 根因（本机复现，不靠猜）

用同一个 release 二进制在本机搭出与 CI 一样的 `.app`，逐项量：

| 检查 | 结果 |
|---|---|
| `lipo -archs` 可执行文件 | `arm64` |
| `codesign -dvvv <可执行文件>` | `flags=0x20002(adhoc,linker-signed)`、`Signature=adhoc`（cargo/ld 在 arm64 上自动签的） |
| `codesign --verify --deep --strict <.app>` | **`code has no resources but signature indicates they must be present`** |
| `spctl -a -t exec -vvv <.app>`（干净） | 同一句错 |
| 加隔离属性后再 `spctl` | 仍是同一句错 |
| `codesign --force --deep --sign - <.app>` 之后 | `valid on disk` + `satisfies its Designated Requirement` |

⇒ 可执行文件里的 linker ad-hoc 签名**声明自己属于一个 bundle**，而 bundle 自己从来没被签过名。
这个不一致状态就是 macOS 弹「已损坏，无法打开。你应该将它移到废纸篓。」的判据 —— 与隔离属性无关
（干净 bundle 也一样报）。

## 改法（`.github/workflows/release.yml`，「准备产物文件」步）

写完 `Info.plist` 之后、`esac/else` 之前插入两行：

```bash
codesign --force --deep --sign - "staging/$BUNDLE"
codesign --verify --deep --strict --verbose=2 "staging/$BUNDLE"
```

- `--sign -` 是 ad-hoc，**不需要任何证书**。
- 第二行是门禁：GitHub 的 bash 是 `-eo pipefail`，verify 失败就让这一步红 —— 而不是让用户
  下到一个打不开的包（这正是「已损坏」这次溜到用户手里的原因：CI 全绿、产物却是坏的）。
- 签名必须在 tar **之前**（`_CodeSignature/CodeResources` 要进包）。

## 本机验证

从 YAML 抽出「准备产物文件 / 打包（其他平台 · tar.gz）」两步按 macos 分支真跑：

- 正向：`staging/…app: valid on disk` / `satisfies its Designated Requirement`。
- 负向（把 `codesign --force` 那行删掉）：`code has no resources but signature indicates they must be present`，
  在 `bash -eo pipefail` 下**真实退出码 = 1** ⇒ 门禁确实会红。
- 往返：tar 出来的包里含 `Contents/_CodeSignature/CodeResources`；解压后 `codesign --verify --deep --strict`
  仍 `valid on disk`。
- `./scripts/check-workflows.sh` → `workflows OK`（`release.yml: jobs=build,release`，15 步不变）。

## 已知取舍（Ruling）

- **R176**：没有证书时，ad-hoc 签名是能做到的上限。它消掉「已损坏」（这个是**硬失败**，右键→打开
  也绕不过），但浏览器下载解压带的隔离属性还会让首次打开多一步（右键→打开，或
  `xattr -dr com.apple.quarantine <app>`）。`spctl -a` 对 ad-hoc 签名仍然 `rejected` —— 这是预期，
  Gatekeeper 的评估只对**带隔离属性**的包生效，所以本机/解压后清掉隔离属性的场景能直接双击。
- 不做：证书、公证（用户明确说不用管）；`.dmg` 打包（不解决签名问题，只多一层）。

## 验收

- 本机三条（正向 / 负向退出码 / tar 往返）全部符合预期
- 最终判据：下一次 tag 的 Release 里，macOS 包解压出的 `.app` 能**双击打开**（首次可需右键→打开）

## 后续（不在本卡）

- **T132b**：checklist 第 37 条与 `docs/development.md` 写清「首次打开那一步」（文档与配置分开提交，
  避免一次改动超过 3 个文件）

## 结果（完成后填写，≤5 行）

- `release.yml` 加两行（ad-hoc 签名 + verify 门禁），本机双向验证通过，`workflows OK`。
- 卡住过两次小坑：本机复跑 tar 那一步要把上一步写进 `GITHUB_ENV` 的 `BUNDLE` 拿出来 export
  （CI 里跨步自动传递，本机不会）；`GITHUB_ENV` 未设时 `echo >>` 会报 `No such file or directory`。
- **实测验收**（tag v0.1.5 重打在 `2598d39`）：Release Build run `39093061289` 六个 job 全绿；
  Release 上恰好 5 个 asset，macOS 两个档案体积已经带上 `_CodeSignature`（`macos_arm64` 4649088、
  `macos_x64` 4602977，分别比签名前多 1 022 / 87 098 B）；CI run `39093029727` 五 job 全绿
  （含新的「workflow 自检」）。
- 终判仍在用户手上：下载解压后双击 `.app`。
