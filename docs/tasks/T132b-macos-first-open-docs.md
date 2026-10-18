# T132b — 把 macOS「首次打开那一步」和签名自检写进文档

状态: ✅　依赖: T132a　验证: 手动（文本核对 + checklist 第 37 条逐句可执行）　类型: 文档

## 目标

T132a 修好了 `.app` 的签名，但「为什么以前是已损坏」「我该怎样确认」只存在于提交信息里。
把可执行的判据写进用户会看的两处：`docs/tasks/STAGE4-manual-checklist.md` 第 37 条与 `docs/development.md`
的发布节。

## 改了什么

- **checklist 第 37 条**（`T119 + T126 + T127 + T132a`）：加两步可执行判据 ——
  先 `codesign --verify --deep --strict --verbose=2 <app>` 应得 `valid on disk` +
  `satisfies its Designated Requirement`，且**不该**再出现
  `code has no resources but signature indicates they must be present`（「已损坏」的判据）；
  再双击打开，首次可能仍需右键→打开或 `xattr -dr com.apple.quarantine <app>`。
- **`docs/development.md` 发布节**：档案名那句补上「ad-hoc 签名（`codesign --force --deep --sign -`，
  不需要证书；T132a）」。

## 明确不做

- 不写证书 / 公证 / `.dmg` 的任何步骤（用户明确说不用管）
- 不在文档里写「双击一定成功」—— ad-hoc 签名消掉的是硬失败「已损坏」，首次打开仍可能要多一步

## 验收

- 第 37 条的每一条命令都能照着敲，且断言是可观察的（`valid on disk` / 不该出现的字符串 / `otool -L`）
- `docs/development.md` 的描述与 `release.yml` 里实际执行的 `codesign` 一致

## 结果（完成后填写，≤5 行）

- 第 37 条改成四段：验签名 → 双击 → 不装 `openssl@3` 也要能起 + `otool -L` → 另外两个平台的形状。
- `docs/development.md` 那一行补了 ad-hoc 签名与首次打开的说明。
- 共 2 个文件、`1/1` 与 `1/1` 行，**0 行源码**。

## 结果（完成后填写，≤5 行）

- 用户 2026-10-18 按第 37 条实测通过：「产物正常、验证已通过」——签名自检那一步的判据写对了。
- 遗留（有意不做）：公证 / 真证书签名，见 T133 的「明确不做」。
