# T122 — Windows 上编 OpenSSL 的 perl：换成 core 模块齐全的那个

状态: ✅　依赖: T119,T120,T121　验证: 手动（run 39054672571 的 annotation 正文）　类型: CI 配置

## 目标

T121 把链接器钉对了（annotation 里留了证据：`MSVC link.exe pinned to ...\MSVC\14.51.36231\bin\Hostx64\x64\link.exe | PATH resolves link.exe to` 同一个）。
Windows 于是露出下一层失败 —— `openssl-src` 编 OpenSSL 时 `perl ./Configure` 直接挂：

```
Can't locate Locale/Maketext/Simple.pm in @INC (you may need to install the
Locale::Maketext::Simple module) (@INC entries checked: .../openssl-build/build/src/util/perl
/usr/lib/perl5/site_perl /usr/lib/perl5/core_perl /usr/share/perl5/core_perl ...)
Compilation failed in require at /usr/share/perl5/core_perl/IPC/Cmd.pm line 59.
cargo:warning=openssl-src: failed to build OpenSSL from source
cargo:warning=configuring OpenSSL build: 'perl' reported failure with exit code: 2
```

`@INC` 全是 `/usr/lib/perl5/...`、`/usr/share/perl5/...` ⇒ PATH 上的 perl 是 **Git for Windows 带的
MSYS 裁剪版**，core 模块被拆掉了。OpenSSL 的 `Configure` 依赖 `IPC::Cmd → Params::Check →
Locale::Maketext::Simple` 这条链，缺最后一个就整条崩。

OpenSSL 不能砍：`crates/kafka-driver/src/real/config.rs:41-42` 在 `security.protocol` 含 `ssl` 时
走 `apply_tls`，SASL/TLS 是产品功能。

## 要改的文件

- 修改 `.github/actions/real-deps/action.yml` —— 新增 Windows 步（复合动作从 1 步变 2 步）

## 必须覆盖

1. `C:\Strawberry\perl\bin\perl.exe` 不在就先 `choco install strawberryperl -y`；装完仍不在就 `throw`。
2. 把 `C:\Strawberry\c\bin`、`...\perl\site\bin`、`...\perl\bin` 写进 `GITHUB_PATH`（runner 会
   **前置**，所以它压过 Git 的 MSYS perl）。
3. 自检 `perl -MLocale::Maketext::Simple -e "exit 0"`：缺模块就当场红，并说明缺的是什么。
4. 留一条 `::notice::` 写清「钉住的 perl」与「当时 PATH 会选哪个 perl」—— 下次不用猜。
5. Linux 步逐字不动（它已经绿了四个平台里的两个）。

## 明确不做

- 不给 MSYS perl 补模块（`pacman -S perl-locale` 之类）——见 R155
- 不动 T121 的链接器钉法（它已被证明生效）
- 不在 Windows 退回「系统 OpenSSL + 随包发 DLL」：那会把 T119 解决的「产物不自包含」重新引回来
- 不砍 `ssl`/`ssl-vendored`（产品要 TLS）

## 已知取舍（Ruling）

- **R155**：换 Strawberry perl，而不是给 MSYS perl 补缺失模块。Git 带的是裁剪版，补上
  `Locale::Maketext::Simple` 只会暴露下一个缺失模块；换完整 perl 是一次性解决。
  代价：Windows job 多一次 choco 安装（约 1~2 分钟）。
- **R156**：把「perl 能不能用」做成 real-deps 里的显式自检。让失败停在能看懂的一层，
  而不是 `openssl-src: 'perl' reported failure with exit code: 2` 这种要人去猜 @INC 的正文。

## 验收

- `action.yml` 复合动作 2 步，`ci.yml` / `release.yml` 仍可解析（`build_steps=14`）
- 判据：Windows job 过 step「编译 release 二进制」，`publish release` 不再 skipped，
  `/releases/tags/v0.1.5` 出现 5 个产物

## 结果（完成后填写，≤5 行）

- `.github/actions/real-deps/action.yml`：+1 个 Windows 步（Strawberry + PATH 前置 + 模块自检 + notice）。
- YAML 验证：`action OK steps=2`、`ci OK`、`release OK build_steps=14`。**0 行源码**。
- 待 v0.1.5 重跑确认 Windows 是否过。
