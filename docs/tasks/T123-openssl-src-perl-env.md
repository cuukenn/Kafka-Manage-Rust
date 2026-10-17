# T123 — 给 openssl-src 一个明确的 perl，而不是跟 PATH 赌

状态: ✅　依赖: T119,T122　验证: 手动（annotation 全量取证 + `openssl-src` 源码）　类型: CI 配置

## 目标

T122 把 Strawberry perl 排到了 PATH 最前，v0.1.5 第三轮（run `39055249047`）却报**一模一样**的错。
把那个 check run 的 12 条 annotation 全量拉下来看（`117461789074`）：

- `[notice] perl pinned to C:\Strawberry\perl\bin\perl.exe | PATH currently resolves perl to`
  —— **同一个路径** ⇒ 我的步确实跑了，Strawberry 确实排在 PATH 最前。
- `[failure] Can't locate Locale/Maketext/Simple.pm in @INC … /usr/share/perl5/core_perl/…`
  —— 真正执行 `./Configure` 的仍然是 MSYS perl。
- job 的 step 3「Run ./.github/actions/real-deps」= success（复合动作的内部步不单独计数，
  所以步号从 11 变成 11 而不是 12 —— 不能靠步号判断内部步跑没跑）。

⇒ **PATH 顺序不是杠杆**。build 步跑在 `bash` 下，Git Bash 会把自己的 `/usr/bin` 再插到最前，
`GITHUB_PATH` 前置的东西被盖掉。

## 上游依据

`openssl-src src/lib.rs:158`：

```rust
env::var("OPENSSL_SRC_PERL").unwrap_or(env::var("PERL").unwrap_or("perl".to_string()));
```

上游本来就留了「指定用哪个 perl」的口子。给它绝对路径，shell 的 PATH 游戏就不再参与。

## 要改的文件

- `.github/actions/real-deps/action.yml` —— Windows 步多写一行
  `"OPENSSL_SRC_PERL=$straw" → $GITHUB_ENV`；notice 改成报 `OPENSSL_SRC_PERL=` 而不是
  「PATH 会解析到哪个」，因为前者才是真正被用的值。

## 明确不做

- 不把 build 步从 `bash` 换成 `pwsh`（会牵动 T120 那套 `tee` + annotation 包装，非 Windows 平台也不需要）
- 不给 MSYS perl 补模块（R155 已裁）
- 不删 PATH 前置（无害，`nmake` 等仍可能靠它）

## 已知取舍（Ruling）

- **R157**：用上游留的显式入口（`OPENSSL_SRC_PERL`），不再在本仓调 PATH 顺序。上游既然给了
  「指定工具」的口子，PATH 层面的任何修法都等于在跟别人的 shell 初始化竞争，赢不了也不可持续。
- **R158**：annotation 要报「真正被用的那个值」，而不是「PATH 会解析到哪个」。上一轮的 notice
  证明的恰好是我没用上的那条杠杆，它误导了我一整轮。

## 验收

- Windows job 的 annotation 出现 `OPENSSL_SRC_PERL=C:\Strawberry\perl\bin\perl.exe`
- 不再出现 `Can't locate Locale/Maketext/Simple.pm`
- 若仍失败，失败点应换到 `nmake` / 编译阶段 —— 那说明 perl 这一层已经过了

## 结果（完成后填写，≤5 行）

待 v0.1.5 第四轮。
