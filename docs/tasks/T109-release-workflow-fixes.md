# T109 — 修正 release workflow 的可执行性缺陷

状态: ✅　依赖: T092　验证: 手动（`ruby -ryaml` 解析 + runner/tar 行为外部取证）　类型: 配置

## 目标

`release.yml` 从未被执行过（`git tag` 为空），静态校验却发现四处会让它**跑不起来**或**产出错格式**的问题：Linux 缺系统依赖、两个 macOS runner label 已退役或即将退役、Windows 那个 `.zip` 其实不是 zip。本卡只修可执行性，不动结构。

## 只读白名单（最多 3 个源文件）

- `.github/workflows/ci.yml`（第 37-45 行的 Linux 依赖清单、第 33 行的 R98 对齐）
- `docs/tasks/T092-ci-matrix-release.md`（原设计意图）
- `Cargo.toml`（`[profile.release]` 与 workspace 版本）

## 要改的文件

- `.github/workflows/release.yml`
- `.github/workflows/ci.yml`

## 必须覆盖

1. **Linux 系统依赖**：两个 Linux job 必须装 `libwebkit2gtk-4.1-dev` / `libgtk-3-dev` / `librsvg2-dev` / `libssl-dev` / `pkg-config` / `cmake` / `build-essential`（与 ci.yml 同一份清单，否则 `tao`/`wry` 链接失败）。
2. **macOS label 在支持期内**：`macos-13`（2025-12-04 退役）→ `macos-15-intel`；`macos-14`（2026-11-02 退役）→ `macos-15`；ci.yml 同一 label 一并换，保住 R98 的「CI 与 release 构建同一镜像」。
3. **Windows 产物是真 zip**：不能用 `tar -a -cf *.zip`（GNU tar 的后缀表里没有 `.zip`）。
4. **release 正文非空**、构建与 CI 一致（`--locked`）。

## 明确不做

- 不改「5 个 job 并发创建同一个 release」的结构（artifact + 汇总 job）——那是改进，不是可执行性。
- 不加 tag 与 `Cargo.toml` 版本的护栏。
- 不动矩阵的产物命名、action 版本、`ext` 字段。

## 已知取舍（Ruling）

- **R124** 用 `Compress-Archive` 而不是 `7z a -tzip`：`pwsh` 在 windows runner 上必然存在，少一个外部依赖；代价是 PS 5.1 的 2 GB 归档上限（本产物约 10 MB，无关）。
- **R125** macOS label 显式钉 `macos-15` / `macos-15-intel` 而非 `macos-latest`：R98 的意图是 CI 与 release 构建同一镜像，`-latest` 会在 GitHub 改指向时同时改动两边且失去可预测性；代价是将来仍需人工跟进退役（正如本次 macos-13/14）。
- **R126** `--locked` 只加在 release 的 `cargo build`：与 ci.yml 的 `cargo test --workspace --locked` 对齐，保证发版解析出的依赖版本与 CI 测过的一致。

## 验收

- 两个 workflow 都能被 `YAML.load_file` 解析
- 矩阵里不再有 `macos-13` / `macos-14`（注释提及不算）
- 不再出现 `tar -a -cf *.zip`
- `generate_release_notes: true` 与 `--locked` 就位

## 结果（完成后填写，≤5 行）

- `release.yml`：补 Linux 系统依赖步骤（与 ci.yml 同一份清单）、`macos-13`→`macos-15-intel`、`macos-14`→`macos-15`、
  Windows 打包拆成 `Compress-Archive`（pwsh）与 `tar -czf` 两步、加 `--locked` 与 `generate_release_notes: true`；
  `ci.yml` 的 `macos-14`→`macos-15` 同步，保住 R98 的对齐。
- 验证：两个文件 `ruby -ryaml` 解析通过；`.github/` 内除注释外无 `macos-13`/`macos-14`；无 `tar -a -cf`。
- 诚实备注：workflow **仍未被真正执行过**（`git tag` 为空，本机也跑不了托管 runner）；结论是静态校验 + 外部事实（runner 生命周期、GNU tar 后缀表）。
- 未修（留后续）：5 个 job 并发建 release 的 `already_exists` 竞态、tag↔`Cargo.toml` 版本护栏。
