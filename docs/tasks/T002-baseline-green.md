# T002 · 全仓基线跑绿 + 首次提交

**状态**: ✅完成 | **依赖**: T001 | **改动上限**: 3 文件 / 250 行

## 目标
确认现有 `kafka-core` + `kafka-driver`（Mock）代码在真编译下是绿的，形成可回滚基线。

## 只读白名单
- 只读编译/测试输出报错涉及的那 1~2 个文件

## 做法
1. `cargo test --workspace`（默认特性，不含 `real`）
2. `cargo clippy --workspace --all-targets -- -D warnings`（若告警过多，退化为不加 `-D warnings`，只记录数量）
3. 只修**编译错误 / 测试失败 / 明显告警**，不做任何功能改动、不做风格重构
4. 通过后：`git add -A`（确认 `.gitignore` 含 `target/`、`.codegraph/`）+ `git commit -m "chore: baseline (core + mock driver)"`

## 验收
- `cargo test --workspace` 全绿，贴出测试计数（各 crate 的 passed 数）
- `cargo clippy --workspace --all-targets` 输出（记录 warning 数量）
- `git log --oneline` 有 1 条提交

## 明确不做
- 不新增测试文件、不补 Mock 功能（那是 T010+）
- 不开 `real` 特性（librdkafka 需要 cmake，另阶段处理）

## 结果（完成后填写，≤5 行）
- `cargo test --workspace`（默认特性）EXIT=0，**108 passed / 0 failed**：
  `kafka-core` 5；`kafka-driver` 11；`kafka-app` 92 = bus 9 · driver_guard 15 · store_clusters 12 ·
  store_db 7 · store_settings 9 · svc_clusters 10 · svc_consume 7 · svc_groups 8 · svc_produce 7 · svc_topics 8。
  其余 target（`webview-bridge`、`main-bin`）0 tests。
- `cargo clippy --workspace --all-targets -- -D warnings` → **EXIT=0，零告警**（未退化：加了 `-D warnings`）。
  这是 T054（kafka-driver 4 条）+ T055（kafka-core 2 条）+ T059（kafka-app 3 条）清零后的结果。
- `.gitignore` 补了 `.codegraph/`（卡片要求）与 `*.rmeta`（仓库根有两个工具链探针残留 `libprobe2/3.rmeta`，
  未删除、只忽略，避免把别人的可疑文件卷进基线）。`target/` 原本已在。
- 首次提交包含 88 个文件：三个 crate 的源码与测试、`Cargo.lock`、`docs/`（PLAN + TASKS + 卡片）、
  `agents.md`、以及用户已有的 `.github/workflows/release.yml`（只在打 `v*` tag 时触发，不影响基线）。
- 本卡未做任何功能改动，只做了 clippy 清零（T054/T055/T059）与 `.gitignore`。