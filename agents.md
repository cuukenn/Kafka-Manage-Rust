# agents.md — Kafka-Manage-Rust（rs-kafka-ui-client）

供编码代理（opencode 等）在修改本项目时参考的常驻约定。

项目一句话：跨平台 Kafka 可视化客户端（Rust 业务 + 系统 WebView UI，事件驱动 NIO/Tokio）。

## 先读项目 skill

本仓库约定已提取为项目 skill：`.opencode/skills/rs-kafka-ui-client/`（入口 `SKILL.md`）。
**改代码前按需读对应模块**：architecture / workspace / driver-abstraction / protocol / conventions / build-test / toolchain。

## 硬约束（必守）

- 单次改动最多改 **3 个文件**、新增 **≤250 行源码**；超出即拆成带字母后缀的多张卡（先例：T074a/b/c、T082a/b/c）。
- 只有**源码**行（Rust / TS / Vue）计入预算；`docs/`、`*.md`、`.github/`、`scripts/`、`frontend/dist/`、`Cargo.lock` 不计入。
- **不要跑 `cargo fmt`**：本仓无 `rustfmt.toml`、代码非 rustfmt-clean；按手写风格对齐（4 空格缩进、约 100 列）。
- 每个任务一张卡 `docs/tasks/Txxx-*.md`：完成后把 `docs/TASKS.md` 状态列置 ✅、在卡尾补 ≤5 行「结果」；含 `手动` / `real` 的卡另在 `docs/tasks/STAGE4-manual-checklist.md` 追加一条。

## 验证门禁

见 skill 的 `build-test` 模块。本机首次使用需 `source "$HOME/.cargo/env"`；`--features real` 自 T119 起走 `ssl-vendored`（OpenSSL 从源码静态编进二进制），**不再需要本机 openssl@3**；`./scripts/real-run.sh` 仍可用，但它钉的 `OPENSSL_DIR` 已被 vendored 忽略（首次编译多花 2~3 分钟编 OpenSSL）。
