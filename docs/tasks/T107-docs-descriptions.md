# T107 — 文档描述优化：agents.md / 项目 skill / README

状态: ✅　依赖: 无　验证: 文档（人工通读 + 与源码逐条核对）　类型: 文档

## 目标

三份供人和代理阅读的说明与代码脱节了：`agents.md` 只有 10 行，没写预算口径也没写验证门禁；项目 skill 的 8 个 md 停在早前的形态（缺 `main-bin` 入口层、缺 `tests/`+`benches/`+`examples/`、`toolchain.md` 还写 rustc 1.99.0）；`README.md` 把三类能力塞进三个巨长句，并且有一处**与代码不符**的声明（「分页」）。本卡把这 10 个文档按当前源码逐条对齐，并统一描述风格。

## 只读白名单（最多 3 个源文件）

- `Cargo.toml`（workspace）—— 依赖与版本
- `crates/kafka-core/src/protocol.rs` + `src/dto.rs` —— 命令/事件常量与 DTO 字段
- `frontend/package.json` + `frontend/src/views/ConsumeView.vue` —— 前端依赖版本与界面真实能力

## 要改的文件

- `agents.md`、`README.md`
- `.opencode/skills/rs-kafka-ui-client/` 下 8 个 md（`SKILL.md`、`architecture`、`workspace`、`driver-abstraction`、`protocol`、`conventions`、`build-test`、`toolchain`）

## 必须覆盖

1. **事实纠错**：删掉 README 的「分页」（前端 grep `pageSize/page/limit/分页` 零命中，实测是「最多 500 行 + 过滤 + 导出」）；起点写准（协议四态含 `timestamp`，界面只有 earliest/latest/offset）；Mock 的 broker/topic/分区编辑加限定语「经 IPC 提供，界面尚未暴露全部入口」。
2. **补齐缺口**：`agents.md` 补预算口径（只有源码行计入；`docs/`、`*.md`、`.github/`、`scripts/`、`frontend/dist/`、`Cargo.lock` 不计）、不要跑 `cargo fmt`、每任务一张卡 + 卡尾结果 + 手动清单；skill 补 `main-bin` 入口层、`KAFKA_UI_DEV_DIR`、`MessageStream`/`StreamControls`、`request.cancel` 与 9 个事件名、真实门禁命令；`toolchain.md` 版本改 1.99.1 并写入 openssl@4 的 `_ENGINE_by_id` 链接坑与两种解法。
3. **描述风格**：`SKILL.md` 的 `description` 从「内容清单」改成**触发式**并含触发词；其余 md 保持「何时读 / 怎么用」的短条目，不写叙事。
4. 每条事实改动都能指回源码（结果一节列出证据位置）。

## 明确不做

- 不改任何源码、配置与 `frontend/dist`（本卡 0 行源码）。
- 不新增 skill、不改 skill 的目录结构。
- 不重写 `docs/TASKS.md` / `docs/PLAN.md` 与历史卡（它们记录的是当时事实）。

## 已知取舍（Ruling）

- **R118** 文档类改动（`*.md`）不计入「≤3 个源文件 / ≤250 行源码」预算：预算的目的是让**代码**改动可审，本次 10 个 md 全是文档、0 行源码；口径与 T103/T104 一致。代价：一次性改了 10 个文件，若按源码口径严格执行则应拆成 4 次提交，收益仅是账面整齐。
- **R119** `SKILL.md` 的 `description` 只写**何时该读**、不概要流程：描述里一旦概括流程，代理会照描述行事而不读正文，反而丢掉正文里的判断细节。
- **R120** 维持 `agents.md` 与 skill 的分工：`agents.md` 只放每个任务都要守的硬约束与入口，模块级细节留在 skill，避免同一事实两处维护。

## 验收

- 10 个 md 通读一遍，与源码逐条对照无残留错误（尤其版本号、命令、路径）
- 确认无工具转义残留（`\_` / `\#`）与「分页」类假声明
- 0 行源码改动

## 结果（完成后填写，≤5 行）

- 改动 **10 个 md、0 行源码**：`agents.md` 10→21、`README.md` 157→167、skill 8 个 md 89→122 行。
- 纠错 3 处：删假「分页」；起点写准（协议另有 `timestamp`，界面三者）；Mock 的 broker/topic/分区编辑加「界面未暴露」限定语。
- 补齐：预算口径与 fmt 禁忌、每任务卡片流程；`main-bin` 入口层与 `KAFKA_UI_DEV_DIR`；`tests/`+`benches/`+`examples/` 与 `active_pumps()` 判据；`request.cancel` 与 9 个事件名；真实门禁命令；`toolchain` 1.99.1 + openssl@4 链接坑。
- 证据：`Cargo.toml`、`protocol.rs`（`request.cancel` 见第 47 行）、`dto.rs:12-50`、`mock/fault.rs:43-72`、`frontend/package.json`（tailwind 4.1.4 / vue 3.5.13 / vite 7）、`ConsumeView.vue:25`。
- 验证：`wc -l` 与 `grep '\\[_#*]'` 全仓 md 复核；仅 T103 卡命中，且是在**引用**被删掉的转义（预期）。R118–R120。
