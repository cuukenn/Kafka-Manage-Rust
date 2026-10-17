# T108 — README 瘦身：读者优先，维护者内容拆出

状态: ✅　依赖: 无　验证: 文档（人工通读 + 链接与围栏校验）　类型: 文档

## 目标

T107 把 README 与源码对齐之后，它变成了一份**同时当两本书用**的文档：167 行里有 90 行（54%）是「构建 / 开发 / 测试」与「运行」里的维护者内容（门禁命令、openssl@4 的根因链、docker broker、数据目录），而「这是个什么、怎么最快跑起来」只占 30 行。第一次打开仓库的人要滚过一半的篇幅才看到自己关心的东西；同时架构层、Workspace 树、协议、技术栈又与 `.opencode/skills/` 重述一遍，将来必然会两处漂移。

用户判断：README 服务「第一次看仓库」的人。本卡据此把它拆成**读者文档**与**开发文档**两份。

## 只读白名单（最多 3 个源文件）

- `README.md` —— 改前的全文（167 行）
- `.opencode/skills/rs-kafka-ui-client/SKILL.md` —— 确认该留在 skill 而非 README 的主题
- `.github/workflows/ci.yml` —— 前置清单的出处

## 要改的文件

- 重写 `README.md`（167 → 72 行）：功能、快速开始、架构一图、文档指针、License
- 新增 `docs/development.md`（117 行）：前置、门禁、技术栈、Workspace 职责、协议、openssl 坑、本地 broker、任务与卡片
- `docs/TASKS.md`、`docs/tasks/T108-readme-reader-first.md`（本卡）

## 必须覆盖

1. README 只留「是什么 / 有什么能力 / 怎么最快跑起来 / 架构一图 / 去哪看细节」，**不再复述**版本号表、门禁命令与故障排查正文，只留一行指针。
2. 三条跑法都要留在 README（Mock 一行、`real-run.sh` 两条、`KAFKA_UI_DEV_DIR` 免重编译、docker broker 两句）——这些是读者真正要照抄的命令。
3. 挪进 `docs/development.md` 的内容一条不丢：按平台前置、五条门禁、不要跑 `fmt`、`git add -f frontend/dist`、`check` 不链接、conformance bootstrap、技术栈版本、Workspace 职责、IPC 协议、openssl@4 根因与两种解法、broker 端口与 TLS 材料、数据目录、卡片与预算约定。
4. 文末补一句读者不需要知道、但贡献者必须知道的兜底：没开 `real` 时非 mock 集群给出明确错误而不是崩。

## 明确不做

- 不新增 `CONTRIBUTING.md`（见 R122）。
- 不改源码、配置、skill 与 CI；不动 `scripts/real-run.sh` 的行为。
- 不删 `docs/development.md` 里的 openssl 排查链——它是 T104 的结论，读者可以跳过、维护者不能没有。

## 已知取舍（Ruling）

- **R121** README 定位为「第一次看仓库的人」，维护者内容拆到 `docs/development.md`。代价：同一件事分成两处描述，将来改门禁要记得同步 README 的指针段落；缓解是 README **只留指针不复述正文**，漂移面收敛到一行链接。
- **R122** 维护者文档放 `docs/development.md` 而不是根目录 `CONTRIBUTING.md`：根目录保持「README + agents.md + Cargo/workflow + License」的干净形状，`docs/` 已是本项目既有的文档层（`TASKS.md` / `PLAN.md` / `reports/`）。
- **R123** 允许 README 与 skill 存在**受控重叠**：架构一张图两边都有，因为「有哪些层」对读者和实现者都是第一手信息；重叠仅限这张图，其余主题单一归属。代价：改分层要动两处。

## 验收

- `README.md` ≤ 80 行，且不含 `cargo clippy` / `openssl@4` 根因 / broker 端口表这类维护者正文
- `docs/development.md` 覆盖「必须覆盖」的 4 条，无内容丢失
- 所有相对链接指向存在的文件；代码围栏成对
- 全部为 `*.md`，按 R118 不占用 ≤3 文件 / ≤250 行源码预算

## 结果（完成后填写，≤5 行）

- `README.md` 167 → **72 行**（功能 8 行、快速开始 33 行、架构 13 行、文档 8 行、License 3 行）；新增 `docs/development.md` **117 行**。
- 移出 README 的：按平台前置、五条门禁与三条注意事项、技术栈版本表、Workspace 职责表、IPC 协议四行、openssl@4 根因与 `real-run.sh` / `~/.cargo/config.toml` 两种解法、docker broker 端口与 `gen-secrets.sh`、数据目录、卡片与预算约定。
- 留在 README 的：定位一句话、7 条能力、4 段可直接照抄的命令、架构一图、文档指针；指针段落一句话概括 `development.md`。
- 校验：围栏成对（README 10 / development 14）；5 个链接目标全部存在；未改源码与配置，**0 行源码**。
- 偏差：无。
