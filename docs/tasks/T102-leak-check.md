# T102 — 长跑内存/句柄泄漏检查

状态: ✅　依赖: T063　验证: 手动 + 报告　类型: 产品码

## 目标

T063 把「关闭订阅 / 释放驱动 / 优雅退出」做成了路径，但只验证了「一次」：
`--test teardown` 能证明收尾会执行，不能证明**反复收尾不累积**。订阅泵漏一个、
驱动句柄漏一个、每轮多留一个文件描述符，在单测里全都看不出来——它们只在几千轮
之后才显形。本卡把这件事做成一件可重复的手工检查，并留下数字：

- **进程内**：探针每轮走一遍完整生命周期（建主题 → 生产一批 → 订阅 → 抽干 →
  `stop` → 抽干到流结束 → 读元数据 → 删主题），全程读驱动自己的存活泵计数；
  退出时若计数不回落则**非零退出**。
- **进程外**：脚本按固定间隔采样 RSS 与打开的文件描述符数。
- **成文**：`docs/reports/T102-long-run-leak-report.md` 记录方法与原始数据。

## 只读白名单（最多 3 个源文件）

- `crates/kafka-driver/src/mock/state.rs` —— `active_pumps()` 存活泵计数（T068 观测）
- `crates/kafka-core/src/message.rs` —— `StreamControls::stop` 与 `MessageStream::next`
- `crates/kafka-app/tests/teardown.rs` —— T063 的收尾路径（本卡复用它反复跑）

## 要改的文件

- 新增 `crates/kafka-driver/examples/leak_probe.rs` —— 长跑探针（两个可调参数：轮数 / 每批条数）
- 新增 `scripts/leak-check.sh` —— 外部采样器（RSS + 文件描述符），并在退出码上转发探针结论
- 新增 `docs/reports/T102-long-run-leak-report.md` —— 方法与实测数据

## 必须覆盖

1. **完整生命周期**：每轮都必须包含 `stop` 之后**把流抽到 `None`**，因为「停止后流
   真的结束」才是 T063 要保证的事；抽不到 `None` 就说明泵没释放。
2. **保留集有界**：每轮结束删掉自己的主题。否则 RSS 上涨永远可以被解释成
   「消息攒多了」，泄漏信号就被噪声吞掉了（R105）。
3. **泵计数归位**：进入长跑前先热身若干轮取基线，长跑结束后计数必须**等于基线**；
   `disconnect()` 之后必须为 0。
4. **外部采样**：RSS 与 fd 数由 `scripts/leak-check.sh` 从进程外用 `ps` / `lsof` 取，
   而不是让被测进程自报（R107）；首个有效样本要跳过 exec 之前的瞬时值。
5. **退出码可信**：探针发现泄漏（泵不归位、主题没删掉、`stop` 后 5 秒流不结束）
   一律非零退出，脚本原样转发。

## 明确不做

- 不做真实驱动（`--features real`）的长跑：需要 Docker broker，且 librdkafka 的
  内部句柄不受本仓控制，检查它属于另一件事（R106）。
- 不设内存阈值断言（「不超过 N MB」在 macOS / 不同分配器上没有稳定含义）。
- 不把长跑接进 `cargo test`：分钟级检查不该拖慢每次门禁。
- 不改 `teardown.rs` 或任何产品码路径——本卡只加探针与采样器。

## 已知取舍（Ruling）

- **R105** 每轮「建主题 → 删主题」而不是共用一个主题：共用主题时 RSS 随消息数线性
  上涨，泄漏信号被真实数据掩盖；删主题把保留集钉在一批，RSS 曲线才能反映「有没有
  东西没释放」。代价：每轮多了两次元数据操作，实测的单轮成本因此略高于纯生产/消费。
- **R106** 只查 mock 驱动：探针不依赖 Docker，任何机器（含 CI 之外的开发机）都能直接
  跑；真实驱动的句柄泄漏留给有 broker 的环境单独立项。代价：`--features real` 路径
  的句柄泄漏本卡不作保证。
- **R107** RSS/fd 从进程外采样：进程无法诚实观测自己的常驻集与描述符表。代价：采样
  是**离散**的，两个采样点之间的尖峰看不见；因此探针自己那一侧必须给出确定性的
  断言（泵计数归位），而不是只依赖曲线好看。

## 验收

- `cargo run -p kafka-driver --example leak_probe -- <cycles> <batch>` 跑完打印
  `pumps=<基线>`、`after_disconnect=0`、`topic_left=false`，且退出码为 0
- `./scripts/leak-check.sh 20000 50 2` 与 `./scripts/leak-check.sh 40000 50 2` 两轮
  对照：末样本 RSS 不再随轮数线性增长、fd 增量不随轮数漂移
- `docs/reports/T102-long-run-leak-report.md` 留下方法与原始数据
- `cargo test -q --workspace` 与 `cargo clippy -q --workspace --all-targets -- -D warnings` 无回归
- 预算：≤250 行 / ≤3 个源文件（`scripts/` 与 `docs/` 不计）

## 结果（完成后填写，≤5 行）

- 新增 `crates/kafka-driver/examples/leak_probe.rs` 162 行（探针：建主题→生产→订阅→抽干→`stop`→抽干到 `None`→读元数据→删主题，泵不归位即非零退出）
  与 `scripts/leak-check.sh` 50 行（进程外 `ps`/`lsof` 采样，`scripts/` 不计预算）；报告 `docs/reports/T102-long-run-leak-report.md`。**1 个源文件 / 162 行**（R105–R107）。
- 实测两轮（`leak-check.sh 20000 50 2` / `40000 50 2`，25.9 s / 54.0 s）：共 **38 个采样点全部** `rss=6672KB`、`fds=13`；
  两轮均 `warmup … pumps=0` / `pumps=0 after_disconnect=0 topic_left=false`，退出码 0。轮数与墙钟翻倍，常驻集与描述符表**零增长**，泵计数全程未离开 0。
- 盲区（报告已写明）：单进程、仅 mock 驱动（R106）、墙钟 ~26/54 s、2 s 离散采样（R107）、分配器可能扣留已释放页；
  进程内泵计数是那一侧的确定性兜底，两轮都是 0。
- 门禁：`cargo test -q --workspace` **281 passed**（与 T101 同数，无回归）；`cargo clippy -q --workspace --all-targets -- -D warnings` 零告警（含 example/bench 目标）。
- 偏差：无。`STAGE4-manual-checklist.md` 已追加第 28 条。
