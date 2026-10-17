# T083a — 日志落盘与轮转

状态: ✅　依赖: T080　验证: `cargo test -p main-bin`　类型: 产品码

> 原 T083「日志导出与轮转」分两半：落盘/轮转在 Rust（本卡），面板导出在前端（T083b）。

## 目标

T080 之后日志只活在 stderr 与前端面板的 500 条环形缓冲里：关掉终端、面板滚动过 500 行，
历史就没了。本卡把进程日志同时写进 `<data_dir>/logs/app.log`，并按**大小轮转**：
写满上限就把当前文件顶成 `.1`（`.1`→`.2`，依次后移，丢掉最旧的），因此磁盘占用被
「`KEEP` 个轮转文件 + 1 个在写文件」封顶。

## 只读白名单（最多 3 个源文件）

- `crates/main-bin/src/logging.rs` — 现有 `install()` 与 `BusLogs`（本卡要改）
- `crates/main-bin/src/glue.rs` — `data_dir()` 的解析口径（本卡要改）
- `crates/kafka-app/src/store/db.rs` — `default_data_dir()`（只读，确认兜底链）

## 要改的文件

- 修改 `crates/main-bin/src/glue.rs` —— `data_dir()` 提为 `pub(crate)`，供 logging 复用
- 修改 `crates/main-bin/src/logging.rs` —— 新增 `FileWriter`（`MakeWriter`）与轮转，接进 `install()`

## 必须覆盖

1. 同一事件既进 stderr 也进 `app.log`：`app.log` 里逐行都在，且**一行一个事件**
   （写入缓冲到 `Drop` 再落盘，轮转不会把一条记录劈成两半）。
2. 超过上限就轮转：`app.log` 变成 `app.log.1`，新文件从空开始；连续轮转时旧文件依次后移，
   `app.log.(KEEP+1)` 永不出现（边缘文件被删）。
3. 目录建不出来（父路径是个普通文件）时**安静降级**：不 panic、不返回错误，只是没有文件。
4. 单条超长行不会因为「自己就超过上限」被无限轮转：文件为空时先写进去，下次才轮转。

## 明确不做

- 不做面板导出按钮与下载（**T083b**，前端）。
- 不做按日期轮转（按大小；日期轮转要时钟格式化，本轮不引入 `tracing-appender`）。
- 不改 `BusLogs` 的总线镜像行为（T080 已收口），不改 `RUST_LOG` 过滤口径（R75）。
- 不引入新依赖（不装 `tracing-appender`：轮转自己写，逻辑与测试都在本仓内可读）。

## 已知取舍（Ruling）

- **R87** 轮转自己实现而不是引 `tracing-appender`：只多约 80 行、行为完全可读可测，
  且不必为一个写文件的小功能改动 workspace 依赖与 `Cargo.lock`。代价：没有按天轮转、
  没有压缩，也没有跨进程的文件去重。
- **R88** 文件写入**绝不失败向上**：`create_dir_all`/`open`/`write_all` 的每一个错误都被
  吞掉并退化成「只进 stderr」。「日志写不下去」不是让应用起不来的理由。
  代价：磁盘满或权限错时**静默**丢日志，用户只能靠 stderr/终端发现。
- **R89** 上限与保留份数写死在 `logging.rs`（`MAX_BYTES = 1 MiB`、`KEEP = 3`、`app.log`），
  不做设置项：阶段 9 的长跑检查要的是「有界」，不是「可调」。代价：想要更长历史必须改代码。

## 验收

- `cargo test -p main-bin`（含 4 个新单测：落盘、轮转与保留份数、目录不可建时降级、超长行）
- `cargo build -p main-bin`、`cargo clippy -q -p main-bin --all-targets --features real -- -D warnings` 零告警
- 手动：跑起来后 `<data_dir>/logs/app.log` 出现，且与面板里的 `rust` 行一致（登记清单第 25 条）

## 结果（完成后填写，≤5 行）

- `glue.rs` +4/-1：`fn data_dir()` → `pub(crate) fn data_dir()`（T083a 让日志文件与本机库
  同目录，两条解析链迟早会走偏）。
- `logging.rs` +217/-4：新增 `FileWriter`/`LineWriter`/`RolledFile` 与 `MAX_BYTES = 1 MiB`、
  `KEEP = 3`；`LineWriter` 把一条记录缓冲到 `Drop` 才落盘，轮转永远不会从中间切断一行；
  `install()` 改成两个 `fmt` layer —— stderr（保留 ANSI）与 `<data_dir>/logs/app.log`（去 ANSI）。
- 4 个新单测：逐行落盘、轮转保留份数（`.1`/`.2` 在、`.3` 无、活文件里是最新行）、目录不可建时
  静默降级（不 panic 也不留文件）、超长行先写进空文件再轮转。验证：`cargo test -p main-bin`
  8 + 6 + 1 全绿；`cargo clippy -q -p main-bin --all-targets --features real -- -D warnings` 零告警。
- 偏差：原 T083 的「导出」半边（面板 Blob 下载）留在 T083b；本卡 **221 行/2 文件**，未引入任何
  新依赖（R87），写文件失败一律吞掉只降级到 stderr（R88），阈值硬编码（R89）。
