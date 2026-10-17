# T101 — criterion 基准：Mock produce/consume 吞吐

状态: ✅　依赖: T030　验证: `cargo bench -p kafka-driver`　类型: 产品码

## 目标

Mock 驱动是默认驱动，也是所有业务层单测的地基，但它的吞吐从来没有量过：`O(1)` 的
内存写队列和「每分区一份 Vec」的读路径到底值多少，只能靠猜。本卡给 `kafka-driver`
加一个 criterion 基准，把两条主路径钉成数字：

- **produce**：一批 100 条文本消息 `ProduceReq` 的端到端耗时与 `Melem/s`。
- **consume**：订阅 `Earliest` + `follow:false` + `max_messages` 有界读回 100 条，
  逐条走 `MessageStream::next()` 抽干，量的是「建流 + 解码 + 投递」的总成本。

## 只读白名单（最多 3 个源文件）

- `crates/kafka-driver/src/driver.rs` — `KafkaDriver` trait 的 `produce` / `subscribe` 签名
- `crates/kafka-core/src/message.rs` — `ProduceReq` / `SubscribeReq` / `MessageStream::next`
- `crates/kafka-driver/tests/mock_produce_consume.rs` — 既有的驱动装配写法

## 要改的文件

- 修改 `Cargo.toml`（根）—— `[workspace.dependencies]` 增 `criterion`（`async_tokio`）
- 修改 `crates/kafka-driver/Cargo.toml` —— dev-dep + `[[bench]]` 目标（`harness = false`）
- 新增 `crates/kafka-driver/benches/mock_throughput.rs` —— 两个基准

## 必须覆盖

1. `mock_produce/batch`：100 条一批，报告 `time` 与 `thrpt`（`Throughput::Elements`）。
2. `mock_consume/drain`：有界读回 100 条，逐条 `next()` 抽干并计数（数量不符即失败）。
3. 运行时只建一次：`Runtime` 与驱动都建在 `b.iter` 之外，量的是请求本身而不是装配。
4. `harness = false`：criterion 自己当 main，`cargo bench -p kafka-driver` 直接可跑；
   `cargo test --workspace` 与 clippy 不受影响（基准不进测试目标）。

## 明确不做

- 不量真实驱动（`--features real`）：基准需要 Docker broker，不进默认门禁。
- 不做性能回归阈值（没有 baseline 存储 / 不比 `main`）。
- 不写 `[[bench]]` 之外的 profile 调优（`[profile.release]` 不动）。
- 不把吞吐做成断言（criterion 只报告，不判过不过）。

## 已知取舍（Ruling）

- **R102** 用 `criterion` 而不是 `#[bench]` 或手写计时：`#[bench]` 仍是 nightly 专属，
  手写计时没有预热/离群点/统计区间。代价：新增一个 dev-dependency（`criterion 0.7`，
  仅 `async_tokio` feature），`Cargo.lock` 因此多出 262 行——lockfile 是生成物，不计入
  250 行预算。
- **R103** 运行时与驱动建在 `b.iter` 之外，用 `rt.block_on(..)` 而不是 criterion 的
  `to_async`：`Runtime::new()` 本身就比被测请求贵，必须在循环外；显式 `block_on` 让
  「量的是什么」一眼可见，也不依赖 `AsyncBencher` 的 API 稳定性。代价：与 criterion 的
  async 惯用法不同，将来升版时这段要人工确认。
- **R104** consume 基准固定 `follow: false` + `max_messages`：无界跟随流永远不会结束，
  不能当迭代体。代价：量到的是「有界读回」的成本，不含长跑跟随时的推进开销。

## 验收

- `cargo bench -p kafka-driver` 跑出两组数字（produce 与 consume）
- `cargo test -q --workspace` 无回归、`cargo clippy -q --workspace --all-targets -- -D warnings` 零告警
- 预算：≤250 行 / ≤3 个源文件（`Cargo.lock` 不计）

## 结果（完成后填写，≤5 行）

- `Cargo.toml`（根）+3：新增 `# benchmarks` 组 `criterion = { version = "0.7", features = ["async_tokio"] }`；
  `crates/kafka-driver/Cargo.toml` +5：`[dev-dependencies] criterion.workspace = true` + `[[bench]] name = "mock_throughput"`
  `harness = false`；新增 `benches/mock_throughput.rs` 87 行（`bench_produce` / `bench_consume` + `bench_driver()` 装配）。
  共 **95 行 / 3 个源文件**（`Cargo.lock` +262 是生成的 lockfile，不计预算）。
- 实测（–quiet 下 criterion 输出）：`mock_produce/batch` 100 条一批
  `[23.377 µs 23.431 µs 23.497 µs]` → **4.27 Melem/s**；`mock_consume/drain` 有界读回 100 条
  `[1.2177 ms 1.2407 ms 1.2753 ms]` → **80.6 Kelem/s**（后者含建流 + 逐条 `next()` 抽干）。
- 验证：`cargo bench -p kafka-driver` 两组都出数（`Gnuplot not found, using plotters backend`）；
  `cargo test -q --workspace` **281 passed** 无回归；`cargo clippy -q --workspace --all-targets -- -D warnings` 零告警。
- 偏差：无。运行的环境变量与 R102–R104 见上。
