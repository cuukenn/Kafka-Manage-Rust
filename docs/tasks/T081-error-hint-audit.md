# T081 — 错误映射审计：每种 `ErrorKind` 的 hint 覆盖

状态: ✅　依赖: T030　验证: `cargo test -p kafka-core`　类型: 产品码

## 目标

`ErrorDto.hint` 是 UI 在错误文案下面显示的行动建议。当前 `protocol.rs` 的 `hint_for`
只覆盖 15 个 `ErrorKind` 中的 7 个，其余 8 个落到 `_ => None` —— 既分不清「故意不给建议」
还是「忘了写」，也没有测试锁住。本卡把这张表补成**穷尽 match**（新增变体会直接编译失败），
并用测试逐个 `ErrorKind` 钉住覆盖结论。

## 只读白名单（最多 3 个源文件）

- `crates/kafka-core/src/error.rs` — `ErrorKind`、`DriverError::kind()`
- `crates/kafka-core/src/protocol.rs` — `ErrorDto` / `hint_for` / 既有单测
- `frontend/src/bridge/rpc.ts` — 确认前端是否消费 `hint`（只读，结论见 R78）

## 要改的文件

- 修改 `crates/kafka-core/src/error.rs` —— 新增 `ErrorKind::ALL`
- 修改 `crates/kafka-core/src/protocol.rs` —— `hint_for` 改为穷尽 match；补 3 个单测

## 必须覆盖

1. `ErrorKind::ALL` 与枚举一一对应（15 个）：新增变体会先在这里编译失败。
2. 每个 kind 的 hint 覆盖都有明确结论：13 个给建议，`InvalidConfig` / `Cancelled` 明确 `None`。
3. `ErrorDto::new` 与 `ErrorDto::from_driver` 对同一 kind 给出**相同**的 hint（两条进 wire 的路径）。
4. hint 不为空白字符串（不留「有个提示但没内容」）。
5. 不同 kind 的 hint **互不相同**（防复制粘贴串行，把 A 的建议挂到 B 上）。

## 明确不做

- 不改 `retryable` 分类（T065 已收口）、不改 `DriverError` 变体与 `kind()` 映射。
- 不在本卡把 hint 接到前端展示 —— 那是新登记的第 15 行卡片 **T081b**。
- 不动已有测试 `error_envelope_carries_kind_and_hint`。

## 已知取舍（Ruling）

- **R76** `InvalidConfig` 与 `Cancelled` 故意不给 hint：前者的 message 由 driver 自己拼出
  （`invalid config for 'topic': topic must not be empty`），已经点名字段与原因；后者是用户
  自己发起的取消。再补一句只是复述。代价：这两类在 UI 里只有一行文案，没有第二行建议。
- **R77** hint 表留在 `protocol.rs`，不搬进 `error.rs`：它是**面向 wire/UI 的文案**，不是
  `DriverError` 的语义；`error.rs` 保持不含 UI 文案（只提供 `ALL` 这种分类事实）。
- **R78**（审计发现，转交 T081b）`hint` 目前**根本到不了用户眼前**：`ErrorDto` 序列化会带上它，
  但 `frontend/src/bridge/rpc.ts:93` 构造 `RpcError(kind, message, retryable)` 时把它丢掉了，
  22 个 `catch` 点只显示 `e.message`。本卡按索引口径只做 core 侧映射与测试。

## 验收

- `cargo test -p kafka-core` 全绿（新增 ≥3 个单测）
- `cargo clippy -p kafka-core --all-targets -- -D warnings` 零告警
- `cargo check --workspace` 通过

## 结果（完成后填写，≤5 行）

- `error.rs` +24：新增 `ErrorKind::ALL`（15 项，声明序），新增变体会先在这里编译失败。
- `protocol.rs` +116/-10：`hint_for` 改穷尽 match（无 `_` 臂）；新增 `NotFound`/`AlreadyExists`/`Group`/
  `Broker`/`Storage`/`Internal` 六条建议，`InvalidConfig`/`Cancelled` 用 `return None` 显式表态（R76）；
  3 个新单测：逐 kind 覆盖结论 + 提示非空白、提示互不复用（防串行）、`from_driver` 与 `new` 一致
  （`Storage` 无对应 `DriverError`，测试里显式跳过并断言它就是 `Storage`）。
- 验证：`cargo test -p kafka-core` 9/9 全绿（3 新增）；`cargo clippy -p kafka-core --all-targets -- -D warnings`
  零告警；`cargo check --workspace` 通过；`cargo test -p kafka-driver` 72/72 无回归。
- 审计发现（转 T081b）：`hint` 进得了 wire 但被 `frontend/src/bridge/rpc.ts:93` 丢掉，22 个 catch 点只显示
  `e.message`，用户看不到任何建议（R78）。按索引口径本卡只做 core 侧映射与测试。
- 预算：140 行 / 2 文件。
