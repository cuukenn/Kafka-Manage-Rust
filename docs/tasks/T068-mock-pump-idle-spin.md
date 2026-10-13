# T068 · 修 Mock pump 在 `follow: true` 空闲时的任务空转

**状态**: ✅完成 | **依赖**: T054（其 `join` 字段的处置把这个问题暴露出来） | **改动上限**: 3 文件 / 250 行

## 目标

`crates/kafka-driver/src/mock/pump.rs` 的 pump 任务有一条**不会退出**的路径：

```
if batch.is_empty() {
    if !follow { break; }   // 非 follow 会退出
    continue;               // follow 且无新消息 → 永远回到循环顶部
}
```

循环只有三个出口：`commands.recv()` 返回 `None`（= `StreamControls` 的发送端被丢弃）、
`tx.send(...).await.is_err()`（= 消息接收端已亡）、以及 `max_messages` 用尽。
**`follow: true` 且没有新消息时，`tx.send` 永远不会被调用**，于是「接收端已亡」这一路永远探测不到。

后果：若消费侧只丢掉消息流而留着 `StreamControls`（`ConsumeService` 为了 pause/resume 会
持有 controls，见 `crates/kafka-app/src/services/consume.rs`），该任务会以 `poll_ms` 为周期
**无限空转**，且每个 tick 取一次 `state.read()` 读锁 → CPU 空耗 + 读锁竞争，
存活期 = 整个 Mock driver 的生命周期。长跑（T102）会直接看到它。

## 只读白名单（最多 3 个源文件，别读更多）

- `crates/kafka-driver/src/mock/pump.rs` — 循环出口与 `tx`
- `crates/kafka-driver/src/mock/driver.rs` — `subscribe` 里 `PumpHandle`/`StreamControls`/`MessageStream` 的归属（`:576-593`）
- `crates/kafka-core/src/message.rs` — `MessageStream` / `StreamControls` 的字段与 Drop 语义

## 做法（先做最小修法，别顺手改生命周期）

1. **最小修法**：`batch.is_empty()` 分支里，`follow` 时先 `if tx.is_closed() { break; }`
   再 `continue`。这是 pump 内部的局部改动，不动任务归属，也不改任何对外语义。
2. **可观测性**（测试需要）：`MockState` 加一个活跃 pump 计数（`AtomicU64` + 一个 RAII guard，
   spawn 时 +1、退出时 -1）。有了它，测试才能断言「任务真的退出了」，而不是靠 sleep 猜。
   —— 若这条计数不想进产品代码，退路是把 guard 只放在 `cfg(test)` 下，但那样真实
   退出路径就没被生产代码覆盖；卡片倾向留在产品代码里，并在注释里写明它只为可观测性存在。
3. 补测试（放 `crates/kafka-driver/tests/mock_stream_control.rs`，与 T013 同一文件；
   若 T013 尚未落地，本卡自己建该文件）：
   - `follow_subscription_exits_when_reader_is_gone`：`follow: true` 订阅 → 只丢消息流、
     保留 `StreamControls` → `tokio::time::pause()` + `advance()` 若干个 poll 周期 →
     断言活跃 pump 计数回到 0。
   - 反向锁：`follow_subscription_survives_while_reader_is_alive`：接收端活着时计数保持 1，
     证明修法没有把正常的 follow 流掐掉。

## 明确不做（防止范围膨胀）

- **不**把 `JoinHandle` 塞进 `MessageStream` / `StreamControls` 去做「drop 即 abort」。
  那会跨到 `kafka-core` 的公开类型，且 `PumpHandle` 在 `driver.rs:591-593` 的丢弃点
  早于流交付给调用方，直接 abort 会掐掉正常的流（T054 已因此删掉 `join` 字段并留了注释）。
  若将来真要改归属，另开卡。
- 不改 `poll_ms` 默认值、不改 `MAX_PER_TICK`、不改 offset 游标的「先试探后提交」语义。
- 不碰 `kafka-app`（消费侧是否应当丢 controls 是 T027/T031 的语义问题）。

## 验收

- `cargo test -p kafka-driver` 全绿（含两条新测试；先看到第一条 RED）
- `cargo clippy -p kafka-driver --all-targets` 保持零告警
- `cargo test --workspace` 全绿

## 结果（完成后填写，≤5 行）
- 修法：`batch.is_empty()` 的 follow 分支先 `if tx.is_closed() { break; }` 再 `continue`；`MockState` 增 `active_pumps: Arc<AtomicU64>`（`#[serde(skip)]`，手写 `PartialEq` 比较全部持久化字段）+ `PumpGuard` RAII（产品代码，注释标明仅可观测性），guard 在任务体首行创建，所有退出路径必经 drop。
- RED（counter+guard 就位、无 break）：`follow_subscription_exits_when_reader_is_gone` 失败「the pump must exit once its reader is gone」（计数停在 1）；加 break 后 GREEN。
- 注意：tokio 1.53 的 `advance` 是一次性跳钟 + `yield_now`，pump 只在测试让出时才被 poll，故断言用「每步 advance 后复查、有界 settle」而非单次 advance 后立即断言。
- 验收：`cargo test -p kafka-driver` 全绿（64 条）、`clippy --all-targets -D warnings` 零告警、`cargo test --workspace` 全绿。
