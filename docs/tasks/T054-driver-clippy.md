# T054 · 清 kafka-driver 的存量 clippy 告警

**状态**: ✅完成 | **依赖**: T053 | **改动上限**: 3 文件 / 250 行

## 目标
`kafka-driver` 以前从没编译过，所以这批 clippy 告警一直是隐形的（T053 编译通了才暴露）。把它们清干净，让 `cargo clippy -p kafka-driver --all-targets` 零告警——T002 的基线验收要求 `cargo clippy --workspace --all-targets` 干净。

## 只读白名单（最多 3 个源文件，别读更多）
- `crates/kafka-driver/src/mock/pump.rs` — `join` 字段
- `crates/kafka-driver/src/mock/driver.rs` — 两处 `any()`
- `crates/kafka-driver/src/mock/fault.rs` — 冗余 cast + Default 后赋值

## 要改的文件
- 修改 `crates/kafka-driver/src/mock/pump.rs`
- 修改 `crates/kafka-driver/src/mock/driver.rs`
- 修改 `crates/kafka-driver/src/mock/fault.rs`

## 必须覆盖
1. `pump.rs:35` warning `field 'join' is never read` —— `PumpHandle.join: JoinHandle<()>` 从未被读。**先判断意图再改**：若句柄应当负责收尾，就给它加 `impl Drop`（`join.abort()`）并让字段真正被使用；若不需要，就删字段。不要直接 `#[allow(dead_code)]` 糊过去
2. `driver.rs:326` `iter().any()` → `req.names.contains(&name)`
3. `driver.rs:655` `iter().any()` → `req.topics.contains(&topic)`
4. `fault.rs:160` 冗余 cast `u64 -> u64` → 去掉 `as u64`
5. `fault.rs:215 / 228 / 237` field assignment outside of initializer（`Default::default()` 之后逐字段赋值）→ 改成结构体字面量 + `..Default::default()`

## 明确不做（防止范围膨胀）
- 不改 pump 的行为语义（只处理 `join` 的归属；若需要改 pump 的启停逻辑，另开卡）
- 不碰 `kafka-core`（那两条 `map().flatten()` 是 T055）
- 不碰 `kafka-app`
- 不引入新的 `#[allow]`，除非能说明为什么这条 lint 在此处是误报

## 验收
- `cargo clippy -p kafka-driver --all-targets` 零告警
- `cargo test -p kafka-driver` 仍全绿（11 passed）

## 结果（完成后填写，≤5 行）
- 改了 3 个文件（白名单内）：`mock/pump.rs`、`mock/driver.rs`、`mock/fault.rs`。
- `join` 字段按卡片第二支走 **删字段**：`PumpHandle` 在 `driver.rs:591-593` 就被解构丢弃，
  加 `impl Drop { abort() }` 会在流交给调用方**之前**掐掉任务 → 不可行。删字段与现状**行为完全一致**
  （今天该字段也是在同一处被丢弃，丢弃 `JoinHandle` 只是 detach）。已在结构体上写注释说明任务如何自行收尾。
- 顺带登记 **T068**：`follow: true` 且无新消息的 pump 永远走不到 `tx.send`，
  若消息接收端先亡而 `StreamControls` 仍活着，任务会空转并每 tick 持有一次状态读锁。
- `driver.rs:326/655` → `contains(&name)` / `contains(&topic)`（`names` 是本地 `Vec<String>`、
  `split_tp_key` 返回 `String`，故 `&` 类型正确）；`fault.rs:160` 去掉 `as u64`；
  `fault.rs` 三处 `Default::default()` + 逐字段赋值 → 结构体字面量 + `..Default::default()`。
- 验收：`cargo clippy -p kafka-driver --all-targets` **零告警**；`cargo test -p kafka-driver` 11/11。
