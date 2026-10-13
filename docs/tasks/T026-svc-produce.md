# T026 · ProduceService

**状态**: 🔵代码完成，待编译验证 | **依赖**: T024 | **改动上限**: 2 文件 / 200 行（实际 3 文件，见结果）

## 目标
发送单条/批量消息，把回执转成前端可展示结构。

## 只读白名单（最多 3 个）
- `crates/kafka-core/src/message.rs` — `ProduceReq`/`ProduceResult`
- `crates/kafka-app/src/services/clusters.rs`
- `crates/kafka-core/src/protocol.rs` — `event::PRODUCE_RESULT`

## 要改的文件
- 新建 `crates/kafka-app/src/services/produce.rs`
- 修改 `crates/kafka-app/src/services/mod.rs`

## 必须覆盖
- `send_one(cluster_id, req)` / `send_batch(cluster_id, reqs)`
- 部分失败：结果里逐条带 `ok`/`error`，不因一条失败整批报错
- 发 `AppEvent::ProduceResult`（含成功数/失败数/每条 offset）
- 空批次 → `InvalidConfig`
- 超长 value（>1MB，可配）→ 前置校验 `InvalidConfig`，不发给驱动

## 明确不做
- 不做压缩、不做事务、不做幂等 producer（真实驱动阶段再说）
- 不做前端表单校验

## 验收
- 新建 `crates/kafka-app/tests/svc_produce.rs`
- `cargo test -p kafka-app --test svc_produce` 全绿

## 结果（完成后填写，≤5 行）
- 产出 3 文件：`services/produce.rs`(~165)、`services/mod.rs`(+2：`pub mod produce;` + `pub use produce::{DEFAULT_MAX_VALUE_BYTES, ProduceReport, ProduceService};`)、`tests/svc_produce.rs`(~250，7 测试)。**偏离 2 文件/200 行上限**。状态：**代码完成，未编译验证**。
- API：`send_one(id, ProduceReq)` / `send_batch(id, &[ProduceReq])` → `ProduceReport { results, ok_count, error_count }` + `is_ok()` / `failures()`（`"topic-p: error"`）。逐条成功判定 = `ProduceResult.error == null`，**没有另造 `ProduceAck`**，与 bus 里 `AppEvent::ProduceResult` 同形状（偏离卡片「逐条带 ok」的字面写法，语义等价）。
- 前置校验（不碰驱动、不发事件）：`ProduceReq::validate()`（空 topic / 空 messages → `InvalidConfig`）、空批次 → `invalid("requests", …)`、单条 value > `max_value_bytes`（默认 1 MiB，`with_max_value_bytes(n)` 链式改，`n=0` 抬到 1）→ `invalid("requests[i].messages[j].value", "value is N bytes, limit is M bytes")`。校验先于连接检查（离线 + 超长 → 仍报 `InvalidConfig`）。
- 部分失败：驱动逐条 `error` → 只汇总计数，调用返回 `Ok`；整请求级错误（如 topic 不存在）→ 为该请求每条消息合成一条失败回执（`partition = message.partition.unwrap_or(0)`）并继续发后续请求。
- 事件：每次调用发一条 `AppEvent::ProduceResult{cluster_id, results, ok_count, error_count}`；离线（`NotConnected`）与校验失败 → 零事件。遗留：未做发送前 topic 存在性检查（交给驱动）、未做限流（T060）。