# T037 · 事件泵：总线 → sink（节流/批量，假 sink 单测）

**状态**: ⬜未开始 | **依赖**: T032 | **改动上限**: 3 文件 / 250 行

## 目标

在 `webview-bridge` 建立 **传输无关** 的事件泵：订阅 `EventBus`，把 `AppEvent`
转成 `EventEnvelope` 批量推给一个 sink 接口（T038 里 sink = `wry::evaluate`）。
wire 契约（agents.md）：**每批 ≤ 200 条且 ≤ 50ms 一刷**。本卡用假 sink 单测，不碰 wry。

## 只读白名单（最多 3 个源文件，别读更多）

- `crates/kafka-app/src/bus.rs` — `AppEvent::event_name()/payload_json()`、`BusReceiver::recv()`、`BusRecvError`
- `crates/kafka-core/src/protocol.rs` — `EventEnvelope` 形状、`now_ms()`
- `crates/webview-bridge/src/lib.rs` — 现有 mod 结构

## 设计（钉死，防跑偏）

```rust
/// 事件出口。实现者不得长时间阻塞（wry 实现只做 post，不等待 JS 结果）。
pub trait EventSink: Send + Sync + 'static {
    fn push_batch(&self, batch_json: &str);
}

pub struct EventPump; // EventPump::spawn(bus: &EventBus, sink: Arc<dyn EventSink>) -> PumpHandle
pub struct PumpHandle { /* JoinHandle + 优雅停机通道 */ }
impl PumpHandle {
    pub async fn shutdown(self);  // 停机前把残余 batch 刷出去
}
```

- 泵循环：`recv()` → 追加 `EventEnvelope { event: ev.event_name(), payload: ev.payload_json(), ts: Some(now_ms()) }`
  到当前批 → **满 200 条立即刷**；否则自第一条入批起 50ms 超时刷（`tokio::time` + `select!`）。
- 一批 = 一个 JSON 数组字符串（`[EventEnvelope, ...]`）→ `sink.push_batch(&json)`。
- `BusRecvError::Lagged(n)` → `tracing::warn!` 后继续（不崩、不吞批）；`Closed` → 刷残余后退出。
- `shutdown()`：停止接收 → 刷残余 → await 任务结束。

## 要改的文件

- 新建 `crates/webview-bridge/src/pump.rs`
- 新建 `crates/webview-bridge/tests/event_pump.rs`（假 sink：`Arc<Mutex<Vec<String>>>`）
- 修改 `crates/webview-bridge/src/lib.rs` — 加 `pub mod pump;`

## 必须覆盖（测试用 `tokio::test(start_paused = true)` 虚拟时钟）

1. envelope 形状：推 1 条 `AppEvent::LogLine` → 假 sink 收到 1 批、数组 1 个元素、
   `{event:"log.line", payload:{...}, ts}` 三字段在（对照 `event::LOG_LINE` 常量）。
2. 时间刷：连推 3 条（<200）→ 50ms 内 sink 未收批；推进虚拟时钟 ≥50ms → 收到 1 批 3 条。
3. 容量刷：连推 250 条 → 不推进时钟也至少刷出一次、首批恰 200 条；剩余在下一个 50ms 窗口刷出。
4. `shutdown()`：推 5 条后立即 shutdown → 全部 5 条到达 sink（不丢），泵任务结束
   （`tokio::runtime::RuntimeMetrics::num_alive_tasks` 或 join 句柄完成断言）。
5. 多订阅隔离：两个泵（两个假 sink）各自收到全量事件（broadcast 语义）。
6. 测试数 ≥ 5；不许裸 `sleep` 死等，全部虚拟时钟 + 有界超时。

## 明确不做（防止范围膨胀）

- 不接 wry/tao（T038 写真 sink）；不做总线→JS 的 evaluate 注入；不改 kafka-app；
  不做消息内容过滤/裁剪（T060/T061）。

## 验收

- `cargo test -p webview-bridge` 全绿（新增测试 ≥ 5）
- `cargo clippy -p webview-bridge --all-targets -- -D warnings` 零告警
- `cargo check --workspace` 通过

## 结果（完成后填写，≤5 行）
-
