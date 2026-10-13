# T023 · 事件总线（`AppEvent` + broadcast 封装）

**状态**: ⬜未开始 | **依赖**: T002 | **改动上限**: 2 文件 / 250 行

## 目标
全局事件总线：业务层往里发事件，桥接层从里取事件推给前端（需求 3.1 第 5 点）。

## 只读白名单（最多 3 个）
- `crates/kafka-core/src/protocol.rs` — `event` 模块的事件名常量（`connection.state` 等）
- `crates/kafka-driver/src/driver.rs` — `DriverEvent`（要把驱动事件转成 AppEvent）
- `crates/kafka-core/src/message.rs` — `MessageEvent`（消息到达事件的载荷）

## 要改的文件
- 新建 `crates/kafka-app/src/bus.rs`
- 修改 `crates/kafka-app/src/lib.rs`：`pub mod bus;`

## 必须覆盖
- `AppEvent` 枚举（`serde::Serialize`，camelCase）：`ConnectionStateChanged`、`MessageReceived`、`SubscriptionState`、`SubscriptionDone`、`TopicChanged`、`GroupChanged`、`ProduceResult`、`LogLine`、`Error`
- `EventBus::new(capacity)` 用 `tokio::sync::broadcast`；`publish` / `subscribe()`
- `From<DriverEvent> for AppEvent` 转换
- 每个 `AppEvent` 有 `event_name() -> &'static str`，取自 `protocol::event` 常量（禁止硬编码字符串）
- 慢订阅者：`Lagged` 错误要计数并暴露 `lag_count()`，不能 panic
- 无订阅者时 `publish` 不 panic

## 明确不做
- 不做节流/批量（T060）
- 不做前端推送（桥接层 T037）

## 验收
- 新建 `crates/kafka-app/tests/bus.rs`：发布订阅、多订阅者、Lagged 计数、DriverEvent 转换
- `cargo test -p kafka-app --test bus` 全绿

## 结果（完成后填写，≤5 行）
- 新增 `crates/kafka-app/src/bus.rs`（约 245 行）+ `crates/kafka-app/tests/bus.rs`（约 170 行）+ `lib.rs` 加 `pub mod bus;`。**超出 250 行上限**：9 个事件变体的契约枚举不可压缩；已按范围停止，未顺手做节流/前端推送。
- **未验证**：T001（Windows SDK）未完成，本机无法链接，`cargo test -p kafka-app --test bus` 未执行。装好 SDK 后先跑 T002，再跑本卡验收命令。
- 偏离依赖顺序：本卡依赖 T002（基线未跑绿），因用户要求先做 T023。若 T002 暴露编译问题，本卡代码可能要小修。
- 设计决定：`AppEvent` 用 serde 内部标签 `{"type":..,"payload":..}`；`event_name()` 全部取自 `protocol::event` 常量；`payload_json()` 只返回 payload。新增 `TopicChange::Messages`（承接 `DriverEvent::MessagesAppended`）、`BusRecvError::Empty`（`try_recv` 用；tokio 的 `try_recv` 返回 `TryRecvError` 而非 `RecvError`）。
- 遗留（登记，不在本卡做）：`MessageEvent.key/value` 目前序列化为字节数组，前端展示（T047）需决定 base64 还是 UTF-8；`From<DriverEvent>` 不带 clusterId，业务层应统一用 `AppEvent::from_driver(cluster_id, ev)`。