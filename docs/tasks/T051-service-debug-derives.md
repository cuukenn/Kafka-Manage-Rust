# T051 · 修掉 `Slot` 与 `EventBus::Inner` 的 `Debug` derive（编译期缺陷）

**状态**: ✅完成 | **依赖**: T056（编译验证） | **改动上限**: 3 文件 / 250 行

## 目标
`crates/kafka-app` 里有**两处** `Debug` derive 必然编译失败，两处都会把整条 service 链拖垮。
根因各只有一处，改两个文件（合计约 10 行）即可全部消掉。

### 根因 1 —— `Slot` 持有 `DynDriver`
`DynDriver = Arc<dyn KafkaDriver>`（`crates/kafka-driver/src/driver.rs:113`），`KafkaDriver` 没有
`Debug` 超 trait，`dyn KafkaDriver` 也不满足 `Debug`，所以 `Mutex<Option<DynDriver>>` 不满足 `Debug`
→ `#[derive(Debug)] struct Slot`（`services/clusters.rs:37`）展开后报
`the trait bound Arc<dyn KafkaDriver>: Debug is not satisfied`。

### 根因 2 —— `EventBus::Inner` 没有 `Debug`
`crates/kafka-app/src/bus.rs:172` 的 `struct Inner { tx: broadcast::Sender<AppEvent>, lag: AtomicU64 }`
**没有** `#[derive(Debug)]`，而 `bus.rs:179` 是 `#[derive(Debug, Clone)] pub struct EventBus { inner: Arc<Inner> }`
→ `Arc<Inner>: Debug` 不成立，同样报 E0277。这一处独立于根因 1，只修 `Slot` 是不够的。

### 级联点（两处根因修完后自动恢复，无需改动）

| 位置 | 类型 | 为什么挂 |
|---|---|---|
| `services/clusters.rs:37` | `struct Slot` | 直接持有 `Mutex<Option<DynDriver>>` ← **根因 1** |
| `bus.rs:179` | `EventBus` | 持有 `Arc<Inner>`，`Inner` 无 `Debug` ← **根因 2** |
| `services/clusters.rs:72` | `ClusterService` | 持有 `Arc<Slot>` + `EventBus` |
| `services/topics.rs:24` | `TopicService` | 持有 `ClusterService` |
| `services/produce.rs:18` | `ProduceService` | 持有 `ClusterService` |
| `services/consume.rs:65` | `ConsumeService` | 持有 `ClusterService` |
| `services/groups.rs:23` | `GroupService` | 持有 `ClusterService` |

> 同类问题已经修过一处：`kafka-app/src/driver_guard.rs` 的 `GuardedDriver` 已改成手写
> `impl fmt::Debug`（`finish_non_exhaustive`）。本卡根因 1 照同一个写法办，保持风格一致。

## 已完成的静态排查（不用再查一遍）
- `Store`（`store/db.rs:27`）**已有手写** `impl std::fmt::Debug`（`db.rs:33`）→ `ClusterService` 的
  `store` 字段成立，`Connection` 不会成为问题。
- 因此根因 1、2 修完后 `ClusterService` 的 `#[derive(Debug, Clone)]` **可以原样保留**，
  不需要手写 impl（本卡早期版本说要把 `ClusterService` 也改成手写，那是多余的）。
- 其余 `Debug` derive 已逐个核对成立：`tokio 1.53.2` 的 `broadcast::Sender/Receiver`、
  `mpsc::Sender/Receiver` 的 `impl<T> fmt::Debug` **无 `T: Debug` 约束**（`broadcast.rs:1740/1752`、
  `mpsc/bounded.rs:742/1637`）；`JoinHandle<T>` 有 `Debug`；`MessageStream`（`kafka-core/src/message.rs:360`）
  有手写 `Debug`（`:366`）；`StreamControls`（`message.rs:319`）有 derive；`ClusterConfig`/`ConnectionState`/
  `TopicInfo`/`ProduceResult`/`SearchFilter`/`MessageEvent`/`SubscriptionId`/`ClusterInfo`/`BrokerInfo`
  全部 derive 了 `Debug`。
- 全仓 `src/` 里不带 `Debug` 的类型只有 4 个：`Store`（手写 ✓）、`MessageStream`（手写 ✓）、
  `BusReceiver`（`bus.rs:221`，没有任何 derive 持有它，**不用改**）、`PlaceholderRealDriver`（`driver.rs:134`，私有占位）。

## 只读白名单（最多 3 个源文件，别读更多）
- `crates/kafka-app/src/services/clusters.rs` — 看 `Slot`（:37）和 `ClusterService`（:72）两个 derive
- `crates/kafka-app/src/bus.rs` — 看 `struct Inner`（:172）与 `EventBus`（:179）
- `crates/kafka-app/src/driver_guard.rs` — 只看 `impl fmt::Debug for GuardedDriver`，抄写法

## 要改的文件
- 修改 `crates/kafka-app/src/services/clusters.rs`
  - `Slot`：去掉 `#[derive(Debug)]`，改手写 `impl fmt::Debug`
  - 顶部补 `use std::fmt;`
  - `ClusterService` 的 `#[derive(Debug, Clone)]` **不动**
- 修改 `crates/kafka-app/src/bus.rs`
  - `struct Inner` 上方加一行 `#[derive(Debug)]`（`broadcast::Sender<AppEvent>` 和 `AtomicU64` 都满足 `Debug`）
- 不要动 `topics.rs` / `produce.rs` / `consume.rs` / `groups.rs` —— 它们的 derive 在两个根因修好后自动成立。

## 必须覆盖
- `Slot` 的手写 `Debug` 打印可安全拿到的信息：`state_of()`、`driver_of().map(|d| d.kind().as_str())`，
  然后 `.finish_non_exhaustive()`（`gate` 是 `tokio::sync::Mutex<()>`，`Debug` 本身没问题，但没必要打）。
- 手写 impl 里**不要**跨锁 await；`Slot` 里 `driver`/`state` 是 `std::sync::Mutex`，
  用 `lock().ok()` 或 `unwrap_or_else(|p| p.into_inner())`，跟同文件现有写法一致，别 panic。
- 保持 `Slot` 其余代码不变（`gate` 的 once-at-a-time 语义是 T024 的设计点）。

## 明确不做（防止范围膨胀）
- 不给 `KafkaDriver` trait 加 `Debug` 超 trait（会让所有 driver 实现被迫写 `Debug`，影响 T070 real 驱动）。
- 不顺手改 `Slot` 的锁结构、不改 `connect`/`disconnect` 逻辑。
- 不给 `BusReceiver` 加 `Debug`（没有需求）。
- 不动 `driver_guard.rs`（已修好）。
- 不试图编译验证 —— T001 未完成前本机没有链接器。

## 验收
- `cargo check -p kafka-app` 通过（当前被 T001 挡住，先以静态审查交付）
- `cargo test -p kafka-app --test svc_clusters` 全绿
- `cargo clippy -p kafka-app --all-targets` 无新增告警

## 结果（完成后填写，≤5 行）
- 已改 2 文件：`services/clusters.rs`（`Slot` 去掉 derive，改手写 `impl fmt::Debug` + `use std::fmt;`）、
  `bus.rs`（`struct Inner` 加 `#[derive(Debug)]`）。`ClusterService` 与 4 个 service 的 derive 一行未动。
- 静态核对已闭环：`Store`/`MessageStream` 有手写 `Debug`，tokio 的 broadcast/mpsc `Debug` 无 `T: Debug` 约束，
  `ClusterId`/`ConnectionState`/`StreamControls` 等全部有 derive → 表中级联点全部成立。
- ~~未验证：T001 未完成，本机无链接器~~ → **已验证**：T001/T052/T053/T056 之后 `cargo check -p kafka-app --lib` → `LIB_EXIT=0`，两处修复成立。手写 impl 里的 `state_of()` 原先写的是 `self.state.lock().copied()`（`Result` 没有 `copied()`），已在 T056 的 #6 修成 `.map(|guard| *guard)`。
