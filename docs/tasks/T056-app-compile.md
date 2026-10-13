# T056 · 修 `kafka-app` 首次编译报错（7 个）

**状态**: ✅完成 | **依赖**: T053 | **改动上限**: 3 文件 / 250 行

## 目标
`kafka-core` / `kafka-driver` 已绿（T052/T053）。`cargo check -p kafka-app --all-targets` 现在只报 **7 个错误**，全在 `src/`，都是机械类型/可见性问题。修完它，T020–T029 那批从未编译过的代码和 T051 的 `Debug` 修复才第一次被真正验证，`tests/svc_*.rs`、`tests/driver_guard.rs` 也才能跑起来。

## 全部 7 个错误（已确诊，直接照定位改）
| # | 位置 | 报错 | 修的方向 |
|---|---|---|---|
| 1 | `src/services/groups.rs:6:26` | E0603 `struct ClusterId is private` | ⚠️ **本卡原写法方向反了**。实际：`ClusterId` 定义在 `kafka-core/src/lib.rs:26`（公开），`cluster.rs:5` 里那个是**私有** `use crate::ClusterId;` → 正确修法是 `use kafka_core::ClusterId;`（全仓其余 12 处都这么写） |
| 2 | `src/driver_guard.rs:256:30` | E0308 expected `&mut Duration`, found `Duration` | 见 #3，两处是一对 |
| 3 | `src/driver_guard.rs:258:32` | E0308 expected `Duration`, found `&mut Duration` | 退避/超时那对调用与签名不一致；确认 helper 到底要值还是 `&mut`，统一它 |
| 4 | `src/driver_guard.rs:360:26` | E0308 expected `tokio::time::Instant`, found `std::time::Instant` | 用 `tokio::time::Instant::now()`（`#[tokio::test(start_paused)]` 下只有它能被虚拟时钟推进） |
| 5 | `src/bus.rs:210:70` | E0308 expected `&Arc<AtomicU64>`, found `&AtomicU64` | 调用点多剥/少剥一层 `Arc` |
| 6 | `src/services/clusters.rs:69:27` | E0599 `no method copied` on `Result<MutexGuard<ConnectionState>, PoisonError>` | **T051 遗留**：手写 `Debug` 里的 `state_of()` 取锁写法不对。改成 `.lock().map(|s| *s)`（确认 `ConnectionState` 是 `Copy`/`Clone`）或 `.lock().unwrap_or(...)` 后按引用打印 |
| 7 | `src/services/mod.rs:63:62` | E0308 expected `AnyError`, found `Error` | **T052 遗留**：`DriverError::Other` 的载荷已从 `anyhow::Error` 换成 `AnyError`，这个调用点要改成 `DriverError::from(...)` 或 `AnyError::from(...)` |

## 只读白名单（最多 3 个源文件，别读更多）
- `crates/kafka-app/src/driver_guard.rs` — #2/#3/#4
- `crates/kafka-app/src/services/clusters.rs` — #6（顺带确认 T051 的手写 `Debug` 是否成立）
- `crates/kafka-app/src/services/mod.rs` — #7

`groups.rs`（#1）和 `bus.rs`（#5）各只有一行，改 import / 改一层解引用即可，**不要**把它们算进白名单去通读。

## 必须覆盖
- 修完 7 个错误后，`cargo check -p kafka-app --all-targets` 会往下走：`tests/*.rs`（`svc_clusters`、`svc_topics`、`svc_produce`、`svc_consume`、`svc_groups`、`store_*`、`bus`、`driver_guard`）此前根本没被编译过，很可能再冒新错。**若测试文件报错，登记新卡（T057+），不要在本卡里扩范围**
- 确认 T051 的两处 `Debug` 修复（`Slot` 手写 impl、`EventBus::Inner` derive）确实成立 → 成立就把 T051 记 ✅
- 确认 `driver_guard.rs` 的超时计时用的是 `tokio::time::Instant`，否则 `start_paused` 的测试里超时不会触发

## 明确不做（防止范围膨胀）
- 不改 `KafkaDriver` trait、不改 `AnyError` 的设计（只改调用点）
- 不改锁结构、不改 `Slot` 的字段
- 不写新测试（T010–T015 / T030 的事）
- 不碰 clippy 告警（T054/T055）

## 验收
- `cargo check -p kafka-app --lib` 全绿 → ✅ 达成（`LIB_EXIT=0`）
- ~~`cargo check -p kafka-app --all-targets` 全绿~~ → 未达成：`src/` 绿之后 `tests/*.rs` 浮出 **9 个新错误 / 5 个文件**，按本卡规则登记为 **T057**，未在本卡扩范围
- ~~`cargo test -p kafka-app` 全绿~~ → 依赖 T057
- `cargo clippy -p kafka-app --lib` → `CLIPPY_LIB_EXIT=0`；`kafka-app` 自身只有 **1** 条告警（`driver_guard.rs:52` 冗余 `u32 -> u32` 转换，是我在 T029 写的代码），登记为 **T058**

## 结果（完成后填写，≤5 行）
- 7 个错误全部修掉，`cargo check -p kafka-app --lib` → `LIB_EXIT=0`，`kafka-app` 的 `src/` 第一次编译通过（5 文件 / 约 8 行改动）。
- #1 的本卡定位是**反的**：`ClusterId` 公开定义在 `kafka-core/src/lib.rs:26`，`cluster.rs:5` 的 `use` 才是私有的；已按 `use kafka_core::ClusterId;` 修，并改正了本卡表格。
- #4 不只是编译修复，是**真 bug**：`driver_guard.rs` 原来用 `std::time::Instant`，在 `start_paused` 下虚拟时钟不推进，`remaining()` 与 `timeout_at` 都会算错 → 已换成 `tokio::time::Instant`。
- #2/#3 是一对：`pause.min(remaining)` 让 rustc 把 `Ord::min` 选在 `Self = &mut Duration` 上，改成 `Duration::min(*pause, remaining)` 一处解决两个错误。
- T051 的两处 `Debug` 修复（`Slot` 手写 impl、`EventBus::Inner` derive）**成立**，已记 ✅。
