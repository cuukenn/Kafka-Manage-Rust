# T016 · 修复 Mock 驱动缺陷（静态审查发现 + 待测试暴露）

**状态**: ✅完成（2026-10-14 验收全绿） | **依赖**: T001, T010–T015 | **改动上限**: 3 文件 / 250 行

## 目标
把 `MockKafkaDriver` 里「读代码就能确定是错的」的地方修掉，并登记只能等编译/测试才能确认的遗留项。
原索引里这张卡是 `⏸待建卡`（范围由 T010–T015 的失败用例决定）；因为 T001 未解、跑不了测试，本卡先收静态可见的缺陷。

## 只读白名单（最多 3 个）
- `crates/kafka-driver/src/mock/driver.rs` — 驱动实现
- `crates/kafka-driver/src/mock/pump.rs` — 订阅泵
- `crates/kafka-driver/src/mock/fault.rs` — 故障注入 / `Rng`

## 要改的文件
- `crates/kafka-driver/src/mock/driver.rs`

## 已修（本会话，静态可判定）
- `will_seed: Arc<AtomicBool>` 字段缺失、`seeded()` 从未置位 → 种子夹具永远不会灌进去。现补字段 + `seeded()` 调 `request_seed()`，并加 `pub fn seed_pending()`；删掉没用的 `static PENDING_SEED` / `pending_seed_flag()` / `will_seed_cell()`。
- `delete_topics` 里对 `tokio::RwLockWriteGuard` 写了 `drop(state.clone())` 和 `state = state;`（非法且会提前放锁）→ 改为先收集 `deleted: Vec<String>`，`drop(state)` 之后再 `emit(TopicDeleted)`，避免持锁发事件死锁。
- 13 处 `self.require_connected().await;` 丢掉了 `Result`（未连接时照样往下走）→ 全部补 `?`。

## 遗留（必须等 T001 之后才能确认）
- `pump.rs::PumpHandle.join` 字段从未被读取 → 大概率 `dead_code` warning；是否要 `#[allow]` 还是提供 `abort()`，等 clippy 结果再定。
- 其余 warning（未用参数、`needless_*` 之类）只能靠 `cargo clippy -p kafka-driver --all-targets` 暴露。
- 真正的行为缺陷要等 T010–T015 的测试跑起来才知道，届时按失败用例追加子卡，不在本卡内扩范围。

## 明确不做
- 不给 Mock 加新特性（快照、故障类型扩展等属于 T017 / 其它卡）。
- 不动 `real` 驱动。

## 验收
- `cargo check -p kafka-driver` 通过（当前环境做不到 → 状态保持 🔵）
- `cargo test -p kafka-driver` 全绿（依赖 T010–T015）
- `cargo clippy -p kafka-driver --all-targets` 无 warning

## 结果（完成后填写，≤5 行）
- 已改 `crates/kafka-driver/src/mock/driver.rs`：补 `will_seed` 字段与 `seeded()`/`request_seed()`/`seed_pending()` 链路、重写 `delete_topics` 的放锁与事件发射、13 处 `require_connected().await?`。
- 未验证：本机缺 Windows SDK（T001 ⛔），`cargo check/test/clippy` 全部跑不了（`link.exe` 缺失）。
- 遗留项见上，等 T001 解掉后一次性跑绿再关卡。
- 2026-10-14 关卡：`cargo check -p kafka-driver` EXIT=0；`cargo test -p kafka-driver --release` **57/57 全绿**
  （debug 跑 mock_groups 遇 AV 锁 os error 5，属 ledger 已登记 ENV FLAKE）；clippy `-D warnings` 零告警。
- T010–T015 六张测试卡 **0 失败用例** → 本卡无行为修复可做；测试暴露的 2 处语义漂移登记为 **T069**；
  `DriverEvent::ConnectionLost` 死 variant 备注留阶段 6（real 驱动应 emit）。
