# T010 · Mock 测试：生命周期 / cluster_info / capabilities

**状态**: ⬜未开始 | **依赖**: T002 | **改动上限**: 1 文件 / 250 行

## 目标
用集成测试锁住 `MockKafkaDriver` 的连接语义，后续所有任务都依赖这个契约。

## 只读白名单（最多 3 个）
- `crates/kafka-driver/src/driver.rs` — trait 签名、`DriverCapabilities`、`DriverKind`
- `crates/kafka-core/src/cluster.rs` — `ClusterConfig` / `ClusterInfo` 字段
- 需要时看 `crates/kafka-driver/src/mock/driver.rs` 的 `connect`/`disconnect` 实现

## 要改的文件
- 新建 `crates/kafka-driver/tests/mock_lifecycle.rs`

## 必须覆盖
- 未连接时调用 `list_topics` 返回 `DriverError::NotConnected`（用 `matches!` 或 `kind()` 断言）
- `connect(cfg)` 成功 → `is_connected() == true` → `cluster_info()` 返回 broker 列表非空、controller 在 broker 集合内
- `disconnect()` 后再调用 → 又变成未连接
- `capabilities()`：Mock 下 `fault_injection == true`、`fixtures == true`、`live_stream == true`
- `kind() == DriverKind::Mock`
- 重复 `connect` 不 panic（幂等或明确错误，按现有实现断言）

## 明确不做
- 不改生产代码。发现缺陷 → 在卡片「结果」里列出，登记到 T016，本会话不修
- 不测 topic/produce/consume（T011+ 各自负责）

## 验收
- `cargo test -p kafka-driver --test mock_lifecycle` 全绿
- 测试数 ≥ 6

## 结果（完成后填写，≤5 行）
-