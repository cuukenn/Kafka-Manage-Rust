# T079 · Mock 快照 `restore_json` 的恢复范围（整状态替换 vs 只恢复数据）

**状态**: ⬜未开始 | **依赖**: T017 | **改动上限**: 3 文件 / 250 行

## 目标
`restore_json`（`crates/kafka-driver/src/mock/driver.rs` ~:119-131）整体替换 `MockState`，
连 identity/connection 字段（`connected`/`config`/`brokers`/`cluster_id`/`controller_id`）
一起换掉，而 `crates/kafka-driver/tests/mock_persist.rs` 的 lossless 断言（~:73-77）把这种
整状态往返锁成了「正确行为」。阶段 3 接线 restore 后，用户改过 mock 集群的 bootstrap
servers/名称再 restore 一份快照，会拿回**旧 broker 列表 + 幽灵 connected 标志**。
先定恢复范围：**只恢复 data**（topics/groups/round_robin/version），或 **restore 后重新套用
live config**（identity/connection 以当前内存状态为准），再按决定改代码与断言。

**必须在阶段 3 任何接线 snapshot restore 的卡片之前落地**（T038 启动装配、`mock.*` 命令）。

## 只读白名单（最多 3 个）
- `crates/kafka-driver/src/mock/driver.rs` — `restore_json`/`snapshot_json`
- `crates/kafka-driver/src/mock/state.rs` — `MockState` 字段分区（persisted vs live identity）
- `crates/kafka-driver/tests/mock_persist.rs` — 现有 lossless 往返断言怎么写

## 要改的文件
- 修改 `crates/kafka-driver/src/mock/driver.rs`（restore 按范围合并）
- 修改 `crates/kafka-driver/tests/mock_persist.rs`（按新范围改写/保留断言）
- 必要时修改 `crates/kafka-driver/src/mock/state.rs`

## 必须覆盖
- 改过 config（bootstrap servers/名称）后 restore：live identity/connection 字段保持当前值
- data 字段（topics/groups/round_robin/version）仍然来自快照
- 「restore after config change keeps live identity」新测试
- 既有「快照往返不丢 data」语义不回退

## 明确不做（防止范围膨胀）
- 不动 SQLite 槽位（`store/settings.rs` 的 save/load_mock_snapshot，那是应用侧路径，见索引 §3 裁定）
- 不做 T078 的 RNG 播种

## 验收
- `cargo test -p kafka-driver` 全绿（含 `--test mock_persist`）
- 新增「改配置后 restore 保留 live identity」测试
- `cargo clippy -p kafka-driver --all-targets -- -D warnings` 零告警

## 结果（完成后填写，≤5 行）
-
