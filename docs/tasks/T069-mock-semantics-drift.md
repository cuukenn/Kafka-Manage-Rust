# T069 · Mock 语义漂移：list_offsets 未知 topic + SearchFilter::default

**状态**: ⬜未开始 | **依赖**: T011, T012 | **改动上限**: 3 文件 / 250 行

## 目标
修掉 T011/T012 测试期间登记的两处语义漂移（Mock/Real 语义对齐是本仓核心约束）：
1. `list_offsets` 对**未知 topic** 静默跳过返回空（`mock/driver.rs:416-433`），而 `describe_topics` 同场景返回
   `TopicNotFound` —— 前端无法区分「不存在该 topic」与「空 topic」。
2. `SearchFilter` 的 derive `Default` 给出 `match_value: false`，而 serde 默认是 `true`
   （`kafka-core/src/message.rs:189-210`）—— Rust 侧 `SearchFilter::default()` 与 wire 侧 `{}` 语义不一致。

## 只读白名单（最多 3 个源文件，别读更多）
- `crates/kafka-driver/src/mock/driver.rs` — `list_offsets`（:416 起）与 `describe_topics` 的错误约定
- `crates/kafka-core/src/message.rs` — `SearchFilter` 定义与 serde 属性（:189-210）
- `crates/kafka-driver/src/driver.rs` — `list_offsets` 签名（:89，返回 `Vec<OffsetRange>`，无逐项错误通道）

## 要改的文件
- 修改 `crates/kafka-driver/src/mock/driver.rs` — 未知 topic → `Err(DriverError::TopicNotFound)`（对齐 `describe_topics`）
- 修改 `crates/kafka-core/src/message.rs` — 手写 `impl Default for SearchFilter`，`match_value: true` 对齐 serde；
  同文件加内联单测锁定 `Default == serde 默认`
- 修改 `crates/kafka-driver/tests/mock_topics.rs` — 加回归测试：未知 topic 的 `list_offsets` → `TopicNotFound`
  （结构模式 + `kind()`）；已知 topic 空分区列表行为不变

## 必须覆盖
- RED-first：两条新断言在修复前必须失败（未知 topic 现返回空 Ok；`SearchFilter::default().match_value` 现为 false）
- 修复后：未知 topic → `TopicNotFound`；`SearchFilter::default()` 与 `serde_json::from_str("{}")` 字段逐项相等
- 既有 57 个 kafka-driver 测试 + kafka-core 测试全部不回归（T012 的 filter 夹具显式写了 `match_value: true`，应不受影响）

## 明确不做（防止范围膨胀）
- 不给 `list_offsets` 加逐项错误通道（DTO 形状改动属协议级，另卡）
- 不动 `real` 驱动；不处理 `DriverEvent::ConnectionLost` 死 variant（留给阶段 6）
- 不改任何既有测试断言

## 验收
- `cargo test -p kafka-core` 全绿（含新 Default 单测）
- `cargo test -p kafka-driver` 全绿（含新回归测试；AV 锁 mock_groups 时用 `--release` 复跑）
- `cargo clippy --workspace --all-targets -- -D warnings` 零告警

## 结果（完成后填写，≤5 行）
- 已完成：`list_offsets` 对未知 topic 由静默跳过改为 `Err(DriverError::TopicNotFound)`（对齐 `describe_topics`）；`SearchFilter` 改手写 `Default`，`match_value: true` 对齐 serde。
- RED 先行：2 条新断言修复前均失败（未知 topic 曾返回空 `Ok([])`；`default().match_value == false` vs serde `true`），修复后转 GREEN。
- 验证：`cargo test -p kafka-core` 6 过 / `cargo test -p kafka-driver` 65 过（T012 filter 夹具无回归）/ `cargo clippy --workspace --all-targets -- -D warnings` 零告警。
