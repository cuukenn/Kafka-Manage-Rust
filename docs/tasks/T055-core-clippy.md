# T055 · 清 kafka-core 的存量 clippy 告警

**状态**: ✅完成 | **依赖**: T052 | **改动上限**: 1 文件 / 250 行

## 目标
`cargo clippy -p kafka-core --all-targets` 还剩 2 条存量告警（都在 `dto.rs`，T052 之前就在）。清掉它们，`cargo clippy --workspace` 才可能干净（T002 基线验收）。

## 只读白名单（最多 3 个源文件，别读更多）
- `crates/kafka-core/src/dto.rs` — 两处 `map(..).flatten()`，以及 base64 + utf8 双通道的既有约定

## 要改的文件
- 修改 `crates/kafka-core/src/dto.rs`

## 必须覆盖
1. `dto.rs:110` `map(..).flatten()` on `Option` → `and_then(|k| B64.decode(k).ok())`
2. `dto.rs:127` 同上 → `and_then(|v| B64.decode(v).ok())`
3. 改完确认语义不变：base64 解不开时仍然是 `None`（走 utf8 兜底通道），不要把错误码语义改掉

## 明确不做（防止范围膨胀）
- 不改 DTO 的字段名 / serde 形状（前端契约，动了要同步 `frontend/`）
- 不碰 `kafka-driver`（T054）
- 不引入 `#[allow]`

## 验收
- `cargo clippy -p kafka-core --all-targets` 零告警
- `cargo test -p kafka-core` 仍全绿（4 passed）

## 结果（完成后填写，≤5 行）
- 只改 1 个文件 `crates/kafka-core/src/dto.rs`：`to_event()` 里两处
  `.map(|k| B64.decode(k).ok()).flatten()` → `.and_then(|k| B64.decode(k).ok())`（语义等价，
  `Option<Option<T>>::flatten() == and_then`）。
- 语义未变：base64 解不开仍回落到 `None` → 走 utf8 兜底通道；DTO 字段名/serde 形状未动。
- 验收：`cargo clippy -p kafka-core --all-targets` **零告警**；`cargo test -p kafka-core` 5/5
  （卡片写的 4 条是 T065 之前的计数，现含 `error_dto_new_uses_the_retry_classification`）。
- 全仓连带结果：`cargo clippy --workspace --all-targets -- -D warnings` 现在 EXIT=0（零告警）。
