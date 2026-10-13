# T059 · 清 `kafka-app` 的 clippy 告警

**状态**: ✅完成 | **依赖**: T057、T058 | **改动上限**: 3 文件 / 250 行

## 目标
T002 的验收含 `cargo clippy --workspace --all-targets`，所以每个 crate 的存量告警都要清干净。`kafka-core` 是 T055，`kafka-driver` 是 T054，本卡管 `kafka-app`。

## 现状（已实测）
`cargo clippy -p kafka-app --lib` → `CLIPPY_LIB_EXIT=0`，`kafka-app` 自身只有 **1** 条告警：

| 位置 | 告警 | 修法 |
|---|---|---|
| `src/driver_guard.rs:52:22` | redundant cast：casting to the same type is unnecessary (`u32` -> `u32`)，help `self.factor.max(1)` | 去掉 `as u32`。这行是我在 T029 写的代码，属于本仓自产噪声 |

## 必须覆盖
- 先跑 `cargo clippy -p kafka-app --all-targets` 取**最新**清单——测试文件在 T057/T058 之前根本没被编译，clippy 也就没检查过它们。届时可能浮出新的（已知候选：`svc_clusters.rs:134` 未用变量 `alpha`、`svc_consume.rs:302` 未用变量 `second_id`，若 T058 没顺手处理就在这里补）。
- 未用变量优先**补断言**（测试里出现未用变量通常意味着断言漏写了），其次才是 `_` 前缀。
- 清单若超过 3 个文件，按文件拆卡，别扩范围。

## 明确不做（防止范围膨胀）
- 不用 `#[allow(clippy::...)]` 糊告警（除非有明确理由并写注释）
- 不碰 `kafka-core`（T055）和 `kafka-driver`（T054）的告警
- 不改公共 API 签名去迁就 lint

## 验收
- `cargo clippy -p kafka-app --all-targets` 零告警
- `cargo test -p kafka-app` 仍全绿

## 结果（完成后填写，≤5 行）
- 实测清单 3 条（不是卡片预期的 1 条）：`src/driver_guard.rs:52` 冗余 `as u32`、
  `tests/svc_topics.rs:52` 对 `Copy` 的 `TopicChange` 用 `.clone()`、`tests/svc_consume.rs:302` 未用 `second_id`。
- 修法：去掉 `as u32`；`change.clone()` → `*change`；未用变量按卡片要求**补断言**
  —— `assert_eq!(list[1].subscription_id, second_id, "start must keep the caller's subscription id")`
  （`ConsumeService::start` 用调用方给的 id 作注册表 key，`consume.rs:103-110`，所以这条断言有真实含义）。
- 改了 3 个文件（正好到上限），未用 `#[allow]`。
- 验收：`cargo clippy -p kafka-app --all-targets` → kafka-app **零告警**（EXIT=0）；
  `cargo test -p kafka-app` 仍 92 tests 全绿。
- 剩余告警属别的卡：`kafka-core` 2 条（T055）、`kafka-driver` 4 条（T054）。
