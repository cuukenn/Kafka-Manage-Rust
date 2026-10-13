# T064 · 修 `AppEvent` 的 serde tag 与 `protocol::event::*` 漂移（camelCase vs 点号）

**状态**: ✅完成 | **依赖**: T057（编译打通后才现形） | **改动上限**: 3 文件 / 250 行

## 目标

T057 让 `tests/svc_groups.rs` 第一次编译通过后，用例
`reset_to_earliest_writes_offsets_and_publishes_once` 失败：
`json["type"]` 得到 `"groupChanged"`，期望 `"group.changed"`。

根因不在测试：`crates/kafka-app/src/bus.rs:54` 的
`#[serde(rename_all = "camelCase", tag = "type", content = "payload")]`
里 `rename_all` **同时**作用于字段名（期望：`cluster_id → clusterId`）和
**variant 名**（副作用：variant 名就是内部 tag 的值）→ 序列化出的 `type`
与 `AppEvent::event_name()`（取自 `kafka_core::protocol::event::*`）不一致。
`bus.rs:107` 的注释「taken from the shared constants so Rust and JS cannot drift」
因此不成立。

## 权威（为什么选点号，不选 camelCase）

- `crates/kafka-core/src/protocol.rs:48-58` 的 `event::*` 常量是前后端共享的 wire 契约，
  点号风格，和 `cmd::*` 的命名域（`cluster.* topic.* produce.* consume.* group.* config.* mock.*`）一致。
- 测试侧现状 2:1 支持点号：`tests/svc_groups.rs:214`（`group.changed`）、
  `tests/svc_produce.rs:256`（`produce.result`）期望点号；只有 `tests/bus.rs:133` 期望 camelCase。
- 前端尚未开工（阶段 4 全 ⏸），改 wire 名的成本此刻≈0；越晚改越贵。

## 要改的文件

- 修改 `crates/kafka-app/src/bus.rs`（9 个 variant 各加显式 `#[serde(rename = "...")]`，
  值取自 `protocol::event::*` 的字符串；保留 `rename_all = "camelCase"` 只管字段）
- 修改 `crates/kafka-app/tests/bus.rs`（`:133` 的期望改成 `event::TOPIC_CHANGED`，
  并新增一条断言：`serde_json` 的 tag 必须等于 `event_name()` —— 这才是防漂移的回归锁）

## 必须覆盖

| # | 位置 | 现象 | 修法 |
|---|---|---|---|
| 1 | `src/bus.rs:54` | tag 被 `rename_all` 改名 | 每个 variant 显式 `#[serde(rename = "connection.state")]` 等 9 个，与 `event_name()` 一一对应 |
| 2 | `tests/bus.rs:133` | 期望 `"topicChanged"` | 改 `event::TOPIC_CHANGED` |
| 3 | `tests/bus.rs`（同一函数内） | 无漂移守卫 | 加 `assert_eq!(json["type"], ev.event_name())` |

## 明确不做（防止范围膨胀）

- 不改 `payload_json()` 的语义（它只取 `payload` 半边，不受 tag 影响）
- 不改字段命名（仍是 camelCase：`clusterId` / `subscriptionId` …）
- 不动 `tests/svc_groups.rs:214` / `tests/svc_produce.rs:256`（它们已经是对的）
- 不引入 `event` 键名（AGENTS.md 摘要写的是 `{ "event","payload" }`，但现实现与测试都用
  `type`；键名统一是另一件事 → 若要做，另开卡，本卡不动）

## 验收

- `cargo test -p kafka-app --test bus` 全绿
- `cargo test -p kafka-app --test svc_groups --test svc_produce` 全绿
- `cargo test -p kafka-app` 无因本卡引入的新失败

## 结果（完成后填写，≤5 行）
- 改了 2 个文件：`src/bus.rs`（9 个 variant 各加 `#[serde(rename = "<点号>", rename_all = "camelCase")]`，
  enum 上不再留 `rename_all`）+ `tests/bus.rs`（期望改 `event::TOPIC_CHANGED`，并加两条**漂移守卫**：
  单 variant 的 `json["type"] == ev.event_name()`，以及 9 个 variant 全量循环里 tag == `event_name()`）。
- 实测比卡片诊断更进一步：原 `rename_all` 只改 tag、**没**改 payload 字段名 → 一并修（见 ledger 的 T064 Ruling）。
- RED→GREEN：加守卫后 3 failed → 修 bus.rs 后 8 passed（剩 1 条属 T065）。
- 验收：`--test bus` 9/9、`--test svc_groups` 8/8、`--test svc_produce` 7/7 全绿。
- 未做（按卡片）：事件 envelope 的键名仍是 `type`（AGENTS.md 摘要写的是 `event`）→ 若要统一，另开卡。
