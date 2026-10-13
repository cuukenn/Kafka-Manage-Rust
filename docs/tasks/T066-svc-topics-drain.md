# T066 · 修 `svc_topics.rs` 的 `describe_and_offsets_pass_through_to_the_driver`：断言前漏掉了对 `create` 事件的排空

**状态**: ✅完成 | **依赖**: T058（该文件此前从未编译，缺陷不可见） | **改动上限**: 3 文件 / 250 行

## 现象

T058 把 `svc_topics.rs` 编译打通后，`cargo test -p kafka-app --test svc_topics` →
7 passed / **1 failed**：

```
---- describe_and_offsets_pass_through_to_the_driver ----
thread panicked at crates\kafka-app\tests\svc_topics.rs:278:5:
assertion failed: topic_changes(&drain(&mut rx)).is_empty()
```

## 诊断（已确诊，非猜测）

测试流程（`svc_topics.rs:217-278`）：

1. `connected(NO_CACHE)` → `connect` 会发 2 条 `connection.state`；
2. `:219` **`drain(&mut rx)`** ← 只排到这里为止；
3. `:220-231` `svc.create(...)` 成功 → `TopicService::create`（`src/services/topics.rs:128-138`）
   在成功后 **必然** 发一条 `TopicChanged{Created}`；
4. `:233-275` 只做 `describe` / `offsets`（读操作，确实不发事件）；
5. `:278` 断言 `topic_changes(drain(..)).is_empty()` → 队列里还躺着第 3 步那条 `Created` → 失败。

排除「驱动重复推送」这条可能：`ClusterService::connect`（`src/services/clusters.rs:195-233`）
**没有** spawn 把 `driver.events()` 转发到总线的 pump，所以 mock 驱动自己发的
`DriverEvent::TopicCreated` 在这个 harness 里根本进不了总线；
`driver_guard::events()`（`src/driver_guard.rs:416-418`）也只是原样转发、不重复订阅。
→ 队列里那条 `Created` 只可能来自 service 本身，**不是**重复推送缺陷。

同文件 `:211` 的 delete 测试正是用 `vec![(format!("{id}:orders"), TopicChange::Deleted)]`
断言「写操作恰好发一条」，所以本卡沿用同一写法。

## 结论：这是测试缺陷，不是产品缺陷

被验证的不变量（「读操作不发领域事件」）没有被违反；是测试**自己**在断言前没排空
写操作产生的事件。修法要让断言真的能验证不变量，而不是把断言删掉。

## 要改的文件

- 修改 `crates/kafka-app/tests/svc_topics.rs`（`create` 之后补一次排空，
  并顺手锁住「写操作恰好发一条 `Created`」——这同时证明没有重复推送）

## 验收

- `cargo test -p kafka-app --test svc_topics` 全绿（8/8）
- `cargo check -p kafka-app --all-targets` 保持全绿

## 明确不做

- 不给 `ClusterService::connect` 加驱动事件 pump（那是 T030/T032 的桥接范围）
- 不改 `TopicService::create` 的发布语义
- 不删断言、不把 `:278` 改成更弱的形式

## 结果（完成后填写，≤5 行）
- 定性为**测试缺陷**（不是产品缺陷）：`create` 之后必然发一条 `TopicChanged{Created}`
  （`services/topics.rs:128-138`），而测试在写入**之前**就 drain 了 → 自己写的那条还堵在队列里。
- 排除了「重复发布」：`ClusterService::connect`（`clusters.rs:195-233`）不 spawn 任何把
  `driver.events()` 转接到 bus 的 pump，`GuardedDriver::events()`（`driver_guard.rs:416-418`）原样转发不重订阅
  → mock 自己的 `DriverEvent::TopicCreated` 在这个 harness 里根本到不了 bus。
- 修法（1 文件）：在 `.expect("create")` 后补
  `assert_eq!(topic_changes(&drain(&mut rx)), vec![(format!("{id}:orders"), TopicChange::Created)]);`
  —— 既排空写入事件，又把「恰好一条、不重复发布」锁死，与 `:211` 的 delete 测试同形。
- 验收：`--test svc_topics` 8/8、`--test svc_clusters` 10/10。
- 事故记录：编辑时 `oldString` 越界到下一条语句而 `newString` 没写回 → 误删 `let details = svc`，
  已按原样补回（教训：跨语句边界的 oldString 必须在 newString 里回显该语句）。
