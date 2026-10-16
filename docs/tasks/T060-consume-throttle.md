# T060 — 消息节流/背压限制：限制与丢弃策略（测试优先）

状态: ⏸　依赖: T037,T027　验证: `cargo test -p kafka-app --test consume_throttle`　类型: 测试+最小产品码

## 既有机制（勿重造）
- EventBus=broadcast，Lagged=drop-oldest+全局 lag_count（bus.rs:192-261）。
- EventPump 批节流 200/50ms + Lagged warn（pump.rs）。T037 已测批形状。

## 范围
1. 新测试 `crates/kafka-app/tests/consume_throttle.rs`：
   - mock 高速 produce（如 5000 条 follow 订阅）→ 断言 bus 不无界增长（broadcast 容量封顶）、`lag_count` 在慢订阅下增长且为 drop-oldest（旧事件先丢）。
   - 快订阅（及时 recv）→ 全量无丢。
   - 订阅转慢→恢复：Lagged 后 recv 继续可用，不 panic 不死锁。
2. 订阅级丢弃可见性（R27）：`SubscriptionInfo` 加 `dropped: u64`（该订阅转发任务 publish 时检测 `tx.receiver_count()==0`? 不——用 bus lag 无法按订阅归属。**实现选择交给你**：最简=转发任务统计 `publish` 返回值 0（无接收者）次数记为 dropped；或对比 delivered 与流计数）。`consume.list` 的 SubscriptionInfo serde 加字段（camelCase `dropped`），前端零改动（多余字段无害）。
3. 若测试暴露真缺口（如 publish 阻塞/无界缓冲），最小修复并说明。

## 边界与预算
产品码 ≤3 文件（consume.rs、bus.rs 可选、protocol/dto 若 SubscriptionInfo 在 core）≤120 新行；测试文件豁免行数。不改 wire 事件名。

## 验证
`cargo test -p kafka-app --test consume_throttle` + `cargo test --workspace` 全绿（239+新增）+ clippy 零。
