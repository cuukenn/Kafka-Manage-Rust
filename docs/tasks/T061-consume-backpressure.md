# T061 — 背压与丢弃策略测试：中间通道容量 + drop-oldest 策略

状态: ⏸　依赖: T060　验证: `cargo test -p kafka-app --test consume_backpressure`　类型: 测试+最小产品码

## 范围（与 T060 不重叠）
新测试 `crates/kafka-app/tests/consume_backpressure.rs` 聚焦**中间通道层**：
1. `EventBus::new(capacity)` 小容量（如 8）：publish 16 条 → 慢订阅者 recv 得到的是**最新** 8 条（drop-oldest 断言，按事件内可辨负载验证顺序）。
2. 多订阅者独立 Lagged：A 慢 B 快 → B 不丢、A 丢且 lag_count 累计精确。
3. MIN_CAPACITY 下限：`new(0)` 不 panic、行为=MIN_CAPACITY。
4. 背压端到端：mock 流 → ConsumeService 转发任务 → 小容量 bus → 慢 BusReceiver：断言转发任务**不阻塞**（delivered 计数持续增长、订阅状态保持 running）、丢弃计入 T060 的 dropped 字段（若 T060 已实现归属机制则复用；未实现则此断言降级为 delivered 单调+无死锁）。
5. 关闭路径：bus 所有订阅 drop 后 publish 返回 0 不 panic（已有行为回归钉桩）。

## 边界与预算
产品码改动 ≤2 文件 ≤60 新行（预期≈0，纯钉桩测试）；测试文件豁免。

## 验证
`cargo test -p kafka-app --test consume_backpressure` + workspace 全绿 + clippy 零。
