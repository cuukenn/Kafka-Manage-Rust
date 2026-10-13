# T015 · Mock 测试：故障注入 + `DriverEvent` 事件流

**状态**: ✅完成 | **依赖**: T010 | **改动上限**: 1 文件 / 250 行

## 目标
验证「Mock 模拟异常」和「驱动事件总线」两条能力，它们是 T029（超时重试）和 T062（重连）的测试夹具。

## 只读白名单（最多 3 个）
- `crates/kafka-driver/src/mock/fault.rs` — `FaultInjection` 字段、`check()`、`latency_ms()`、`set_leader_down()`（已有单测，别重复测纯函数）
- `crates/kafka-driver/src/driver.rs` — `DriverEvent` 枚举、`events()` 默认实现
- `crates/kafka-driver/src/mock/driver.rs` — `set_faults`/`faults`/`request_seed`/事件发送点

## 要改的文件
- 新建 `crates/kafka-driver/tests/mock_faults.rs`

## 必须覆盖
- 注入高失败率 → 多次调用后至少一次返回错误，且错误 `is_retryable()` 为 true
- 注入延迟 → 调用耗时明显增加（用 `elapsed()` 下界断言，别用上界，避免抖动误报）
- `set_leader_down(topic, partition, true)` → 该分区 produce 失败（NotLeader/类似），其它分区正常
- `FaultInjection::clear()` 后恢复正常
- `events()` 收到 `TopicCreated` / `TopicDeleted` / `MessagesAppended` / `GroupOffsetsReset`
- 两个 `events()` 订阅者都能收到（broadcast 语义）

## 明确不做
- 不改生产代码；缺陷登记 T016
- 不测 `fault.rs` 内部纯函数（已有单测）

## 验收
- `cargo test -p kafka-driver --test mock_faults` 全绿，测试数 ≥ 6
- 用固定 seed 的随机（`request_seed` 或 `Rng::new(seed)`）保证可重复，禁止偶发失败

## 结果（2026-10-14）
- 新建 `crates/kafka-driver/tests/mock_faults.rs`（239 行，7 个测试全绿，0 ignored；未触发 T016 缺陷）。
- 覆盖：失败率 `1.0` 恒失败 / `0.0` 从不失败 + 超时种类热切换 + `clear()` 恢复；固定延迟用暂停钟 `tokio::time::Instant` 做下界断言；`set_leader_down` 分区级 produce/subscribe 失败、邻分区正常；`events()` 广播语义（`TopicCreated`/`MessagesAppended`/`TopicDeleted`/`GroupOffsetsReset`，双订阅者同收 + dry-run 不发事件 + 掉线订阅者不干扰）；`fail_connect` 拒绝连接 + disconnect 清状态。
- 验收：`cargo test -p kafka-driver --test mock_faults` 7/7；整 crate **57** 个测试全绿（lib 11 + mock_faults 7 + 其余 5 个 mock 集成目标 Topics 8 / lifecycle 7 / produce_consume 10 / stream_ctl 6 / groups 8 全部复跑通过，其中 mock_groups 因安全软件锁定其 debug 可执行文件改用 `--release` 跑 8/8）；`cargo clippy -p kafka-driver --all-targets -- -D warnings` 零告警。
- 备注：卡片里的 `PartitionOffsetsUpdated`/`GroupChanged` 两个事件名在 `DriverEvent` 中不存在（已按实际 enum：`GroupOffsetsReset` 等覆盖）；`ConnectionLost` 死 variant（无人 emit）登记备查，不阻塞本卡。