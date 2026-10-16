# T062 — 重连自动恢复：指数退避表 + 状态事件

状态: ⏸　依赖: T024　验证: `cargo test -p kafka-app --test reconnect`　类型: 产品码+测试

## 裁定 R28（管辖本卡）
重连监督放 **ClusterService**（驱动无关）；GuardedDriver 不动；不加新 wire 事件——复用 `connection.state`（payload {clusterId,state,reason?}，state 已有 degraded/connecting/connected/error）。

## 范围
1. **触发**：运行期调用返回连接类 DriverError（kind：`timeout`/`transport`/`connection_lost` 等——grep kafka-core error.rs ErrorKind 确认可用 kind 集合，选连接类）→ 集群标记 `degraded`（发 connection.state degraded, reason）。
2. **退避表**：1s→2s→4s→8s→16s→30s 封顶（常量表可测）；每次成功 connected 重置。
3. **重连动作**：对 mock/real 统一 = 驱动 `disconnect` + `connect`（grep ClusterService::connect/disconnect 现签名复用）；成功后发 connected，失败发 error 并按表等待下次。
4. **取消**：手动 `disconnect`/`delete` 取消该集群重连任务（token/flag，grep 现有实现风格）；集群断开时 stop_all 已有联动勿破坏。
5. **测试** `crates/kafka-app/tests/reconnect.rs`：mock FaultInjection 注入连接类错误 → 断言 degraded→connecting→connected 事件序列、退避表纯函数单测（不真等 30s：注入小间隔或表可注入加速）、手动 disconnect 后无重连任务（计数/事件断言）。tokio::time::pause 可用则用。

## 边界与预算
产品码 ≤3 文件（clusters.rs 主、可能 bus.rs 小加、lib.rs 接线）≤250 新行；测试豁免。

## 验证
`cargo test -p kafka-app --test reconnect` + workspace 全绿 + clippy 零。
