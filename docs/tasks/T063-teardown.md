# T063 — 资源回收：订阅终止 / 流释放 / 干净退出

状态: ⏸　依赖: T027　验证: `cargo test -p kafka-app --test teardown`　类型: 产品码+测试

## 裁定 R29（管辖本卡）
`ConsumeService::stop()` 现用 `task.abort()` 硬掐。改为**优雅收尾**：置 finished + 发 Stop 命令 → `tokio::time::timeout(2s, join)` → 超时才 abort。转发任务收到流结束/Stop 应自然退出（grep consume.rs 转发循环确认已响应流终止）。

## 范围
1. 产品码：stop() 优雅化（如上）；确认转发任务在 driver 流结束（Ok(None)/Err）时自行退出并置 Done+SubscriptionDone（若已有则钉桩）。
2. 新测试 `crates/kafka-app/tests/teardown.rs`：
   - 订阅→stop→mock `active_pumps()` 最终归 0（轮询+超时上限 2s，别 sleep 死等）。
   - 有界订阅读尽自然 Done 后：任务退出、active_count 减、prune_done 清账。
   - `App::shutdown()` 幂等：连调两次不 panic；shutdown 后 active_count=0、active_pumps=0。
   - stop_all 与单 stop 竞态：并发调用同 id 只发一次 SubscriptionDone（delivered 事件计数断言）。
   - 断集群→stop_all 联动回归（T030 已有行为钉桩）。
3. 若发现真泄漏（任务不退出/泵不降），最小修复并说明。

## 边界与预算
产品码 ≤2 文件（consume.rs 主）≤80 新行；测试豁免。

## 验证
`cargo test -p kafka-app --test teardown` + workspace 全绿 + clippy 零。
