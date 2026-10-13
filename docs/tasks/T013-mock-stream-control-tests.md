# T013 · Mock 测试：流控制 pause / resume / seek / cancel

**状态**: ⬜未开始 | **依赖**: T012 | **改动上限**: 1 文件 / 250 行

## 目标
锁住消息流的控制指令语义（前端「暂停/恢复/停止」按钮的契约）。

## 只读白名单（最多 3 个）
- `crates/kafka-core/src/message.rs` — `MessageStream`、`StreamCommand`、`pause()`/`resume()`/`seek()`/`cancel()` 之类的方法与 `id`
- `crates/kafka-driver/src/mock/driver.rs` — `subscribe` 返回流的地方
- 需要时看 `crates/kafka-driver/src/mock/pump.rs`

## 要改的文件
- 新建 `crates/kafka-driver/tests/mock_stream_ctl.rs`

## 必须覆盖
- `pause` 后一段时间内不再收到消息；`resume` 后恢复推送
- `seek` 到指定 offset 后，下一条消息 offset 正确
- `cancel`/drop 后：流结束，`recv` 返回错误/None，且 pump 任务退出（可用 `tokio::time::timeout` 断言不再有新消息）
- 同一 topic 开两个订阅互不影响
- 订阅 drop 后驱动仍可正常 `produce`（无死锁）

## 明确不做
- 不改生产代码；缺陷登记 T016
- 不做节流/背压（阶段 5 的 T060/T061）

## 验收
- `cargo test -p kafka-driver --test mock_stream_ctl` 全绿，测试数 ≥ 5

## 结果（完成后填写，≤5 行）
- 新建 `crates/kafka-driver/tests/mock_stream_ctl.rs`（210 行，6 个测试，仅公开 `KafkaDriver` trait + `StreamControls`）；`cargo test -p kafka-driver --test mock_stream_ctl` 6/6 全绿（0.44s 含编译，测试本体 0.00s，0 ignored），clippy 零告警。
- 覆盖：pause 静默且流不结束 + resume 补齐顺序 + 恢复后仍活跃；seek 重定位游标并精确回放；seek 无关分区被忽略；stop 后流结束且后续消息不送达；drop 后流结束且 driver 无死锁、日志续写；同 topic 双订阅互不影响（各有游标）。
- 未改任何生产代码，未触发 T016；pump 任务退出只按可观察契约断言（流结束 / 静默），未加计数器，T068 落地不受影响。