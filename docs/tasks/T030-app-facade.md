# T030 · `App` 门面装配 + 冒烟测试

**状态**: ✅完成 | **依赖**: T025–T029 | **改动上限**: 3 文件 / 250 行

## 目标
一个 `App` 结构把 store + bus + 5 个 service 装起来，给桥接层唯一入口。

## 只读白名单（最多 3 个）
- `crates/kafka-app/src/services/*.rs`（各 service 的构造签名）
- `crates/kafka-app/src/store/mod.rs`、`crates/kafka-app/src/bus.rs`
- `crates/kafka-app/src/driver_guard.rs`

## 要改的文件
- 修改 `crates/kafka-app/src/lib.rs`：`App { store, bus, clusters, topics, produce, consume, groups }`，`App::new(Store, EventBus)`、`App::builder()`
- 新建 `crates/kafka-app/tests/app_smoke.rs`
- （可选）`crates/kafka-app/src/error.rs`：把 `StorageError` 与 `DriverError` 统一到 `AppError`

## 必须覆盖（冒烟链路，全 Mock）
1. 建内存 SQLite → 保存一个 `mock=true` 集群
2. `clusters.connect()` → `topics.create()` → `topics.list()` 能看到
3. `produce.send_one()` → `consume.start()` 从 earliest → 总线收到该消息
4. `groups.reset()` → 事件里有 `GroupChanged`
5. `consume.stop()` → `SubscriptionDone`
6. 关掉 App 后无残留任务

## 明确不做
- 不做 SQLite 落盘到真实数据目录（T038 启动时做）
- 不做前端、不做命令路由

## 验收
- `cargo test -p kafka-app` 全绿（含之前所有测试）
- `cargo clippy --workspace --all-targets` 无新增告警

## 结果（完成后填写，≤5 行）
- `lib.rs`：`App{store,bus,clusters,topics,produce,consume,groups}` + `App::new(Store, EventBus)` + 最小 `App::builder()`（store/bus 可选，缺省内存库/默认总线）+ 7 个访问器 + `shutdown()`（stop_all→disconnect_all）；可选 `src/error.rs` 按裁量跳过（`ServiceError` 已覆盖冒烟链路，YAGNI）。
- `tests/app_smoke.rs` 2 测覆盖 6 步链路，全 Mock、`start_paused` 虚拟时钟 + 有界超时；步骤 6 用 `RuntimeMetrics::num_alive_tasks()` 断言回落到启动基线（`MockState::active_pumps` 经公共服务 API 不可达，未加外部访问器），并有正向对照断言。
- 验收：`cargo test -p kafka-app` 94/94 绿（debug exe 被 AV 锁 LNK1104 时按已知 flake 改 `--release` 复验）；`cargo clippy --workspace --all-targets -- -D warnings` 零告警；新增 2 文件 / 250 行整。
