# T014 · Mock 测试：消费组 / 位点查询 / reset / commit

**状态**: ⬜未开始 | **依赖**: T011 | **改动上限**: 1 文件 / 250 行

## 目标
锁住消费组相关语义，供 T028（GroupService）与前端消费组页依赖。

## 只读白名单（最多 3 个）
- `crates/kafka-core/src/group.rs` — `GroupInfo`/`GroupDetail`/`ListGroupOffsetsReq`/`GroupOffsetEntry`/`ResetOffsetsReq`/`ResetOffsetsResult`/`CommitOffsetsReq`
- `crates/kafka-driver/src/driver.rs` — 消费组方法签名
- 需要时看 `crates/kafka-driver/src/mock/state.rs` 里的 group 结构

## 要改的文件
- 新建 `crates/kafka-driver/tests/mock_groups.rs`

## 必须覆盖
- `list_consumer_groups` 初始状态（空 或 内置 fixture 组，按实现断言）
- `describe_consumer_groups`：成员、分配分区、state 字段
- `list_group_offsets`：committed 与 lag 的数值关系正确（lag = end - committed）
- `reset_offsets` 到 earliest / latest / 指定 offset / 指定时间戳 四种策略各自结果正确
- `reset_offsets` 的 `dry_run=true`：不落库，只返回将应用的条目
- `commit_offsets` 后 `list_group_offsets` 能看到新位点
- 不存在的 group → 明确错误（不是 panic）

## 明确不做
- 不改生产代码；缺陷登记 T016

## 验收
- `cargo test -p kafka-driver --test mock_groups` 全绿，测试数 ≥ 7

## 结果（完成后填写，≤5 行）
- `cargo test -p kafka-driver --test mock_groups`：8/8 全绿（0 ignored，约 0.00s）；`cargo clippy -p kafka-driver --all-targets -- -D warnings` 零告警。
- 覆盖：初始组列表（空集群 vs seeded fixture）、describe（成员/分配分区/state）、committed 与 lag（end−committed）、earliest/latest/指定 offset/指定时间戳四种 reset、dry_run 不落库、commit 后 `list_group_offsets` 可见新位点、陌生组 `GroupNotFound`（非 panic）。
- 新增 `crates/kafka-driver/tests/mock_groups.rs`（246 行）为唯一代码改动，未动生产代码，未触发 T016 缺陷。