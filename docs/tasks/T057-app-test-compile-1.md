# T057 · 修 `kafka-app` 测试编译错误（第一批：`svc_groups` / `driver_guard` / `store_settings`）

**状态**: ✅完成 | **依赖**: T056 | **改动上限**: 3 文件 / 250 行

## 目标
T056 让 `kafka-app` 的 `src/` 第一次编译通过（`LIB_EXIT=0`）。`--all-targets` 接着浮出 **9 个错误 / 5 个测试文件**——这些测试文件此前从未被编译过。本卡只管其中 3 个文件里那 5 个机械错误；剩下 2 个文件（`svc_clusters`、`svc_topics`）在 T058。

## 要改的文件
- 修改 `crates/kafka-app/tests/svc_groups.rs`
- 修改 `crates/kafka-app/tests/driver_guard.rs`
- 修改 `crates/kafka-app/tests/store_settings.rs`

## 必须覆盖（已确诊，直接照定位改）
| # | 位置 | 报错 | 修法 |
|---|---|---|---|
| 1 | `svc_groups.rs:6:42` | E0603 `struct ClusterId is private` | 该行是 `use kafka_core::cluster::{ClusterConfig, ClusterId};`。`ClusterId` 公开在 `kafka-core/src/lib.rs:26`，`cluster.rs:5` 的 `use` 是私有的 → 拆成 `use kafka_core::cluster::ClusterConfig;` + `use kafka_core::ClusterId;`（与 T056 #1 同一类缺陷） |
| 2 | `svc_groups.rs:344:9` | E0308 expected `Result<Vec<GroupInfo>, ServiceError>`, found `Result<Vec<GroupOffsetEntry>, _>` | 见 #3 |
| 3 | `svc_groups.rs:353:9` | E0308 expected `Result<Vec<GroupInfo>, _>`, found `Result<ResetOffsetsResult, ServiceError>` | `for result in [ groups.list(..).await, groups.offsets(..).await, groups.reset(..).await ]` 三个 `Ok` 类型互不相同（`Vec<GroupInfo>` / `Vec<GroupOffsetEntry>` / `ResetOffsetsResult`），数组要求同型 → 给每个加 `.map(\|_\| ()).map_err(\|e\| e.kind())`，数组元素统一成 `Result<(), ErrorKind>`，循环体改成 `assert_eq!(result.expect_err("offline cluster must refuse"), ErrorKind::NotConnected)`。断言语义不变 |
| 4 | `driver_guard.rs:186:40` | E0308 expected `&Arc<dyn KafkaDriver>`, found `&Arc<Counting>` | `connected()` 里 `let driver: DynDriver = Arc::clone(&counting);` —— `Arc<Counting> → Arc<dyn KafkaDriver>` 的 unsized 协变**不会**穿过 `Arc::clone` 的参数位 → 改成 `let driver: DynDriver = counting.clone();`（方法调用先得到 `Arc<Counting>`，在 `let` 标注处完成协变） |
| 5 | `driver_guard.rs:193:40` | 同上 | `unguarded()` 里同一写法，同样改成 `counting.clone()`（`counting: &Arc<Counting>`，方法解析走 `Arc<T>::clone`，结果仍是 `Arc<Counting>`） |
| 6 | `store_settings.rs:34:68` | E0308 expected `Option<Vec<String>>`, found `Option<Vec<&str>>` | `assert_eq!(store.get_json::<Vec<String>>("app.list").unwrap(), Some(vec!["a", "b"]))` 里字面量是 `Vec<&str>` → 改 `Some(vec!["a".to_string(), "b".to_string()])` |

## 明确不做（防止范围膨胀）
- 不改 `src/` 下任何文件（`src/` 已绿）
- 不给 `KafkaDriver` 加 `Debug` supertrait（T051 已明确否决；`svc_clusters` 的 `Debug` 问题在 T058 用测试侧写法解决）
- 不改 `GroupService::list/offsets/reset` 的签名去迁就测试
- 不写新测试、不碰 clippy 告警（T059）

## 验收
- `cargo check -p kafka-app --all-targets` 里这 3 个 target 不再报错（`svc_clusters`/`svc_topics` 允许仍红，那是 T058）
- `cargo test -p kafka-app --test svc_groups --test driver_guard --test store_settings` 全绿
- `cargo clippy -p kafka-app --tests` 这 3 个文件无新增告警

## 结果（完成后填写，≤5 行）
- 6 处机械错按卡片表格逐条修完，只改了 3 个测试文件（`svc_groups` / `driver_guard` / `store_settings`）。
- `cargo test -p kafka-app --test svc_groups --test driver_guard --test store_settings`
  → driver_guard 15/15、store_settings 9/9、svc_groups 8/8 全绿。
- 编译打通后浮出 1 个**行为**失败（非本卡范围）：`AppEvent` 的 serde tag/字段名与
  `protocol::event::*` 漂移 → 登记 **T064**（本会话紧接修完，故 svc_groups 现在 8/8）。
- `--all-targets` 仍有错：只剩 `svc_clusters`(2) + `svc_topics`(1) → T058。
