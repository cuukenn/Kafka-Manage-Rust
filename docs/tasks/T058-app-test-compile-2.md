# T058 · 修 `kafka-app` 测试编译错误（第二批：`svc_clusters` / `svc_topics`）

**状态**: ✅完成 | **依赖**: T056（可与 T057 并行） | **改动上限**: 3 文件 / 250 行

## 目标
T057 之外的另外 2 个测试文件、共 3 个错误 + 2 个告警。这批不是纯机械错，涉及一个**设计信号**（`Arc<dyn KafkaDriver>` 没有 `Debug`），所以单独一张卡，避免和 T057 混在一起把范围撑大。

## 要改的文件
- 修改 `crates/kafka-app/tests/svc_clusters.rs`
- 修改 `crates/kafka-app/tests/svc_topics.rs`

## 必须覆盖（已确诊）
| # | 位置 | 报错 | 修法 |
|---|---|---|---|
| 1 | `svc_clusters.rs:98:31` | E0277 `dyn KafkaDriver` doesn't implement `Debug` | `let err = svc.driver(&id).expect_err("driver must be gone");` —— `expect_err` 要求 **Ok 类型**可 `Debug`，而 Ok 是 `DynDriver = Arc<dyn KafkaDriver>` |
| 2 | `svc_clusters.rs:115:36` | 同上 | `let err = svc.driver(&missing).expect_err("no live driver");` 同一处写法 |
| 3 | `svc_topics.rs:316:19` | E0596 cannot borrow `rx` as mutable | `assert!(drain(&mut rx).is_empty());` —— 该测试函数里 `rx` 的绑定没写 `mut`。找到它的定义处补 `mut`（`drain` 收 `&mut`） |
| 4 | `svc_clusters.rs:134:9` | warning unused variable `alpha` | 见下 |
| 5 | `svc_consume.rs:302:9` | warning unused variable `second_id` | 见下（若改 `svc_consume.rs` 会超 3 文件上限 → 把这条挪到 T059，别硬塞） |

### #1/#2 的取舍（要判断，别顺手改 trait）
T051 已经**明确否决**过「给 `KafkaDriver` 加 `Debug` supertrait」这条路（`GuardedDriver` 自己就是因为 `Arc<dyn KafkaDriver>: Debug` 不成立才改手写 impl 的）。所以本卡走测试侧：
```rust
let err = match svc.driver(&id) {
    Ok(_) => panic!("driver must be gone"),
    Err(err) => err,
};
```
或者干脆用 `assert!(matches!(svc.driver(&id), Err(ServiceError::Driver(DriverError::NotConnected(_)))))` 一步到位，连 `err.kind()` 那行都能省。

**如果后面发现这个模式在测试里反复出现**（`driver()` 返回 `Result<DynDriver, _>` 的地方都要这么写），那才值得单开一张卡讨论「给 `KafkaDriver` 加 `Debug` supertrait，代价是所有实现者必须 `Debug`」——本卡不做这个决定。

## 明确不做（防止范围膨胀）
- 不给 `KafkaDriver` / `DynDriver` 加 `Debug`
- 不改 `ClusterService::driver()` 的返回类型
- 不改测试要验证的行为语义（`NotConnected`、事件不重复推送等）
- 不写新测试

## 验收
- `cargo check -p kafka-app --all-targets` 全绿（配合 T057 之后）
- `cargo test -p kafka-app --test svc_clusters --test svc_topics` 全绿
- `cargo clippy -p kafka-app --all-targets` 除 T059 清单外无新增告警

## 结果（完成后填写，≤5 行）
- 按卡片表格修完 3 个编译错（`svc_clusters.rs:98` 改 `match` 取 `Err`、`:115` 改 `matches!` 一步、
  `svc_topics.rs:283` 补 `mut rx`），只改 2 个文件，未动 `KafkaDriver` 的 `Debug`（T051 已否决）。
- `cargo check -p kafka-app --all-targets` → **EXIT=0**；`--test svc_clusters` 10/10。
- 顺手处理了 `svc_clusters.rs:134` 的未用变量 `alpha` → `let _alpha`（本就在本卡文件白名单内，
  1 字符改动；T059 清单相应减少，已记 ledger）。
- `--test svc_topics` 编译通了但跑出 1 个**行为**失败（`:278`）→ 不在本卡修，登记 **T066**。
- 全量 `cargo test -p kafka-app` 之后 92 tests 全绿（见 T066/T067 的修复）。
