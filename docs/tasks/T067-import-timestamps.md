# T067 · 修导入把 `updated_at` 改成导入时刻（导出→导入往返不是恒等的）

**状态**: ✅完成 | **依赖**: T058（`store_clusters` 目标此前从未跑过） | **改动上限**: 3 文件 / 250 行

## 现象

`cargo test -p kafka-app --test store_clusters` → 10 passed / **1 failed**：

```
---- export_then_import_round_trips_between_stores ----
crates\kafka-app\tests\store_clusters.rs:177:5: assertion `left == right` failed
```

两侧只差 `updated_at`：`created_at` 完全一致，
但导入后的行 `updated_at` 变成了**导入时刻**（源库里是 `…221` / `…222`，目标库里两条都是 `…223`）。

## 诊断（已确诊）

`crates/kafka-app/src/store/clusters.rs:169-203` 的 `save_with_conn`：

- `created_at`：**只在 0 时补**（`:176-184`，还会回读旧行的 `created_at`）→ 载荷里的值得保留；
- `updated_at`：`:185` **无条件** `stored.updated_at = now`。

而 `import_clusters`（`:157`）把每条导入的 config 都送进 `save_with_conn`
→ 导入必然改写 `updated_at`。同一个函数对两个时间戳用了**相反**的策略，
所以「导出 → 导入 → 再导出」每走一轮就把 `updated_at` 往前推一次。

## 判断（哪一侧是对的）

T021 卡片只写了「导入：JSON 数组 → 批量插入，冲突策略」，没写时间戳语义 → 需要裁定：

- **`save_cluster`（UI 编辑）必须打戳**：前端把 `cluster.list` 拿到的 config 原样改完送回来，
  里面带着**旧的** `updated_at`。若导入那样保留，「最后修改时间」永远不会前进 → 更糟。
- **`import_clusters` 必须保留载荷里的时间戳**：导出文件是「这份配置最后一次被编辑」的记录；
  导入 100 个集群就把它们全标成「刚刚改过」是错的，且往返不恒等会让「导出→导入→导出」
  产生无意义的 diff。
- 现有代码对 `created_at` 已经是「非 0 就保留」→ 保留 `updated_at` 与既有约定一致，
  不是新发明。

## 要改的文件

- 修改 `crates/kafka-app/src/store/clusters.rs`：`save_with_conn` 增加 `Stamp { Now, Preserve }`
  （模块私有），`save_cluster` 传 `Now`、`import_clusters` 传 `Preserve`
- 修改 `crates/kafka-app/tests/store_clusters.rs`：新增
  `saving_stamps_updated_at_even_when_the_payload_carries_one`
  —— 现有测试（`:55-70`）只锁了 `created_at` 在更新后存活，**没有任何测试**锁住
  「save 会把 `updated_at` 推到 now」，而本卡正是改这块，必须有回归锁

## 验收

- `cargo test -p kafka-app --test store_clusters` 全绿（12/12）
- `cargo test -p kafka-app --test svc_clusters` 全绿（`ClusterService::save` 走 `save_cluster`，确认未被牵连）

## 明确不做

- 不改 `created_at` 的补值逻辑（已经正确）
- 不给 `ClusterConfig` 加新字段、不动 schema
- 不引入加密（`TODO(security)` 保持）

## 结果（完成后填写，≤5 行）
- 根因：`store/clusters.rs` 的 `save_with_conn` 对两个时间戳处理相反 —— `created_at` 仅在 0 时补
  （`:176-184`），`updated_at` 无条件盖当前时间（`:185`）；`import_clusters` 走同一条路（`:157`）
  → export→import→export 每轮都把 `updated_at` 往前推，往返不恒等。
- Ruling（T021 卡片未规定时间戳语义）：`save_cluster`（UI 编辑）**必须**盖时间戳（前端会回传带着旧
  `updated_at` 的配置，不推进的「最后修改时间」没有意义）；`import_clusters` **必须**保留载荷里的时间戳
  （导出文件就是「那次编辑的记录」，且非恒等往返会让 export→import→export 变成差异生成器）。
- 修法（2 文件）：新增模块私有 `enum Stamp { Now, Preserve }`，`save_with_conn(conn, cfg, stamp)`；
  `save_cluster` → `Now`，`import_clusters` → `Preserve`（`Preserve` 下仅当为 0 才补）。`created_at` 逻辑未动。
- 诚实说明：新测试 `saving_stamps_updated_at_even_when_the_payload_carries_one` 是**回归锁**
  （改前也会过），真正的 RED 证据是 `:177` 原有的往返失败。
- 验收：`--test store_clusters` 12/12、`--test svc_clusters` 10/10（证明 `save` 仍盖戳）。
