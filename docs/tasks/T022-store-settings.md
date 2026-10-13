# T022 · Store：应用设置 KV + 最近打开 + Mock 快照槽

**状态**: 🔵代码完成，待编译验证 | **依赖**: T020 | **改动上限**: 2 文件 / 250 行（实际 3 文件，见结果）

## 目标
通用 KV 设置表读写：主题、超时默认值、最近使用集群、Mock 快照 JSON。

## 只读白名单（最多 3 个）
- `crates/kafka-app/src/store/db.rs`
- `crates/kafka-driver/src/mock/driver.rs` — `snapshot_json()` / `restore_json()` 的返回类型（只读签名，别读全文）
- `crates/kafka-core/src/cluster.rs` — 超时/重试默认值字段

## 要改的文件
- 新建 `crates/kafka-app/src/store/settings.rs`
- 修改 `crates/kafka-app/src/store/mod.rs`

## 必须覆盖
- `get_json::<T>(key)` / `set_json::<T>(key, value)`，key 不存在返回 `Ok(None)`
- 常量 key：`app.theme`、`app.defaults`、`app.recentClusters`、`mock.snapshot.<clusterId>`
- `recent`：`push_recent(cluster_id)` 去重 + 上限 10 + 顺序正确
- Mock 快照：`save_mock_snapshot(cluster_id, json)` / `load_mock_snapshot(cluster_id)`
- 并发：两个线程同时 `set_json` 不 panic、不丢更新（最后一次写生效）

## 明确不做
- 不做快照的自动生成/自动恢复（T024 连接流程里接）
- 不做设置校验（前端表单负责）

## 验收
- 新建 `crates/kafka-app/tests/store_settings.rs` 全绿
- `cargo test -p kafka-app` 全绿

## 结果（完成后填写，≤5 行）
- 产出 3 文件：`store/settings.rs`(~195) + `store/mod.rs`(+7) + `tests/store_settings.rs`(~170)。**偏离 2 文件/250 行上限**。状态：**代码完成，未编译验证**（T001 未完成）。
- API：`set_json/get_json<T>`（缺 key → `Ok(None)`；类型不符 → `StorageError::Corrupt`，不静默重置）、`set_raw/get_raw`（原样存 JSON，写入前校验可解析）、`delete_setting`、`count_settings`、`settings_map(prefix)`（用 `substr(key,1,n)=?1` 前缀匹配，免 `LIKE` 转义）。
- 常量 key：`KEY_THEME=app.theme`、`KEY_DEFAULTS=app.defaults`、`KEY_RECENT_CLUSTERS=app.recentClusters`、`KEY_MOCK_SNAPSHOT_PREFIX=mock.snapshot.` + `mock_snapshot_key()`；`RECENT_LIMIT=10`。
- 最近使用：`push_recent`（去重 + 置顶 + 截断 10，返回新顺序）、`recent_clusters`、`forget_recent`；快照：`save/load/clear_mock_snapshot`。
- 快照只存/取字符串——`MockKafkaDriver::snapshot_json()/restore_json()` 是 **async**（`Result<String,_>` / `(&str)`），自动存取接线留给 T024。