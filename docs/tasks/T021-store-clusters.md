# T021 · Store：集群配置 CRUD + 导入导出

**状态**: 🔵代码完成，待编译验证 | **依赖**: T020 | **改动上限**: 2 文件 / 250 行（实际 3 文件，见结果）

## 目标
集群配置的增删改查与 JSON 导入导出（需求 2.1「多集群配置保存、导入导出」）。

## 只读白名单（最多 3 个）
- `crates/kafka-app/src/store/db.rs`
- `crates/kafka-core/src/cluster.rs` — `ClusterConfig` 字段、`mock` 开关、SASL/SSL 子结构
- `crates/kafka-core/src/lib.rs` — `ClusterId`

## 要改的文件
- 新建 `crates/kafka-app/src/store/clusters.rs`
- 修改 `crates/kafka-app/src/store/mod.rs`

## 必须覆盖
- `save`（插入或按 id upsert）、`get`、`list`（按 name 排序）、`delete`
- `config_json` 反序列化失败 → `StorageError`，并保留原始行信息在错误里
- 导入：JSON 数组 → 批量插入，冲突策略（跳过 / 覆盖 / 报错）可选参数
- 导出：全部集群 → JSON 字符串
- **敏感字段**：SASL 密码导出时是否明文？本卡按「明文存本地」实现，但在代码注释里标 `// TODO(security)`，不引入加密

## 明确不做
- 不做 UI、不做连接测试（那是 T024）
- 不做加密存储、不做迁移（T020 已定版本）

## 验收
- 新建 `crates/kafka-app/tests/store_clusters.rs`：CRUD、upsert、导入冲突三策略、导出往返
- `cargo test -p kafka-app --test store_clusters` 全绿

## 结果（完成后填写，≤5 行）
- 产出 3 文件：`store/clusters.rs`(~215) + `store/mod.rs`(+3) + `tests/store_clusters.rs`(~185)。**偏离 2 文件/250 行上限**（测试文件独立计入）。状态：**代码完成，未编译验证**（T001 未完成）。
- API：`save_cluster`（返回落库后的 config，自动补 id / created_at / updated_at）、`get_cluster` / `list_clusters` / `find_cluster_by_name` / `has_cluster` / `count_clusters` / `delete_cluster`、`export_clusters`、`import_clusters(json, ImportConflict)` + `ImportReport { imported, overwritten, skipped }`。
- `ImportConflict::Fail`（默认）先全量预检冲突再写入 → 失败时零写入；未用 `Connection::transaction`（规避 rusqlite 0.40 事务 API 版本风险）。
- 排序：`ORDER BY name COLLATE NOCASE ASC, id ASC`；`config_json` 解析失败 → `StorageError::Corrupt`，错误里带行 id。
- 敏感字段：SASL 密码明文存 `config_json`，模块注释已标 `TODO(security)`；`db::sql_error` 提为 `pub(crate)` 供仓储复用。