# T020 · SQLite 底座（连接 / 迁移 / 错误映射）

**状态**: 🔵代码完成，待编译验证 | **依赖**: T002 | **改动上限**: 3 文件 / 250 行（实际 4 文件，见结果）

## 目标
`kafka-app` 的本地存储底座，只做连接与版本化迁移，不写业务表以外的东西。

## 只读白名单（最多 3 个）
- `crates/kafka-core/src/error.rs` — `StorageError` 变体与构造方式
- `crates/kafka-core/src/cluster.rs` — 后面要存的 `ClusterConfig`（本卡只读，不实现 CRUD）
- `crates/kafka-app/Cargo.toml` — 已有 `rusqlite`(bundled)、`dirs`、dev `tempfile`

## 要改的文件
- 新建 `crates/kafka-app/src/store/mod.rs`、`crates/kafka-app/src/store/db.rs`
- 修改 `crates/kafka-app/src/lib.rs`：`pub mod store;`

## 必须覆盖（`db.rs`）
- `Store::open(path)` / `Store::open_in_memory()`（测试用），内部 `Connection` 包 `Arc<Mutex<Connection>>`（rusqlite 非 Sync）
- `user_version` 迁移：`migrate()` 按版本号顺序执行，重复打开不重复建表
- 建表：`clusters(id TEXT PRIMARY KEY, name TEXT, config_json TEXT NOT NULL, created_at INTEGER, updated_at INTEGER)`、`settings(key TEXT PRIMARY KEY, value_json TEXT NOT NULL)`
- 开启 `foreign_keys`、`journal_mode=WAL`
- rusqlite 错误 → `StorageError`（带上下文，不泄露 SQL 细节给 UI）
- `Store::default_data_dir()` 用 `dirs` 给出 `~/.local/share|AppData/rs-kafka-ui-client/data.db`

## 明确不做
- 不做集群 CRUD（T021）、不做设置 KV（T022）
- 不加连接池、不做异步封装（业务层用 `tokio::task::spawn_blocking`，T024 再说）

## 验收
- 新建 `crates/kafka-app/tests/store_db.rs`：打开/迁移幂等、损坏文件报错、WAL 生效
- `cargo test -p kafka-app` 全绿

## 结果（完成后填写，≤5 行）
- 产出 4 文件：`store/db.rs`(170) + `store/mod.rs`(10) + `lib.rs`(+1) + `tests/store_db.rs`(128)。**偏离卡片上限**（3 文件/250 行 → 4 文件/约 310 行，测试文件独立计入）。
- 状态：**代码完成，未编译验证**（Windows SDK 缺失，T001 未完成）。
- 设计：`Store { Arc<Mutex<Connection>>, path: Option<PathBuf> }`，内存库跳过 WAL；迁移按 `user_version` 递增执行且语句全部 `IF NOT EXISTS` 可重放；`user_version > SCHEMA_VERSION` 判为 `Corrupt`（旧程序拒绝新库）。
- 错误映射：`not a database` / `malformed` → `Corrupt`，其余 rusqlite → `Sql`，建目录失败 → `Io`；`Connection::open` 也走 `sql_error`（SQLite 打开是惰性的，损坏文件在首条 PRAGMA 才暴露）。
- 遗留：T021 需在 `Store::conn()` 之上加 `spawn_blocking` 包装；`settings` 表已建但本卡不提供 KV API（T022）。