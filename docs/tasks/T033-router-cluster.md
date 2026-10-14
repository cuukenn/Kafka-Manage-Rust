# T033 · Router：`cluster.*` + `config.*`（真实 App handler 第一批）

**状态**: ✅ | **依赖**: T032 | **改动上限**: 3 文件 / 250 行

## 目标

把 T032 的 Router 内核接到真实 `App`：注册 8 个 `cluster.*` + 2 个 `config.*` 命令，
handler 调 ClusterService / Store，错误统一转 `ErrorDto`。同时建立后续卡片
（T034–T036、T085）复用的共享 helper 与 handler 模板。

## 只读白名单（最多 3 个源文件，别读更多）

- `crates/webview-bridge/src/router.rs` — `Router::register/handle`、`Handler` 类型签名
- `crates/kafka-app/src/services/clusters.rs` — ClusterService 方法签名 + `ClusterView`（Serialize camelCase，config flatten）
- `crates/kafka-core/src/protocol.rs` — `cmd::CLUSTER_*` / `cmd::CONFIG_*` 常量、`ErrorDto` 形状

## 预先钉死的接口事实（不必去读其它文件）

- `App` 访问器（kafka-app/src/lib.rs）：`store() bus() clusters() topics() produce() consume() groups()`；各 service 均廉价 `Clone`。
- `ServiceError::to_dto() -> ErrorDto` 已存在（kafka-app services/mod.rs），handler 错误直接用它。
- Store-only 命令走 `app.store()` + `tokio::task::spawn_blocking`（services 里的 `store_task` 是 `pub(crate)`，桥接用不了）：
  - `Store::export_clusters() -> Result<String, StorageError>`（JSON 数组字符串）
  - `Store::import_clusters(json: &str, on_conflict: ImportConflict) -> Result<ImportReport, StorageError>`（`kafka_app::store`）
  - `ImportConflict` wire 字符串：`"skip" | "overwrite" | "fail"`（缺省 fail）；`ImportReport{imported,overwritten,skipped}` 无 Serialize，手写 `json!({...})`。
  - `Store::set_raw(key, &str) / get_raw(key) -> Result<Option<String>, StorageError>`（config.* 用 KV）
- `StorageError -> ErrorDto`：`ErrorDto::new(err.kind(), err.to_string())`（retryable 由 kind 决定，T065 语义）。
- mock 集群夹具：`ClusterConfig { id: ClusterId::new(), name: "m".into(), bootstrap_servers: vec!["mock".into()], mock: true, ..Default::default() }` 可过 `validate()`。

## 要改的文件

- 新建 `crates/webview-bridge/src/handlers.rs` — 共享 helper + `register_cluster(app: &kafka_app::App, router: &Router)` + `register_config(...)`
- 新建 `crates/webview-bridge/tests/router_cluster.rs`
- 修改 `crates/webview-bridge/src/lib.rs` — 加 `pub mod handlers;`

## 必须覆盖

1. 共享 helper（命名钉死，后续卡片复用）：
   - `parse_payload<T: DeserializeOwned>(&Value) -> Result<T, ErrorDto>`：serde 失败 → `kind=invalid_config`、不可重试、message 带 cmd 名；
   - `to_data<T: Serialize>(T) -> Result<Value, ErrorDto>`：失败 → `internal`；
   - `cluster_id_of(&Value) -> Result<ClusterId, ErrorDto>`：读 `clusterId` 字符串，缺失 → `invalid_config`。
2. handler 模板：闭包 `Arc::new(move |payload| Box::pin(async move { ... }))`；注册函数内把需要的 service 句柄从 `App` clone 进闭包（闭包必须 `'static + Send + Sync`）。
3. `cluster.list` → `clusters().list()` → `[ClusterView]`（camelCase，config 展平，含 `state`/`driver`）。
4. `cluster.save` payload=ClusterConfig（camelCase）→ `save(&cfg)` → 回显保存后的配置。
5. `cluster.delete` / `cluster.connect` / `cluster.disconnect` / `cluster.info` payload `{clusterId}`；connect→`ClusterInfo`；disconnect→`data:null`；delete→bool。
6. `cluster.export` → export_clusters 的 JSON 字符串 parse 成 `Value` 数组放进 data（parse 失败 → `internal`）；`cluster.import` payload `{json, onConflict?}` → data=ImportReport 手写 JSON。
7. `config.set` payload `{key, value}` → `set_raw(key, serde_json::to_string(&value))` → `data:null`；`config.get` `{key}` → `get_raw` parse 成 Value（缺 key → `data:null`）。
8. 错误路径：未连接 `cluster.info` → `error.kind=="not_connected"`；未知 clusterId `cluster.connect` → `not_found`；payload 缺 `clusterId` → `invalid_config`。所有错误响应 `kind/message/retryable` 三字段齐全。
9. 测试必须走 `router.handle(raw_json_str)` 端到端（不是直调 service）：`App::builder().build()`（内存 SQLite）+ 保存 mock 集群 + 经 router `cluster.connect` 连通。测试数 ≥ 8。

## 明确不做（防止范围膨胀）

- 不接 topic/produce/consume/group/mock 命令（T034–T036、T085）
- 不做事件泵、不碰 tao/wry
- 不修改 kafka-app / kafka-core 源码（helper 全在桥接侧）

## 验收

- `cargo test -p webview-bridge` 全绿（新增测试 ≥ 8）
- `cargo clippy -p webview-bridge --all-targets -- -D warnings` 零告警
- `cargo check --workspace` 通过

## 结果（完成后填写，≤5 行）
- 新增 `handlers.rs`（共享 helper：`parse_payload` / `to_data` / `cluster_id_of` / `handler` / `register_with` / `blocking`）+ 10 个命令注册；9 个端到端测试全绿，clippy 零告警。
- 偏差 1：`parse_payload` 多带一个 `cmd` 参数（"message 带 cmd 名" 与单参签名不可兼得）。
- 偏差 2：`StorageError::NotFound` 在桥接侧重打为 `not_found`（`StorageError::kind()` 恒为 `storage`，否则第 8 条的 not_found 断言无法满足）。
- 行为补充：`config.get` 取到非法 JSON 时返回 `internal`（brief 未规定）。
- 备注：`docs/TASKS.md` 工作区已含既有的 T036/T084/T085 拆分改动（非本卡所改），本次提交未额外处理。
