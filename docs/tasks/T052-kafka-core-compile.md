# T052 · 修 kafka-core 首次编译报错（anyhow / serde Default / base64）

**状态**: ✅完成 | **依赖**: T001 | **改动上限**: 4 文件 / 250 行

## 目标
`kafka-core` 从未编译过。T001 修好链接环境后暴露出 7 个错误 + 1 个告警，全部修掉，让 `cargo check -p kafka-core --all-targets` 变绿——这是全仓第一块可编译的地基。

## 只读白名单（最多 3 个源文件，别读更多）
- `crates/kafka-core/src/error.rs` — `DriverError`/`StorageError` 的 derive 与 `Other` 变体
- `crates/kafka-core/src/message.rs` — `MessageEvent` 的 serde 容器属性、`MessageStream::poll_next`
- `crates/kafka-core/src/dto.rs` — `B64` 常量用法

## 要改的文件
- 修改 `crates/kafka-core/Cargo.toml`
- 修改 `crates/kafka-core/src/error.rs`
- 修改 `crates/kafka-core/src/message.rs`
- 修改 `crates/kafka-core/src/dto.rs`

## 必须覆盖
- `anyhow` 依赖缺失（E0433 ×3：error.rs:114/164/185）→ 补 `anyhow.workspace = true`（根 `[workspace.dependencies]` 早有 `anyhow = "1.0"`，属漏写）
- `DriverError` 要 `Clone`，但 `anyhow::Error` 不是 `Clone`（E0277）→ 保留 `Clone` 语义，新增 `pub struct AnyError(Arc<anyhow::Error>)` 包装：`Display` 用 `{:#}` 渲染整条因果链，手写 `impl std::error::Error`，并补 `impl From<anyhow::Error> for DriverError`（`?` 不会串两次转换）
- `MessageEvent` 缺 `Default`（E0277，容器属性 `#[serde(..., default)]` 要求）→ derive 补 `Default`，与 `ProduceMessage` 既有写法一致
- `dto.rs:91` `B64::encode`（E0433）→ `B64` 是 `general_purpose::STANDARD` 常量，改方法调用 `B64.encode(k)`
- `message.rs:402` `mut self: Pin<&mut Self>` 多余 `mut`（unused_mut）→ 去掉

## 明确不做（防止范围膨胀）
- 不动 `StorageError::Other(#[from] anyhow::Error)`——它没有 `Clone` derive，本来就编译得过
- 不把 `DriverError::Other` 降级成 `String`（会丢掉 anyhow 的设计意图）
- 不删 `DriverError` 的 `Clone`（注释明确写了「失败要同时进日志和 UI」）
- 不碰 kafka-driver / kafka-app

## 验收
- `cargo check -p kafka-core --all-targets` 全绿
- `cargo clippy -p kafka-core` 无新增告警

## 结果（完成后填写，≤5 行）
- `cargo check -p kafka-core --all-targets` → `Finished dev profile`，退出码 0，零告警。
- 意外发现：本构建下 `anyhow::Error` **不**实现 `std::error::Error`（已用 `rustc --emit=metadata` 探针证实），但 thiserror 2.0.21 仍接受 `#[from] anyhow::Error`——所以包装类型必须自己手写 `Error` impl。
- 新增 `kafka_core::error::AnyError`（`Arc` 载荷 + `inner()` 取链），`DriverError::Other` 改用它，`Clone` 与因果链两个意图都保住。
- 改动 4 文件、约 45 行新增，未越界。
