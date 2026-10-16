# T047b — 消费表格搜索过滤 · 实现报告

状态: ✅ 完成　卡: docs/tasks/T047b-search-filter.md　基线 HEAD: b0f3fb9

## 改动

仅 `frontend/src/views/ConsumeView.vue`（+37 行 / -2 行，预算 ≤50 达标；未抽 filter.ts）。

1. **过滤条**（表格上方）：文本框 `filterQuery` + 三个 checkbox：匹配 Value（默认开）/ 匹配 Key（默认关）/ 区分大小写（默认关）——语义镜像 kafka-core `SearchFilter`（`query` / `match_value=true` / `match_key=false` / `case_sensitive=false`，camelCase wire；`regex` 字段本卡未做，卡范围未含）。
2. **客户端过滤**：`displayRows` computed 派生显示列表，对已到达 ≤500 行 buffer 做子串匹配（key/value 经 `renderBytes(…, 'auto')` 解码后比较；matchValue/matchKey 任一命中即显示，两者全不勾选则无行命中——与 DTO `matches()` 语义一致）。不改订阅、不发 wire 命令。
3. **计数**：过滤激活时显示「N/rows.length」（即 N/≤500）；清空 query 即恢复全量。空态区分「无匹配消息」/「暂无消息」。

## SearchFilter DTO 事实登记（grep 一次，crates/kafka-core/src/message.rs）

- L189-200: `SearchFilter { query, match_value(默认 true), match_key, case_sensitive, regex }`，`rename_all="camelCase"`。
- **L274: `filter: Option<SearchFilter>` 已存在于订阅请求（SubscribeReq）——wire 订阅期过滤已支持 filter 键。按卡要求仅登记事实，本卡未接线。**

## 验证（R23 slim）

1. `npm run build`：✓ 40 modules，新 bundle `dist/assets/index-DxPpQr6k.js`（159.56 kB）。
2. `cargo build -p main-bin`：✓ Finished dev profile，exit=0（零 Rust 改动，git status 确认仅 ConsumeView.vue + dist）。
3. 冒烟：main-bin 运行 8s → CloseMainWindow `closed=True exited=True`，日志「bridge shut down cleanly」，stderr 0 行、无 ERROR。

## 并发/边界

- 外来未提交改动：`agents.md`（-104 行，非本卡所为）与未跟踪 `.opencode/`——按指令仅登记、未触碰、未 stage。
- dist 重建提交（`git add -f`，hash 换为 index-DxPpQr6k.js）。无新依赖、无 Rust 改动。

## 窗口手动项（留批量）

订阅后输入 query 只见匹配行；勾选匹配 Key 切换命中集；清空恢复全量与计数消失。
