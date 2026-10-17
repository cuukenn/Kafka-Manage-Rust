# 任务拆分索引（小步执行 / 上下文友好）

> 用法：**一个会话只做一张任务卡**。新会话只喂两块内容：
> 1. `docs/TASKS.md`（本文件，只读索引）
> 2. `docs/tasks/Txxx-*.md`（本次要做的那一张卡）
>
> 不要一次性喂多张卡，也不要让会话自己"顺手"做下一个任务。

---

## 0. 上下文协议（每个会话都遵守）

| 规则 | 说明 |
|---|---|
| 只读白名单 | 任务卡里的「只读这些文件」列表，最多 3 个源文件；超出即视为跑偏 |
| 改动上限 | 单任务最多改 **3 个文件**、新增 **≤ 250 行**；超了就停下拆分 |
| 验证命令 | 任务卡里的验收命令必须跑绿才算完成 |
| 收尾动作 | 完成后：① 本索引状态列改 `✅`；② 卡片末尾追加「结果」小节（≤5 行）；③ 若发现新问题，只登记新任务卡，不在本会话实现 |
| 上下文告警 | 若会话已读大量文件仍没进展，直接结束会话并写「阻塞」状态，不要硬撑 |

**新会话启动语模板**（复制即用）：
```
读 docs/TASKS.md 和 docs/tasks/<卡片文件名>，只做这一张卡。
严格遵守卡片的「只读白名单」「改动上限」「验收命令」，完成后更新索引状态和卡片结果小节。
```

状态图例：`⬜未开始` `🔵进行中` `✅完成` `⛔阻塞` `⏸待建卡`（索引里有 ID，但卡片未写）

---

## 1. 当前进度快照（2026-10-13）

- ✅ **环境已通（T001 完成，2026-10-13）**：Windows SDK `10.0.26100` 已用
  `winget install --id Microsoft.WindowsSDK.10.0.26100 --exact` 装好，`Lib`/`Include`/`bin` 齐全，
  `rustc hello.rs` 裸链接通过并打印 `ok`。VS Build Tools 2022（MSVC 14.44.35207）本来就在，
  之前只缺 SDK。rustc 通过注册表自动发现，无需配 PATH。代价：C: 盘剩 6.59 GB。
- ✅ 已编译通过：`kafka-core`（cluster/topic/message/group/error/dto/protocol，含 protocol 单测）
  —— `cargo check -p kafka-core --all-targets` 全绿，缺陷见 T052（已修）。
- ✅ 已编译通过：`kafka-driver`（`KafkaDriver` trait、`MockKafkaDriver`、`MockState`、`MockPump`、
  `FaultInjection`）—— T053 修完 6 处缺陷后 `cargo check -p kafka-driver --all-targets` 全绿、
  `cargo test -p kafka-driver` **11 passed / 0 failed**。阶段 1（T010–T017）现在可以真正写测试了。
- ✅ `src/` 已编译通过：`kafka-app`（`store`、`bus`、`services/*`、`driver_guard`）—— T056 修完 7 个错误后
  `cargo check -p kafka-app --lib` → `LIB_EXIT=0`，T051 的 `Debug` 修复同时得到验证（已转 ✅）。
- ✅ **`kafka-app` 已全绿**：`cargo test -p kafka-app` → **92 tests / 0 failed**（9 个集成目标 + lib + doc），
  `cargo check -p kafka-app --all-targets` → EXIT=0，`cargo clippy -p kafka-app --all-targets` → kafka-app **零告警**。
  路径：T056（`src/` 7 错）→ T057（3 文件 6 个机械错）→ T058（`svc_clusters`/`svc_topics` 3 错）→ T059（3 条 clippy）。
  分布：bus 9 / driver_guard 15 / store_clusters 12 / store_settings 9 / svc_clusters 10 /
  svc_consume 7 / svc_groups 8 / svc_produce 7 / svc_topics 8。
- ✅ 编译打通后才现形的 **4 个行为缺陷**（都不是新引入，均已修并加回归锁）：
  **T064** `AppEvent` 的 serde tag 被 `rename_all` 改成 camelCase、payload 字段却没改 →
  9 个 variant 各自 `#[serde(rename = "<点号>", rename_all = "camelCase")]` + 「tag == `event_name()`」全量守卫；
  **T065** `ErrorDto::new` 写死 `retryable:false` → 改由 `ErrorKind::is_retryable()` 决定；
  **T066** `svc_topics.rs:278` 断言失败是**测试缺陷**（在写入前 drain）→ 补「恰好一条 `TopicChanged{Created}`」；
  **T067** `import_clusters` 覆盖 `updated_at` 使 export→import→export 不恒等 → 新增 `Stamp::{Now,Preserve}`。
- ✅ **存量 clippy 已全部清零**：`cargo clippy --workspace --all-targets -- -D warnings` → **EXIT=0，零告警**。
  `kafka-driver` 4 条（T054）、`kafka-core` 2 条（T055）、`kafka-app` 3 条（T059）。
  T054 里 `PumpHandle.join` 的处置是**删字段**而非加 `Drop`——`PumpHandle` 在 `driver.rs:591-593`
  就被解构丢弃，`Drop` 里 abort 会在流交付前掐掉它；由此暴露的空转问题登记为 **T068**。
- ✅（2026-10-17 补记）`webview-bridge`、`main-bin` **已非空壳**：前者有 WebView2 运行时检测（T091），后者有 glue/logging 与 E2E（T080/T083a/T100）。
- ✅ T020–T029 已全部转 ✅（各自的验收命令本会话都跑绿，见 §2 索引括号里的计数）。
- ✅ **阶段 1 + 阶段 2 全部完成（2026-10-14 终审批次）**：阶段 1（T010–T017）+ 缺陷卡
  **T068**/**T069** + 阶段 2 收尾 **T030**（`App` 门面装配 + 冒烟测试）均转 ✅；
  `cargo test` → **core 6 / driver 66（65 + 终审批次 C1 暂停态回归 1）/ app 94**，
  `cargo clippy --workspace --all-targets -- -D warnings` 零告警。
  下一步关键路径 = **阶段 3**（T031/T032 ⏸待建卡）。
  rust-analyzer 的报错在这个仓库里是过期缓存，不可作为依据。
- 备注：`kafka-driver` 的 `default = []`，`real`（rdkafka）是 opt-in → 阶段 1/2 无需 librdkafka/cmake。
- 工具链：rustc 1.99.0 / cargo 1.99.0 / VS Build Tools 2022（MSVC 14.44.35207）/ Windows SDK 10.0.26100 / edition 2024 / rust-version 1.85
- ✅ **T002 完成：基线已提交**（2026-10-13）。`cargo test --workspace` → **108 passed / 0 failed**
  （core 5 + driver 11 + app 92）；`cargo clippy --workspace --all-targets -- -D warnings` → 零告警。
  `.gitignore` 补了 `.codegraph/` 与 `*.rmeta`（根目录两个探针残留文件只忽略、未删除）。
- 仓库状态：**已有 1 条基线提交**（`chore: baseline (core + mock driver)`，88 个文件）。
- ✅ **阶段 6 开工（2026-10-16，macOS arm64 新环境）**：本机为 macOS（rustc 1.99.1 / cargo 1.99.1），
  另装 `cmake 4.4.4` + `pkg-config` + brew `openssl@3`（`PKG_CONFIG_PATH=/opt/homebrew/opt/openssl@3/lib/pkgconfig`）。
  真实 Kafka 用 Docker Desktop：`scripts/kafka/docker-compose.yml`（KRaft 单节点，localhost:9092，
  cluster `5L6g3nShT-eMCtK--X86sw`）。**T070 已 ✅**（real 模块骨架 + 连接生命周期，2 文件 248 行；
  真集群实测 connect/cluster_info/disconnect 通过）。**T071 按 250 行上限拆成 T071a/T071b**：T071a（会话脚手架
  `Live`/`fetch_metadata` + `list_topics`）已 ✅，T071b（`describe_topics`：分区明细 + 起止位点 + 只回被覆盖的
  配置）已 ✅；**T072 同样按 250 行上限拆成 T072a/T072b**：T072a（`create_topic` / `delete_topics` + admin 脚手架）
  已 ✅，**T072b（`update_topic_config`：先读回被覆盖项、合并后整体替换）已 ✅**；**T073（produce：整批入队 + 投递回执，并按请求顺序回填）已 ✅**，同时显式关掉 `allow.auto.create.topics`，消掉了 T072b 探测不存在主题时把主题建出来的副作用；**T074 按 250 行上限拆成 T074a/T074b/T074c**：T074a（`list_offsets`：分区展开 + 一次批量取水印 + 借一次性 consumer 解析时间戳）已 ✅；T074b（subscribe 流核心：`assign` 起读 + 有界/追尾 + `stop` + 消息映射）已 ✅——真集群实测发现 `rd_kafka_assign` 需要 `group.id`，故配置合成组名（R50）；T074c（投递成形与控制：客户端 filter / max_messages、pause/resume/seek，并处理「命令在 `poll` 阻塞期间到达」的竞态与暂停消息暂存）已 ✅；**T075 同样按 250 行上限拆成 T075a/T075b/T075c**：T075a（消费组列表与详情：`fetch_group_list` + 本地解码成员分配，并处理 `coordinator_id` / `is_local` 不可得的取舍）已 ✅；**T075b（消费组位点与滞后：借一次性 consumer 读 `committed_offsets`，再对提交过的分区取一次水印算 lag）已 ✅**；**T075c（reset/commit：`CommitMode::Sync` 写回，四种 ResetSpec 复用 `watermarks`/`at_or_after`，`dry_run` 只算不写）已 ✅**；**T076（SASL/SSL 参数映射）已 ✅**——dev broker 追加 `SASL:9094`（PLAIN）与 `SSL:9095` 两个监听器（密钥由新增的 `scripts/kafka/gen-secrets.sh` 生成，`secrets/` 不入库），清单实测 SASL 登录、错密码、SSL+`ca.pem`、无信任锚、关校验、证书缺私钥与 `saslProtocol` 覆盖共八条；清单第 8 条当场暴露「凭据分支按枚举判断」的缺陷（`No provider for SASL mechanism GSSAPI`），已改为按生效协议串判断（R65）；**T077（Mock/Real 行为对齐测试架）已 ✅**——一段共享脚本在 mock 与 real 上逐行比对（实测 23 行逐字相等：建表/重复建/分区与副本数/配置写改撤/produce/起止位点/有界读回/六条拒绝/删后 NotFound），六条拒绝路径按错误类别对齐，R30 与 R33 两项知情差异由第二条测试显式钉住（R69–R72）；**Stage 6 至此全部收尾**；**Stage 7 起步：T080（tracing → `log.line` → 前端日志面板）已 ✅**——`main-bin` 新增 `logging.rs`：订阅器 = `EnvFilter` + `BusLogs` layer + `fmt::layer`，每条 tracing 事件压成 `AppEvent::LogLine{level,target,message}` 上总线，经既有事件泵进前端，面板按新来源 `rust` 显示（R73 槽位式 attach、R74 单行压平、R75 不过滤级别）；4 个新单测含「tracing→layer→总线→事件泵→假 sink」端到端，`cargo test -p main-bin` 4+6+1 全绿；**T081（错误映射审计）已 ✅**——`hint_for` 从「7 个给建议 + `_ => None`」改成穷尽 match（15 个 kind 逐个有结论：13 给建议、`InvalidConfig`/`Cancelled` 明确不给，R76/R77），新增 `ErrorKind::ALL` 让新增变体先在这张表上编译失败，3 个新单测锁住「每个 kind 都有结论 / 提示非空白 / 提示不复用 / 两个构造器一致」；审计顺带发现 `hint` 序列化后在前端 `rpc.ts:93` 被丢掉、到不了用户眼前（R78）；**T081b（前端接上 `error.hint`）已 ✅**——`RpcError` 增 `hint` 字段且显示文案 = `message` + 换行 + 建议（R79/R80），22 个只读 `e.message` 的 catch 点与日志面板一行改动全部覆盖，`npm run build` 重建 dist；**T082 实测 345 行/3 文件超限，按 250 行上限拆成 T082a/T082b/T082c**：T082a（路由层全局超时：`Router.timeout` + `with_timeout`，`dispatch` 把 handler 任务挂上 `tokio::time::timeout`，超时 `abort()` 回 `timeout`；R81/R84）已 ✅——默认 30 s 仅作兜底；**T082b（取消在途请求）已 ✅**——`Router` 加在途表（按信封 `id` 记住 handler 的 `AbortHandle`）+ 内建命令 `request.cancel`（命中即 `abort()`、原请求回 `cancelled`，未命中回 `cancelled:false` 而非报错），并公开 `cancel()`/`inflight()`（R82/R83）；**T082c（前端接线）已 ✅**——`rpc.ts` 用 `AbortController` 按信封 id 登记在途请求，`cancelInflight()` 逐个 `abort()` 并向 Rust 补发 `request.cancel`（`AbortError` 归类为 `cancelled` 而非 `network`，R85/R86），顶栏在 `pending.count > 0` 时出现「取消」按钮并 toast 报数，`npm run build` 重建 dist；**T083 实测含前后端两半，按 250 行上限拆成 T083a/T083b**：**T083a（日志落盘与轮转）已 ✅**——`logging.rs` 新增手写 `FileWriter`（`LineWriter` 缓冲到 `Drop`，轮转不会切断一行）写 `<data_dir>/logs/app.log`，超 1 MiB 滚动、连 `.3` 共留 4 个文件，`install()` 变成 stderr + 文件两个 `fmt` layer（文件去 ANSI），写不动就静默降级到 stderr（R88），阈值硬编码、不引新依赖（R87/R89），4 个新单测含超长行与不可建目录；**T083b（日志面板导出）已 ✅**——`render.ts` 抽出 `downloadText`（`downloadJson` 改为委托它，行为不变），`LogView.vue` 加「导出」按钮，把**当前筛选**的 `rows` 反转成最旧在前（R90/R91）写成 `kafka-logs-<时间戳>.txt`，每行 `[ISO] LEVEL source text`，空时按钮禁用；`npm run build` 重建 dist；**Stage 7（T080–T083b）至此全部收尾**；**Stage 8 起步：T091（Windows WebView2 运行时检测与首启提示）已 ✅**——新增 `webview2.rs` 用**文件系统**（而非注册表/WebView2 API）探测 `%ProgramFiles(x86)%\Microsoft\EdgeWebView\Application\<version>\msedgewebview2.exe`并取数值最大的版本，非 Windows 直接视为可用（R93）；`shell.rs` 两个开窗函数首行各加 `preflight()?`：缺运行时先 `warn!` 带下载地址、尽力打开下载页、再 `bail!` 以非零码退出（R94/R95）；`KAFKA_UI_FAKE_WEBVIEW2_MISSING=1` 供 mac 上手动验缺分支（R96），4 个新单测；**T092（CI 矩阵 + 产物上传）已 ✅**——新增 `.github/workflows/ci.yml`（`ubuntu-22.04`/`macos-14`/`windows-latest` 三平台矩阵，装 Linux GTK 依赖 → Node 22 建前端 → `cargo test --workspace --locked` → clippy `-D warnings` → `cargo build --release -p main-bin` → 上传产物；另加 Ubuntu 专属 `real-check` 跑 `cargo check --features real`），并把 `release.yml` 的 `node-version: 20` 改成 **22**（Vite 7 要求 ≥22.12，原值会让打 tag 时的前端构建必失败），R97–R100；本机无法跑 CI，YAML 用 `ruby -ryaml` 验过、三门禁在本机等价通过。**T093（Linux webkitgtk 依赖说明 + README）已 ✅**——`README.md` 在「前置（Windows）」旁补上「前置（Linux）」，把 T092 的 `ci.yml` 实际 `apt-get install` 的那份清单（`libwebkit2gtk-4.1-dev`/`libgtk-3-dev`/`librsvg2-dev`/`libssl-dev`/`pkg-config`，`--features real` 另需 `cmake`/`build-essential`）原样写进去，并点明缺包时表现为 `webview-bridge` 编译失败（R101：只覆盖源码构建这一层，不含分发产物的运行期依赖）；**Stage 8 至此全部收尾**；**T101（criterion 基准）已 ✅**——`criterion 0.7`（仅 `async_tokio`）作为 dev-dep 加入，`crates/kafka-driver/benches/mock_throughput.rs` 在 `b.iter` 外只建一次 `Runtime` 与驱动（R103），量两条主路径：`mock_produce/batch`（100 条一批）`23.43 µs` → **4.27 Melem/s**，`mock_consume/drain`（有界读回 100 条，逐条 `next()` 抽干）`1.24 ms` → **80.6 Kelem/s**（R104）；`[[bench]] harness = false`，`cargo test --workspace` 281 passed 无回归、clippy 零告警；**T102（长跑内存/句柄泄漏检查）已 ✅——计划内最后一张卡完成**：新增 `crates/kafka-driver/examples/leak_probe.rs`（每轮建主题→生产→订阅→抽干→`stop`→抽干到 `None`→读元数据→删主题，泵不归位即非零退出）与 `scripts/leak-check.sh`（进程外 `ps`/`lsof` 采样，R107），并留下报告 `docs/reports/T102-long-run-leak-report.md`；每轮删主题把保留集钉在一批（R105）、检查范围限定 mock 驱动（R106）。实测 2 万 / 4 万轮两轮共 38 个采样点**全部** `rss=6672KB`、`fds=13`，两轮 `pumps=0 after_disconnect=0 topic_left=false`、退出码 0：轮数与墙钟翻倍而常驻集与描述符表零增长。门禁 281 passed、clippy 零告警。 **至此 §2 索引 103 张卡全部 ✅（Stage 0–9 收官），计划内无待建卡任务**；上一条「下一步关键路径 = 阶段 3」是 2026-10-14 当次会话的记录，已被本节末尾覆盖。
- ✅ **本机 CI 等价门禁全绿（2026-10-17 收官扫描）**：`cargo test -q --workspace` → **281 passed**；`cargo clippy -q --workspace --all-targets -- -D warnings` → **零告警**；`cargo build --release -p main-bin` → `Finished release profile in 7.70s`；`cargo check -p kafka-driver --features real` → `Finished dev profile`；`cargo bench -p kafka-driver` → 两组数字；`frontend` 在 Node 22（v22.23.3）下 `npm run build` → `✓ built in 646ms`，且 `frontend/dist` 未产生字节变化（构建可复现）。**CI 本身仍未在托管 runner 上执行过**（本机无法触发），T092 卡片里那条诚实备注不变。

- ✅ **T103（阶段 10 · 文档补遗，2026-10-17）**：README 原本只讲构建与前置、从没说怎么把程序跑起来，补 `## 运行`（4 条路径：`cargo run -p main-bin`、`KAFKA_UI_DEV_DIR` + Node 22 免重编译、`--features real` + macOS openssl、docker compose broker）；同时修掉两个前置引用被空行切碎、代码块里的 `\#` 转义与 4 处标题后多余空行。仅动 `README.md`，R108–R109。
- ✅ **T104（阶段 10 · 文档补遗，2026-10-17）**：用户报「无法编译 real」。复现并定位为**宿主 OpenSSL 版本**：Homebrew 默认 `openssl` 已指向 openssl@4 4.0.3，而 OpenSSL 4 不再导出 ENGINE API，vendored librdkafka 2.12.1 仍调用 `ENGINE_by_id` 等 5 个符号，链接阶段 `Undefined symbols`（退出码 101）；`openssl@3`（3.5.9）已装但 keg-only，pkg-config 默认找不到。新增 `scripts/real-run.sh`（`brew --prefix openssl@3` → 导出 `OPENSSL_DIR`/`PKG_CONFIG_PATH` → `exec cargo "$@"`）并重写 README「运行」一节的 real 段（根因 + 脚本入口 + 手动等价命令 + 「`cargo check` 不链接」警告）。仅动文档与脚本，源码 0 行；R110（不提交 `.cargo/config.toml` 的 `[env]`，否则 Linux runner 会因该绝对路径失败）、R111（不改依赖绕过）。第二轮把裸命令也修好：机器级 `~/.cargo/config.toml` 的 `[env]` 钉住 `OPENSSL_DIR` / `PKG_CONFIG_PATH`（R112；`force` 默认 false，不覆盖显式值与 CI）；验证在两者全 unset 时 `cargo build -p main-bin --features real` 退出码 0，`cargo run -p main-bin --features real` 成功起窗。
- ✅ **T105（阶段 11 · 缺陷修复，2026-10-17）**：用户报「真 Kafka 订阅后一行数据都没有」。逐层实测证明通路无缺陷（驱动/router/bus/前端事件桥与 RPC 桥、T077 一致性、T100 E2E 全绿；真驱动下 `message.received` 一律晚于 RPC 回复，故「前端吞首批消息」的竞态被否定），真因是该 broker 上除探针主题外只有一个**空的** `__consumer_offsets`（50 分区、0 条消息），而消费页默认选中主题列表第一个、`topic.list` 又不保证顺序；对空主题 earliest+follow 订阅**正确地**得到 0 行，界面却不给解释。只改 `frontend/src/views/ConsumeView.vue`（+17/−2、1 个源文件）：默认选第一个非 `__` 主题，空态拆成「未订阅 / 已订阅无数据 / 过滤无匹配」三分支并对内部主题点明。R113–R114。
- ✅ **T106（阶段 11 · 缺陷修复，2026-10-17）**：用户报「订阅切换 tab 就不订阅了」。根因是主视图区用 `v-if` 串在 `ui.currentView` 上，切走 consume 会**卸载** `ConsumeView`，而它在 `onBeforeUnmount` 里 `void unsubscribe()`（原注释「leave page -> no leaked subscription」）——于是「看一眼别的 tab」被当成「离开页面」，订阅被前端主动取消。修法：把它移出 `v-if` 链、改由 `KeepAlive` 包裹（切 tab 只 deactivate），并补 `onActivated` 让主题列表回页面时仍刷新（R115–R117；`Shell.vue` +7/−1、`ConsumeView.vue` +9/−2；`npm run build` Node 22 / vite 7 通过）。
- ✅ **T107（阶段 10 · 文档补遗，2026-10-17）**：把三份说明按当前源码逐条对齐——`agents.md`（10→21 行）补预算口径（只有源码行计入；`docs/`、`*.md`、`.github/`、`scripts/`、`frontend/dist/`、`Cargo.lock` 不计）、不要跑 `cargo fmt`、每任务一张卡 + 卡尾 ≤5 行结果 + 手动清单；项目 skill 8 个 md（89→122 行）补 `main-bin` 入口层与 `KAFKA_UI_DEV_DIR`、`MessageStream`/`StreamControls`、`request.cancel` 与 9 个事件名、`tests/`+`benches/`+`examples/` 与 `active_pumps()` 判据、真实门禁命令，`toolchain` 版本改 1.99.1 并写入 openssl@4 的 `_ENGINE_by_id` 链接坑与解法；`README.md`（157→167 行）把三类能力拆成分节条目，**删掉与代码不符的「分页」**、起点写准、Mock 能力加限定语、技术栈补 criterion 与 vendored librdkafka 2.12.1、构建节补 clippy/bench/前端门禁。共 10 个 md、**0 行源码**（R118–R120）。
- ✅ **T108（阶段 10 · 文档补遗，2026-10-17）**：README 从「同时当两本书」收敛为**只服务第一次看仓库的人**（167→72 行：功能 7 条 + 快速开始 4 段可照抄命令 + 架构一图 + 文档指针 + License）；维护者正文（按平台前置、五条门禁与三条注意事项、技术栈版本表、Workspace 职责、IPC 协议、openssl@4 根因与两种解法、broker 端口、数据目录、卡片与预算约定）整体移入新增的 `docs/development.md`（117 行）；README 只留一行指针，漂移面收敛到链接。围栏成对、5 个链接目标均在；全部 `*.md`、0 行源码（R121–R123）。
- ✅ **T109（阶段 11 · 缺陷修复，2026-10-17）**：`release.yml` 从未跑过（`git tag` 为空），静态校验却查出四处硬伤——① Linux 两个 job **没有系统依赖步骤**（ci.yml 有、release.yml 漏了）⇒ `cargo build` 必失败；② `macos-13` **2025-12-04 已退役**、③ `macos-14` **2026-11-02 退役**（10 月已有 brownout）⇒ 改 `macos-15-intel` / `macos-15`，ci.yml 同一 label 一并换以保住 R98 的对齐；④ `tar -a -cf *.zip` 在 windows runner 上走 Git 的 GNU tar，而后缀表里没有 `.zip` ⇒ 产出**扩展名撒谎的未压缩 tar**，改用 `Compress-Archive`（pwsh）。另补 `--locked` 与 `generate_release_notes: true`。`ruby -ryaml` 双文件解析通过；仍**未在托管 runner 上执行过**（R124–R126）。
- ✅ **T110（阶段 11 · 缺陷修复，2026-10-17）**：发现 `release.yml` 一直只跑 `cargo build --release -p main-bin`，**漏了 `--features real`** —— Mock 是默认驱动，于是发布产物是 Mock-only 的，用户下载后接真实集群只会拿到 `real Kafka driver is not compiled in; rebuild with --features real`。已加 `--features real`，并补齐这条命令新引入的前置：Linux 由已有 apt 清单覆盖（`libssl-dev`+`cmake`）、macOS 显式钉 keg-only 的 `openssl@3` 并导出 `OPENSSL_DIR`/`PKG_CONFIG_PATH`（R128）、Windows 定位带 `libcrypto.lib` 的安装根目录且找不到就 `throw`（R129）；real 驱动动态链接 OpenSSL，故 Windows 产物把 `libcrypto-3-x64.dll`/`libssl-3-x64.dll` 与 exe 一起打包。取证：本机跑同一条命令 exit 0、`strings` 查不到占位文案，而反向对照（不带 features）能查到 ⇒ 判据有效。仍未在任何 runner 上执行过（R127–R129）。
- ✅ **T111（阶段 11 · 缺陷修复，2026-10-17）**：v0.1.1 只有 1 个产物、4 个 job 全红的根因查清了——T110 的 `--features real` 让 librdkafka 的 C 源码参与构建，而 `rdkafka_conf.c:60` 的 `#include <curl/curl.h>` 由 **`WITH_OAUTHBEARER_OIDC`** 守卫（不是 `WITH_CURL`），`rdkafka-sys` 又无条件 `WITH_SASL_OAUTHBEARER=1`，于是三个平台都需要 curl 头。Docker 里照抄 workflow 的 apt 清单复现出同样的报错与 101；改为给 rdkafka 打开 `curl` feature（`curl-sys` 提供头/库）后，同一容器**不装任何 curl 开发包**即 `CARGO_EXIT=0`；本机 `--features real` 与随后 `--locked` 构建都绿；Windows 的 OpenSSL 探测也按镜像真实布局改正（根 `C:\Program Files\OpenSSL`，在 `lib\` 下递归找 `libcrypto.lib`），DLL 改 glob 取名。真实验收待下一次打 tag（R130–R132）。

---

## 2. 任务索引

### 阶段 0 · 环境与基线（阻塞项）

| ID | 任务 | 依赖 | 验证 | 状态 |
|---|---|---|---|---|
| T001 | 修复 MSVC 链接环境（装 Windows SDK） | — | `rustc hello.rs` 能链接；`cargo check -p kafka-core -p kafka-driver` 通过 | ✅（环境已通；第 3 步的代码缺陷转 T052/T053） |
| T002 | 全仓基线跑绿 + 首次提交 | T001 | `cargo test --workspace`、`cargo clippy --workspace --all-targets` | ✅（108 tests 全绿；`-D warnings` 也零告警） |

### 阶段 1 · Mock 驱动补测与补齐（`kafka-driver`）

| ID | 任务 | 依赖 | 验证 | 状态 |
|---|---|---|---|---|
| T010 | Mock 测试：生命周期 / cluster_info / capabilities | T002 | `cargo test -p kafka-driver --test mock_lifecycle` | ✅ |
| T011 | Mock 测试：topic CRUD / describe / 配置 / offsets | T002 | `--test mock_topics` | ✅（8/8 全绿，未触发 T016 缺陷） |
| T012 | Mock 测试：produce + subscribe 消息流 + offset 语义 | T002 | `--test mock_produce_consume` | ✅（10/10 全绿，未触发 T016 缺陷） |
| T013 | Mock 测试：流控制 pause/resume/seek/cancel | T002 | `--test mock_stream_ctl` | ✅（6/6 全绿，未触发 T016 缺陷） |
| T014 | Mock 测试：消费组 / 位点查询 / reset / commit | T002 | `--test mock_groups` | ✅（8/8 全绿，未触发 T016 缺陷） |
| T015 | Mock 测试：故障注入 + `DriverEvent` 事件流 | T002 | `--test mock_faults` | ✅（7/7 全绿，无 ignore；mock_groups 8/8 以 `--release` 复跑通过） |
| T016 | 修复 T010–T015 暴露的 Mock 缺陷（范围由失败用例决定） | T010–T015 | 上述测试全绿 | ✅（静态修复经 57 测试全绿验证；0 失败用例，观测到的 2 处语义漂移登记 T069） |
| T017 | Mock 数据落盘：snapshot JSON 保存/恢复 | T002 | `cargo test -p kafka-driver --test mock_persist` | ✅（5/5 全绿；整仓 62 测试 + clippy 零告警） |

### 阶段 2 · 业务层（`kafka-app`，全部用 Mock 测试）

| ID | 任务 | 依赖 | 验证 | 状态 |
|---|---|---|---|---|
| T020 | SQLite 底座：连接 / 迁移 / 错误映射 | T002 | `cargo test -p kafka-app` | ✅（92 tests 全绿，T059 后验证） |
| T021 | Store：集群配置 CRUD（含导入导出 JSON） | T020 | `--test store_clusters` | ✅（12/12；导入时间戳缺陷由 T067 修掉） |
| T022 | Store：应用设置 KV + 最近打开 + Mock 快照 | T020 | `--test store_settings` | ✅（9/9） |
| T023 | 事件总线：`AppEvent` + broadcast 封装 | T002 | `--test bus` | ✅（9/9；tag/字段名漂移由 T064 修掉并加守卫） |
| T024 | ClusterService + 驱动注册表（连接/切换/断开） | T021,T023 | `--test svc_clusters` | ✅（10/10） |
| T025 | TopicService（透传 + 事件驱动刷新） | T024 | `--test svc_topics` | ✅（8/8；drain 时序缺陷由 T066 修掉） |
| T026 | ProduceService（单条/批量 + 回执映射） | T024 | `--test svc_produce` | ✅（7/7） |
| T027 | ConsumeService（订阅注册表 + 暂停/恢复/seek/停止） | T024 | `--test svc_consume` | ✅（7/7；补了「保留调用方 subscription id」断言） |
| T028 | GroupService（列表/详情/位点/reset） | T024 | `--test svc_groups` | ✅（8/8） |
| T029 | 驱动护栏：超时 + 重试包装（用故障注入测） | T024,T015 | `--test driver_guard` | ✅（15/15；T065 未改变可重试集合） |
| T030 | `App` 门面装配（store+bus+services）+ 冒烟测试 | T025–T029 | `cargo test -p kafka-app` | ✅（94/94，其中 `app_smoke` 2/2；clippy 零告警） |

### 缺陷卡（发现即登记；T057–T059 是空闲号段）

| ID | 任务 | 依赖 | 验证 | 状态 |
|---|---|---|---|---|
| T051 | 修 `Slot` + `EventBus::Inner` 的 `Debug` derive（`Arc<dyn KafkaDriver>` 不满足 `Debug`，级联到 4 个 service） | T053 | `cargo check -p kafka-app` | ✅（T056 后验证） |
| T052 | 修 `kafka-core` 首次编译报错（缺 anyhow 依赖 / `Clone` vs `anyhow::Error` / serde `Default` / `B64::encode`） | T001 | `cargo check -p kafka-core --all-targets` | ✅ |
| T053 | 修 `kafka-driver` 首次编译报错（mock：4 个缺 `Default`、`f32: Eq`、`produce` 双重借用、种子夹具循环） | T001,T052 | `cargo check -p kafka-driver --all-targets` | ✅ |
| T054 | 清 `kafka-driver` 存量 clippy 告警（实测 4 条：`pump.rs` 未读字段 / `contains()` ×2 / 冗余 cast；另 3 处 Default 后赋值） | T053 | `cargo clippy -p kafka-driver --all-targets` 零告警 | ✅（`join` 字段删除；空闲空转转 **T068**） |
| T055 | 清 `kafka-core` 存量 clippy 告警 2 条（`dto.rs` 的 `map().flatten()`） | T052 | `cargo clippy -p kafka-core --all-targets` 零告警 | ✅ |
| T068 | Mock pump 在 `follow: true` 空闲时永不退出（`tx.send` 走不到 → 探测不到接收端已亡） | T054 | `cargo test -p kafka-driver`（含 2 条新测试） | ✅（`tx.is_closed()` 空闲退出；`active_pumps` 可观测计数 + 2 条测试） |
| T069 | Mock 语义漂移：list_offsets 未知 topic 静默跳过 + SearchFilter::default 与 serde 默认不一致 | T011,T012 | `cargo test -p kafka-core -p kafka-driver` | ✅（未知 topic → `TopicNotFound`；`SearchFilter` 手写 Default 对齐 serde；2 条新测试 RED→GREEN，全量回归全绿） |
| T056 | 修 `kafka-app` 首次编译报错 7 个（含 T051/T052 的 2 处连带影响，见卡片表格） | T053 | `cargo check -p kafka-app --all-targets` | ✅（`src/` 已绿；`--all-targets` 剩 9 错拆给 T057/T058） |
| T057 | 修 `kafka-app` 测试编译错误第一批（`svc_groups`/`driver_guard`/`store_settings`，6 个机械错） | T056 | `--test svc_groups --test driver_guard --test store_settings` | ✅（15+9+8 全绿） |
| T058 | 修 `kafka-app` 测试编译错误第二批（`svc_clusters`/`svc_topics`，含 `Arc<dyn KafkaDriver>: Debug` 设计信号） | T056 | `--test svc_clusters --test svc_topics` | ✅（`--all-targets` 已 EXIT=0；跑绿后浮出 T066） |
| T059 | 清 `kafka-app` 存量 clippy 告警（实测 3 条：冗余 cast / `Copy` 上的 `.clone()` / 未用变量补断言） | T057,T058 | `cargo clippy -p kafka-app --all-targets` 零告警 | ✅（92 tests 仍全绿） |
| T064 | 修 `AppEvent` 的 serde 形状与 wire 契约漂移（tag 变 camelCase、payload 字段没变 camelCase） | T057 | `--test bus --test svc_groups --test svc_produce` | ✅（含 9 variant 的 tag 漂移守卫） |
| T065 | 修 `ErrorDto::new` 硬编码 `retryable:false`（与 `is_retryable` 分类漂移） | T057 | `cargo test -p kafka-core` + `--test bus --test driver_guard` | ✅ |
| T066 | 修 `svc_topics.rs:278`「读操作不发事件」断言失败（测试在写入前 drain，自己那条 `TopicChanged{Created}` 还在队列） | T058 | `--test svc_topics --test svc_clusters` | ✅（补断言顺带锁「恰好一条不重复发布」） |
| T067 | 修 `import_clusters` 覆盖 `updated_at` 导致 export→import→export 不恒等 | T058 | `--test store_clusters --test svc_clusters` | ✅（新增 `Stamp::{Now,Preserve}`） |
| T078 | Mock `rng()` 每次调用混入 `now_ms`（`seeded()`/`request_seed()` 只管夹具、不给 roll 播种）→ 0<p<1 的故障序列不可复现 | T015 | `cargo test -p kafka-driver` + pinned-seed 中段概率新测试 | ✅（2/2 全绿：p=0.5 序列 + 延迟抖动序列均跨实例复现；套件 68/68；clippy 零告警） |
| T079 | `restore_json` 整状态替换（含 `connected`/`config`/`brokers` 等 identity 字段）→ 改配置后 restore 拿回旧值 + 幽灵 connected；须在阶段 3 接线 restore 之前落地 | T017 | `cargo test -p kafka-driver --test mock_persist` + 「改配置后 restore 保留 live identity」新测试 | ✅（新测试先 RED 后 GREEN；driver 69/69、app 94/94、clippy 零告警） |

### 阶段 3 · WebView 桥接（`webview-bridge` + `main-bin`）

| ID | 任务 | 依赖 | 验证 | 状态 |
|---|---|---|---|---|
| T031 | tao+wry 窗口启动 + 静态资源加载（dev/embedded） | T030 | 手动：窗口打开显示占位页 | ✅（T039 条目1 实测 2026-10-15） |
| T032 | Router 内核：envelope 解析 + 命令表 + 假 App 单测 | T030 | `cargo test -p webview-bridge` | ✅ |
| T033 | Router：`cluster.*` + `config.*` | T032 | `--test router_cluster` | ✅ |
| T034 | Router：`topic.*` + `produce.*` | T032 | `--test router_topic_produce` | ✅ |
| T035 | Router：`consume.*` | T032 | `--test router_consume` | ✅ |
| T036 | Router：`group.*`（`mock.*` 拆分为 T084/T085，见 ledger 裁定） | T032 | `--test router_group` | ✅ |
| T084 | Mock 控制接缝：`KafkaDriver::as_any`（默认 None + Mock 覆写） | T032 | `cargo test -p kafka-driver`（含 conformance.rs） | ✅ |
| T085 | Router：`mock.*`（经 T084 downcast） | T084 | `--test router_mock` | ✅ |
| T037 | 事件泵：总线 → JS `evaluate`（节流/批量） | T032 | `--test event_pump`（假 sink） | ✅ |
| T038a | GuardedDriver 接入 + as_any 转发 + Mock 快照存取（SQLite slot） | T084,T085 | `cargo test -p kafka-app` + workspace | ✅ |
| T038b | `main.rs` 启动装配：日志、数据目录、App、Router IPC、事件泵、窗口 | T031,T033–T037,T085,T038a | 手动：Mock 集群可连通 | ✅（T039 条目2-8/10 实测 2026-10-15；R14：JS 侧用相对 /rpc） |
| T039 | 端到端冒烟（Mock）：连接→列 topic→发消息→收事件 | T038b | 手动清单全过 | ✅（用户全 10 条通过 2026-10-15） |

### 阶段 4 · 前端 UI（HTML + Vue3 + Tailwind，全部对接 Mock）

| ID | 任务 | 依赖 | 验证 | 状态 |
|---|---|---|---|---|
| T040 | 资源骨架 + 本地 vendored Vue/Tailwind + `rpc.js`/`events.js` | T039 | 手动：页面加载无 404 | ✅（ae3824f，用户实测 2026-10-15：Vue 应用渲染/无404/样式正常） |
| T041 | 应用外壳：布局 / 导航 / 主题切换 / 状态栏 | T040 | 手动 | ✅待手动验证（代码审查已过 c71c2d0；窗口项并入批量清单） |
| T042a | 集群页：列表+连接/断开/删除+导航接线（R22 拆分） | T041 | npm build+cargo build+冒烟 | ✅审查过+修复过（70fd711+398d38f），窗口项批量 |
| T042b | 集群编辑弹窗（Mock 开关、SASL/SSL 字段） | T042a | npm build+cargo build+冒烟 | ✅同上（edit 回写已修 398d38f） |
| T043 | 集群树（topic→partition）+ 连接状态徽标 | T042 | npm build+cargo build+冒烟 | ✅审查过+修复过（b545a72+df55ee5），窗口项批量 |
| T044a | Topic 表格 + 创建/删除（R22 预拆） | T043 | npm build+cargo build+冒烟 | ✅审查过（c505656），window.confirm 待窗口实测 |
| T044b | Topic 配置编辑（wire 不支持则 BLOCKED 登后端缺口卡） | T044a | npm build+cargo build+冒烟 | ✅审查过（d24323f，0C/0I），窗口项批量 |
| T045 | 生产面板（key/value/headers/partition + JSON 编辑器） | T044 | npm build+cargo build+冒烟 | ✅审查过（7d4ca0e，0C/0I），窗口项批量 |
| T046a | 消费面板：订阅+实时表格（R22 预拆） | T045 | npm build+cargo build+冒烟 | ✅审查过（987c570，0C/0I），窗口项批量 |
| T046b | 消费控制 暂停/恢复/seek（wire 缺则 BLOCKED 登缺口卡） | T046a | npm build+cargo build+冒烟 | ✅审查过（e2f2ad6，0C/1I→T046c），窗口项批量 |
| T046c | 消费徽标同步：订阅 subscription.state/done 事件 | T046b | npm build+cargo build+冒烟 | ✅审查过（c83e999，0C/0I），窗口项批量 |
| T047 | 消息解码渲染（utf8/JSON/base64/hex）+ 复制/导出（R25 改口径） | T046 | npm build+cargo build+冒烟 | ✅审查过（8082df9，0C/0I），窗口项批量 |
| T047b | 消费表格搜索过滤（query/key/value/大小写，客户端过滤） | T047 | npm build+cargo build+冒烟 | ✅审查过（a04dd92，0C/0I），窗口项批量 |
| T048a | 消费组页：列表+详情（成员/位点/lag）（R22 预拆） | T044 | npm build+cargo build+冒烟 | ✅审查过（3c6341c，0C/1I→R26 并入 T048b），窗口项批量 |
| T048b | 位点重置弹窗（wire 缺则 BLOCKED 登缺口卡） | T048a | npm build+cargo build+冒烟 | ✅审查过（5547e74+9ceaf71，修复闭环），窗口项批量 |
| T049a | 全局 toast + 全局 loading（R22 预拆） | T047b | npm build+cargo build+冒烟 | ✅审查过（9c14cc9，0C/0I），窗口项批量 |
| T049b | 日志面板（环形缓冲+事件/RPC 摘要） | T049a | npm build+cargo build+冒烟 | ✅审查过（fa606cd，0C/0I），窗口项批量 |
| T050a | 空状态组件+集群/topic/消费组三视图接线（R22 预拆） | T049b | npm build+cargo build+冒烟 | ✅审查过（faf8421+d501ccc 修复闭环），窗口项批量 |
| T050b | 消费空状态 + Alt+1..6 导航快捷键 | T050a | npm build+cargo build+冒烟 | ✅审查过（7d9caf7，0C/0I），窗口项批量 |
| T050c | 弹窗 Esc 关闭（四弹窗各接局部 keydown） | T050b | npm build+cargo build+冒烟 | ✅审查过（189168e，0C/0I），窗口项批量 |

### 阶段 5 · 事件驱动 NIO 流强化

| ID | 任务 | 依赖 | 验证 | 状态 |
|---|---|---|---|---|
| T060 | 消息节流/批量策略（速率 + 缓冲上限） | T037,T027 | `cargo test -p kafka-app --test consume_throttle` | ✅（65e8a4f；243 tests=239+4，clippy 零；审查 Approved 0C/0I/4m） |
| T061 | 背压与丢弃策略（有界通道 + 溢出计数） | T060 | `--test consume_backpressure` | ✅（2282b2a；产品码 0 改动、仅 5 测试 +324 行；248 tests=243+5，clippy 零；审查 Approved 0C/0I/5m） |
| T062 | 断线自动重连（指数退避 + 状态事件） | T024 | `--test reconnect` | ✅（3607dde+8cf2ec3；产品码 clusters.rs 1 文件 +166−6，无新 wire 事件；6 测试；254 tests=248+6，clippy 零；主模型审查 Approved，1 Important→已补过滤器/info 契约测试，余 Minor） |
| T063 | 资源回收：关闭订阅 / 释放驱动 / 优雅退出 | T027 | `--test teardown` | ✅（32ffe28 + 修复波；产品码仅 consume.rs +12/−2；259 tests=254+5，clippy 零；审查 Approved 0C/1I/6m，I1 为诚实性已修：teardown 套件无法区分优雅 join 与 abort，已在测试/注释中如实标注，mock「忽略 Stop」旋钮列为后续卡） |

### 阶段 6 · RealKafka 驱动（`--features real`，此阶段才需 Kafka）

| ID | 任务 | 依赖 | 验证 | 状态 |
|---|---|---|---|---|
| T070 | `real` 模块骨架 + 配置映射 + 连接生命周期 | T001 | `cargo check -p kafka-driver --features real` | ✅（248 行/2 文件；check+clippy 绿；Docker Kafka 实测 connect/cluster_info/disconnect；R30/R32） |
| T071a | real：每操作会话脚手架 + `list_topics` | T070 | 真集群手动清单 | ✅（92 行/2 文件；check+clippy 绿；Docker Kafka 实测 list_topics 出 t071-demo internal=false 3 分区 rf=1 与 __consumer_offsets internal=true 50 分区；断开 → NotConnected；R33/R35） |
| T071b | real：`describe_topics`（分区明细 + 起止位点 + 配置） | T071a | 真集群手动清单 | ✅（212 行/2 文件；check+clippy 绿；Docker Kafka 实测 3 分区 leader/副本/ISR、空 topic low==high==0、生产后高水位 1、点名 nope → TopicNotFound、include_internal 切换、显式点名 internal 放行、只回被覆盖配置；R34/R36） |
| T072a | real：create / delete topic | T071b | 真集群手动清单 | ✅（193 行/3 文件；check+clippy 绿；mock 72/72；Docker Kafka 实测建 3 分区 rf=1 带 retention.ms、重复建 → AlreadyExists、一次删三名逐项汇报（成功 / `unknown topic` / 拒绝 internal）、broker 侧 `--list` 复核已消失；R38/R40） |
| T072b | real：update topic config（合并后整体替换） | T072a | 真集群手动清单 | ✅（94 行/3 文件；check+clippy 绿；mock 72/72；Docker Kafka 实测双项写入、只写一项时另一项保留、unset 单项/全部、不存在的主题 → NotFound、空名 → InvalidConfig，并用 `kafka-configs.sh --describe` 复核；R37/R39/R41） |
| T073 | real：produce（批量 + 回执） | T070 | 真集群手动清单 | ✅（212 行/3 文件；R42/R43/R44） |
| T074a | real：list_offsets（起止位点 + 时间点解析） | T073 | 真集群手动清单 | ✅（206 行/2 文件；R45/R46/R47/R48） |
| T074b | real：subscribe 流核心（有界/追尾 + stop） | T074a | 真集群手动清单 | ✅（248 行/2 文件；R49/R50/R51/R52） |
| T074c | real：subscribe 投递成形与控制（filter/max + pause/resume/seek） | T074b | 真集群手动清单 | ✅（95 行/1 文件；R53/R54） |
| T075a | real：消费组列表与详情（list / describe） | T071b | 真集群手动清单 | ✅（170 行/2 文件；check+clippy 绿；mock 72/72；Docker Kafka 实测列出活跃组 t075a-g（Stable/1 人/range）与空闲组 t071-g（Empty/0 人）、成员 id/host/分配分区与 `kafka-consumer-groups.sh --describe` 逐列一致、不存在的组 → GroupNotFound；R55/R56/R57） |
| T075b | real：消费组位点 + 滞后 | T075a | 真集群手动清单 | ✅（144 行/3 文件；check+clippy 绿；mock 72/72；Docker Kafka 实测 reset 到 earliest 后读回 offset=0 且 lag 3/1/1 与 broker LAG 列一致、`with_lag=false` 全 None、未提交主题回 0 行、ghost 组/主题与空白组名的三种错误路径；R58/R59/R60） |
| T075c | real：reset_offsets / commit_offsets | T075b | 真集群手动清单 | ✅（213 行/3 文件；check+clippy 绿；mock 72/72；Docker Kafka 实测 commit 1/0/2 后 broker CURRENT-OFFSET 一致且 `metadata` 回读成功、四种 spec（earliest/latest/时间戳×2/指定 offset）写出位点与现场一致、dry_run 不落盘、越界 → OffsetOutOfRange、ghost 组/主题/分区与三种入参错误路径；R61/R62/R63） |
| T076 | real：SASL/SSL 参数映射（来自 `ClusterConfig`） | T070 | 真集群手动清单 | ✅（87 行/2 文件；check+clippy 零告警；mock 72/72；dev broker 追加 SASL:9094 与 SSL:9095 监听器后实测：PLAINTEXT 回归 OK、SASL/PLAIN alice 登录 OK、错密码失败、SSL+`ca.pem` 且校主机名 OK、无信任锚失败、关校验 OK、证书缺私钥 → InvalidConfig、`saslProtocol` 覆盖生效；R64–R68） |
| T077 | Mock/Real 行为对齐测试架（`#[ignore]` + 环境变量门控） | T071–T076 | `cargo test -p kafka-driver -- --ignored` | ✅（249 行/1 文件；默认套件 72/72 + 本卡 2 ignored；`clippy --all-targets -- -D warnings` 零告警；Docker dev broker 实测两条 `#[ignore]` 测试通过：transcript 23 行 mock 与 real **逐字相等**（create/twice AlreadyExists/partitions 3/replicas 1/retention.ms 60000→120000→absent/produced 3/delivery errors 0/earliest [0,0,0]/latest [1,1,1]/read back p0,p1,p2/六条拒绝 InvalidConfig·InvalidConfig·NotFound·NotFound·Unsupported·OffsetOutOfRange/after delete NotFound）；R30 与 R33 钉子同时通过；跑完 broker 无 `t077-*` 残留；R69–R72） |

### 阶段 7 · 健壮性与可观测

| ID | 任务 | 依赖 | 验证 | 状态 |
|---|---|---|---|---|
| T080 | tracing → `log.line` 事件 → 前端日志面板 | T037,T049 | 手动 | ✅（250 行/3 文件；4 个新单测含端到端；clippy 零告警；R73–R75） |
| T081 | 错误映射审计：每种 `ErrorKind` 的 hint 覆盖测试 | T030 | `cargo test -p kafka-core` | ✅（140 行/2 文件；3 个新单测穷尽 15 个 kind；R76–R78） |
| T081b | 前端接上 `error.hint`（R78：`rpc.ts` 目前把它丢掉） | T081 | `npm run build` + 手动 | ✅（19 行/1 文件；22 个 catch 点一处改动全覆盖；R79/R80） |
| T082a | 路由层全局超时：handler 截止时间 + `with_timeout` | T032 | `--test global_deadline` | ✅（114 行/2 文件；R81/R84） |
| T082b | 取消在途请求：`request.cancel` + 在途表 + `cancel()` | T082a | `--test cancel_inflight` | ✅（245 行/3 文件；3 个新测试；R82/R83） |
| T082c | 前端接线：`AbortController` + 顶栏取消按钮 | T082b | `npm run build` + 手动 | ✅（65 行/2 文件；R85/R86） |
| T083a | 日志落盘与轮转（`<data_dir>/logs/app.log`，按大小保留 3 份） | T080 | `cargo test -p main-bin` | ✅（221 行/2 文件；4 个新单测；无新依赖；R87–R89） |
| T083b | 日志面板导出（当前筛选结果 → Blob 下载 `.txt`） | T083a | `npm run build` + 手动 | ✅（33 行/2 文件；抽出 `downloadText` 共用；R90–R92） |

### 阶段 8 · 打包与跨平台

| ID | 任务 | 依赖 | 验证 | 状态 |
|---|---|---|---|---|
| T090 | 前端资源 `include_dir` 嵌入 + 资源路由 | T040 | `cargo run -p main-bin` 显示 UI | ✅（R18 absorbed，随 T040 用户实测确认） |
| T091 | Windows WebView2 运行时检测与首启提示 | T090 | 手动 + `cargo test -p webview-bridge` | ✅（218 行/3 文件；4 个新单测；R93–R96） |
| T092 | CI 矩阵（win/linux/macos）+ 产物上传 | T090 | CI 绿 | ✅（新增 ci.yml；release.yml node 20→22；R97–R100） |
| T093 | Linux webkitgtk 依赖说明 + README | T092 | 文档 | ✅（README 增「前置（Linux）」，与 ci.yml 的 apt 清单一致；R101） |

### 阶段 9 · 系统测试与性能

| ID | 任务 | 依赖 | 验证 | 状态 |
|---|---|---|---|---|
| T100 | Mock 全流程脚本化 E2E 测试 | T063,T050 | `cargo test --workspace` | ✅（3d55d75；仅新增 main-bin E2E 测试 +535 行、产品码 0 改动；260 tests=259+1，clippy 零；审查 Approved 0C/0I/9m） |
| T101 | criterion 基准：Mock produce/consume 吞吐 | T030 | `cargo bench -p kafka-driver` | ✅（95 行/3 源文件；produce 4.27 Melem/s、consume 80.6 Kelem/s；R102–R104） |
| T102 | 长跑内存/句柄泄漏检查 | T063 | 手动 + 报告 | ✅（1 源文件/162 行；20k+40k 轮 38 个采样点 rss/fds 零增长、泵计数全程 0；R105–R107；报告 docs/reports/T102-long-run-leak-report.md） |

### 阶段 10 · 文档补遗

| ID | 任务 | 依赖 | 验证 | 状态 |
|---|---|---|---|---|
| T103 | README 补「运行」章节 + 修前置块排版 | 无 | 文档 | ✅（仅 README.md；新增 4 条运行路径，修掉被切碎的两个前置引用与 `\#` 转义残留；R108–R109） |
| T104 | `--features real` 链接失败：Homebrew openssl@4 缺 ENGINE API | 无 | 手动（`scripts/real-run.sh build -p main-bin --features real`） | ✅（仅 README.md + scripts/real-run.sh；默认 pkg-config 已是 openssl@4 4.0.3 且不导出 `_ENGINE_*`，vendored librdkafka 2.12.1 仍调用 → 链接 101；脚本定位 keg-only openssl@3 后退出码 0，并在机器级 `~/.cargo/config.toml` 钉住 openssl@3 使裸命令直接可编译；R110–R112） |
| T107 | agents.md / 项目 skill / README 描述对齐当前源码 | 无 | 文档（人工通读 + 与源码逐条核对） | ✅（10 个 md、0 行源码；删掉假「分页」等 3 处纠错，补齐预算口径 / 入口层 / 门禁 / openssl 坑；R118–R120） |
| T108 | README 瘦身：读者优先，维护者内容拆成 development.md | 无 | 文档（人工通读 + 链接与围栏校验） | ✅（README 167→72 行 + 新增 docs/development.md 117 行；0 行源码；R121–R123） |

### 阶段 11 · 缺陷修复

| 卡 | 主题 | 依赖 | 验证 | 状态 |
|---|---|---|---|---|
| T105 | 消费页空态：默认不选内部主题 + 说明「已订阅但没有数据」 | 无 | 手动（`npm run build` + 真 Kafka 订阅观察） | ✅（仅 `ConsumeView.vue` +17/−2；默认落在非 `__` 主题；空态三分支；R113–R114） |
| T106 | 切 tab 不再中断消费订阅 | 无 | 手动（npm run build + 切 tab 观察订阅仍在） | ✅（`Shell.vue` +7/−1 把 `ConsumeView` 移出 `v-if` 链、改由 `<KeepAlive>` 包裹；`ConsumeView.vue` +9/−2 增加 `onActivated` 刷新主题列表、订正 `onBeforeUnmount` 注释；R115–R117） |
| T109 | 修正 release workflow：Linux 依赖 / 退役 runner label / Windows 真 zip | T092 | 手动（YAML 解析 + 外部事实核对） | ✅（release.yml 补齐四处可执行性缺陷，ci.yml 的 macos-14 同步；0 行源码；R124–R126） |
| T110 | 发布产物内置 real 驱动（release 用 `--features real`） | T109 | 手动（产物字符串取证 + YAML 解析） | ✅（release 构建加 `--features real`，macOS/Windows 各自备 OpenSSL，Windows 随包发两个 DLL；0 行源码；R127–R129） |
| T111 | 修 release 的 4/5 平台失败：librdkafka 需要 curl 头 | T110 | 手动（Docker 复现 + 本机 --locked 构建） | ✅（根 `Cargo.toml` 的 rdkafka 加 `curl` feature，由 `curl-sys` 提供头/库；`Cargo.lock` +16 行新增 `curl-sys`；Linux 容器复现 exit 101 → 修后 `CARGO_EXIT=0`；Windows OpenSSL 探测按镜像真实布局重写；R130–R132） |

---

## 3. 关键路径

```
T001 ✅ → T052 ✅ → T053 ✅ → {T010..T015} → T016 → T017
                            ↘ T020 → T021/T022 → T024 → {T025..T029} → T030 → T032 → {T033..T037} → T038a → T038b → T039 → 阶段4
                            ↘ T056 ✅ → T057 ✅ → {T064 ✅, T065 ✅} → T058 ✅ → {T066 ✅, T067 ✅} → T059 ✅ → T054 ✅ → T055 ✅ → T002 ✅ 基线已提交
```

**基线已建立（T002）**：`cargo test --workspace` → **108 passed / 0 failed**
（`kafka-core` 5 + `kafka-driver` 11 + `kafka-app` 92 / 10 个集成目标）；
`cargo clippy --workspace --all-targets -- -D warnings` → **零告警**。首次提交已落地。

下一步的关键路径有两条，可并行：
1. **阶段 1 补测**：T010–T015（Mock 各域集成测试，现在编译与测试都通了，可以真正写起来）→ T016 → T017。
2. **阶段 2 收尾**：**T030**（`App` 门面装配 + 冒烟测试）→ T032（桥接层）→ 阶段 4 前端。

新登记的缺陷卡：**T068**（Mock pump 在 `follow: true` 空闲时永不退出）。
本会话修掉的 4 个行为缺陷：**T064**（`AppEvent` serde 形状漂移 + 漂移守卫）、
**T065**（`ErrorDto::new` 写死 `retryable:false`）、**T066**（`svc_topics` drain 时序，测试缺陷）、
**T067**（`import_clusters` 覆盖 `updated_at`，往返不恒等）。

**快照双路径裁定（终审批次）**：应用侧唯一路径是 SQLite 槽位（`store/settings.rs` 的
`save/load_mock_snapshot`，T022 引入，T038a 接线）；`MockPersistence`（文件，T017）
定位为 dev/debug + 导入导出工具，不是应用主路径。
restore 的恢复范围（整状态 vs 只恢复 data）由 **T079** 裁定，且 T079 必须先于阶段 3 任何接线 restore 的卡片。

阶段 4（前端）与阶段 2/3 可在 T039 之后并行，但**同一会话仍只做一张卡**。

---

## 4. 建卡规则（后续阶段）

索引里 `⏸待建卡` 的任务，在**即将开始它的那个会话**里才建卡，卡片放 `docs/tasks/`，模板见 `docs/tasks/_TEMPLATE.md`。
建卡时只允许读：本索引 + 直接依赖任务的「结果」小节（不要读它们的完整 diff）。
