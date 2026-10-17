# 阶段 4 批量手动窗口验收清单（T040–T050c）

状态: ✅全部 21 项通过（用户批量验收 2026-10-16，反馈「通过」）——`docs/TASKS.md` T040–T050c 各行已 🔶→✅

前置：`cargo run -p main-bin`（窗口+devtools F12）。除注明外全部在 UI 里点按操作。

## A. 壳与基础

1. **T040/T090**：窗口渲染 Vue 应用（非占位页）；Network 无 404；Tailwind 样式生效。
2. **T041**：左侧导航切换 5 视图；主题切换后**重启仍保持**；顶栏徽标随连接变色。
3. **T050a**：全新启动各页显示友好空状态；集群页「新建集群」按钮打开弹窗。
4. **T050b**：Alt+1..5 切导航；**在搜索框输入时按 Alt+2 不切换**（输入焦点守卫）。
5. **T049a**：新建一个真实集群（假 broker）连接 → 顶栏 spinner 出现 → 失败弹**红色 toast**（6s 自动消失）。

## B. 集群与树

6. **T042a**：建 mock 集群 → 连接（徽标变绿）→ 断开 → 再连 → 删除（列表消失）。
7. **T042b**：编辑集群改名字 → 保存 → 重开弹窗**回填**新名字（SASL/SSL 字段往返）。
8. **T043**：左树展开集群→topics→partitions（leader/ISR 徽标）；建/删 topic 时树自动更新；**生产消息时树不闪断**（终审 A1 修复项）。
9. **T049b**：日志面板出现 topic.changed/rpc 摘要行；级别过滤；清空生效。

## C. Topic 与生产

10. **T044a**：topic 表格创建 `smoke-ui`；删除走**琥珀色内联确认行**（无原生弹窗——终审 B1 修复项），二次点击才删；树同步消失。
11. **T044b**：配置编辑器读到 topic configs；set 一个键、unset 一个键 → 重开确认已生效。
12. **T045**：生产面板选 `smoke-ui`：文本消息 1 条 + JSON 模式 1 条（非法 JSON 应行内报错不下发）；带 key+header+指定 partition 各试一次 → 回执表显示 offset/partition。

## D. 消费与渲染

13. **T046a**：消费页订阅 `smoke-ui` earliest+follow → 第 12 条的消息出现在表格里；离开页面再回来，旧订阅不残留（行数不翻倍）。
14. **T046b**：暂停 → 生产 1 条 → 表格**不增长** → 恢复 → 增长；seek 到 0 → 重放历史。
15. **T046c**：暂停/恢复时状态徽标同步（running/paused）；非 follow 订阅读完后徽标 done。
16. **T047**：JSON 值美化显示；中文 utf8 正常；切 Base64/Hex 模式；复制按钮粘贴出正确文本；导出下载 .txt。
17. **T047b**：过滤框输入某子串表格收窄并显示 N/总数；只勾 matchKey 时按 key 过滤；清空恢复全量。

## E. 消费组与收尾

18. **T048a**：消费组页：mock 未订阅时显示空状态提示（含「Mock 需先有消费者组」）；订阅后回此页应见组（describe 成员/offsets/lag 列）。
19. **T048b**：位点重置弹窗：dryRun 预览 → 执行 → 表格刷新；部分失败时表格也刷新（终审修复项）；执行中 Esc **不关**弹窗。
20. **T050c**：四个弹窗 Esc 均可关（集群/topic/config/重置位点）。
21. **全局**：操作全程无 devtools 未捕获异常；日志面板无 error 行（除故意注入的失败）。
22. **T080**：启动应用 → 日志面板出现来源为 `rust` 的行（例如 `glue event pump online`），级别徽标按 tracing 级别着色（info/warn/error）；`Assembly` 构建期的两行按 R73 只进 stderr；清空后新日志继续追加。
23. **T081b**：故意触发一次失败（未连接时刷新 topic 列表 / 输入不存在的 topic），错误文案**第二行**出现 T081 的行动建议（如 `Check bootstrap servers and network reachability.`）；`InvalidConfig` 与 `Cancelled` 两类按 R76 只有一行；`rpc.ts` 记进日志面板的 `rpc failed` 行同样带建议。
24. **T082c**：触发一次慢请求（连一个不可达地址的集群，或对超大 topic 列表点刷新），顶栏 spinner 旁出现琥珀色「取消」按钮；按下后 spinner 归零、弹出「已取消 N 个在途请求」、视图的错误条显示 `cancelled`（不是 `network`）；日志面板里对应行是 `rpc failed (cancelled)`。后端兜底另验：把某次请求拖过 30 s（R81）应回 `timeout` 且 UI 不再卡住。
25. **T083a**：跑起来后 `<data_dir>/logs/` 下出现 `app.log`（macOS 为 `~/Library/Application Support/rs-kafka-ui-client/logs/app.log`），内容与日志面板里 `rust` 来源的行一致（只是带 target/字段的完整形态）；把文件撑过 1 MiB 后应出现 `app.log.1`，且 `.1`/`.2`/`.3` 最多三份滚动文件、`app.log.4` 永不出现；改坏目录权限（或把 `logs` 换成同名普通文件）后启动应**只**降级到 stderr，不弹错、不崩。
26. **T083b**：日志面板点「导出」→ 得到 `kafka-logs-<YYYYMMDD-HHmmss>.txt`；把级别筛选切到 `error` 再导出，文件里只有 error 行；文件顺序与面板相反（**最旧在前**）、每行是 `[ISO 时间] LEVEL source 内容`；日志为空时「导出」按钮置灰；顺带确认消费页「导出 JSON」仍能下载（T047 那条没被这次重构弄坏）。
27. **T091**：`KAFKA_UI_FAKE_WEBVIEW2_MISSING=1 cargo run -p main-bin` → stderr 出现「the WebView2 runtime is not installed」+ 下载地址，浏览器自动打开下载页，进程**非零码**退出；去掉该环境变量后一切照旧正常开窗（macOS 上 `is_available()` 恒真）。
28. **T102**：`cargo run -p kafka-driver --example leak_probe -- 5000 50` 应打印 `warmup done … pumps=0`、结尾 `pumps=0 after_disconnect=0 topic_left=false` 且**退出码 0**；再跑 `./scripts/leak-check.sh 20000 50 2` 与 `./scripts/leak-check.sh 40000 50 2`，两轮**末样本 RSS 应接近**（不随轮数翻倍线性增长）、fd 增量不随轮数漂移；数字与判读见 `docs/reports/T102-long-run-leak-report.md`。

29. **T104**：本机已把 OpenSSL 钉在**机器级** `~/.cargo/config.toml` 的 `[env]`（R112），因此在**未导出** `PKG_CONFIG_PATH` / `OPENSSL_DIR` 的干净 shell 里直接跑 `cargo build -p main-bin --features real` 应成功、退出码 0，且 `cargo run -p main-bin --features real` 能正常起窗（stderr 有 `local store opened` / `router ready: …consume/group/mock registered` / `event pump online`）。要复现原始缺陷，就显式压回 openssl@4：`cargo --config 'env.OPENSSL_DIR="/opt/homebrew/opt/openssl@4"' --config 'env.PKG_CONFIG_PATH="/opt/homebrew/opt/openssl@4/lib/pkgconfig"' build -p main-bin --features real` → 应报 `Undefined symbols … _ENGINE_by_id`（退出码 101）。另确认 `cargo check -p kafka-driver --features real` 在**两个**方向上都「通过」（它不链接），故不能拿 check 当 real 可构建的凭据。

30. **T105**：消费页需人工确认两处表达——(a) 主题列表含内部主题（`__consumer_offsets` 等）时，默认选中应落在**非 `__` 开头**的主题上；(b) 订阅一个**空的**主题后，消息表应显示「已订阅，当前没有可读消息：该 topic 暂时是空的…」，而不是只显示「暂无消息」；若手选的是 `__*` 主题，还应追加「这是 Kafka 内部 topic，通常没有业务数据」。想看真实数据：主题页新建一个 topic（如 `demo`）→ 生产页发几条 → 回消费页选 `demo` 订阅，行应实时出现。
31. **T106**：消费页订阅后**切到别的 tab 再切回来**——页面状态应仍是「订阅中」，行数继续增长（不再被当成「离开页面」而取消）。同时确认切回时主题下拉仍是最新的（先在主题页新建 `demo`，切回消费页应能看到它）；关闭应用后重开，订阅不应残留（T063 teardown 仍生效）。

32. **T111**：下一次打 tag（如 `v0.1.2`）后到 Releases 页确认 —— 五个平台的产物都在（`linux-aarch64` / `linux-x86_64` / `macos-arm64` / `macos-x86_64` / `windows-x86_64`）；Linux 两个 job 不再报 `curl/curl.h: No such file or directory`；Windows 的 zip 里除 exe 还有 `libcrypto-*.dll` 与 `libssl-*.dll`，解压后能直接启动。

33. **T112**：推送后到 GitHub Actions 看 `ci` workflow（push/PR 即触发，**不必打 tag**）——`real feature check` job 应**真正编译并链接** `main-bin --features real`（不再秒过），日志里能看到 `real-deps` 复合作按 runner 平台走不同分支（Linux 装 apt 全清单 / macOS 装 keg-only `openssl@3` / Windows 在 `C:\Program Files\OpenSSL` 下递归找到 `libcrypto.lib`）。本机无法执行 Actions，此项须在推送后确认。

34. **T113**：下一次打 tag 后到 Releases 页确认 —— release **只被创建一次**（不会出现 `already_exists` 导致的产物缺失），正文里 `**Full Changelog**` **只出现一次**（v0.1.0 曾重复 5 次）；五个平台的产物仍齐全；Actions 里能看到 `build` 五个 job 各自上传 artifact、随后 `release` job 汇总发布。本机无法执行 Actions，须在推送后确认。

35. **T114**：下次打 tag 时，若 tag 与 `Cargo.toml` 的 `[workspace.package] version` 不一致（例如 tag `v9.9.9` 而文件里是 `0.1.0`），`release` workflow 应在 **第一个 job `version-guard`** 就红，且 `build`/`release` 一个都不启动（Actions 页看不到五个平台 runner）；tag 与版本一致时照常发布。

36. **T115**：发布流程改为走 `scripts/release.sh <version>`——先 `--dry-run` 看清会改什么，真跑后本地应多出 `chore(release): vX.Y.Z` 提交与同名 annotated tag，且推送后 `version-guard` job 通过；同时确认 `Cargo.lock` 里五个成员版本与 tag 一致（否则 `--locked` 构建会失败）。另：`v0.1.3` 的 tag 已被重新指向带 0.1.3 版本的提交，Actions 里应有一次新的运行。

## 记录方式

用户按编号反馈（如「1-12 过，13 行数翻倍」）；失败项我登记缺陷卡并走修复循环。
