# T129 — ubuntu 的 reconnect 测试为什么红（`settle()` 是在赌调度，不是在等待）

状态: ✅（本机证据闭环，CI 复核待跑）　依赖: T062,T120　验证: 手动（本机阈值实验 + 负载实验 + 诊断脚本实测）　类型: 测试代码 + CI 配置

## 现象

`ci.yml` 的 ubuntu-22.04「测试」步间歇性红，红的是固定两个测试：

```
test connection_failure_degrades_then_reconnects_through_connecting_connected ... FAILED
test failed_reconnect_retries_on_the_backoff_table ... FAILED
```

而它们是 **flaky**，不是坏：

| CI run | commit | 结论 |
|---|---|---|
| 39058922757 | `c697ec0` | failure |
| 39070964864 | `2279ce7` | **success** |
| 39075949193 | `1139ecb` | failure |

本机 `cargo test -p kafka-app --test reconnect` 7/7 全绿，0.02s。

## 诊断链

1. 两个失败测试都在 `crates/kafka-app/tests/reconnect.rs`，都经过同一个助手 `settle()`。
2. `settle()` 的实现是**固定 100 次 `yield_now()`** —— 它假设「让出 100 次，别人就走完了」。
3. 但 supervisor 的每一步（`disconnect_inner` / `connect_inner`）都要经
   `crates/kafka-app/src/services/mod.rs:77` 的 `spawn_blocking` 落 SQLite ——
   那是**真实阻塞线程池**：`yield_now()` 等不到它，而 `start_paused = true` 下
   `tokio::time::sleep` 是虚拟时间、也不会替它等。
4. 把 `settle()` 改成 1 次 yield ⇒ 本机 5/5 必红，失败形状正是「没走完」而不是「走错了」：
   `left: [Degraded]` / `right: [Degraded, Disconnected, Connecting, Connected]`
   —— supervisor 卡在第一次 store 调用之后，和 CI 红的测试集合一致。
5. 阈值实验（本机空载、10 核）：

   | yield 次数 | 12 次运行里失败几次 |
   |---|---|
   | 1 | 12 |
   | 10 | 5 |
   | 20 / 30 / 40 / 50 / 60 / 80 / 100 | 0 |

   ⇒ 100 次只比阈值高 **5~10 倍**。ubuntu runner 是 2 核共享、`cargo test --workspace`
   并发跑几十个测试二进制、还有 steal time —— 这点余量直接蒸发。
6. 排除了虚拟时间这条：mock driver 的注入延迟在这里不生效
   （`apply_faults` 在 `all_off()` 时直接 return；`FaultInjection::default()` 的 latency 是 0，
   `latency_ms > 0` 才 `sleep`）⇒ 不是「虚拟时间没推进」，是「真实线程没跑完」。

## 改法

`settle()` 改成**有界的真实静默窗口**：轮询总线，直到连续 250ms 没有新事件（上限 5s 防挂死）。
思路沿用仓库里已有的 `crates/main-bin/tests/e2e_mock_full_flow.rs:115` 的 `drain_for`
（"bounded quiet window, never a fixed sleep-and-hope"），但 paused-time 下 `tokio::time::sleep`
是虚拟的，所以这里用 `std::time::Instant` + `std::thread::sleep`。
测试任务全程 runnable ⇒ tokio 不会 auto-advance 虚拟时间 ⇒ 下面那些 `advance(999ms)` 的
backoff 断言语义不变。

### 顺带修好 CI 诊断（这条卡的第二半）

T120 那版 annotation 提取**永远拿不到 assertion diff**，本机复现出原因：

- libtest 把失败详情放在 `failures:` 之后，旧 grep 的 `tail -n 30` 只留摘要行；
- `CARGO_TERM_COLOR=always` 给 `---- name stdout ----` 这类行加了 ANSI 码 ⇒ 匹配不上 `^---- ` 锚点；
- 一次 run 只留下 10 条 annotation ⇒ 一行一条会把最有价值的东西挤掉。

新法：去 ANSI → 从第一个 `failures:` 取到文件末尾 → 压成**一条**多行 annotation（换行 `%0A`）。
本机用真实失败日志验证过：产出 1 条 1526 字符的 annotation，解码回 33 行，含 3 处
`panicked at …:113:5` 与 `left/right` 对照；`exit` 仍是 1。

## 只读白名单（最多 3 个源文件）

- `crates/kafka-app/tests/reconnect.rs` —— 改动前的 `settle()`（100 次 yield）与三个依赖它的测试
- `.github/workflows/ci.yml` —— 改动前的「测试」步（T120 版提取逻辑）
- `crates/main-bin/tests/e2e_mock_full_flow.rs` —— 仓库里既有的 `drain_for` 静默窗口惯例（只读参照）

## 要改的文件

- 修改 `crates/kafka-app/tests/reconnect.rs` —— `settle()` 换成有界真实静默窗口（+`Instant` import）
- 修改 `.github/workflows/ci.yml` —— 「测试」步的失败提取（`.github/` 不计入源码预算）

## 必须覆盖

1. 三个依赖 `settle()` 的测试仍然断言**同样的事件序列**（没有放宽任何一条 assert）。
2. `advance(999ms)` → 期望空、`advance(2ms)` → 期望 `Connecting, Error` 的语义不变（不 auto-advance 虚拟时间）。
3. `cargo test --workspace --locked` 与 `cargo clippy --workspace --all-targets -- -D warnings` 全绿。
4. CI 红的时候，annotation 里能直接看到 `panicked at` 与 `left/right`。

## 明确不做

- 不放宽断言、不删测试、不给测试加 `#[ignore]`
- 不改生产代码：CI 的 diff 是「没走完」而不是「走错了」，没有生产 bug 的证据
- 不给 CI 加 `--no-fail-fast`、不加 retry 步（掩盖 flaky 不是修 flaky）
- 不动 `app_smoke.rs:73` 的单次 `yield_now`（它不在失败集合里，且没有同类证据）

## 已知取舍（Ruling）

- **R168**：固定 yield 次数不是等待。要等真实阻塞线程池就只能用真实时间；在 paused-time 测试里，
  同时满足「等真实线程」和「不推进虚拟时间」的手段只有 `std::thread::sleep` + 让任务保持 runnable。
  代价：测试真实耗时增加（每次 settle 至少 250ms），整个 reconnect 文件从 0.02s 变成 ~1s 量级。
- **R169**：修 flaky 先修**测试的等待方式**，不动生产、不放宽断言。断言本身没错 ——
  错的是它假设 100 次让出足以让另一个线程池跑完。
- **R170**：失败诊断要「一次 run 一条多行 annotation」，而不是一行一条。一次 run 只留 10 条
  annotation，一行一条必然把 assertion diff 挤掉。
- **R171**：**跑门禁之前，先确认被测的就是改动后的那份代码。** 本卡第一次「全绿」是假的：
  为了做对照实验，我用 `cp` + `git checkout --` 来回换 settle 版本，其中一次 `cp` 把**旧版本**
  存成了「修复版」备份，之后所有恢复都在还原旧版本 —— 于是 workspace 测试、clippy、负载实验
  全绿的是**没修的代码**，而且第一次 T129 提交里根本没有 `reconnect.rs`。
  教训：对照实验用 `git stash` 或独立副本目录，别在同一个路径上换版本；
  报绿之前 `grep` 一眼改动是否还在（`grep QUIET` 一秒就能发现）。

## 验收

- 本机：`cargo test --workspace --locked` 全绿；clippy `-D warnings` 全绿
- 本机负载实验：`2×核数` 个 CPU 自旋进程下重跑 reconnect —— 修复前 0/40 红（说明 CPU 争用本身不够），
  修复后 0/40 与 0/12 全绿；阈值实验给出 1→12/12 红、10→5/12 红、20+→0/12 红
- 诊断脚本：用真实 colored 失败日志跑一遍，产出 1 条含 assertion diff 的 annotation
- **最终判据在 CI**：ubuntu-22.04 连续绿；若再红，annotation 会直接给出 diff

## 结果（完成后填写，≤5 行）

- `settle()`：`QUIET=250ms` / `CAP=5s` 的真实静默窗口（`Instant` + `std::thread::sleep(5ms)` + 每轮 16 次 yield）。
  代价实测：该测试文件 0.02s → **1.26s**。
- 门禁（在改动确实存在于工作区的前提下重跑过一遍）：`cargo test --workspace --locked` 失败 0 行、
  `cargo clippy --workspace --all-targets -- -D warnings` 退出 0、2×核数负载下 reconnect **20/20 绿**、
  30 个测试二进制并发 ×3 轮 **0/90 失败**。
- `ci.yml`「测试」步：去 ANSI → 从第一个 `failures:` 取到末尾 → 一条多行 annotation
  （本机用真实 colored 失败日志实测：1 条 1526 字符、解码回 33 行、含 3 处 `panicked at` 与 `left/right`）。
- 源码预算：`reconnect.rs` +27/−6 行（测试代码）、`ci.yml` 属 `.github/` 不计 ⇒ 2 个源文件、<250 行。
- 踩坑见 **R171**：第一次提交 `3450c4b` 里其实没有 `reconnect.rs`（对照实验的还原把旧版存成了备份），
  补在后续提交。CI 复核待跑：ubuntu 连续绿才算闭环。
