# T102 长跑内存 / 句柄泄漏检查报告

状态: 完成　日期: 2026-10-17　主机: macOS arm64　工具链: cargo 1.99.1 (5f94df478 2026-08-27) / rustc 1.99.1 (58cf8f9c3 2026-10-13)

## 方法

实际运行的两条命令（先 `cargo build --release -p kafka-driver --example leak_probe` 预热）：

- `./scripts/leak-check.sh 20000 50 2`
- `./scripts/leak-check.sh 40000 50 2`

探针（`crates/kafka-driver/examples/leak_probe.rs`）每轮走一遍完整生命周期：
建主题 → 生产 50 条 → 打开 live 订阅 → 抽干这批 → `stop()` 控制 → 把流抽到 `None` →
读一次 `list_topics` → 删主题。删主题把保留集钉在一批以内，RSS 上涨无法被解释成
「数据攒多了」（R105）。

三个观测仪表：

- 进程内：驱动自己的存活泵计数 `active_pumps()`，热身 25 轮后取基线，每 250 轮打印一次，
  退出时与基线比对，`disconnect()` 后必须为 0。
- 进程外 RSS：脚本每 2 秒 `ps -o rss=` 采样（跳过 <4096KB 的瞬时值）。
- 进程外 fd 数：同一循环里 `lsof -p PID | wc -l`。

RSS 与 fd 之所以从进程外采样（R107）：进程无法诚实地观测自己的描述符表，
探针也看不见自己的常驻集；进程内那一侧只负责泵计数这种确定性断言。

## 原始数据

20k 轮 run 的 summary 块（原样）：

```
--- summary ---
cycles=20000 batch=50 samples=12 probe_exit=0
rss: first=6672KB last=6672KB delta=0KB
fds: first=13 last=13 delta=0
```

20k 轮 run 的最后 3 个采样与探针收尾行：

```
sample=10 rss=6672KB fds=13
sample=11 rss=6672KB fds=13
sample=12 rss=6672KB fds=13
warmup done after 25 cycles: pumps=0
done: cycles=20000 batch=50 elapsed=25.9s pumps=0 after_disconnect=0 topic_left=false
```

40k 轮 run 的 summary 块（原样）：

```
--- summary ---
cycles=40000 batch=50 samples=26 probe_exit=0
rss: first=6672KB last=6672KB delta=0KB
fds: first=13 last=13 delta=0
```

40k 轮 run 的最后 3 个采样与探针收尾行：

```
sample=24 rss=6672KB fds=13
sample=25 rss=6672KB fds=13
sample=26 rss=6672KB fds=13
warmup done after 25 cycles: pumps=0
done: cycles=40000 batch=50 elapsed=54.0s pumps=0 after_disconnect=0 topic_left=false
```

两轮探针退出码均为 0。完整转写在本机 `$TMPDIR/t102/run-20k.txt` / `run-40k.txt`（临时文件，不入库）。

## 判读

- RSS 是否随轮数线性增长：没有。20k 末样本 6672KB，40k 末样本 6672KB——轮数翻倍、
  墙钟翻倍，常驻集一个字节没多。且两轮里全部样本（12 个 / 26 个）都停在 6672KB：
  连「先涨后平」的前段都没观察到，首个有效采样（exec 后约 2 秒）时常驻集已经就位，
  之后全程平台。
- fd 是否漂移：没有。两轮的首个有效样本都是 13，末样本都是 13，增量 0。
  每轮开订阅、建/删主题、读元数据，没有留下任何句柄。
- 泵计数是否离开过 0：没有。基线 0，20k run 的 80 个报告行、40k run 的 160 个报告行
  全部 `pumps=0`；退出时 `pumps=0`（等于基线），`after_disconnect=0`，
  `topic_left=false`。

## 结论

在本次检查覆盖的范围内未发现泄漏：mock 驱动的订阅泵、主题保留集、fd 表在 2 万 /
4 万轮完整生命周期后全部归位，常驻集零增长。这个检查要漏掉泄漏，需要泄漏发生在
它的盲区里：单进程、只跑 mock 驱动（R106，真实 librdkafka 的内部句柄不在覆盖内）、
墙钟只有约 26 / 54 秒、2 秒离散采样看不见采样点之间的尖峰（R107）、以及分配器
可能扣留已释放的页——那种堆泄漏 RSS 上看不出来，只能靠进程内的泵计数这类
确定性断言兜底，而后者两轮都是 0。

## 复现

```
source "$HOME/.cargo/env"
cargo build --release -p kafka-driver --example leak_probe
./scripts/leak-check.sh 20000 50 2
./scripts/leak-check.sh 40000 50 2
```

探针也可单独跑：`cargo run -q -p kafka-driver --example leak_probe -- <cycles> <batch>`，
发现泄漏（泵不归位、`stop` 后 5 秒流不结束、主题没删掉）时非零退出，脚本原样转发。
回归门禁 `cargo test -q --workspace` 与 `cargo clippy -q --workspace --all-targets -- -D warnings`
由主 agent 另行执行，不在本报告的数据采集范围内。
