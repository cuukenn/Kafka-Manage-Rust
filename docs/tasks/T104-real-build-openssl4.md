# T104 — `--features real` 链接失败：Homebrew openssl@4 缺少 ENGINE API

状态: ✅　依赖: 无　验证: 手动（`scripts/real-run.sh build -p main-bin --features real`）　类型: 文档

## 目标

用户报「无法编译 real」。复现后确认：在本机 `--features real` 的**链接**阶段失败，
报 `Undefined symbols: _ENGINE_by_id …`。本卡把它定位到根因（宿主 OpenSSL 版本），
给出可复制的入口，并把「为什么」写进 README，避免下一个人再从 undefined symbol 猜起。

## 只读白名单（最多 3 个源文件）

- `Cargo.lock` — 锁定 `rdkafka-sys 4.10.0+2.12.1`、`openssl-sys 0.9.117`
- `crates/kafka-driver/Cargo.toml` — `rdkafka` 0.39 的 `real` 特性来源
- `crates/main-bin/src/main.rs` — `real = ["kafka-app/real", "kafka-driver/real"]`

## 要改的文件

- 新增 `scripts/real-run.sh` — 自动定位 keg-only 的 openssl@3 再 `cargo "$@"`
- 修改 `README.md` — 「运行」一节补根因、脚本入口，并标注 `cargo check` 不链接的坑

## 必须覆盖

1. 复现：`env -u PKG_CONFIG_PATH -u OPENSSL_DIR cargo build -p main-bin --features real`
   → 退出码 101，`_ENGINE_by_id` 等五个符号 undefined（来自 `rdkafka_ssl.c.o`）。
2. 定位：默认 `pkg-config --modversion openssl` = **4.0.3**（`Cellar/openssl@4`）；
   `nm -gU /opt/homebrew/opt/openssl@4/lib/libcrypto.4.dylib | grep _ENGINE_by_id` = **0**；
   `openssl@3`（3.5.9）已安装但 keg-only。
3. 修复后同一条命令经脚本跑通：`env -u … ./scripts/real-run.sh build -p main-bin --features real`
   → 退出码 0，且日志里 `Compiling openssl-sys` / `rdkafka-sys` 说明环境变量变化确实
   使缓存失效并换了 OpenSSL 重新链接。
4. 脚本对无 Homebrew 的环境必须退回系统 pkg-config，找不到 OpenSSL 时给出安装提示并非零退出。
5. README 必须点明 `cargo check` 不做链接，因此它在上面的环境缺失时会「通过」。

## 明确不做

- 不改任何 Rust 源码、不动依赖版本（见 R111）。
- 不为 CI 做任何改动：Linux runner 装的是 `libssl-dev`（3.x），不受本问题影响。
- 不提交 `.cargo/config.toml` 的 `[env]`（见 R110）。

## 已知取舍（Ruling）

- **R110** 用 `scripts/real-run.sh` 而不是提交 `.cargo/config.toml` 的 `[env]` 硬编码路径：
  `.cargo/config.toml` 的 `[env]` 在 CI 上同样生效，而 `/opt/homebrew/opt/openssl@3`
  在 Linux runner 上并不存在，会把 CI 弄坏；脚本能用 `brew --prefix` 自适应，且在无
  Homebrew 的机器上原样退回系统 pkg-config。代价：多一个必须记得用的入口，
  所以 README 里把脚本放在首选位置。
- **R111** 不靠改依赖绕过（不开 rdkafka 的 vendored/自建 OpenSSL，也不 hack
  `-DOPENSSL_NO_ENGINE`）：这是宿主工具链选择问题，装上 openssl@3 即可解；改依赖会
  显著拖慢首次构建并改变 CI 的形态，收益不成比例。
- **R112** 第二轮把「裸命令」也修好：把这两行环境变量钉进**机器级** `~/.cargo/config.toml`
  的 `[env]`（本机此前不存在该文件），使 `cargo run -p main-bin --features real` 直接可用。
  `[env]` 默认 `force = false`：显式设置的环境变量仍然优先，CI runner 自带的值也不会被覆盖，
  故与 R110 拒绝「仓库级 `.cargo/config.toml`」并不矛盾——仓库级的会污染 Linux CI，
  用户级的只作用于本机。

## 验收

- `scripts/real-run.sh build -p main-bin --features real` 在**未导出** `PKG_CONFIG_PATH` /
  `OPENSSL_DIR` 的 shell 里退出码 0
- README「运行」一节含根因、脚本入口、手动等价命令与 `cargo check` 警告
- 预算：本次仅动文档 + 脚本，未消耗源码行数

## 结果（完成后填写，≤5 行）

- **根因**：本机 Homebrew 的默认 `openssl` 已指向 **openssl@4 4.0.3**（`pkg-config --modversion
  openssl` = 4.0.3，libdir 在 `Cellar/openssl@4`），而 `nm -gU libcrypto.4.dylib | grep
  _ENGINE_by_id` = **0**：OpenSSL 4 不再导出 ENGINE API，vendored 的 librdkafka 2.12.1 仍调用
  5 个 `ENGINE_*`，链接因此失败（退出码 101）。openssl@3 3.5.9 已装，但 keg-only。
- **修复**：新增 `scripts/real-run.sh`（`brew --prefix openssl@3` → 导出 `OPENSSL_DIR` /
  `PKG_CONFIG_PATH` → `exec cargo "$@"`，默认 `run -p main-bin --features real`，参数透传）；
  README「运行」一节补根因、脚本入口、手动等价的两行环境变量，并标注 `cargo check` 不链接、
  在该环境缺失时照样「通过」的坑。
- **验证**：`env -u PKG_CONFIG_PATH -u OPENSSL_DIR ./scripts/real-run.sh build -p main-bin
  --features real` → `openssl: pinned to /opt/homebrew/opt/openssl@3 (OpenSSL 3.5.9)`、
  `Compiling openssl-sys` / `rdkafka-sys`（说明缓存确实按环境变量失效重链）、
  `Finished dev profile`、**退出码 0**；同一条件下不经脚本的裸 cargo 为 **101**。
- **预算**：仅 `README.md`（文档）+ `scripts/real-run.sh`（脚本）+ 本卡，源码 **0 行**。

## 后续（第二轮：裸命令也要能编译）

用户反馈「`cargo run -p main-bin --features real` 无法编译」。复现确认是**同一个根因**
（链接参数里是 `-L /opt/homebrew/opt/openssl@4/lib`，报 `_ENGINE_by_id` undefined，退出码 101）——
脚本可用，但没有人会记得先套脚本。故按 R112 补做：

- 新建 `~/.cargo/config.toml`（本机此前没有该文件），`[env]` 钉住 `OPENSSL_DIR` /
  `PKG_CONFIG_PATH` 指向 keg-only 的 `openssl@3`。
- 验证（`PKG_CONFIG_PATH` / `OPENSSL_DIR` 全部 unset）：
  - `cargo build -p main-bin --features real` → **退出码 0**（此前同一条件下为 101）；
  - `cargo run -p main-bin --features real` → 编译成功**并起窗**，stderr 打印
    `local store opened …/rs-kafka-ui-client/data.db`、
    `router ready: cluster/config/topic+produce/consume/group/mock registered`、
    `event pump online` —— real 驱动确实编进来了（也是本机首次真正跑起这个应用）。
- 该改动位于 `$HOME/.cargo/`，**不在仓库内**：不进版本库、不占预算、不影响 CI；README 已把
  它写成「嫌每次导出麻烦」的一次性方案，`scripts/real-run.sh` 保留给换机器/换平台的场景。
