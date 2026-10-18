# 构建 / 测试门禁

```sh
# 常规（提交前）
cargo test -q --workspace                                 # 当前基线 281 passed
cargo clippy -q --workspace --all-targets -- -D warnings  # 必须零告警
cargo build --release -p main-bin                         # 发版二进制（含 webview-bridge）
cargo bench -p kafka-driver                               # criterion 吞吐基准（可选，较慢）

# 前端（Node 22；宿主默认 Node 16 会被 vite 7 拒绝）
export NVM_DIR="$HOME/.nvm"; source "$NVM_DIR/nvm.sh"; nvm use 22
cd frontend && npm run build                              # 唯一前端门禁（无 typecheck/lint 脚本）

# 真驱动（需可达 broker）
cargo build -p main-bin --features real --locked           # T119: OpenSSL 走 ssl-vendored，本机不需要 openssl@3
./scripts/real-run.sh build -p main-bin --features real    # 仍可用，但它钉的 OPENSSL_DIR 已被 vendored 忽略
cargo check -p kafka-driver --features real               # ⚠️ check 不链接，不能当「real 能构建」的凭据
KAFKA_CONFORMANCE_BOOTSTRAP=localhost:9092 cargo test -p kafka-driver --features real -- --ignored

# 单套件
cargo test -p kafka-driver --test mock_lifecycle
cargo test -p kafka-driver --test conformance
```

- **不要跑 `cargo fmt`**（无 `rustfmt.toml`，代码非 rustfmt-clean）。
- `frontend/dist` 里有被追踪的文件：前端重建后产物若有变化，需 `git add -f frontend/dist`。
- 起壳自测：`cargo run -p main-bin`（默认 mock 驱动）；接真 Kafka 用 `--features real` 并在界面里取消勾选「Mock 驱动」。
- 本机工具链与环境（含 openssl@3 坑）见 [toolchain](toolchain.md)。
- **改了 `.github/` 之后跑 `./scripts/check-workflows.sh`**（T130）。GitHub 拒收一份 workflow 时**不给任何错误文本**：
  只会生成一个 0 个 job、瞬间失败、名字显示成文件路径（而不是 `name:`）的 run。`permissions` 只认 schema 里
  那几个 scope（`additionalProperties:false`，**没有 `releases`** —— 建 release 用 `contents: write`）；
  同层重复 key 也会被拒收。ci.yml 的「workflow 自检」job 是这条的 CI 版本。
