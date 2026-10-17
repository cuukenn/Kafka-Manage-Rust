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
./scripts/real-run.sh build -p main-bin --features real    # 脚本自动钉 openssl@3
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
