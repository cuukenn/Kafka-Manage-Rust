# 构建 / 测试命令

```
cargo check -p kafka-core -p kafka-driver
cargo test  -p kafka-driver --test mock_lifecycle
cargo test  --workspace
cargo check -p kafka-driver --features real
```
