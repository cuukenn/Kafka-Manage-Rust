# T053 · 修 kafka-driver 首次编译报错（mock 模块）

**状态**: ✅完成 | **依赖**: T001、T052 | **改动上限**: 3 文件 / 250 行

## 目标
`kafka-driver` 从未编译过。T001 打通链接、T052 修好 `kafka-core` 之后，`cargo check -p kafka-driver --lib` 暴露 24 个错误 + 2 个告警，归并为 6 个缺陷点。修完它们，阶段 1 的 mock 驱动才第一次真正可编译、可跑测试——T010–T017 全部依赖这一步。

## 只读白名单（最多 3 个源文件，别读更多）
- `crates/kafka-driver/src/mock/state.rs` — 三个 `Mock*` 类型的 derive 与带 `#[serde(default)]` 的容器字段
- `crates/kafka-driver/src/mock/fault.rs` — `FaultInjection`/`BrokerFault` 的 derive 与 f32 字段
- `crates/kafka-driver/src/mock/driver.rs` — `produce` 的双重可变借用、种子夹具循环

## 要改的文件
- 修改 `crates/kafka-driver/src/mock/state.rs`
- 修改 `crates/kafka-driver/src/mock/fault.rs`
- 修改 `crates/kafka-driver/src/mock/driver.rs`

## 必须覆盖（已确诊，按报错定位）
1. `state.rs:26` `MockMessage` 缺 `Default`（E0277）——由 `state.rs:54 pub messages: Vec<MockMessage>` 上的 `#[serde(default)]` 触发
2. `state.rs:101` `MockTopic` 缺 `Default`——由 `state.rs:201 pub topics: BTreeMap<String, MockTopic>` 触发
3. `state.rs:152` `MockGroup` 缺 `Default`——由 `state.rs:203 pub groups: BTreeMap<String, MockGroup>` 触发
4. `fault.rs:27` `BrokerFault` 缺 `Default`——由 `fault.rs:69 pub broker_error: Option<BrokerFault>` 触发
5. `fault.rs:52` E0277 `f32: Eq` 不成立——`FaultInjection`（derive 在 :39）带 `pub fail_probability: f32`，**去掉 `Eq`**（保留 `PartialEq`），不要改成定点数
6. `driver.rs:469` E0499 `produce` 里 `state` 被可变借用两次：:460 `state.topics.get_mut(...)` 的 `topic` 活到 :469 `state.next_round_robin(ids.len())` → 先把 `ids` 和 round-robin 序号取出来，再拿 `get_mut`
7. `driver.rs:866` E0308 + `:885` E0425 种子夹具循环自相矛盾：`for (partition, key, payload) in [...]` 的数组元素是二元组、模式要三元，且 `index` 未定义（`timestamp: now - (1000 * (60 - index as i64))`）→ **先读 :840-900 这段**再改，保持「时间戳递增、分区轮转」的原意
8. 告警：未用 import `SubscriptionId`（driver.rs:21）、`MockTopic`（driver.rs:33）→ 删掉（若第 7 点修完确实需要 `MockTopic`，就保留并说明）

## 明确不做（防止范围膨胀）
- 不改 `KafkaDriver` trait 签名、不动 `driver.rs` 顶层的 `DynDriver`/`driver_for`
- 不动 `mock/pump.rs`（`pub(crate)`，编译错误不在那里）
- 不重排锁结构、不改 `snapshot_json`/`restore_json` 的 JSON 形状（改了会让已写好的 T017 测试失效）
- 不开 `real` feature、不装 rdkafka/cmake
- 不在本卡里写 mock 的行为测试（那是 T010–T015）

## 验收
- `cargo check -p kafka-driver --all-targets` 全绿、零告警
- `cargo clippy -p kafka-driver` 无新增告警
- `cargo test -p kafka-driver` 全绿（现有单测）

## 结果（完成后填写，≤5 行）
- `cargo check -p kafka-driver --all-targets` → `Finished dev profile`，退出码 0；`cargo test -p kafka-driver` → **11 passed / 0 failed**。这个 crate 第一次编译通过。
- 6 处全修：`MockMessage`/`MockTopic`/`MockGroup`/`BrokerFault` 补 `Default`（既有测试早就在写 `..Default::default()`，本就是原设计）；`FaultInjection` 去掉 `Eq`（`f32` 概率不可能 bit-equal，注释已写明）；`produce` 把 `ids` + 轮转序号提前预留，新增 `MockState::take_round_robin_picks`；种子夹具循环改 `for (index, (partition, key, payload))`，`index` 归位、时间戳仍递增。
- 改动 3 文件（state/fault/driver），新增约 30 行，未越界；未动 trait、未动 JSON 形状、未开 `real`。
- 顺带清掉 T052 自己引入的一条 clippy 告警（`kafka-core/src/error.rs:74` 冗余 `&`，1 行）——属 T052 收尾，不另开卡。
- 遗留：8 条**存量** clippy 告警（`pump.rs:35` `join` 未读、`driver.rs:326/655` 该用 `contains()`、`fault.rs:160` 冗余 cast、`fault.rs:215/228/237` Default 后赋值、`dto.rs:110/127` `map().flatten()`）→ 已登记 **T054**。
