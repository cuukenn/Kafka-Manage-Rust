# T042b — 集群编辑弹窗（新建/编辑共用，Mock 开关、SASL/SSL 字段）

状态: ⏸　依赖: T042a　验证: `npm run build` + `cargo build -p main-bin` + 冒烟（R23）　类型: 前端

## 范围

1. **ClusterModal.vue**：新建/编辑共用。字段：name、bootstrapServers（逗号分隔）、**Mock 开关**、clientId、securityProtocol 选择；SASL 区（mechanism/username/password/saslProtocol）、SSL 区（caPem/caPath/certPem/keyPem/verifyCertificate/verifyHostname）。表单直接映射 ClusterConfig；未填留默认（serde default 兜底），不做过度校验。
2. **保存**：`cluster.save`（payload = ClusterConfig 本体，camelCase）。成功后刷新列表并关窗；RpcError 弹窗内显示。
3. **入口**：T042a 的占位按钮换成真入口（顶部「新建集群」+ 行内「编辑」，编辑预填现值）。

## 线事实（窄 grep，limit≤15）

- ClusterConfig 完整字段与嵌套形状：grep crates/kafka-core/src/cluster.rs `pub `（limit 20）+ SecurityProtocol/SaslMechanism 枚举值 grep 同文件。
- cluster.save payload：T042a 报告已核实则复用其记录。

## 边界与预算

src 文件 ≤2（ClusterModal.vue 新 + ClustersView.vue 接线改），≤250 行；CSS/配置/dist 豁免；无新依赖；**不改 Rust**；dist 重建提交。

## 验证（R23）

`npm run build` + `cargo build -p main-bin` + 冒烟 8s 干净退出。
窗口手动项（批量清单）：新建 mock 集群→编辑改 name→重开弹窗见新值→连接/断开/删除全链路。
