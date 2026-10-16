# T049a — 全局 toast + 全局 loading

状态: ⏸　依赖: T047b　验证: npm build + cargo build + 冒烟（R23）　类型: 前端
拆分说明: R22 口径预拆；日志面板在 T049b。

## 范围（只做这些）

1. **toast 存储**（store/toast.ts）：`push(level:'error'|'warn'|'info', text)` + 自动消失（error 6s，其余 3s）+ 手动关闭；上限 5 条（旧溢出丢弃）。
2. **Toast.vue**：右下角堆叠，Tailwind 深浅色，级别配色；挂载于 Shell。
3. **接线**：Shell.vue 的集群连接/断开等处 catch → toast（保留各视图现有行内错误条，不迁移）；**不**给每个视图调用点都加 toast（避免重复噪音）。
4. **全局 loading**：rpc.ts 增加 pending 计数导出（+≤8 行，R21 第 4 文件口径）；Shell 顶栏小 spinner 在 pending>0 时显示（现有连接徽标旁）。

## 边界与预算

src 文件 ≤4（toast.ts、Toast.vue 新；Shell.vue、rpc.ts 改），≤150 行；CSS/配置/dist 豁免；无新依赖；**不改 Rust**；dist 重建提交。

## 验证（R23）

npm build + cargo build -p main-bin + 冒烟。窗口手动项（批量）：连一个不存在的真实集群 → 失败出现红色 toast 且 6s 消失；连接 mock 期间顶栏 spinner 出现。
