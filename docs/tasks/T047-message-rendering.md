# T047 — 消息渲染与解码（utf8/JSON/base64/hex）+ 复制/导出

状态: ⏸　依赖: T046a-c　验证: npm build + cargo build + 冒烟（R23）　类型: 前端

## 范围（只做这些）

1. **解码视图**（ConsumeView 表格 value 列升级，可抽 MessageCell 组件）：字节数组（number[]，T046a 已证推送路径形状）按模式渲染——`自动`（合法 utf8→文本，否则 base64）/`JSON`（可解析则折叠美化，不可解析回退自动）/`Base64`/`Hex`；模式选择器全局一个（存组件状态即可）。key 列同组件复用（默认自动）。
2. **复制**：行内复制按钮（value 原文按当前模式文本化；navigator.clipboard 在非安全上下文可能不可用——grep 无门可查，实现 navigator.clipboard?.writeText 失败回退 textarea+execCommand；成功/失败微反馈）。
3. **导出**：「导出」按钮把当前表格行（≤500）下载为 JSON 文件（Blob + a[download]；WebView2 下载行为属手动项）。文件名含 topic+时间戳。
4. 长值折叠：超过 ~200 字符折叠显示，点击展开。

## 线事实

无新命令——纯前端渲染层。字节数组形状沿用 T046a 报告（key/value: number[]）。utf8 解码用 TextDecoder(fatal:false)；base64 用 btoa(String.fromCharCode(...)) 分块防溢出；hex 手拼。

## 边界与预算

src 文件 ≤3（MessageCell.vue 新 + ConsumeView.vue 改 + 可选 render.ts 工具），≤250 行；CSS/配置/dist 豁免；无新依赖；**不改 Rust**；dist 重建提交。

## 验证（R23）

npm build + cargo build -p main-bin + 冒烟。窗口手动项（批量）：JSON 美化显示；中文 utf8 正确；复制粘贴出内容；导出下载成功（WebView2 下载 UI）。
