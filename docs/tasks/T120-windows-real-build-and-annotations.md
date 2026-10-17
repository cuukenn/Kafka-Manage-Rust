# T120 — Windows 的 real 构建红了，而我读不到日志：先让 CI 自己把报错吐出来

状态: 🔄（等重跑的 annotation 结论）　依赖: T119　验证: Actions API（annotations）　类型: CI 配置

## 现象（v0.1.5，run 39049237261）

```
macos-arm64      success
macos-x86_64     success
linux-x86_64     success
linux-aarch64    success
windows-x86_64   failure   ← step 9「编译 release 二进制（含 real 驱动）」
publish release  skipped   → Release 不存在（/releases/tags/v0.1.5 返回 404）
```

同一次 push 的 CI（run 39049138702）里 `real feature check`（ubuntu，`--features real`
真实链接）**success** ⇒ T119 的 vendored OpenSSL 在 Linux 侧成立，问题只在 MSVC。

## 观测障碍（这条卡的一半动机）

匿名身份：
- `GET /actions/runs/{id}/jobs/{job_id}/logs` → **404**（拿不到日志正文）
- `GET /check-runs/{id}/annotations` → **200**，但只有 `Process completed with exit code 101`

⇒ 只要 CI 不主动把报错写进 annotation，我就永远只能看到「退出码 101」。
本卡给 build/test 步加 `::error::` 包装，把日志尾部直接打成 annotation。

## 要改的文件

- 修改 `.github/workflows/release.yml` —— 新增 `MSVC 开发环境（Windows）` 步
  （`ilammy/msvc-dev-cmd@v1`，`if: matrix.platform == 'windows'`）；build 步改成
  `tee build.log` + 失败时 `tail -n 25` 逐行 `::error::`（`%`→`%25`、`"`→`%22` 转义），
  最后 `exit "$status"` 保持原来的红
- 修改 `.github/workflows/ci.yml` —— 「测试」步同样包装，`grep -nE
  "^(failures:|test result:|error|---- )|FAILED$"` 取尾部 30 行打 annotation
  （ubuntu 那条红了很久的测试失败，第一次有可能看到真正的 `failures:`）

## 已知取舍（Ruling）

- **R151**：用 `::error::` annotation 作为失败可读通道，而不是「请人肉贴日志」。匿名身份
  读不到 `logs` 端点（404），但 `annotations` 读得到（200）——这是唯一不需要凭据的通道。
  代价：annotation 有数量/长度上限，只能看到尾部几十行，不是完整日志。
- **R152**：先加 `ilammy/msvc-dev-cmd@v1` 而不是继续盲猜。依据：Windows 镜像的工具列表里
  只有 CMake，**没有 nmake/vcvars**；而 `openssl-src` 在 MSVC 上要跑 `perl Configure` +
  `nmake install`（CMake 走 VS generator 时不需要这些，所以 librdkafka 一直是好的——
  这也解释了为什么只有 vendored OpenSSL 这一段红）。代价：引入一个第三方 action。

## 明确不做

- 不把 Windows 退回「系统 OpenSSL + 随包发 DLL」——那是 T119 刚拆掉的路；只有当 vendored
  在 MSVC 上确认不可行，才作为下一张卡的备选重新评估
- 不改测试代码（ubuntu 的测试失败仍然没有正文，本卡先解决「看得见」）

## 验收

- `ruby -ryaml` 两个 workflow 都可解析；`build` 13 步、`test` 10 步
- 重跑后：`windows-x86_64` 变绿，**或** annotations 里出现真正的 cargo 报错正文
- ubuntu 的「测试」若再红，annotations 里必须出现 `failures:` / `test result: FAILED` 行
