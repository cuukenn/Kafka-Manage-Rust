# T121 — Windows 的 link.exe 被 MSYS 抢走了

状态: ✅　依赖: T119,T120　验证: 手动（YAML 解析）+ 待 v0.1.5 重跑　类型: CI 配置

## 触发

T120 的 annotation 通道第一次生效，v0.1.5 的 Windows 失败正文就出来了（run 39053986277）：

```
note: `link.exe` returned an unexpected error
note: /usr/bin/link: extra operand '...\target\release\build\quote-...\build_script_build-...rcgu.o'
note: "C:\Program Files\Git\usr\bin\link.exe" "/NOLOGO" ...
error: linking with `link.exe` failed: exit code: 1
```

rustc 调的是 **Git for Windows 自带的 `link.exe`**（MSYS coreutils 的「建硬链接」，只收两个操作数），
不是 MSVC 的链接器。同一轮里 `linux-x86_64` / `linux-aarch64` / `macos-arm64` / `macos-x86_64`
四个 job 全绿 ⇒ vendored OpenSSL 本身没问题，Windows 是**链接器选择**的问题。

## 改法

`release.yml` 在 `ilammy/msvc-dev-cmd@v1` 之后加一步，把 MSVC 链接器的**绝对路径**交给 cargo：

```
CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_LINKER=<VCToolsInstallDir>\bin\Hostx64\x64\link.exe
```

`VCToolsInstallDir` 是 vcvars 自己设的变量（msvc-dev-cmd 就是跑 vcvars），所以不需要 vswhere、
不需要猜 VS 目录。同时把 `where.exe link.exe` 的第一个命中打成 annotation —— 下次一眼能看出
PATH 上站在前面的到底是谁。

## 已知取舍（Ruling）

- **R153**：用 `target.<triple>.linker` 显式指定，而不是去调 PATH 顺序。PATH 上 MSYS 与 MSVC
  的 `link.exe` 同名，谁在前面取决于镜像与步骤顺序；显式路径把这件事从「顺序问题」变成「常量」。
- **R154**：保留 `ilammy/msvc-dev-cmd@v1`。它不是多余的 —— `openssl-src` 在 MSVC 上要跑
  `perl Configure` + `nmake`，nmake/SDK 工具仍得由它提供；本次只是补上它没管的链接器歧义。

## 验收

- `release.yml` YAML 可解析，`build` 14 步（新增第 9 步）
- 重跑 v0.1.5：`windows-x86_64` 应过，`publish release` 应产出 5 个 target 三元组命名的产物
- 若仍失败，annotation 里应出现 `MSVC link.exe pinned to ... | PATH resolves link.exe to ...`，
  那就不再是猜

## 结果（完成后填写，≤5 行）

- 待 v0.1.5 在 `8a8367b` 之后的重跑结论。
