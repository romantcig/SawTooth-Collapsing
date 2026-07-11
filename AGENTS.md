# 项目代理说明

## Windows Go 工具链

- 命令默认使用 MSYS2/Git Bash：`exec_command` 设置 `shell="bash"`、`login=false`。
- `login=false` 不会自动加载 `C:\Users\romantcig\.bashrc`。凡是执行 CGO、`go test -race` 或 Windows 原生构建，必须在同一条 Bash 命令中先运行：

  ```bash
  source /c/Users/romantcig/.bashrc
  ```

- 执行前必须确认 Go 使用 MinGW-w64，而不是 Cygwin GCC：

  ```bash
  test "$($CC -dumpmachine)" = "x86_64-w64-mingw32"
  go env GOOS GOARCH CGO_ENABLED CC
  ```

- 禁止让 Go 使用 `/usr/bin/gcc`；该编译器目标为 `x86_64-pc-cygwin`，不能构建 Windows 原生 race runtime。
- 推荐的 race 测试命令：

  ```bash
  source /c/Users/romantcig/.bashrc
  test "$($CC -dumpmachine)" = "x86_64-w64-mingw32"
  go test -race ./internal/proxy -count=1
  ```

- 推荐的 Windows 原生构建命令：

  ```bash
  source /c/Users/romantcig/.bashrc
  test "$($CC -dumpmachine)" = "x86_64-w64-mingw32"
  go build -o sawtooth-proxy.exe ./cmd/proxy/
  ```
