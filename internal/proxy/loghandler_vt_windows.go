//go:build windows

package proxy

import (
	"os"

	"golang.org/x/sys/windows"
)

// enableVT 在 Windows conhost 上启用 VT 转义序列处理，
// 使 ANSI 色码正常渲染而不是显示为 ←[32m 乱码。
// 仅修改自身进程的 console mode；失败时调用方回退无色输出。
func enableVT(f *os.File) error {
	h := windows.Handle(f.Fd())
	var mode uint32
	if err := windows.GetConsoleMode(h, &mode); err != nil {
		return err
	}
	return windows.SetConsoleMode(h, mode|windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING)
}
