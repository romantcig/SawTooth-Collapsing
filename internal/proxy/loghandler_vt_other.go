//go:build !windows

package proxy

import "os"

// enableVT 非 Windows 平台 no-op——类 Unix 终端原生支持 ANSI 转义序列。
func enableVT(_ *os.File) error {
	return nil
}
